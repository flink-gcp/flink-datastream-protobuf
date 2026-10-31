# Copyright 2026 The flink-gcp authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Synthetic releases only: never download or execute a retained bundle."""

import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import tarfile

import pytest

from conftest import load_script

fetcher = load_script("baseline-fetch.py")

TAG = "v0.1.0"
BUNDLE = "flink1-pb3"
ARCHIVE = TAG + "-" + BUNDLE + ".tar.gz"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return data


def bundle_files(bundle=BUNDLE):
    files = {"library.jar": b"library", "tools.jar": b"tools", "manifest.properties": b"bundle=" + bundle.encode(),
             "fixtures/1.20.4/hashmap/baseline/savepoint.zip": b"state"}
    sums = "".join(digest(data) + "  " + name + "\n" for name, data in sorted(files.items()))
    files["SHA256SUMS"] = sums.encode()
    return files


def tar_bytes(members):
    """Members are (name, data) for files, (name, None) for directories, or (TarInfo, data)."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, data in members:
            if isinstance(name, tarfile.TarInfo):
                tar.addfile(name, io.BytesIO(data or b""))
                continue
            info = tarfile.TarInfo(name)
            if data is None:
                info.type = tarfile.DIRTYPE
                tar.addfile(info)
            else:
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def default_members(bundle=BUNDLE):
    return [(bundle + "/", None)] + [(bundle + "/" + name, data) for name, data in sorted(bundle_files(bundle).items())]


def replaced(members, name, data):
    return [(member, data if member == BUNDLE + "/" + name else body) for member, body in members]


class Release:
    """A synthetic pre-release with the v0.1.0 asset layout and its pin.

    The release records the archive built from the given members, so member checks alone must reject
    hostile content. An altered release serves different bytes from the recorded archive.
    """

    def __init__(self, root, bundles=(BUNDLE,), members=None, altered=False, mutate=None):
        self.root = root
        final = {"format_version": 1, "milestone": TAG, "milestone_final": True, "bundles": []}
        inventory, assets = {}, {}
        for bundle in bundles:
            files = bundle_files(bundle)
            name = TAG + "-" + bundle + ".tar.gz"
            recorded = tar_bytes(members if members is not None and bundle == BUNDLE else default_members(bundle))
            served = bytearray(recorded)
            if altered:
                served[len(served) // 2] ^= 1
            write(root / name, bytes(served))
            assets[name] = recorded
            inventory[bundle] = [{"path": bundle + "/" + path, "bytes": len(body), "sha256": digest(body)}
                                 for path, body in sorted(files.items())]
            final["bundles"].append({
                "name": bundle,
                "asset": {"file": name, "bytes": len(recorded), "sha256": digest(recorded)},
                "manifest": {"library.sha256": digest(files["library.jar"]),
                             "consumer.sha256": digest(files["tools.jar"])},
                "inventory_sha256": digest(files["SHA256SUMS"]),
                "file_count": len(files),
                "uncompressed_bytes": sum(len(body) for body in files.values()),
            })
        if mutate:
            mutate(final, inventory)
        assets["FINAL-BASELINE.json"] = write(root / "FINAL-BASELINE.json", json.dumps(final).encode())
        assets["INVENTORY.json"] = write(root / "INVENTORY.json", json.dumps(inventory).encode())
        self.lines = [digest(body) + "  " + name for name, body in sorted(assets.items())]
        self.rewrite(self.lines)
        self.downloads = []

    def rewrite(self, lines):
        self.pin = digest(write(self.root / "SHA256SUMS", "".join(line + "\n" for line in lines).encode()))
        return self.pin

    def download(self, tag, names, directory):
        # gh fails when no requested pattern matches an asset, and succeeds when at least one does.
        self.downloads.append(list(names))
        present = [name for name in names if (self.root / name).exists()]
        if not present:
            raise fetcher.Rejected("gh release download failed for " + tag)
        for name in present:
            shutil.copyfile(self.root / name, Path(directory) / name)


@pytest.fixture
def output(tmp_path):
    return tmp_path / "output"


@pytest.fixture
def checked():
    return []


@pytest.fixture
def release(tmp_path, checked, monkeypatch):
    """Return a factory that serves a fresh Release to the fetcher and records each checked bundle."""

    def create(**options):
        created = Release(tmp_path / "release", **options)
        monkeypatch.setattr(fetcher, "download", created.download)
        monkeypatch.setattr(fetcher, "baseline_check", checked.append)
        monkeypatch.setattr(fetcher, "RELEASES", {TAG: created.pin})
        return created

    return create


@pytest.fixture
def rejected(tmp_path, output):
    """Return a check that the fetch is rejected with the message and leaves no output or staging."""

    def check(message, bundles=(BUNDLE,), cache=None):
        with pytest.raises(fetcher.Rejected) as raised:
            fetcher.fetch(TAG, str(output), list(bundles), cache)
        assert message in str(raised.value)
        assert not os.path.lexists(output)
        assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".")] == []
        return str(raised.value)

    return check


def test_extracts_verified_bundle_and_keeps_release_inventory(release, output, checked):
    release()
    fetcher.fetch(TAG, str(output), [BUNDLE])
    for name, data in bundle_files().items():
        assert (output / BUNDLE / name).read_bytes() == data
    assert {p.name for p in (output / "release").iterdir()} == {"SHA256SUMS", "FINAL-BASELINE.json", "INVENTORY.json",
                                                                ARCHIVE}
    # The check runs in the staging directory, which becomes the output only afterwards.
    assert [path.name for path in checked] == [BUNDLE]
    assert checked[0].parent.name.startswith(".output.")
    assert not os.path.lexists(checked[0])


def test_fetches_every_bundle_by_default_and_each_selected_bundle_once(release, tmp_path, output, checked):
    release(bundles=fetcher.BUNDLES)
    fetcher.fetch(TAG, str(output), [])
    assert [path.name for path in checked] == list(fetcher.BUNDLES)
    for bundle in fetcher.BUNDLES:
        assert (output / bundle / "manifest.properties").read_bytes() == bundle_files(bundle)["manifest.properties"]
    checked.clear()
    fetcher.fetch(TAG, str(tmp_path / "second"), ["flink2-pb4", "flink1-pb3", "flink2-pb4"])
    assert [path.name for path in checked] == ["flink2-pb4", "flink1-pb3"]
    assert {p.name for p in (tmp_path / "second").iterdir()} == {"release", "flink2-pb4", "flink1-pb3"}


def test_rejects_altered_archive_byte(release, rejected, checked):
    release(altered=True)
    rejected("Checksum mismatch: " + ARCHIVE)
    assert checked == []


def test_rejects_altered_checksum_inventory_before_other_downloads(release, rejected):
    created = release()
    with open(created.root / "SHA256SUMS", "ab") as sums:
        sums.write(b"\n")
    rejected("does not match the pinned digest")
    assert created.downloads == [["SHA256SUMS"]]


@pytest.mark.parametrize(("lines", "message"), [
    (lambda lines: lines + [lines[0]], "Duplicate SHA256SUMS entry"),
    (lambda lines: lines + ["not a checksum"], "Malformed SHA256SUMS line"),
], ids=["duplicate", "malformed"])
def test_rejects_malformed_or_duplicate_checksum_lines(release, rejected, monkeypatch, lines, message):
    created = release()
    monkeypatch.setitem(fetcher.RELEASES, TAG, created.rewrite(lines(created.lines)))
    rejected(message)


def test_rejects_missing_assets(release, rejected):
    created = release()
    (created.root / ARCHIVE).unlink()
    rejected("Missing release asset: " + ARCHIVE)
    (created.root / "SHA256SUMS").unlink()
    rejected("gh release download failed")


def final_field(path, value):
    def mutate(final, inventory):
        target = final
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value(target[path[-1]])
    return mutate


def release_designation(designated, **fields):
    """Rewrites the milestone designation as a format 2 release designation, then applies `fields`."""
    def mutate(final, inventory):
        for key in ("milestone", "milestone_final"):
            final.pop(key)
        final.update({"format_version": 2, "tag": designated, "final": True} | fields)
    return mutate


def test_accepts_a_release_designation(release, tmp_path):
    release(mutate=release_designation(TAG))
    fetcher.fetch(TAG, str(tmp_path / "output"), [BUNDLE])
    assert (tmp_path / "output" / BUNDLE).is_dir()


@pytest.mark.parametrize(("mutate", "message"), [
    (final_field(["format_version"], lambda _: 2), "does not designate " + TAG),
    (final_field(["milestone"], lambda _: "v0.0.9"), "does not designate " + TAG),
    (final_field(["milestone_final"], lambda _: "true"), "does not designate " + TAG),
    (final_field(["bundles"], lambda b: b + b), "Duplicate bundle in FINAL-BASELINE.json"),
    (final_field(["bundles"], lambda _: []), "does not record bundle " + BUNDLE),
    (final_field(["bundles", 0, "asset", "file"], lambda _: "renamed.tar.gz"), "Unexpected asset name"),
    (final_field(["bundles", 0, "asset", "sha256"], lambda _: "0" * 64),
     "FINAL-BASELINE.json and SHA256SUMS disagree"),
    (final_field(["bundles", 0, "asset", "bytes"], lambda size: size + 1), "Size mismatch: " + ARCHIVE),
    (release_designation("v0.0.9"), "does not designate " + TAG),
    (release_designation(TAG, final="true"), "does not designate " + TAG),
    (release_designation(TAG, format_version=1), "does not designate " + TAG),
], ids=["format", "milestone", "final", "duplicate", "absent", "name", "checksum", "size",
        "release-tag", "release-final", "release-format"])
def test_rejects_designation_that_does_not_select_the_archive(release, rejected, mutate, message):
    release(mutate=mutate)
    rejected(message)


def bundle_record(key, value):
    def mutate(final, inventory):
        record = final["bundles"][0]
        target = record["manifest"] if key in record["manifest"] else record
        target[key] = value(target[key])
    return mutate


@pytest.mark.parametrize(("key", "value"), [
    ("file_count", lambda n: n + 1),
    ("uncompressed_bytes", lambda n: n + 1),
    ("inventory_sha256", lambda _: "0" * 64),
    ("library.sha256", lambda _: "0" * 64),
    ("consumer.sha256", lambda _: "0" * 64),
], ids=["file_count", "uncompressed_bytes", "inventory_sha256", "library.sha256", "consumer.sha256"])
def test_rejects_each_disagreement_between_designation_and_inventory(release, rejected, key, value):
    release(mutate=bundle_record(key, value))
    rejected("FINAL-BASELINE.json and INVENTORY.json disagree on " + BUNDLE)


def inventory_entry(path, duplicate=False):
    def mutate(final, inventory):
        files = inventory[BUNDLE]
        files.append(dict(files[0], path=path, bytes=files[0]["bytes"] + 1) if duplicate
                     else dict(files[0], path=path))
    return mutate


@pytest.mark.parametrize(("mutate", "message"), [
    (inventory_entry("other/file"), "Inventory path outside bundle"),
    (inventory_entry(BUNDLE), "Inventory path outside bundle"),
    (inventory_entry(BUNDLE + "/../escape"), "Unsafe path"),
    (inventory_entry(BUNDLE + "/SHA256SUMS", duplicate=True), "Duplicate inventory path"),
    (lambda final, inventory: inventory.pop(BUNDLE), "INVENTORY.json does not record"),
], ids=["other-directory", "bundle-directory", "parent", "duplicate", "absent"])
def test_rejects_inventory_paths_that_cannot_be_extracted_safely(release, rejected, mutate, message):
    release(mutate=mutate)
    rejected(message)


def test_rejects_archive_missing_an_inventory_file(release, rejected):
    release(members=[m for m in default_members() if not m[0].endswith("tools.jar")])
    rejected("Archive is missing inventory files: " + BUNDLE + "/tools.jar")


@pytest.mark.parametrize(("data", "message"), [(b"TOOLS", "Checksum mismatch: "), (b"tools!", "Size mismatch: ")],
                         ids=["checksum", "size"])
def test_rejects_member_whose_bytes_differ_from_inventory(release, rejected, data, message):
    release(members=replaced(default_members(), "tools.jar", data))
    rejected(message + BUNDLE + "/tools.jar")


def special_member(name, kind, linkname=""):
    member = tarfile.TarInfo(BUNDLE + "/" + name)
    member.type, member.linkname = kind, linkname
    return member


@pytest.mark.parametrize(("member", "message"), [
    ("../escape", "Unsafe path"),
    ("/absolute", "Unsafe path"),
    (BUNDLE + "/../escape", "Unsafe path"),
    (BUNDLE + "//double", "Unsafe path"),
    ("other/file", "Archive member outside bundle"),
    (BUNDLE + "/extra", "Unexpected or duplicate archive member"),
    (BUNDLE + "/library.jar", "Unexpected or duplicate archive member"),
    (special_member("link", tarfile.SYMTYPE, "../.."), "Unsupported archive member type"),
    (special_member("hard", tarfile.LNKTYPE, BUNDLE + "/tools.jar"), "Unsupported archive member type"),
    (special_member("fifo", tarfile.FIFOTYPE), "Unsupported archive member type"),
], ids=["parent", "absolute", "bundle-parent", "double-slash", "other-directory", "extra", "duplicate", "symlink",
        "hardlink", "fifo"])
def test_rejects_unsafe_and_unexpected_members_even_if_checksummed(release, rejected, tmp_path, member, message):
    # A payload behind the link would land outside the output if links were followed.
    release(members=default_members() + [(member, b"x"), (BUNDLE + "/link/escape", b"x")])
    rejected(message)
    assert not (tmp_path / "escape").exists()


def test_rejects_existing_destination_before_download(release, output):
    created = release()
    output.mkdir()
    with pytest.raises(fetcher.Rejected) as raised:
        fetcher.fetch(TAG, str(output), [BUNDLE])
    assert "Output already exists" in str(raised.value)
    assert created.downloads == []
    assert list(output.iterdir()) == []


def test_does_not_take_over_a_destination_created_during_fetch(release, tmp_path, output, monkeypatch):
    release()
    monkeypatch.setattr(fetcher, "baseline_check", lambda bundle: output.mkdir())
    with pytest.raises(fetcher.Rejected) as raised:
        fetcher.fetch(TAG, str(output), [BUNDLE])
    assert "Output appeared during fetch" in str(raised.value)
    assert list(output.iterdir()) == []
    assert [p.name for p in tmp_path.iterdir() if p.name != "release"] == ["output"]


def test_releases_the_claimed_destination_when_the_final_rename_fails(release, tmp_path, output, monkeypatch):
    release()
    def fail(source, target):
        raise OSError("synthetic rename failure")
    # os.rename is global, so restore it before the assertions.
    with monkeypatch.context() as patched, pytest.raises(OSError):
        patched.setattr(fetcher.os, "rename", fail)
        fetcher.fetch(TAG, str(output), [BUNDLE])
    assert not os.path.lexists(output)
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".")] == []


@pytest.mark.parametrize(("destination", "cache"), [
    ("Repository", None),
    ("Repository/target/new", None),
    ("link/new", None),
    ("output", "Repository/cache"),
    ("REPOSITORY/new", None),
], ids=["repository", "target", "link", "cache", "case-variant"])
def test_rejects_destination_and_cache_inside_repository(release, tmp_path, monkeypatch, destination, cache):
    created = release()
    repository = tmp_path / "Repository"
    (repository / "target").mkdir(parents=True)
    (tmp_path / "link").symlink_to(repository)
    monkeypatch.setattr(fetcher, "ROOT", repository)
    if destination.startswith("REPOSITORY") and not (tmp_path / "REPOSITORY").exists():
        # Only a case-insensitive filesystem resolves the variant to the same checkout.
        pytest.skip("case-sensitive filesystem: the variant names a different directory")
    with pytest.raises(fetcher.Rejected) as raised:
        fetcher.fetch(TAG, str(tmp_path / destination), [BUNDLE], cache and str(tmp_path / cache))
    assert "must be outside the repository" in str(raised.value)
    assert [p.name for p in repository.iterdir()] == ["target"]
    assert created.downloads == []


@pytest.mark.parametrize(("tag", "bundles", "message"), [
    ("v9.9.9", [BUNDLE], "No pinned retained baseline"),
    (TAG, ["flink3-pb5"], "Unknown bundle"),
], ids=["tag", "bundle"])
def test_rejects_unknown_tag_and_bundle(release, output, tag, bundles, message):
    release()
    with pytest.raises(fetcher.Rejected) as raised:
        fetcher.fetch(tag, str(output), bundles)
    assert message in str(raised.value)


def test_failed_bundle_check_leaves_no_output(release, rejected, monkeypatch):
    release()
    def fail(bundle):
        raise fetcher.Rejected("baseline-check failed for " + bundle.name)
    monkeypatch.setattr(fetcher, "baseline_check", fail)
    rejected("baseline-check failed for " + BUNDLE)


def test_reuses_verified_cached_archive(release, tmp_path, output):
    created = release()
    cache = tmp_path / "cache"
    fetcher.fetch(TAG, str(output), [BUNDLE], str(cache))
    assert [p.name for p in cache.iterdir()] == [ARCHIVE]
    assert ARCHIVE not in [p.name for p in (output / "release").iterdir()]
    created.downloads.clear()
    fetcher.fetch(TAG, str(tmp_path / "second"), [BUNDLE], str(cache))
    assert all(ARCHIVE not in names for names in created.downloads)
    assert (tmp_path / "second" / BUNDLE / "tools.jar").read_bytes() == b"tools"


def test_restored_cache_without_its_archive_fails_instead_of_downloading(release, tmp_path, output):
    created = release()
    cache = tmp_path / "cache"
    cache.mkdir()
    with pytest.raises(fetcher.Rejected) as raised:
        fetcher.fetch(TAG, str(output), [BUNDLE], str(cache), restored=True)
    assert "Restored archive cache lacks " + ARCHIVE in str(raised.value)
    assert all(ARCHIVE not in names for names in created.downloads)
    assert not os.path.lexists(output)
    with pytest.raises(fetcher.Rejected) as raised:
        fetcher.fetch(TAG, str(output), [BUNDLE], None, restored=True)
    assert "--restored requires --archives" in str(raised.value)


def test_does_not_cache_an_altered_download(release, rejected, tmp_path):
    release(altered=True)
    cache = tmp_path / "cache"
    rejected("Checksum mismatch: " + ARCHIVE, cache=str(cache))
    assert list(cache.iterdir()) == []


def test_rejects_corrupted_or_linked_cached_archive_without_replacing_it(release, rejected, tmp_path):
    created = release()
    cache = tmp_path / "cache"
    corrupted = write(cache / ARCHIVE, b"corrupted")
    message = rejected("Size mismatch: " + ARCHIVE, cache=str(cache))
    assert "remove the cached copy" in message
    assert (cache / ARCHIVE).read_bytes() == corrupted
    (cache / ARCHIVE).unlink()
    (cache / ARCHIVE).symlink_to(created.root / ARCHIVE)
    rejected("Missing release asset: " + ARCHIVE, cache=str(cache))
    assert (cache / ARCHIVE).is_symlink()


def run_result(monkeypatch, returncode):
    """Replace subprocess.run with a stand-in that returns the code, and return its recorded calls."""
    calls = []
    def run(command, **options):
        calls.append((command, options))
        return type("Result", (), {"returncode": returncode})()
    monkeypatch.setattr("subprocess.run", run)
    return calls


def test_download_reads_the_project_release(monkeypatch):
    calls = run_result(monkeypatch, 0)
    fetcher.download(TAG, ["SHA256SUMS", ARCHIVE], Path("/assets"))
    assert calls[0][0] == ["gh", "release", "download", TAG, "--repo", "flink-gcp/flink-datastream-protobuf",
                           "--dir", "/assets", "--pattern", "SHA256SUMS", "--pattern", ARCHIVE]
    run_result(monkeypatch, 1)
    with pytest.raises(fetcher.Rejected) as raised:
        fetcher.download(TAG, ["SHA256SUMS"], Path("/assets"))
    assert "gh release download failed for " + TAG in str(raised.value)


def test_bundle_check_runs_without_download_credentials(monkeypatch):
    calls = run_result(monkeypatch, 0)
    for key in list(os.environ):
        monkeypatch.delenv(key)
    for key, value in {"PATH": "/bin", "GH_TOKEN": "a", "GITHUB_TOKEN": "b",
                       "GH_ENTERPRISE_TOKEN": "c", "GITHUB_ENTERPRISE_TOKEN": "d"}.items():
        monkeypatch.setenv(key, value)
    fetcher.baseline_check(Path("/bundles") / BUNDLE)
    command, options = calls[0]
    assert command == ["java", "-cp", "/bundles/" + BUNDLE + "/tools.jar",
                       "io.github.flink.gcp.protobuf.BaselineTool", "check", "/bundles/" + BUNDLE]
    assert options["env"] == {"PATH": "/bin"}
    run_result(monkeypatch, 1)
    with pytest.raises(fetcher.Rejected) as raised:
        fetcher.baseline_check(Path("/bundles") / BUNDLE)
    assert "baseline-check failed for " + BUNDLE in str(raised.value)


def test_asset_prints_pinned_archive_checksum(tmp_path, monkeypatch, capsys):
    release = Release(tmp_path)
    monkeypatch.setattr(fetcher, "download", release.download)
    monkeypatch.setattr(fetcher, "RELEASES", {TAG: release.pin})
    assert fetcher.main(["asset", TAG, BUNDLE]) == 0
    with pytest.raises(fetcher.Rejected) as raised:
        fetcher.asset(TAG, "flink2-pb3")
    recorded = json.loads((tmp_path / "FINAL-BASELINE.json").read_text())
    assert capsys.readouterr().out == recorded["bundles"][0]["asset"]["sha256"] + "\n"
    assert "No SHA256SUMS entry for v0.1.0-flink2-pb3.tar.gz" in str(raised.value)


def raise_from_fetch(failure):
    def fetch(*arguments, **options):
        raise failure
    return fetch


@pytest.mark.parametrize(("failure", "message"), [
    (None, "baseline-fetch: No pinned retained baseline for tag v9.9.9"),
    (json.JSONDecodeError("bad", "", 0), "baseline-fetch: unreadable or malformed"),
    (tarfile.ReadError("bad"), "baseline-fetch: unreadable or malformed"),
], ids=["rejected", "json", "tar"])
def test_main_reports_rejection_and_malformed_input(monkeypatch, capsys, failure, message):
    if failure is not None:
        monkeypatch.setattr(fetcher, "fetch", raise_from_fetch(failure))
    assert fetcher.main(["fetch", "v9.9.9", "/nonexistent/output"]) == 1
    assert message in capsys.readouterr().err
