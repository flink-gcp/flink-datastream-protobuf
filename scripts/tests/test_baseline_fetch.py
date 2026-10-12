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

import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import baseline_fetch as fetcher

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


class FetchTest(unittest.TestCase):

    def setUp(self):
        self.temporary = Path(tempfile.mkdtemp()).resolve()
        self.addCleanup(shutil.rmtree, self.temporary)
        self.output = self.temporary / "output"
        self.checked = []
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(contextlib.redirect_stdout(io.StringIO()))

    def release(self, **options):
        shutil.rmtree(self.temporary / "release", ignore_errors=True)
        release = Release(self.temporary / "release", **options)
        self.stack.enter_context(patch.object(fetcher, "download", release.download))
        self.stack.enter_context(patch.object(fetcher, "baseline_check", self.checked.append))
        self.stack.enter_context(patch.dict(fetcher.RELEASES, {TAG: release.pin}, clear=True))
        return release

    def rejected(self, message, bundles=(BUNDLE,), cache=None):
        with self.assertRaises(fetcher.Rejected) as raised:
            fetcher.fetch(TAG, str(self.output), list(bundles), cache)
        self.assertIn(message, str(raised.exception))
        self.assertFalse(os.path.lexists(self.output))
        self.assertEqual([], [p.name for p in self.temporary.iterdir() if p.name.startswith(".")])
        return str(raised.exception)

    def test_extracts_verified_bundle_and_keeps_release_inventory(self):
        self.release()
        fetcher.fetch(TAG, str(self.output), [BUNDLE])
        for name, data in bundle_files().items():
            self.assertEqual(data, (self.output / BUNDLE / name).read_bytes())
        self.assertEqual({"SHA256SUMS", "FINAL-BASELINE.json", "INVENTORY.json", ARCHIVE},
                         {p.name for p in (self.output / "release").iterdir()})
        # The check runs in the staging directory, which becomes the output only afterwards.
        self.assertEqual([BUNDLE], [path.name for path in self.checked])
        self.assertTrue(self.checked[0].parent.name.startswith(".output."))
        self.assertFalse(os.path.lexists(self.checked[0]))

    def test_fetches_every_bundle_by_default_and_each_selected_bundle_once(self):
        self.release(bundles=fetcher.BUNDLES)
        fetcher.fetch(TAG, str(self.output), [])
        self.assertEqual(list(fetcher.BUNDLES), [path.name for path in self.checked])
        for bundle in fetcher.BUNDLES:
            self.assertEqual(bundle_files(bundle)["manifest.properties"],
                             (self.output / bundle / "manifest.properties").read_bytes())
        self.checked.clear()
        fetcher.fetch(TAG, str(self.temporary / "second"), ["flink2-pb4", "flink1-pb3", "flink2-pb4"])
        self.assertEqual(["flink2-pb4", "flink1-pb3"], [path.name for path in self.checked])
        self.assertEqual({"release", "flink2-pb4", "flink1-pb3"},
                         {p.name for p in (self.temporary / "second").iterdir()})

    def test_rejects_altered_archive_byte(self):
        self.release(altered=True)
        self.rejected("Checksum mismatch: " + ARCHIVE)
        self.assertEqual([], self.checked)

    def test_rejects_altered_checksum_inventory_before_other_downloads(self):
        release = self.release()
        with open(release.root / "SHA256SUMS", "ab") as sums:
            sums.write(b"\n")
        self.rejected("does not match the pinned digest")
        self.assertEqual([["SHA256SUMS"]], release.downloads)

    def test_rejects_malformed_or_duplicate_checksum_lines(self):
        release = self.release()
        for lines, message in ((release.lines + [release.lines[0]], "Duplicate SHA256SUMS entry"),
                               (release.lines + ["not a checksum"], "Malformed SHA256SUMS line")):
            with self.subTest(message=message):
                fetcher.RELEASES[TAG] = release.rewrite(lines)
                self.rejected(message)

    def test_rejects_missing_assets(self):
        release = self.release()
        (release.root / ARCHIVE).unlink()
        self.rejected("Missing release asset: " + ARCHIVE)
        (release.root / "SHA256SUMS").unlink()
        self.rejected("gh release download failed")

    def test_rejects_designation_that_does_not_select_the_archive(self):
        def field(path, value):
            def mutate(final, inventory):
                target = final
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value(target[path[-1]])
            return mutate
        cases = {
            "format": (field(["format_version"], lambda _: 2), "does not designate " + TAG),
            "milestone": (field(["milestone"], lambda _: "v0.0.9"), "does not designate " + TAG),
            "final": (field(["milestone_final"], lambda _: "true"), "does not designate " + TAG),
            "duplicate": (field(["bundles"], lambda b: b + b), "Duplicate bundle in FINAL-BASELINE.json"),
            "absent": (field(["bundles"], lambda _: []), "does not record bundle " + BUNDLE),
            "name": (field(["bundles", 0, "asset", "file"], lambda _: "renamed.tar.gz"), "Unexpected asset name"),
            "checksum": (field(["bundles", 0, "asset", "sha256"], lambda _: "0" * 64),
                         "FINAL-BASELINE.json and SHA256SUMS disagree"),
            "size": (field(["bundles", 0, "asset", "bytes"], lambda size: size + 1), "Size mismatch: " + ARCHIVE),
        }
        for case, (mutate, message) in cases.items():
            with self.subTest(case=case):
                self.release(mutate=mutate)
                self.rejected(message)

    def test_rejects_each_disagreement_between_designation_and_inventory(self):
        def bundle(key, value):
            def mutate(final, inventory):
                record = final["bundles"][0]
                target = record["manifest"] if key in record["manifest"] else record
                target[key] = value(target[key])
            return mutate
        for key, value in (("file_count", lambda n: n + 1), ("uncompressed_bytes", lambda n: n + 1),
                           ("inventory_sha256", lambda _: "0" * 64), ("library.sha256", lambda _: "0" * 64),
                           ("consumer.sha256", lambda _: "0" * 64)):
            with self.subTest(key=key):
                self.release(mutate=bundle(key, value))
                self.rejected("FINAL-BASELINE.json and INVENTORY.json disagree on " + BUNDLE)

    def test_rejects_inventory_paths_that_cannot_be_extracted_safely(self):
        def entry(path, duplicate=False):
            def mutate(final, inventory):
                files = inventory[BUNDLE]
                files.append(dict(files[0], path=path, bytes=files[0]["bytes"] + 1) if duplicate
                             else dict(files[0], path=path))
            return mutate
        for mutate, message in ((entry("other/file"), "Inventory path outside bundle"),
                                (entry(BUNDLE), "Inventory path outside bundle"),
                                (entry(BUNDLE + "/../escape"), "Unsafe path"),
                                (entry(BUNDLE + "/SHA256SUMS", duplicate=True), "Duplicate inventory path"),
                                (lambda final, inventory: inventory.pop(BUNDLE), "INVENTORY.json does not record")):
            with self.subTest(message=message):
                self.release(mutate=mutate)
                self.rejected(message)

    def test_rejects_archive_missing_an_inventory_file(self):
        self.release(members=[m for m in default_members() if not m[0].endswith("tools.jar")])
        self.rejected("Archive is missing inventory files: " + BUNDLE + "/tools.jar")

    def test_rejects_member_whose_bytes_differ_from_inventory(self):
        for data, message in ((b"TOOLS", "Checksum mismatch: "), (b"tools!", "Size mismatch: ")):
            with self.subTest(message=message):
                self.release(members=replaced(default_members(), "tools.jar", data))
                self.rejected(message + BUNDLE + "/tools.jar")

    def test_rejects_unsafe_and_unexpected_members_even_if_checksummed(self):
        link = tarfile.TarInfo(BUNDLE + "/link")
        link.type, link.linkname = tarfile.SYMTYPE, "../.."
        hard = tarfile.TarInfo(BUNDLE + "/hard")
        hard.type, hard.linkname = tarfile.LNKTYPE, BUNDLE + "/tools.jar"
        fifo = tarfile.TarInfo(BUNDLE + "/fifo")
        fifo.type = tarfile.FIFOTYPE
        cases = {
            "../escape": "Unsafe path",
            "/absolute": "Unsafe path",
            BUNDLE + "/../escape": "Unsafe path",
            BUNDLE + "//double": "Unsafe path",
            "other/file": "Archive member outside bundle",
            BUNDLE + "/extra": "Unexpected or duplicate archive member",
            BUNDLE + "/library.jar": "Unexpected or duplicate archive member",
            link: "Unsupported archive member type",
            hard: "Unsupported archive member type",
            fifo: "Unsupported archive member type",
        }
        for member, message in cases.items():
            with self.subTest(member=getattr(member, "name", member)):
                # A payload behind the link would land outside the output if links were followed.
                self.release(members=default_members() + [(member, b"x"), (BUNDLE + "/link/escape", b"x")])
                self.rejected(message)
                self.assertFalse((self.temporary / "escape").exists())

    def test_rejects_existing_destination_before_download(self):
        release = self.release()
        self.output.mkdir()
        with self.assertRaises(fetcher.Rejected) as raised:
            fetcher.fetch(TAG, str(self.output), [BUNDLE])
        self.assertIn("Output already exists", str(raised.exception))
        self.assertEqual([], release.downloads)
        self.assertEqual([], list(self.output.iterdir()))

    def test_does_not_take_over_a_destination_created_during_fetch(self):
        self.release()
        with patch.object(fetcher, "baseline_check", lambda bundle: self.output.mkdir()):
            with self.assertRaises(fetcher.Rejected) as raised:
                fetcher.fetch(TAG, str(self.output), [BUNDLE])
        self.assertIn("Output appeared during fetch", str(raised.exception))
        self.assertEqual([], list(self.output.iterdir()))
        self.assertEqual(["output"], [p.name for p in self.temporary.iterdir() if p.name != "release"])

    def test_releases_the_claimed_destination_when_the_final_rename_fails(self):
        self.release()
        def fail(source, target):
            raise OSError("synthetic rename failure")
        with patch.object(fetcher.os, "rename", fail), self.assertRaises(OSError):
            fetcher.fetch(TAG, str(self.output), [BUNDLE])
        self.assertFalse(os.path.lexists(self.output))
        self.assertEqual([], [p.name for p in self.temporary.iterdir() if p.name.startswith(".")])

    def test_rejects_destination_and_cache_inside_repository(self):
        release = self.release()
        repository = self.temporary / "Repository"
        (repository / "target").mkdir(parents=True)
        (self.temporary / "link").symlink_to(repository)
        self.stack.enter_context(patch.object(fetcher, "ROOT", repository))
        cases = [(str(repository), None), (str(repository / "target" / "new"), None),
                 (str(self.temporary / "link" / "new"), None), (str(self.output), str(repository / "cache"))]
        variant = self.temporary / "REPOSITORY"
        if variant.exists():
            # A case-insensitive filesystem resolves the variant to the same checkout.
            cases.append((str(variant / "new"), None))
        for output, cache in cases:
            with self.subTest(output=output, cache=cache), self.assertRaises(fetcher.Rejected) as raised:
                fetcher.fetch(TAG, output, [BUNDLE], cache)
            self.assertIn("must be outside the repository", str(raised.exception))
        self.assertEqual(["target"], [p.name for p in repository.iterdir()])
        self.assertEqual([], release.downloads)

    def test_rejects_unknown_tag_and_bundle(self):
        self.release()
        for tag, bundles, message in (("v9.9.9", [BUNDLE], "No pinned retained baseline"),
                                      (TAG, ["flink3-pb5"], "Unknown bundle")):
            with self.subTest(tag=tag), self.assertRaises(fetcher.Rejected) as raised:
                fetcher.fetch(tag, str(self.output), bundles)
            self.assertIn(message, str(raised.exception))

    def test_failed_bundle_check_leaves_no_output(self):
        self.release()
        def fail(bundle):
            raise fetcher.Rejected("baseline-check failed for " + bundle.name)
        with patch.object(fetcher, "baseline_check", fail):
            self.rejected("baseline-check failed for " + BUNDLE)

    def test_reuses_verified_cached_archive(self):
        release = self.release()
        cache = self.temporary / "cache"
        fetcher.fetch(TAG, str(self.output), [BUNDLE], str(cache))
        self.assertEqual([ARCHIVE], [p.name for p in cache.iterdir()])
        self.assertNotIn(ARCHIVE, [p.name for p in (self.output / "release").iterdir()])
        release.downloads.clear()
        fetcher.fetch(TAG, str(self.temporary / "second"), [BUNDLE], str(cache))
        self.assertTrue(all(ARCHIVE not in names for names in release.downloads))
        self.assertEqual(b"tools", (self.temporary / "second" / BUNDLE / "tools.jar").read_bytes())

    def test_restored_cache_without_its_archive_fails_instead_of_downloading(self):
        release = self.release()
        cache = self.temporary / "cache"
        cache.mkdir()
        with self.assertRaises(fetcher.Rejected) as raised:
            fetcher.fetch(TAG, str(self.output), [BUNDLE], str(cache), restored=True)
        self.assertIn("Restored archive cache lacks " + ARCHIVE, str(raised.exception))
        self.assertTrue(all(ARCHIVE not in names for names in release.downloads))
        self.assertFalse(os.path.lexists(self.output))
        with self.assertRaises(fetcher.Rejected) as raised:
            fetcher.fetch(TAG, str(self.output), [BUNDLE], None, restored=True)
        self.assertIn("--restored requires --archives", str(raised.exception))

    def test_does_not_cache_an_altered_download(self):
        self.release(altered=True)
        cache = self.temporary / "cache"
        self.rejected("Checksum mismatch: " + ARCHIVE, cache=str(cache))
        self.assertEqual([], list(cache.iterdir()))

    def test_rejects_corrupted_or_linked_cached_archive_without_replacing_it(self):
        release = self.release()
        cache = self.temporary / "cache"
        corrupted = write(cache / ARCHIVE, b"corrupted")
        message = self.rejected("Size mismatch: " + ARCHIVE, cache=str(cache))
        self.assertIn("remove the cached copy", message)
        self.assertEqual(corrupted, (cache / ARCHIVE).read_bytes())
        (cache / ARCHIVE).unlink()
        (cache / ARCHIVE).symlink_to(release.root / ARCHIVE)
        self.rejected("Missing release asset: " + ARCHIVE, cache=str(cache))
        self.assertTrue((cache / ARCHIVE).is_symlink())


class CommandTest(unittest.TestCase):

    def run_result(self, returncode, calls):
        def run(command, **options):
            calls.append((command, options))
            return type("Result", (), {"returncode": returncode})()
        return patch.object(fetcher.subprocess, "run", run)

    def test_download_reads_the_project_release(self):
        calls = []
        with self.run_result(0, calls):
            fetcher.download(TAG, ["SHA256SUMS", ARCHIVE], Path("/assets"))
        self.assertEqual(["gh", "release", "download", TAG, "--repo", "flink-gcp/flink-datastream-protobuf",
                          "--dir", "/assets", "--pattern", "SHA256SUMS", "--pattern", ARCHIVE], calls[0][0])
        with self.run_result(1, []), self.assertRaises(fetcher.Rejected) as raised:
            fetcher.download(TAG, ["SHA256SUMS"], Path("/assets"))
        self.assertIn("gh release download failed for " + TAG, str(raised.exception))

    def test_bundle_check_runs_without_download_credentials(self):
        calls = []
        environment = {"PATH": "/bin", "GH_TOKEN": "a", "GITHUB_TOKEN": "b",
                       "GH_ENTERPRISE_TOKEN": "c", "GITHUB_ENTERPRISE_TOKEN": "d"}
        with patch.dict(os.environ, environment, clear=True), self.run_result(0, calls):
            fetcher.baseline_check(Path("/bundles") / BUNDLE)
        command, options = calls[0]
        self.assertEqual(["java", "-cp", "/bundles/" + BUNDLE + "/tools.jar",
                          "io.github.flink.gcp.protobuf.BaselineTool", "check", "/bundles/" + BUNDLE], command)
        self.assertEqual({"PATH": "/bin"}, options["env"])
        with self.run_result(1, []), self.assertRaises(fetcher.Rejected) as raised:
            fetcher.baseline_check(Path("/bundles") / BUNDLE)
        self.assertIn("baseline-check failed for " + BUNDLE, str(raised.exception))

    def test_asset_prints_pinned_archive_checksum(self):
        with tempfile.TemporaryDirectory() as temporary:
            release = Release(Path(temporary))
            output = io.StringIO()
            with patch.object(fetcher, "download", release.download), \
                    patch.dict(fetcher.RELEASES, {TAG: release.pin}, clear=True), \
                    contextlib.redirect_stdout(output):
                self.assertEqual(0, fetcher.main(["asset", TAG, BUNDLE]))
                with self.assertRaises(fetcher.Rejected) as raised:
                    fetcher.asset(TAG, "flink2-pb3")
            recorded = json.loads((Path(temporary) / "FINAL-BASELINE.json").read_text())
            self.assertEqual(recorded["bundles"][0]["asset"]["sha256"] + "\n", output.getvalue())
            self.assertIn("No SHA256SUMS entry for v0.1.0-flink2-pb3.tar.gz", str(raised.exception))

    def test_main_reports_rejection_and_malformed_input(self):
        for failure, message in ((None, "baseline-fetch: No pinned retained baseline for tag v9.9.9"),
                                 (json.JSONDecodeError("bad", "", 0), "baseline-fetch: unreadable or malformed"),
                                 (tarfile.ReadError("bad"), "baseline-fetch: unreadable or malformed")):
            with self.subTest(message=message):
                error = io.StringIO()
                with contextlib.ExitStack() as stack:
                    if failure is not None:
                        stack.enter_context(patch.object(fetcher, "fetch", side_effect=failure))
                    stack.enter_context(contextlib.redirect_stderr(error))
                    self.assertEqual(1, fetcher.main(["fetch", "v9.9.9", "/nonexistent/output"]))
                self.assertIn(message, error.getvalue())


if __name__ == "__main__":
    unittest.main()
