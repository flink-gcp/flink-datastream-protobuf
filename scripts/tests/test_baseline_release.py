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

import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import zipfile

import pytest

from conftest import load_script

fetcher = load_script("baseline-fetch.py")
release = load_script("baseline-release.py")

TAG = "v0.9.0"
REVISION = "a" * 40
TAG_OBJECT = "b" * 40
CI = {"conclusion": "success", "headSha": REVISION, "headBranch": "main", "event": "push", "workflowName": "CI",
      "url": "https://example.org/runs/1",
      "jobs": [{"conclusion": "success", "name": "Freeze source revision", "url": "u", "databaseId": 7},
               {"conclusion": "success", "name": "CI passed", "url": "https://example.org/jobs/1", "extra": 1}]}
FREEZE_LOG = ("Freeze source revision\tUNKNOWN STEP\t2026-10-28T15:22:57Z [command]/usr/bin/git log -1 --format=%H\n"
              "Freeze source revision\tUNKNOWN STEP\t2026-10-28T15:22:57Z " + REVISION + "\n")
BUILD_LOG = ("[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0\n"
             "[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1 s -- in X\n"
             "[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0\n[INFO] BUILD SUCCESS\n")
REJECTIONS = {name: message for name, _, _, _, message, _ in release.NEGATIVE_CONTROLS}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data if isinstance(data, bytes) else data.encode())


def jar(members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def seal(root):
    (root / "SHA256SUMS").unlink(missing_ok=True)
    write(root / "SHA256SUMS", "".join(digest(p.read_bytes()) + "  " + p.relative_to(root).as_posix() + "\n"
                                       for p in sorted(root.rglob("*")) if p.is_file()))


def capture_bundle(root, bundle, revision=REVISION, **manifest):
    """Writes a synthetic bundle with the files that assemble reads and fetch verifies."""
    line = bundle.split("-")[0]
    files = {"library.jar": b"library " + bundle.encode(), "tools.jar": b"tools " + bundle.encode(),
             "source.zip": b"source", "build.log": BUILD_LOG,
             # A key header literal two archives deep, as in shaded dependency bytecode.
             "dependencies/flink.jar": jar({"lib/netty.jar": jar({"io/netty/OpenSsl.class": b"-----BEGIN PRIVATE KEY-----"})})}
    for _, version, _ in release.reader_names(bundle, line):
        files["capture-%s/result.properties" % version] = "case.count=%d\nstatus=passed\n" % (21 if line == "flink1" else 20)
    values = {"status": "development-tooling-baseline", "milestone.final": "false", "format.version": "2",
              "source.revision": revision, "line": line, "protobuf.major": bundle[-1],
              "writer.artifact": "io.github.flink-gcp\\:flink-datastream-protobuf\\:0.9.0-SNAPSHOT",
              "library.sha256": digest(files["library.jar"]), "consumer.sha256": digest(files["tools.jar"])}
    values.update(manifest)
    files["manifest.properties"] = "".join("%s=%s\n" % item for item in values.items())
    for name, data in files.items():
        write(root / name, data)
    seal(root)


def reader_outputs(work, overrides=None, head=REVISION):
    """Writes passing reader results and failed controls as `read` would."""
    verification = work / "verification"
    shutil.rmtree(verification, ignore_errors=True)
    outcomes = []
    for bundle in release.BUNDLES:
        values = release.properties(work / "bundles" / bundle / "manifest.properties")
        for name, version, jdk in release.reader_names(bundle, values["line"]):
            result = {"status": "passed", "flink.version": version,
                      "java.version": "17.0.20+8" if jdk == 17 else "21.0.5+11-LTS",
                      "library.sha256": values["library.sha256"], "consumer.sha256": values["consumer.sha256"],
                      "case.count": "21" if values["line"] == "flink1" else "20"}
            result.update((overrides or {}).get(name, {}))
            write(verification / name / "result.properties", "".join("%s=%s\n" % kv for kv in result.items()))
            outcomes.append({"name": name, "exit": 0, "output_created": True, "negative_control": None})
    for name, *_, label in release.NEGATIVE_CONTROLS:
        write(verification / (name + ".log"), "Exception: java.lang.IllegalArgumentException: " + REJECTIONS[name])
        outcomes.append({"name": name, "exit": 1, "output_created": False, "negative_control": label})
    record = {"capture_checkout": {"path": "/capture", "head": head}, "outcomes": outcomes}
    write(verification / "readers.json", json.dumps(record))
    return record


def reviewed(work, disposition="Public dependency bytecode literal"):
    """Writes a review that explains exactly the current findings."""
    _, findings = release.scan(work)
    record = {"scope": "test", "raw_jvm_diagnostics_or_credential_files": [], "unresolved_matches": [],
              "reviewed_files": [], "reviewed_matches": [dict(f, disposition=disposition) for f in findings]}
    write(work / release.REVIEW, json.dumps(record))
    return record


def git(directory, *arguments):
    environment = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@example.org",
                       GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@example.org", GIT_CONFIG_GLOBAL=os.devnull)
    return subprocess.run(["git", *arguments], cwd=directory, check=True, capture_output=True, text=True,
                          env=environment).stdout.strip()


class Answers:
    """What the replaced tag, CI run, and job log lookups answer, and the bundles that were checked."""

    def __init__(self):
        self.checked = []
        self.ci = json.loads(json.dumps(CI))
        self.freeze_log = FREEZE_LOG


@pytest.fixture
def answers(monkeypatch):
    answers = Answers()
    monkeypatch.setattr(release, "tag_identity", lambda tag: (TAG_OBJECT, REVISION))
    monkeypatch.setattr(release, "github_run", lambda run_id: answers.ci)
    monkeypatch.setattr(release, "job_log", lambda run_id, job: answers.freeze_log if job == 7 else "")
    monkeypatch.setattr(release.layout, "baseline_check", answers.checked.append)
    return answers


@pytest.fixture
def work(tmp_path, answers):
    """Writes a work directory that assembles, with git and GitHub answered by `answers`."""
    work = tmp_path / "work"
    for bundle in release.BUNDLES:
        capture_bundle(work / "bundles" / bundle, bundle)
    reader_outputs(work)
    write(work / "README.md", "# %s\n\nCommit %s, tag object %s.\n" % (TAG, REVISION, TAG_OBJECT))
    reviewed(work)
    return work


def assemble(work):
    release.assemble(TAG, work, "1")
    return work / "release"


def assert_rejected(work, message):
    with pytest.raises(release.Rejected) as raised:
        assemble(work)
    assert message in str(raised.value)
    assert not (work / "release").exists()
    assert list(work.glob(".release.*")) == []


def source_settings(work, data=b"<settings/>"):
    """Puts a tracked CI Maven settings file into the source archive that every bundle retains."""
    source = jar({".github/maven/settings.xml": data})
    for bundle in release.BUNDLES:
        write(work / "bundles" / bundle / "source.zip", source)
        seal(work / "bundles" / bundle)


def capture_checkout(tmp_path):
    checkout = tmp_path / "capture"
    checkout.mkdir()
    git(checkout, "init", "-q")
    git(checkout, "commit", "-q", "--allow-empty", "-m", "c")
    return checkout, git(checkout, "rev-parse", "HEAD")


def test_assembled_release_passes_the_retrieval_checks(work, answers, tmp_path, monkeypatch):
    assets = assemble(work)
    assert sorted(path.name for path in assets.iterdir()) == sorted(
        ["FINAL-BASELINE.json", "INVENTORY.json", "README.md", "SHA256SUMS", TAG + "-verification.tar.gz"]
        + [TAG + "-" + b + ".tar.gz" for b in release.BUNDLES])
    assert answers.checked == [work / "bundles" / b for b in release.BUNDLES]
    final = json.loads((assets / "FINAL-BASELINE.json").read_text())
    assert (final["milestone"], final["source_revision"], final["annotated_tag_object"],
            final["embedded_artifact_version"], final["ci_url"]) == (
        TAG, REVISION, TAG_OBJECT, "0.9.0-SNAPSHOT", "https://example.org/runs/1")
    # The manifest is recorded as written, with the properties escape kept, as in v0.1.0.
    assert "\\:" in final["bundles"][0]["manifest"]["writer.artifact"]
    assert len(final["readers"]) == 10
    assert {bundle["clean_verify_tests"] for bundle in final["bundles"]} == {7}
    pin = digest((assets / "SHA256SUMS").read_bytes())

    def download(tag, names, directory):
        for name in names:
            shutil.copyfile(assets / name, Path(directory) / name)

    output = tmp_path / "fetched"
    monkeypatch.setitem(fetcher.RELEASES, TAG, pin)
    monkeypatch.setattr(fetcher, "download", download)
    monkeypatch.setattr(fetcher, "baseline_check", lambda bundle: None)
    fetcher.fetch(TAG, str(output), [])
    for bundle in release.BUNDLES:
        assert (output / bundle / "SHA256SUMS").read_bytes() == (work / "bundles" / bundle / "SHA256SUMS").read_bytes()
    with tarfile.open(assets / (TAG + "-verification.tar.gz")) as tar:
        assert {"verification/ci.json", "verification/PUBLICATION-REVIEW.json", "verification/readers.json",
                "verification/audit-readers.json", "verification/audit-flink1-pb4.json"} <= set(tar.getnames())
        ci = json.load(tar.extractfile("verification/ci.json"))
    assert "extra" not in ci["jobs"][0]


def test_review_must_explain_exactly_the_current_findings(work):
    record = reviewed(work)
    write(work / release.REVIEW, json.dumps(dict(record, unresolved_matches=[{"path": "x", "kind": "k"}])))
    assert_rejected(work, "Publication review has flagged files or unresolved matches")
    write(work / release.REVIEW, json.dumps(dict(record, raw_jvm_diagnostics_or_credential_files=["x"])))
    assert_rejected(work, "Publication review has flagged files or unresolved matches")
    reviewed(work, disposition=" ")
    assert_rejected(work, "needs a disposition")
    # Deleting findings instead of explaining them, or a review older than the files, is rejected.
    write(work / release.REVIEW, json.dumps(dict(record, reviewed_matches=record["reviewed_matches"][1:])))
    assert_rejected(work, "does not match the current files")
    reviewed(work)
    write(work / "verification" / "read-flink2-pb3-2.2.1-jdk17" / "runtime.log", "password=hunter22")
    assert_rejected(work, "does not match the current files")
    # A reviewed finding whose bytes change keeps its path and rule but not its identity.
    write(work / "verification" / "read-flink2-pb3-2.2.1-jdk17" / "runtime.log", "password=synthetic")
    reviewed(work)
    write(work / "verification" / "read-flink2-pb3-2.2.1-jdk17" / "runtime.log",
          "password=synthetic\npassword=hunter22")
    assert_rejected(work, "does not match the current files")
    (work / "verification" / "read-flink2-pb3-2.2.1-jdk17" / "runtime.log").unlink()
    reviewed(work)
    write(work / "README.md", "# %s\n\nCommit %s, tag object %s.\ntoken=hunter22\n" % (TAG, REVISION, TAG_OBJECT))
    assert_rejected(work, "README.md or a disposition matches a credential rule")


@pytest.mark.parametrize("name", ["hs_err_pid42.log", "recording.jfr", "heap.hprof.gz"])
def test_review_must_list_every_sensitive_file(work, name):
    write(work / "verification" / name, "dump")
    assert_rejected(work, "Sensitive files do not match the reviewed files")


@pytest.mark.parametrize("disposition", ["Synthetic value password=hunter22", 'Quoted {"password":"hunter22"}'],
                         ids=["plain", "quoted"])
def test_review_dispositions_must_not_match_a_credential_rule(work, disposition):
    reviewed(work, disposition=disposition)
    assert_rejected(work, "README.md or a disposition matches a credential rule")


@pytest.mark.parametrize(("key", "value"), [
    ("library.sha256", "c" * 64), ("consumer.sha256", "c" * 64), ("case.count", "13"),
    ("java.version", "17.0.20+8"), ("status", "failed"), ("flink.version", "2.2.1"),
], ids=["library.sha256", "consumer.sha256", "case.count", "java.version", "status", "flink.version"])
def test_readers_must_cover_every_claimed_combination_with_the_retained_jars(work, key, value):
    reader_outputs(work, {"read-flink2-pb4-2.3.0-jdk21": {key: value}})
    reviewed(work)
    assert_rejected(work, "Reader result does not match its retained bundle: read-flink2-pb4-2.3.0-jdk21")


def test_readers_must_all_pass_from_the_tagged_capture_checkout(work):
    record = reader_outputs(work)
    record["outcomes"] = [o for o in record["outcomes"] if o["name"] != "read-flink2-pb3-2.3.0-jdk17"]
    write(work / "verification" / "readers.json", json.dumps(record))
    reviewed(work)
    assert_rejected(work, "Missing or failed reader: read-flink2-pb3-2.3.0-jdk17")
    reader_outputs(work, head="c" * 40)
    reviewed(work)
    assert_rejected(work, "build output of the tagged capture checkout")


@pytest.mark.parametrize("change", ["exit", "output_created", "directory", "message"])
def test_negative_controls_must_fail_with_their_message_before_creating_output(work, change):
    record = reader_outputs(work)
    control = record["outcomes"][-1]
    if change == "exit":
        control["exit"] = 0
    elif change == "output_created":
        control["output_created"] = True
    elif change == "directory":
        (work / "verification" / control["name"]).mkdir()
    else:
        write(work / "verification" / (control["name"] + ".log"), "Error: could not start the JVM")
    write(work / "verification" / "readers.json", json.dumps(record))
    reviewed(work)
    assert_rejected(work, "Negative control did not reject with its message before creating output: negative-lts-jdk21")


@pytest.mark.parametrize("manifest", [{"source.revision": "c" * 40}, {"milestone.final": "true"}, {"status": "other"}],
                         ids=["source.revision", "milestone.final", "status"])
def test_bundles_must_be_development_baselines_of_the_tagged_revision(work, manifest):
    capture_bundle(work / "bundles" / "flink1-pb4", "flink1-pb4", **manifest)
    reviewed(work)
    assert_rejected(work, "Bundle was not captured as a development baseline of " + REVISION)


def test_bundles_must_match_their_name_and_one_source_archive(work):
    capture_bundle(work / "bundles" / "flink1-pb4", "flink1-pb4", **{"protobuf.major": "3"})
    reviewed(work)
    assert_rejected(work, "Bundle name does not match its artifact line and Protobuf profile: flink1-pb4")
    capture_bundle(work / "bundles" / "flink1-pb4", "flink1-pb4")
    write(work / "bundles" / "flink1-pb4" / "source.zip", b"other source")
    seal(work / "bundles" / "flink1-pb4")
    reviewed(work)
    assert_rejected(work, "different source archives")


@pytest.mark.parametrize(("key", "value"), [
    ("headSha", "c" * 40), ("conclusion", "failure"), ("workflowName", "Dependency submission"),
    ("event", "workflow_dispatch"), ("headBranch", "other"),
    ("jobs", [{"conclusion": "failure", "name": "CI passed", "url": "u"}]),
], ids=["headSha", "conclusion", "workflowName", "event", "headBranch", "jobs"])
def test_ci_must_be_the_successful_main_branch_push_run_of_the_tagged_commit(work, answers, key, value):
    answers.ci = dict(CI, **{key: value})
    assert_rejected(work, "is not a successful main-branch CI push run of " + REVISION)


def test_ci_must_have_frozen_the_tagged_commit_in_one_job(work, answers):
    # A push run tests the branch tip at its start, which can be a later commit.
    answers.freeze_log = FREEZE_LOG.replace(REVISION, "c" * 40)
    assert_rejected(work, "did not test " + REVISION)
    answers.freeze_log = ""
    assert_rejected(work, "did not test " + REVISION)
    answers.ci["jobs"] = answers.ci["jobs"][1:]
    assert_rejected(work, "has no single source-freezing job")


def test_readme_and_tag_must_name_the_release(work):
    write(work / "README.md", "# " + TAG + "\n")
    assert_rejected(work, "README.md must name")
    with pytest.raises(release.Rejected) as raised:
        release.assemble("v1.0.0", work, "1")
    assert "Expected a development milestone tag" in str(raised.value)


@pytest.mark.parametrize("text", [
    BUILD_LOG + "[ERROR] Tests run: 9, Failures: 0, Errors: 1, Skipped: 0\n",
    BUILD_LOG + "[WARNING] Tests run: 9, Failures: 0, Errors: 0, Skipped: 2\n",
    BUILD_LOG.replace("BUILD SUCCESS", "BUILD FAILURE"),
], ids=["error", "skipped", "failure"])
def test_capture_builds_must_report_passing_tests(work, text):
    log = work / "bundles" / "flink2-pb3" / "build.log"
    log.write_text(text)
    seal(log.parent)
    reviewed(work)
    assert_rejected(work, "does not report a successful build")


def test_capture_builds_must_report_some_tests(work):
    log = work / "bundles" / "flink2-pb3" / "build.log"
    log.write_text("[INFO] BUILD SUCCESS\n")
    seal(log.parent)
    reviewed(work)
    assert_rejected(work, "reports no tests")


def test_release_output_is_never_replaced_or_left_partial(work, monkeypatch):
    (work / "release").mkdir()
    with pytest.raises(release.Rejected):
        assemble(work)
    (work / "release").rmdir()

    def fail(*arguments):
        raise OSError("disk full")

    with monkeypatch.context() as patched:
        patched.setattr(release, "evidence_directory", fail)
        with pytest.raises(OSError):
            assemble(work)
    assert not (work / "release").exists()
    assert list(work.glob(".release.*")) == []
    # An empty output created during assembly is not silently replaced by the rename.
    original = release.evidence_directory

    def racing(*arguments):
        original(*arguments)
        (work / "release").mkdir()

    monkeypatch.setattr(release, "evidence_directory", racing)
    with pytest.raises(release.Rejected) as raised:
        assemble(work)
    assert "appeared during assembly" in str(raised.value)
    assert list((work / "release").iterdir()) == []


def test_inputs_must_be_retrievable_and_contain_no_links(work, tmp_path):
    (work / "bundles" / "flink2-pb3" / "notes copy").mkdir()
    reviewed(work)
    assert_rejected(work, "Unsafe path")
    (work / "bundles" / "flink2-pb3" / "notes copy").rmdir()
    (work / "verification" / "ci.json").symlink_to(work / "bundles" / "flink2-pb3" / "build.log")
    with pytest.raises(release.Rejected) as raised:
        reviewed(work)
    assert "Symbolic links are not retained inputs" in str(raised.value)
    assert "BUILD SUCCESS" in (work / "bundles" / "flink2-pb3" / "build.log").read_text()
    (work / "verification" / "ci.json").unlink()
    reviewed(work)
    (work / "verification").rename(tmp_path / "elsewhere")
    (work / "verification").symlink_to(tmp_path / "elsewhere")
    assert_rejected(work, "Run read before review")
    (work / "verification").unlink()
    (tmp_path / "elsewhere").rename(work / "verification")
    (work / "README.md").rename(tmp_path / "README.md")
    (work / "README.md").symlink_to(tmp_path / "README.md")
    assert_rejected(work, "must be files in the work directory")


def test_tag_identity_requires_an_annotated_tag(tmp_path):
    repository = tmp_path / "repository"
    repository.mkdir()
    git(repository, "init", "-q")
    git(repository, "commit", "-q", "--allow-empty", "-m", "c")
    git(repository, "tag", "lightweight")
    git(repository, "tag", "-a", "annotated", "-m", "t")
    assert release.tag_identity("annotated", repository) == (git(repository, "rev-parse", "annotated"),
                                                             git(repository, "rev-parse", "HEAD"))
    with pytest.raises(release.Rejected) as raised:
        release.tag_identity("lightweight", repository)
    assert "not annotated" in str(raised.value)


def test_root_is_the_repository():
    assert (release.ROOT / "justfile").is_file()
    assert release.ROOT == fetcher.ROOT


def test_scan_searches_nested_archives_and_reports_what_it_cannot_search(work):
    flagged, findings = release.scan(work)
    assert flagged == []
    assert {(f["path"], f["kind"], f["sha256"]) for f in findings} == {
        (b + "/dependencies/flink.jar:lib/netty.jar:io/netty/OpenSsl.class", "private-key",
         digest(b"-----BEGIN PRIVATE KEY-----")) for b in release.BUNDLES}
    assert release.CREDENTIAL_RULES is load_script("benchmark-evidence.py").CREDENTIAL_RULES
    nested = b"password=hunter22"
    for _ in range(release.MAX_ARCHIVE_DEPTH + 1):
        nested = jar({"inner.jar": nested})
    write(work / "bundles" / "flink2-pb3" / "deep.jar", nested)
    write(work / "verification" / "key.pem", "x")
    flagged, findings = release.scan(work)
    assert [f["path"] for f in flagged] == ["verification/key.pem"]
    assert "archive-too-deep" in {f["kind"] for f in findings if f["path"].startswith("flink2-pb3/deep.jar")}


def test_scan_searches_bytes_outside_members_and_inside_streams():
    findings = []
    member = jar({"a.properties": b"password=hunter22"})
    launcher = b"#!/bin/sh\ntoken=hunter22\n" + member
    found = dict(release.members("launcher", launcher, findings))
    assert b"token=hunter22" in found["launcher"]
    assert found["launcher:a.properties"] == b"password=hunter22"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("a.class", b"x")
        archive.comment = b"token=hunter22"
    found = dict(release.members("c.jar", buffer.getvalue(), findings))
    assert b"token=hunter22" in found["c.jar"]
    assert found["c.jar:a.class"] == b"x"
    assert findings == []


@pytest.mark.parametrize("tar_format", [tarfile.GNU_FORMAT, tarfile.USTAR_FORMAT, tarfile.PAX_FORMAT],
                         ids=["v7", "ustar", "pax"])
def test_scan_searches_compressed_tar_streams(tar_format):
    findings = []
    body = b"password=hunter22"
    log = gzip.compress(body)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tar_format) as tar:
        info = tarfile.TarInfo("evidence/run.log.gz")
        info.size = len(log)
        tar.addfile(info, io.BytesIO(log))
    data = buffer.getvalue()
    if tar_format == tarfile.GNU_FORMAT:
        # A V7 header has no magic; its checksum covers the header with the checksum field blank.
        header = bytearray(data[:512])
        header[257:265] = b"\0" * 8
        header[148:156] = b" " * 8
        header[148:156] = b"%06o\0 " % sum(header)
        data = bytes(header) + data[512:]
    found = dict(release.members("e.tar.gz", gzip.compress(data), findings))
    assert found["e.tar.gz:e.tar:evidence/run.log.gz:run.log"] == body
    assert findings == []


def test_scan_handles_polyglot_truncated_and_trailing_archives():
    body = b"password=hunter22"

    def tar_bytes(members, tar_format=tarfile.USTAR_FORMAT):
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w", format=tar_format) as tar:
            for member, data in members:
                info = tarfile.TarInfo(member)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
        return buffer.getvalue()

    archive = tar_bytes([("run.log.gz", gzip.compress(body))])
    findings = []
    # A tar archive followed by a JAR also opens as a prefixed ZIP; the tar members are still searched.
    found = dict(release.members("x.tar", archive + jar({"a.class": b"x"}), findings))
    assert found["x.tar:run.log.gz:run.log"] == body
    found = dict(release.members("y.tar", archive + b"!", findings))
    assert found["y.tar:run.log.gz:run.log"] == body
    assert b"!" in found["y.tar"]
    assert findings == []
    # Other data of a tar archive's size is searched as itself, without an unreadable finding.
    # Its checksum field happens to hold octal digits that do not match the header.
    text = b"A" * 148 + b"0000123 " + b"A" * (1024 - 156)
    assert list(release.members("log.txt", text, findings)) == [("log.txt", text)]
    assert findings == []
    truncated = tar_bytes([("a.log", body), ("b.log", b"x" * 2048)])[:512 * 4]
    list(release.members("t.tar", truncated, findings))
    assert [(f["path"], f["kind"]) for f in findings] == [("t.tar", "unreadable-archive")]


def test_scan_searches_zip_bytes_except_member_contents():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
        info = zipfile.ZipInfo("first.txt")
        info.extra = b"\xfe\xca\x0e\x00token=hunter22"
        archive.writestr(info, b"FIRSTBODY")
        archive.writestr("second.txt", b"SECONDBODY")
    data = buffer.getvalue()
    # zipfile writes the extra field to the central directory too; keep the marker in the local header only.
    directory = data.rindex(b"PK\x01\x02", 0, data.rindex(b"PK\x01\x02"))
    data = data[:directory] + data[directory:].replace(b"token=hunter22", b"other=xxxxxxxx", 1)
    found = dict(release.members("z.zip", b"LAUNCHER" + data + b"TRAILER", []))
    outside = found["z.zip"]
    for expected in (b"LAUNCHER", b"token=hunter22", b"first.txt", b"second.txt", b"TRAILER"):
        assert expected in outside
    assert b"FIRSTBODY" not in outside
    assert b"SECONDBODY" not in outside
    assert found["z.zip:first.txt"] == b"FIRSTBODY"


def test_scan_does_not_open_archives_past_the_depth_limit(monkeypatch):
    opened = []
    nested = gzip.compress(b"x")
    for _ in range(release.MAX_ARCHIVE_DEPTH):
        nested = jar({"inner": nested})
    findings = []
    monkeypatch.setattr(release, "STREAMS", ((b"\x1f\x8b", lambda data: opened.append(data) or b""),))
    list(release.members("deep.jar", nested, findings))
    assert opened == []
    assert [f["kind"] for f in findings] == ["archive-too-deep"]


def test_scan_identifies_unreadable_archives_by_their_bytes():
    broken = b"PK\x03\x04" + b"\x00" * 40
    findings = []
    assert list(release.members("b.jar", broken, findings)) == [("b.jar", broken)]
    assert [(f["path"], f["kind"], f["sha256"]) for f in findings] == [("b.jar", "unreadable-archive", digest(broken))]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("a.class", b"data")
    data = bytearray(buffer.getvalue())
    data[8:10] = (99).to_bytes(2, "little")  # an unsupported compression method in the local header
    central = data.rfind(b"PK\x01\x02")
    data[central + 10:central + 12] = (99).to_bytes(2, "little")
    findings = []
    assert [name for name, _ in release.members("x.jar", bytes(data), findings)] == ["x.jar"]
    # A replaced payload under the same member name changes the container, and so the finding.
    assert [(f["path"], f["kind"], f["sha256"]) for f in findings] == [
        ("x.jar:a.class", "unreadable-archive-member", digest(bytes(data)))]


def test_reviewed_source_file_is_accepted_only_with_its_bytes(work):
    # A tracked repository file can carry a sensitive name without a secret.
    source_settings(work)
    record = reviewed(work)
    assert_rejected(work, "Sensitive files do not match the reviewed files: flink2-pb3/source.zip:.github/maven/settings.xml")
    flagged, _ = release.scan(work)
    assert {(f["path"], f["kind"], f["sha256"]) for f in flagged} == {
        (bundle + "/source.zip:.github/maven/settings.xml", "sensitive-name", digest(b"<settings/>"))
        for bundle in release.BUNDLES}
    files = [dict(f, disposition="Repository CI settings without credentials") for f in flagged]
    write(work / release.REVIEW, json.dumps(dict(record, reviewed_files=files[:-1] + [dict(files[-1], disposition=" ")])))
    assert_rejected(work, "needs a disposition")
    write(work / release.REVIEW, json.dumps(dict(record, reviewed_files=files[1:])))
    assert_rejected(work, "Sensitive files do not match the reviewed files")
    write(work / release.REVIEW, json.dumps(dict(record, reviewed_files=files)))
    assemble(work)
    assert (work / "release").is_dir()


def test_reviewed_source_file_with_changed_bytes_is_rejected(work):
    source_settings(work)
    record = reviewed(work)
    flagged, _ = release.scan(work)
    files = [dict(f, disposition="Repository CI settings without credentials") for f in flagged]
    source_settings(work, b"<settings><servers/></settings>")
    write(work / release.REVIEW, json.dumps(dict(reviewed(work), reviewed_files=files)))
    assert_rejected(work, "Sensitive files do not match the reviewed files")
    assert record["reviewed_matches"] != []


# Neither a reader output nor an archive in the reader outputs named like a source archive qualifies.
@pytest.mark.parametrize(("name", "data"), [("settings.xml", b"<settings/>"), ("source.zip", jar({"recording.jfr": b"x"}))],
                         ids=["settings.xml", "source.zip"])
def test_a_flagged_file_outside_bundle_source_archives_cannot_be_reviewed(work, name, data):
    write(work / "verification" / name, data)
    record = reviewed(work)
    flagged, _ = release.scan(work)
    files = [dict(f, disposition="Reader output named like a credential file") for f in flagged]
    write(work / release.REVIEW, json.dumps(dict(record, reviewed_files=files)))
    assert_rejected(work, "Only members of a bundle's source archive can be reviewed files")


def test_an_unreadable_sensitive_member_is_flagged(work):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
        archive.writestr("recording.jfr", b"x" * 64)
    data = bytearray(buffer.getvalue())
    # Corrupting the stored content fails its CRC, so the member cannot be read.
    data[data.index(b"x" * 64)] ^= 1
    write(work / "verification" / "diagnostics.zip", bytes(data))
    flagged, _ = release.scan(work)
    assert [(f["path"], f["kind"], f["sha256"]) for f in flagged] == [
        ("verification/diagnostics.zip:recording.jfr", "sensitive-name", digest(bytes(data)))]


def test_a_flagged_archive_is_identified_by_its_contents(work):
    # A same-size change inside a flagged archive changes its identity, not only its outside bytes.
    def archive(payload):
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as tar:
            info = tarfile.TarInfo("payload.bin")
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))
        return buffer.getvalue()
    first, second = archive(b"aaaa"), archive(b"bbbb")
    write(work / "verification" / "core.tar", first)
    identity, _ = release.scan(work)
    write(work / "verification" / "core.tar", second)
    changed, _ = release.scan(work)
    assert [(f["path"], f["sha256"]) for f in identity] == [("verification/core.tar", digest(first))]
    assert [(f["path"], f["sha256"]) for f in changed] == [("verification/core.tar", digest(second))]


def test_scan_flags_diagnostics_inside_containers_and_compression(work):
    write(work / "verification" / "core.42.zip", jar({"payload.bin": b"x"}))
    write(work / "verification" / "recording.jfr.gz", gzip.compress(b"x"))
    flagged, _ = release.scan(work)
    assert {f["path"] for f in flagged} == {"verification/core.42.zip", "verification/recording.jfr.gz:recording.jfr"}


def test_review_writes_every_finding_unresolved_once(work):
    (work / release.REVIEW).unlink()
    release.review(work)
    record = json.loads((work / release.REVIEW).read_text())
    assert len(record["unresolved_matches"]) == 4
    assert record["reviewed_matches"] == []
    assert record["reviewed_files"] == []
    assert "BEGIN" not in json.dumps(record)
    with pytest.raises(release.Rejected):
        release.review(work)


def test_read_runs_every_claimed_combination_and_both_controls(work, tmp_path, monkeypatch):
    shutil.rmtree(work / "verification")
    checkout, head = capture_checkout(tmp_path)
    commands = []

    def run(command, cwd, log):
        commands.append(command)
        assert cwd == work
        name = Path(command[-1]).name
        if name in REJECTIONS:
            Path(log).write_text(REJECTIONS[name])
            return 1
        Path(command[-1]).mkdir()
        Path(log).write_text("")
        return 0

    monkeypatch.setattr(release, "run", run)
    release.read(work, checkout, "java17", "java21")
    assert len(commands) == 12
    assert {Path(c[-1]).name for c in commands if c[0] == "java21"} == {
        "read-flink2-pb3-2.2.1-jdk21", "read-flink2-pb3-2.3.0-jdk21", "read-flink2-pb4-2.2.1-jdk21",
        "read-flink2-pb4-2.3.0-jdk21", "negative-lts-jdk21"}
    assert json.loads((work / "verification" / "readers.json").read_text())["capture_checkout"]["head"] == head
    (checkout / "target").mkdir()
    shutil.rmtree(work / "verification")
    with pytest.raises(release.Rejected) as raised:
        release.read(work, checkout, "java17", "java21")
    assert "Move the capture build output" in str(raised.value)


@pytest.mark.parametrize(("mode", "message"), [
    ("control-passes", "negative-unsupported-flink"),
    ("control-creates-output", "negative-unsupported-flink"),
    ("reader-fails", "Failed reader: read-flink1-pb4-1.20.5-jdk17"),
], ids=["control-passes", "control-creates-output", "reader-fails"])
def test_read_rejects_controls_that_pass_or_create_output_and_failed_readers(work, tmp_path, monkeypatch, mode,
                                                                             message):
    checkout, _ = capture_checkout(tmp_path)
    shutil.rmtree(work / "verification")

    def run(command, cwd, log):
        name = Path(command[-1]).name
        if name in REJECTIONS:
            Path(log).write_text(REJECTIONS[name])
            if mode == "control-creates-output":
                Path(command[-1]).mkdir()
            return 0 if mode == "control-passes" else 1
        Path(command[-1]).mkdir()
        return 1 if mode == "reader-fails" and name == "read-flink1-pb4-1.20.5-jdk17" else 0

    monkeypatch.setattr(release, "run", run)
    with pytest.raises(release.Rejected) as raised:
        release.read(work, checkout, "java17", "java21")
    assert message in str(raised.value)


def test_run_removes_credentials_from_the_instrument_environment(tmp_path, monkeypatch):
    log = tmp_path / "environment.log"
    monkeypatch.setenv("GH_TOKEN", "secret")
    monkeypatch.setenv("KEEP", "kept")
    status = release.run([sys.executable, "-c", "import os; print(os.environ.get('GH_TOKEN'), "
                          "os.environ.get('KEEP'))"], tmp_path, log)
    assert status == 0
    assert log.read_text().strip() == "None kept"


@pytest.mark.parametrize("change", [None, "changed", "extra"], ids=["unchanged", "changed", "extra"])
def test_verify_compares_every_published_asset(work, monkeypatch, change):
    assets = assemble(work)

    def download(tag, names, directory):
        assert names == []
        for path in assets.iterdir():
            shutil.copyfile(path, directory / path.name)
        if change == "changed":
            (directory / "README.md").write_text("changed")
        elif change == "extra":
            (directory / "stray.txt").write_text("stray")

    monkeypatch.setattr(release.layout, "download", download)
    if change:
        with pytest.raises(release.Rejected):
            release.verify(TAG, work)
    else:
        release.verify(TAG, work)


def test_main_maps_arguments_and_reports_rejection(work, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(release, "read", lambda *arguments: calls.append(arguments))
    assert release.main(["read", str(work), "--capture-checkout", "c", "--java17", "j17", "--java21", "j21"]) == 0
    assert calls[-1] == (work, Path("c").absolute(), "j17", "j21")
    assert release.main(["review", str(work)]) == 1
    assert "baseline-release: Publication review already exists" in capsys.readouterr().err
