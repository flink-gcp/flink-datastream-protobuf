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

import contextlib
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
import tempfile
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import baseline_fetch as fetcher
import baseline_release as release

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


class ReleaseTest(unittest.TestCase):

    def setUp(self):
        self.temporary = Path(tempfile.mkdtemp()).resolve()
        self.addCleanup(shutil.rmtree, self.temporary)
        self.work = self.temporary / "work"
        for bundle in release.BUNDLES:
            capture_bundle(self.work / "bundles" / bundle, bundle)
        reader_outputs(self.work)
        write(self.work / "README.md", "# %s\n\nCommit %s, tag object %s.\n" % (TAG, REVISION, TAG_OBJECT))
        reviewed(self.work)
        self.checked = []
        self.ci = json.loads(json.dumps(CI))
        self.freeze_log = FREEZE_LOG
        self.patchers = [patch.object(release, "tag_identity", lambda tag: (TAG_OBJECT, REVISION)),
                         patch.object(release, "github_run", lambda run_id: self.ci),
                         patch.object(release, "job_log", lambda run_id, job: self.freeze_log if job == 7 else ""),
                         patch.object(release.fetch, "baseline_check", self.checked.append)]
        for patcher in self.patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def assemble(self):
        with contextlib.redirect_stdout(io.StringIO()):
            release.assemble(TAG, self.work, "1")
        return self.work / "release"

    def assertRejected(self, message):
        with self.assertRaises(release.Rejected) as raised:
            self.assemble()
        self.assertIn(message, str(raised.exception))
        self.assertFalse((self.work / "release").exists())
        self.assertEqual([], list(self.work.glob(".release.*")))

    def test_assembled_release_passes_the_retrieval_checks(self):
        assets = self.assemble()
        self.assertEqual(sorted(["FINAL-BASELINE.json", "INVENTORY.json", "README.md", "SHA256SUMS",
                                 TAG + "-verification.tar.gz"] + [TAG + "-" + b + ".tar.gz" for b in release.BUNDLES]),
                         sorted(path.name for path in assets.iterdir()))
        self.assertEqual([self.work / "bundles" / b for b in release.BUNDLES], self.checked)
        final = json.loads((assets / "FINAL-BASELINE.json").read_text())
        self.assertEqual((TAG, REVISION, TAG_OBJECT, "0.9.0-SNAPSHOT", "https://example.org/runs/1"),
                         (final["milestone"], final["source_revision"], final["annotated_tag_object"],
                          final["embedded_artifact_version"], final["ci_url"]))
        # The manifest is recorded as written, with the properties escape kept, as in v0.1.0.
        self.assertIn("\\:", final["bundles"][0]["manifest"]["writer.artifact"])
        self.assertEqual(10, len(final["readers"]))
        self.assertEqual({7}, {bundle["clean_verify_tests"] for bundle in final["bundles"]})
        pin = digest((assets / "SHA256SUMS").read_bytes())

        def download(tag, names, directory):
            for name in names:
                shutil.copyfile(assets / name, Path(directory) / name)

        output = self.temporary / "fetched"
        with patch.dict(fetcher.RELEASES, {TAG: pin}), patch.object(fetcher, "download", download), \
                patch.object(fetcher, "baseline_check", lambda bundle: None), \
                contextlib.redirect_stdout(io.StringIO()):
            fetcher.fetch(TAG, str(output), [])
        for bundle in release.BUNDLES:
            self.assertEqual((self.work / "bundles" / bundle / "SHA256SUMS").read_bytes(),
                             (output / bundle / "SHA256SUMS").read_bytes())
        with tarfile.open(assets / (TAG + "-verification.tar.gz")) as tar:
            self.assertTrue({"verification/ci.json", "verification/PUBLICATION-REVIEW.json",
                             "verification/readers.json", "verification/audit-readers.json",
                             "verification/audit-flink1-pb4.json"} <= set(tar.getnames()))
            ci = json.load(tar.extractfile("verification/ci.json"))
        self.assertNotIn("extra", ci["jobs"][0])

    def test_review_must_explain_exactly_the_current_findings(self):
        record = reviewed(self.work)
        write(self.work / release.REVIEW, json.dumps(dict(record, unresolved_matches=[{"path": "x", "kind": "k"}])))
        self.assertRejected("Publication review has flagged files or unresolved matches")
        write(self.work / release.REVIEW, json.dumps(dict(record, raw_jvm_diagnostics_or_credential_files=["x"])))
        self.assertRejected("Publication review has flagged files or unresolved matches")
        reviewed(self.work, disposition=" ")
        self.assertRejected("needs a disposition")
        # Deleting findings instead of explaining them, or a review older than the files, is rejected.
        write(self.work / release.REVIEW, json.dumps(dict(record, reviewed_matches=record["reviewed_matches"][1:])))
        self.assertRejected("does not match the current files")
        reviewed(self.work)
        write(self.work / "verification" / "read-flink2-pb3-2.2.1-jdk17" / "runtime.log", "password=hunter22")
        self.assertRejected("does not match the current files")
        # A reviewed finding whose bytes change keeps its path and rule but not its identity.
        write(self.work / "verification" / "read-flink2-pb3-2.2.1-jdk17" / "runtime.log", "password=synthetic")
        reviewed(self.work)
        write(self.work / "verification" / "read-flink2-pb3-2.2.1-jdk17" / "runtime.log",
              "password=synthetic\npassword=hunter22")
        self.assertRejected("does not match the current files")
        (self.work / "verification" / "read-flink2-pb3-2.2.1-jdk17" / "runtime.log").unlink()
        reviewed(self.work)
        for name in ("hs_err_pid42.log", "recording.jfr", "heap.hprof.gz"):
            with self.subTest(name=name):
                write(self.work / "verification" / name, "dump")
                self.assertRejected("Sensitive files do not match the reviewed files")
                (self.work / "verification" / name).unlink()
        for disposition in ("Synthetic value password=hunter22", 'Quoted {"password":"hunter22"}'):
            with self.subTest(disposition=disposition):
                reviewed(self.work, disposition=disposition)
                self.assertRejected("README.md or a disposition matches a credential rule")
        reviewed(self.work)
        write(self.work / "README.md", "# %s\n\nCommit %s, tag object %s.\ntoken=hunter22\n" % (TAG, REVISION, TAG_OBJECT))
        self.assertRejected("README.md or a disposition matches a credential rule")

    def test_readers_must_cover_every_claimed_combination_with_the_retained_jars(self):
        for key, value in (("library.sha256", "c" * 64), ("consumer.sha256", "c" * 64), ("case.count", "13"),
                           ("java.version", "17.0.20+8"), ("status", "failed"), ("flink.version", "2.2.1")):
            with self.subTest(key=key):
                reader_outputs(self.work, {"read-flink2-pb4-2.3.0-jdk21": {key: value}})
                reviewed(self.work)
                self.assertRejected("Reader result does not match its retained bundle: read-flink2-pb4-2.3.0-jdk21")
        record = reader_outputs(self.work)
        record["outcomes"] = [o for o in record["outcomes"] if o["name"] != "read-flink2-pb3-2.3.0-jdk17"]
        write(self.work / "verification" / "readers.json", json.dumps(record))
        reviewed(self.work)
        self.assertRejected("Missing or failed reader: read-flink2-pb3-2.3.0-jdk17")
        reader_outputs(self.work, head="c" * 40)
        reviewed(self.work)
        self.assertRejected("build output of the tagged capture checkout")

    def test_negative_controls_must_fail_with_their_message_before_creating_output(self):
        for change in ("exit", "output_created", "directory", "message"):
            with self.subTest(change=change):
                record = reader_outputs(self.work)
                control = record["outcomes"][-1]
                if change == "exit":
                    control["exit"] = 0
                elif change == "output_created":
                    control["output_created"] = True
                elif change == "directory":
                    (self.work / "verification" / control["name"]).mkdir()
                else:
                    write(self.work / "verification" / (control["name"] + ".log"), "Error: could not start the JVM")
                write(self.work / "verification" / "readers.json", json.dumps(record))
                reviewed(self.work)
                self.assertRejected("Negative control did not reject with its message before creating output: "
                                    "negative-lts-jdk21")

    def test_bundles_must_be_development_baselines_of_the_tagged_revision(self):
        for manifest in ({"source.revision": "c" * 40}, {"milestone.final": "true"}, {"status": "other"}):
            with self.subTest(manifest=manifest):
                capture_bundle(self.work / "bundles" / "flink1-pb4", "flink1-pb4", **manifest)
                reviewed(self.work)
                self.assertRejected("Bundle was not captured as a development baseline of " + REVISION)
        capture_bundle(self.work / "bundles" / "flink1-pb4", "flink1-pb4", **{"protobuf.major": "3"})
        reviewed(self.work)
        self.assertRejected("Bundle name does not match its artifact line and Protobuf profile: flink1-pb4")
        capture_bundle(self.work / "bundles" / "flink1-pb4", "flink1-pb4")
        write(self.work / "bundles" / "flink1-pb4" / "source.zip", b"other source")
        seal(self.work / "bundles" / "flink1-pb4")
        reviewed(self.work)
        self.assertRejected("different source archives")

    def test_ci_must_be_the_successful_main_branch_push_run_of_the_tagged_commit(self):
        for key, value in (("headSha", "c" * 40), ("conclusion", "failure"), ("workflowName", "Dependency submission"),
                           ("event", "workflow_dispatch"), ("headBranch", "other"),
                           ("jobs", [{"conclusion": "failure", "name": "CI passed", "url": "u"}])):
            with self.subTest(key=key):
                self.ci = dict(CI, **{key: value})
                self.assertRejected("is not a successful main-branch CI push run of " + REVISION)
        self.ci = json.loads(json.dumps(CI))
        # A push run tests the branch tip at its start, which can be a later commit.
        self.freeze_log = FREEZE_LOG.replace(REVISION, "c" * 40)
        self.assertRejected("did not test " + REVISION)
        self.freeze_log = ""
        self.assertRejected("did not test " + REVISION)
        self.ci["jobs"] = self.ci["jobs"][1:]
        self.assertRejected("has no single source-freezing job")

    def test_readme_and_tag_must_name_the_release(self):
        write(self.work / "README.md", "# " + TAG + "\n")
        self.assertRejected("README.md must name")
        with self.assertRaises(release.Rejected) as raised:
            release.assemble("v1.0.0", self.work, "1")
        self.assertIn("Expected a development milestone tag", str(raised.exception))

    def test_capture_builds_must_report_passing_tests(self):
        log = self.work / "bundles" / "flink2-pb3" / "build.log"
        for text in (BUILD_LOG + "[ERROR] Tests run: 9, Failures: 0, Errors: 1, Skipped: 0\n",
                     BUILD_LOG + "[WARNING] Tests run: 9, Failures: 0, Errors: 0, Skipped: 2\n",
                     BUILD_LOG.replace("BUILD SUCCESS", "BUILD FAILURE")):
            with self.subTest(text=text[-40:]):
                log.write_text(text)
                seal(log.parent)
                reviewed(self.work)
                self.assertRejected("does not report a successful build")
        log.write_text("[INFO] BUILD SUCCESS\n")
        seal(log.parent)
        reviewed(self.work)
        self.assertRejected("reports no tests")

    def test_release_output_is_never_replaced_or_left_partial(self):
        (self.work / "release").mkdir()
        with self.assertRaises(release.Rejected):
            self.assemble()
        (self.work / "release").rmdir()

        def fail(*arguments):
            raise OSError("disk full")

        with patch.object(release, "evidence_directory", fail):
            with self.assertRaises(OSError):
                self.assemble()
        self.assertFalse((self.work / "release").exists())
        self.assertEqual([], list(self.work.glob(".release.*")))
        # An empty output created during assembly is not silently replaced by the rename.
        original = release.evidence_directory

        def racing(*arguments):
            original(*arguments)
            (self.work / "release").mkdir()

        with patch.object(release, "evidence_directory", racing):
            with self.assertRaises(release.Rejected) as raised:
                self.assemble()
        self.assertIn("appeared during assembly", str(raised.exception))
        self.assertEqual([], list((self.work / "release").iterdir()))

    def test_inputs_must_be_retrievable_and_contain_no_links(self):
        (self.work / "bundles" / "flink2-pb3" / "notes copy").mkdir()
        reviewed(self.work)
        self.assertRejected("Unsafe path")
        (self.work / "bundles" / "flink2-pb3" / "notes copy").rmdir()
        (self.work / "verification" / "ci.json").symlink_to(self.work / "bundles" / "flink2-pb3" / "build.log")
        with self.assertRaises(release.Rejected) as raised:
            reviewed(self.work)
        self.assertIn("Symbolic links are not retained inputs", str(raised.exception))
        self.assertIn("BUILD SUCCESS", (self.work / "bundles" / "flink2-pb3" / "build.log").read_text())
        (self.work / "verification" / "ci.json").unlink()
        reviewed(self.work)
        (self.work / "verification").rename(self.temporary / "elsewhere")
        (self.work / "verification").symlink_to(self.temporary / "elsewhere")
        self.assertRejected("Run read before review")
        (self.work / "verification").unlink()
        (self.temporary / "elsewhere").rename(self.work / "verification")
        (self.work / "README.md").rename(self.temporary / "README.md")
        (self.work / "README.md").symlink_to(self.temporary / "README.md")
        self.assertRejected("must be files in the work directory")

    def test_tag_identity_requires_an_annotated_tag(self):
        repository = self.temporary / "repository"
        repository.mkdir()
        git(repository, "init", "-q")
        git(repository, "commit", "-q", "--allow-empty", "-m", "c")
        git(repository, "tag", "lightweight")
        git(repository, "tag", "-a", "annotated", "-m", "t")
        for patcher in self.patchers:
            patcher.stop()
        self.patchers = []
        self.assertEqual((git(repository, "rev-parse", "annotated"), git(repository, "rev-parse", "HEAD")),
                         release.tag_identity("annotated", repository))
        with self.assertRaises(release.Rejected) as raised:
            release.tag_identity("lightweight", repository)
        self.assertIn("not annotated", str(raised.exception))

    def test_scan_searches_nested_archives_and_reports_what_it_cannot_search(self):
        flagged, findings = release.scan(self.work)
        self.assertEqual([], flagged)
        self.assertEqual({(b + "/dependencies/flink.jar:lib/netty.jar:io/netty/OpenSsl.class", "private-key",
                           digest(b"-----BEGIN PRIVATE KEY-----")) for b in release.BUNDLES},
                         {(f["path"], f["kind"], f["sha256"]) for f in findings})
        self.assertEqual(release.sibling("benchmark_evidence").CREDENTIAL_RULES.keys(),
                         release.CREDENTIAL_RULES.keys())
        nested = b"password=hunter22"
        for _ in range(release.MAX_ARCHIVE_DEPTH + 1):
            nested = jar({"inner.jar": nested})
        write(self.work / "bundles" / "flink2-pb3" / "deep.jar", nested)
        write(self.work / "verification" / "key.pem", "x")
        flagged, findings = release.scan(self.work)
        self.assertEqual(["verification/key.pem"], [f["path"] for f in flagged])
        self.assertIn("archive-too-deep", {f["kind"] for f in findings if f["path"].startswith("flink2-pb3/deep.jar")})

    def test_scan_searches_bytes_outside_members_and_inside_streams(self):
        findings = []
        member = jar({"a.properties": b"password=hunter22"})
        launcher = b"#!/bin/sh\ntoken=hunter22\n" + member
        found = dict(release.members("launcher", launcher, findings))
        self.assertIn(b"token=hunter22", found["launcher"])
        self.assertEqual(b"password=hunter22", found["launcher:a.properties"])
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("a.class", b"x")
            archive.comment = b"token=hunter22"
        found = dict(release.members("c.jar", buffer.getvalue(), findings))
        self.assertIn(b"token=hunter22", found["c.jar"])
        self.assertEqual(b"x", found["c.jar:a.class"])
        body = b"password=hunter22"
        log = gzip.compress(body)
        for tar_format in (tarfile.GNU_FORMAT, tarfile.USTAR_FORMAT, tarfile.PAX_FORMAT):
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
            with self.subTest(tar_format=tar_format):
                found = dict(release.members("e.tar.gz", gzip.compress(data), findings))
                self.assertEqual(body, found["e.tar.gz:e.tar:evidence/run.log.gz:run.log"])
        self.assertEqual([], findings)

    def test_scan_handles_polyglot_truncated_and_trailing_archives(self):
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
        self.assertEqual(body, found["x.tar:run.log.gz:run.log"])
        found = dict(release.members("y.tar", archive + b"!", findings))
        self.assertEqual(body, found["y.tar:run.log.gz:run.log"])
        self.assertIn(b"!", found["y.tar"])
        self.assertEqual([], findings)
        # Other data of a tar archive's size is searched as itself, without an unreadable finding.
        # Its checksum field happens to hold octal digits that do not match the header.
        text = b"A" * 148 + b"0000123 " + b"A" * (1024 - 156)
        self.assertEqual([("log.txt", text)], list(release.members("log.txt", text, findings)))
        self.assertEqual([], findings)
        truncated = tar_bytes([("a.log", body), ("b.log", b"x" * 2048)])[:512 * 4]
        list(release.members("t.tar", truncated, findings))
        self.assertEqual([("t.tar", "unreadable-archive")], [(f["path"], f["kind"]) for f in findings])

    def test_scan_searches_zip_bytes_except_member_contents(self):
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
            self.assertIn(expected, outside)
        self.assertNotIn(b"FIRSTBODY", outside)
        self.assertNotIn(b"SECONDBODY", outside)
        self.assertEqual(b"FIRSTBODY", found["z.zip:first.txt"])

    def test_scan_does_not_open_archives_past_the_depth_limit(self):
        opened = []
        nested = gzip.compress(b"x")
        for _ in range(release.MAX_ARCHIVE_DEPTH):
            nested = jar({"inner": nested})
        findings = []
        with patch.object(release, "STREAMS", ((b"\x1f\x8b", lambda data: opened.append(data) or b""),)):
            list(release.members("deep.jar", nested, findings))
        self.assertEqual([], opened)
        self.assertEqual(["archive-too-deep"], [f["kind"] for f in findings])

    def test_scan_identifies_unreadable_archives_by_their_bytes(self):
        broken = b"PK\x03\x04" + b"\x00" * 40
        findings = []
        self.assertEqual([("b.jar", broken)], list(release.members("b.jar", broken, findings)))
        self.assertEqual([("b.jar", "unreadable-archive", digest(broken))],
                         [(f["path"], f["kind"], f["sha256"]) for f in findings])
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("a.class", b"data")
        data = bytearray(buffer.getvalue())
        data[8:10] = (99).to_bytes(2, "little")  # an unsupported compression method in the local header
        central = data.rfind(b"PK\x01\x02")
        data[central + 10:central + 12] = (99).to_bytes(2, "little")
        findings = []
        self.assertEqual(["x.jar"], [name for name, _ in release.members("x.jar", bytes(data), findings)])
        # A replaced payload under the same member name changes the container, and so the finding.
        self.assertEqual([("x.jar:a.class", "unreadable-archive-member", digest(bytes(data)))],
                         [(f["path"], f["kind"], f["sha256"]) for f in findings])

    def source_settings(self, data=b"<settings/>"):
        """Puts a tracked CI Maven settings file into the source archive that every bundle retains."""
        source = jar({".github/maven/settings.xml": data})
        for bundle in release.BUNDLES:
            write(self.work / "bundles" / bundle / "source.zip", source)
            seal(self.work / "bundles" / bundle)

    def test_reviewed_source_file_is_accepted_only_with_its_bytes(self):
        # A tracked repository file can carry a sensitive name without a secret.
        self.source_settings()
        record = reviewed(self.work)
        self.assertRejected("Sensitive files do not match the reviewed files: flink2-pb3/source.zip:.github/maven/settings.xml")
        flagged, _ = release.scan(self.work)
        self.assertEqual({(bundle + "/source.zip:.github/maven/settings.xml", "sensitive-name", digest(b"<settings/>"))
                          for bundle in release.BUNDLES},
                         {(f["path"], f["kind"], f["sha256"]) for f in flagged})
        files = [dict(f, disposition="Repository CI settings without credentials") for f in flagged]
        write(self.work / release.REVIEW, json.dumps(dict(record, reviewed_files=files[:-1] + [dict(files[-1], disposition=" ")])))
        self.assertRejected("needs a disposition")
        write(self.work / release.REVIEW, json.dumps(dict(record, reviewed_files=files[1:])))
        self.assertRejected("Sensitive files do not match the reviewed files")
        write(self.work / release.REVIEW, json.dumps(dict(record, reviewed_files=files)))
        self.assemble()
        self.assertTrue((self.work / "release").is_dir())

    def test_reviewed_source_file_with_changed_bytes_is_rejected(self):
        self.source_settings()
        record = reviewed(self.work)
        flagged, _ = release.scan(self.work)
        files = [dict(f, disposition="Repository CI settings without credentials") for f in flagged]
        self.source_settings(b"<settings><servers/></settings>")
        write(self.work / release.REVIEW, json.dumps(dict(reviewed(self.work), reviewed_files=files)))
        self.assertRejected("Sensitive files do not match the reviewed files")
        self.assertNotEqual(record["reviewed_matches"], [])

    def test_a_flagged_file_outside_bundle_source_archives_cannot_be_reviewed(self):
        # Neither a reader output nor an archive in the reader outputs named like a source archive qualifies.
        for name, data in (("settings.xml", b"<settings/>"), ("source.zip", jar({"recording.jfr": b"x"}))):
            with self.subTest(name):
                write(self.work / "verification" / name, data)
                record = reviewed(self.work)
                flagged, _ = release.scan(self.work)
                files = [dict(f, disposition="Reader output named like a credential file") for f in flagged]
                write(self.work / release.REVIEW, json.dumps(dict(record, reviewed_files=files)))
                self.assertRejected("Only members of a bundle's source archive can be reviewed files")
                (self.work / "verification" / name).unlink()

    def test_an_unreadable_sensitive_member_is_flagged(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
            archive.writestr("recording.jfr", b"x" * 64)
        data = bytearray(buffer.getvalue())
        # Corrupting the stored content fails its CRC, so the member cannot be read.
        data[data.index(b"x" * 64)] ^= 1
        write(self.work / "verification" / "diagnostics.zip", bytes(data))
        flagged, _ = release.scan(self.work)
        self.assertEqual([("verification/diagnostics.zip:recording.jfr", "sensitive-name", digest(bytes(data)))],
                         [(f["path"], f["kind"], f["sha256"]) for f in flagged])

    def test_a_flagged_archive_is_identified_by_its_contents(self):
        # A same-size change inside a flagged archive changes its identity, not only its outside bytes.
        def archive(payload):
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w") as tar:
                info = tarfile.TarInfo("payload.bin")
                info.size = len(payload)
                tar.addfile(info, io.BytesIO(payload))
            return buffer.getvalue()
        first, second = archive(b"aaaa"), archive(b"bbbb")
        write(self.work / "verification" / "core.tar", first)
        identity, _ = release.scan(self.work)
        write(self.work / "verification" / "core.tar", second)
        changed, _ = release.scan(self.work)
        self.assertEqual([("verification/core.tar", digest(first))], [(f["path"], f["sha256"]) for f in identity])
        self.assertEqual([("verification/core.tar", digest(second))], [(f["path"], f["sha256"]) for f in changed])

    def test_scan_flags_diagnostics_inside_containers_and_compression(self):
        write(self.work / "verification" / "core.42.zip", jar({"payload.bin": b"x"}))
        write(self.work / "verification" / "recording.jfr.gz", gzip.compress(b"x"))
        flagged, _ = release.scan(self.work)
        self.assertEqual({"verification/core.42.zip", "verification/recording.jfr.gz:recording.jfr"},
                         {f["path"] for f in flagged})

    def test_review_writes_every_finding_unresolved_once(self):
        (self.work / release.REVIEW).unlink()
        with contextlib.redirect_stdout(io.StringIO()):
            release.review(self.work)
        record = json.loads((self.work / release.REVIEW).read_text())
        self.assertEqual(4, len(record["unresolved_matches"]))
        self.assertEqual([], record["reviewed_matches"])
        self.assertEqual([], record["reviewed_files"])
        self.assertNotIn("BEGIN", json.dumps(record))
        with self.assertRaises(release.Rejected):
            release.review(self.work)

    def checkout(self):
        checkout = self.temporary / "capture"
        checkout.mkdir()
        git(checkout, "init", "-q")
        git(checkout, "commit", "-q", "--allow-empty", "-m", "c")
        return checkout, git(checkout, "rev-parse", "HEAD")

    def test_read_runs_every_claimed_combination_and_both_controls(self):
        shutil.rmtree(self.work / "verification")
        checkout, head = self.checkout()
        commands = []

        def run(command, cwd, log):
            commands.append(command)
            self.assertEqual(self.work, cwd)
            name = Path(command[-1]).name
            if name in REJECTIONS:
                Path(log).write_text(REJECTIONS[name])
                return 1
            Path(command[-1]).mkdir()
            Path(log).write_text("")
            return 0

        with patch.object(release, "run", run), contextlib.redirect_stdout(io.StringIO()):
            release.read(self.work, checkout, "java17", "java21")
        self.assertEqual(12, len(commands))
        self.assertEqual({"read-flink2-pb3-2.2.1-jdk21", "read-flink2-pb3-2.3.0-jdk21", "read-flink2-pb4-2.2.1-jdk21",
                          "read-flink2-pb4-2.3.0-jdk21", "negative-lts-jdk21"},
                         {Path(c[-1]).name for c in commands if c[0] == "java21"})
        self.assertEqual(head, json.loads((self.work / "verification" / "readers.json").read_text())
                         ["capture_checkout"]["head"])
        (checkout / "target").mkdir()
        shutil.rmtree(self.work / "verification")
        with self.assertRaises(release.Rejected) as raised:
            release.read(self.work, checkout, "java17", "java21")
        self.assertIn("Move the capture build output", str(raised.exception))

    def test_read_rejects_controls_that_pass_or_create_output_and_failed_readers(self):
        checkout, _ = self.checkout()
        for mode, message in (("control-passes", "negative-unsupported-flink"),
                              ("control-creates-output", "negative-unsupported-flink"),
                              ("reader-fails", "Failed reader: read-flink1-pb4-1.20.4-jdk17")):
            with self.subTest(mode=mode):
                shutil.rmtree(self.work / "verification", ignore_errors=True)

                def run(command, cwd, log):
                    name = Path(command[-1]).name
                    if name in REJECTIONS:
                        Path(log).write_text(REJECTIONS[name])
                        if mode == "control-creates-output":
                            Path(command[-1]).mkdir()
                        return 0 if mode == "control-passes" else 1
                    Path(command[-1]).mkdir()
                    return 1 if mode == "reader-fails" and name == "read-flink1-pb4-1.20.4-jdk17" else 0

                with patch.object(release, "run", run), contextlib.redirect_stdout(io.StringIO()), \
                        self.assertRaises(release.Rejected) as raised:
                    release.read(self.work, checkout, "java17", "java21")
                self.assertIn(message, str(raised.exception))

    def test_run_removes_credentials_from_the_instrument_environment(self):
        log = self.temporary / "environment.log"
        with patch.dict(os.environ, {"GH_TOKEN": "secret", "KEEP": "kept"}):
            status = release.run([sys.executable, "-c", "import os; print(os.environ.get('GH_TOKEN'), "
                                  "os.environ.get('KEEP'))"], self.temporary, log)
        self.assertEqual(0, status)
        self.assertEqual("None kept", log.read_text().strip())

    def test_verify_compares_every_published_asset(self):
        assets = self.assemble()
        for change, rejected in ((None, False), ("changed", True), ("extra", True)):
            with self.subTest(change=change):
                def download(tag, names, directory):
                    self.assertEqual([], names)
                    for path in assets.iterdir():
                        shutil.copyfile(path, directory / path.name)
                    if change == "changed":
                        (directory / "README.md").write_text("changed")
                    elif change == "extra":
                        (directory / "stray.txt").write_text("stray")

                with patch.object(release.fetch, "download", download), contextlib.redirect_stdout(io.StringIO()):
                    if rejected:
                        with self.assertRaises(release.Rejected):
                            release.verify(TAG, self.work)
                    else:
                        release.verify(TAG, self.work)

    def test_main_maps_arguments_and_reports_rejection(self):
        with patch.object(release, "read") as read:
            self.assertEqual(0, release.main(["read", str(self.work), "--capture-checkout", "c",
                                              "--java17", "j17", "--java21", "j21"]))
        self.assertEqual((self.work, Path("c").absolute(), "j17", "j21"), read.call_args.args)
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            self.assertEqual(1, release.main(["review", str(self.work)]))
        self.assertIn("baseline-release: Publication review already exists", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
