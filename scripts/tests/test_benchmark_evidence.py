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

"""Synthetic evidence only: never load a recording, credential, or retained measurement."""

import contextlib
import gzip
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import benchmark_evidence as gate

RESULT = "PROTOBUF_NATIVE-scalar-serialize-0.json"
EVENTS = "PROTOBUF_NATIVE-scalar-serialize-allocation-execution-events.json"
COMMAND = "PROTOBUF_NATIVE-scalar-serialize-command.json"


def encoded(value):
    return json.dumps(value).encode()


def metric():
    return {"score": 1.0, "scoreError": "NaN", "scoreConfidence": ["NaN", "NaN"],
            "scorePercentiles": {"0.0": 1, "100.0": 1}, "scoreUnit": "ns/op", "rawData": [[1]]}


def jmh():
    result = {key: "synthetic" for key in "jmhVersion benchmark mode jvm jdkVersion vmName vmVersion warmupTime measurementTime".split()}
    result.update({key: 1 for key in "threads forks warmupIterations warmupBatchSize measurementIterations measurementBatchSize".split()})
    result.update(jvmArgs=[], params={"deterministic": "false", "lane": "PROTOBUF_NATIVE", "workload": "scalar"},
                  primaryMetric=metric(), secondaryMetrics={"gc.count": metric()})
    return [result]


def events():
    thread = {"osName": "worker", "osThreadId": 1, "javaName": "worker", "javaThreadId": 1,
              "group": {"name": "main", "parent": None}}
    klass = {"classLoader": {"type": None, "name": "app"}, "name": "example.Record",
             "package": {"name": "example", "module": {"name": None, "version": None,
                         "location": None, "classLoader": None}, "exported": True},
             "modifiers": 1, "hidden": False}
    stack = {"truncated": False, "frames": [{"method": {"type": klass, "name": "run",
             "descriptor": "()V", "modifiers": 1, "hidden": False}, "lineNumber": 1,
             "bytecodeIndex": 0, "type": "JIT compiled"}]}
    return {"recording": {"events": [
        {"type": "jdk.ObjectAllocationSample", "values": {"startTime": "2026-01-01T00:00:00Z",
         "eventThread": thread, "stackTrace": stack, "objectClass": klass, "weight": 1}},
        {"type": "jdk.ExecutionSample", "values": {"startTime": "2026-01-01T00:00:00Z",
         "sampledThread": thread, "stackTrace": stack, "state": "STATE_RUNNABLE"}}]}}


def archive(entries, mutate=None):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for name, data in entries:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            if mutate:
                mutate(info)
            tar.addfile(info, io.BytesIO(data))
    return gzip.compress(stream.getvalue(), mtime=0)


class EvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def rejected(self, data, rule=None):
        with self.assertRaises(gate.Rejected) as caught:
            gate.Inventory().archive(data)
        if rule:
            self.assertEqual(rule, str(caught.exception))

    def test_supported_formats(self):
        summary = {"PROTOBUF_NATIVE-scalar-serialize": {
            "allocation_sample_weight_top": [["example.Record", 1]],
            "execution_sample_top": [["example.Record.run", 1]],
            "allocation_stack_examples": {"example.Record": ["example.Record.run"]}}}
        cases = [(RESULT, encoded(jmh())), (EVENTS, encoded(events())),
                 (COMMAND, encoded(["java", "-Xmx256m"])),
                 ("allocation-and-execution-samples.json", encoded(summary)),
                 ("manifest.properties", b"java.version=17\nsource.status=\n"),
                 ("PROTOBUF_CHILL-opened.properties", b"lane=PROTOBUF_CHILL\nstatus=unsupported\nreason=synthetic\n"),
                 ("measurement-source.patch", b"--- a/source\n+++ b/source\n"),
                 ("console.log", b"Incomplete benchmark; synthetic failure\n")]
        cases += [(name, (header + "\n").encode()) for name, header in gate.CSV_HEADERS.items()]
        inventory = gate.Inventory()
        inventory.archive(archive(cases))
        self.assertEqual(len(cases), len(inventory.members))
        for item, (name, data) in zip(inventory.members, cases):
            self.assertEqual((name, len(data), gate.digest(data)), (item["path"], item["bytes"], item["sha256"]))

    def test_jfr_rejects_other_events_and_metadata_at_every_level(self):
        for event_type in ("jdk.InitialEnvironmentVariable", "jdk.InitialSystemProperty", "jdk.JVMInformation"):
            value = events()
            value["recording"]["events"][0]["type"] = event_type
            with self.subTest(event=event_type):
                self.rejected(archive([(EVENTS, encoded(value))]), "jfr-event")
        for location in (lambda v: v, lambda v: v["recording"],
                         lambda v: v["recording"]["events"][0]["values"],
                         lambda v: v["recording"]["events"][0]["values"]["objectClass"]["classLoader"]):
            value = events()
            location(value)["environment"] = "synthetic metadata"
            self.rejected(archive([(EVENTS, encoded(value))]), "json-fields")
        value = events()
        value["recording"]["events"][0]["values"]["weight"] = "1"
        self.rejected(archive([(EVENTS, encoded(value))]), "json-type")
        self.rejected(archive([("console.log", encoded(events()))]), "misnamed-jfr")

    def test_compressed_secrets_and_decoded_escapes(self):
        token = "ghp_" + "SyntheticSecret" * 3
        cases = [("console.log", ("token " + token).encode()),
                 ("invocation.txt", b"password=synthetic-password"),
                 ("manifest.properties", b"java.version=password\\u003dsynthetic-password"),
                 (COMMAND, ('["' + token.replace("g", "\\u0067", 1) + '"]').encode())]
        cases.append((COMMAND, b'[ {"pass\\u0077ord": "synthetic-password"} ]'))
        value = events()
        value["recording"]["events"][0]["values"]["eventThread"]["javaName"] = token
        cases.append((EVENTS, encoded(value)))
        for name, data in cases:
            with self.subTest(name=name):
                with self.assertRaises(gate.Rejected) as caught:
                    gate.Inventory().archive(archive([(name, data)]))
                self.assertTrue(str(caught.exception).startswith("credential-"))
                self.assertNotIn(token, str(caught.exception))

    def test_each_credential_rule_has_a_synthetic_positive_control(self):
        values = ["-----BEGIN PRIVATE KEY-----", "github_pat_" + "a" * 30,
                  "AKIA" + "A" * 16, "AIza" + "A" * 35, "ya29." + "a" * 30,
                  "xoxb-" + "1" * 20, "Authorization: Bearer syntheticvalue",
                  "client_secret=synthetic-value", "AWS_SECRET_ACCESS_KEY=synthetic-value",
                  "ANTHROPIC_API_KEY=synthetic-value", "-Dpassword=synthetic-value"]
        for value in values:
            with self.subTest(rule=value[:8]), self.assertRaises(gate.Rejected):
                gate.scan(value)

    def test_unsafe_paths_duplicates_and_collisions(self):
        for name in ("/console.log", "../console.log", "a/../console.log", "./console.log",
                     "a//console.log", "C:/console.log", "a\\console.log", "console.log\n", "console.log/child"):
            with self.subTest(name=name):
                self.rejected(archive([(name, b"safe")]))
        self.rejected(archive([("console.log", b"a"), ("console.log", b"b")]), "duplicate-member")
        inventory = gate.Inventory()
        inventory.add("console.log", b"safe")
        with self.assertRaisesRegex(gate.Rejected, "member-path-collision"):
            inventory.add("console.log/child", b"safe")

    def test_binary_and_nested_archives_even_with_text_names(self):
        for data in (b"FLR\x00recording", b"JAVA PROFILE 1.0\x00", b"PK\x03\x04zip",
                     b"\x7fELFbinary", b"\x00binary", b"\xffinvalid utf8",
                     archive([("console.log", b"safe")])):
            with self.subTest(magic=data[:4]):
                source = self.root / "input.tar.gz"
                source.write_bytes(archive([("console.log", data)]))
                with contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(1, gate.main(["check", str(source), str(self.root / "out")]))
        self.rejected(archive([("unknown.bin", b"innocent text")]), "member-inventory")
        self.rejected(archive([("unexpected.log", b"innocent text")]), "member-inventory")

    def test_archive_metadata_and_special_members(self):
        for attribute, value in (("uid", 1), ("uname", "synthetic"), ("mtime", 1),
                                 ("type", tarfile.SYMTYPE), ("type", tarfile.LNKTYPE),
                                 ("type", tarfile.FIFOTYPE), ("type", tarfile.XHDTYPE)):
            with self.subTest(attribute=attribute, value=value):
                self.rejected(archive([("console.log", b"safe")], lambda info: setattr(info, attribute, value)))
        data = bytearray(archive([("console.log", b"safe")]))
        data[4] = 1
        self.rejected(bytes(data), "gzip-header")
        self.rejected(gzip.compress(gzip.decompress(data), mtime=1), "gzip-header")

    def test_truncation_extra_streams_and_tar_trailers(self):
        data = archive([("console.log", b"safe")])
        for value in (data[:-1], data + b"extra", data + data):
            self.rejected(value, "gzip-stream")
        raw = gzip.decompress(data)
        self.rejected(gzip.compress(raw[:1024], mtime=0), "tar-trailer")
        self.rejected(gzip.compress(raw + bytes(511) + b"x", mtime=0), "tar-trailer")
        self.rejected(gzip.compress(raw[:-1], mtime=0), "tar-alignment")
        changed = bytearray(raw)
        changed[512 + len(b"safe")] = 1
        self.rejected(gzip.compress(changed, mtime=0), "tar-padding")

    def test_json_csv_and_properties_fail_closed(self):
        cases = [(RESULT, b"[{"), (RESULT, b'[{"x":1,"x":2}]'),
                 (RESULT, b"[NaN]"), (RESULT, b"[1e9999]"),
                 (EVENTS, encoded({"recording": {"events": None}})),
                 ("measurements.csv", b"unexpected,header\n"),
                 ("manifest.properties", b"unknown=synthetic\n"),
                 ("manifest.properties", b"java.version=17\njava.version=21\n")]
        for name, data in cases:
            with self.subTest(name=name):
                source = self.root / "input.tar.gz"
                source.write_bytes(archive([(name, data)]))
                with contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(1, gate.main(["check", str(source), str(self.root / "out")]))
        value = jmh()
        value[0]["systemProperties"] = {}
        self.rejected(archive([(RESULT, encoded(value))]), "json-fields")

    def test_limits_are_enforced_before_acceptance(self):
        with patch.object(gate, "MAX_MEMBERS", 1):
            self.rejected(archive([("console.log", b"a"), ("build.log", b"b")]), "member-count")
        with patch.object(gate, "MAX_FILE", 3):
            self.rejected(archive([("console.log", b"four")]), "member-size")
        with patch.object(gate, "MAX_TOTAL", 6):
            inventory = gate.Inventory()
            inventory.add("console.log", b"four")
            with self.assertRaisesRegex(gate.Rejected, "total-size"):
                inventory.add("build.log", b"four")
        with patch.object(gate, "MAX_DEPTH", 2):
            with self.assertRaisesRegex(gate.Rejected, "json-depth"):
                gate.parse_json('[[[0]]]')
        with patch.object(gate, "MAX_TOTAL", 1), patch.object(gate, "MAX_MEMBERS", 1):
            # Compressed bytes fit under a separate input bound in ordinary operation;
            # directly exercise the decompression cap with a compact expansion bomb.
            bomb = gzip.compress(bytes(100_000), mtime=0)
            with patch.object(gate, "MAX_TOTAL", len(bomb)):
                self.rejected(bomb, "gzip-stream")

    def test_directory_copy_and_manifest_bind_exact_bytes(self):
        source = self.root / "input"
        source.mkdir()
        original = b"synthetic incomplete run\n"
        (source / "console.log").write_bytes(original)
        output = self.root / "output"
        gate.check(source, output)
        (source / "console.log").write_bytes(b"changed after validation")
        self.assertEqual(original, (output / "evidence/console.log").read_bytes())
        report = json.loads((output / "inventory.json").read_text())
        self.assertEqual(gate.digest(original), report["members"][0]["sha256"])
        self.assertTrue(report["humanReviewRequired"])
        self.assertEqual(gate.digest(Path(gate.__file__).read_bytes()), report["validatorSha256"])
        with self.assertRaisesRegex(gate.Rejected, "output-exists"):
            gate.check(source, output)

    def test_archive_copy_is_not_repacked(self):
        source = self.root / "evidence.tar.gz"
        data = archive([("console.log", b"safe")])
        source.write_bytes(data)
        output = self.root / "output"
        gate.check(source, output)
        self.assertEqual(data, (output / "evidence/evidence.tar.gz").read_bytes())
        self.assertEqual(gate.digest(data), json.loads((output / "inventory.json").read_text())["source"]["sha256"])

    def test_failures_leave_no_output_or_sensitive_diagnostics(self):
        sentinel = "ghp_" + "NotARealCredential" * 3
        source = self.root / "input.tar.gz"
        source.write_bytes(archive([("console.log", sentinel.encode())]))
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            status = gate.main(["check", str(source), str(self.root / "out")])
        self.assertEqual(1, status)
        self.assertNotIn(sentinel, stdout.getvalue() + stderr.getvalue())
        self.assertNotIn(str(source), stderr.getvalue())
        self.assertFalse((self.root / "out").exists())
        self.assertEqual([source], list(self.root.iterdir()))
        source.write_bytes(archive([("console.log", b"safe"), ("build.log", b"\xff")]))
        with contextlib.redirect_stderr(stderr):
            self.assertEqual(1, gate.main(["check", str(source), str(self.root / "out")]))
        self.assertFalse((self.root / "out").exists())

    def test_symlinks_hardlinks_special_files_and_unreadable_inputs(self):
        source = self.root / "input"
        source.mkdir()
        external = self.root / "external"
        external.write_bytes(b"safe")
        member = source / "console.log"
        member.symlink_to(external)
        with self.assertRaisesRegex(gate.Rejected, "input-symlink"):
            gate.check(source, self.root / "out")
        member.unlink()
        os.link(external, member)
        with self.assertRaisesRegex(gate.Rejected, "input-not-regular"):
            gate.check(source, self.root / "out")
        member.unlink()
        os.mkfifo(member)
        with self.assertRaisesRegex(gate.Rejected, "input-not-regular"):
            gate.check(source, self.root / "out")
        member.unlink()
        member.write_bytes(b"safe")
        with patch.object(gate, "regular_bytes", side_effect=PermissionError("sensitive detail")):
            error = io.StringIO()
            with contextlib.redirect_stderr(error):
                self.assertEqual(1, gate.main(["check", str(source), str(self.root / "out")]))
            self.assertNotIn("sensitive detail", error.getvalue())

    def test_empty_input_and_output_inside_input(self):
        source = self.root / "input"
        source.mkdir()
        with self.assertRaisesRegex(gate.Rejected, "empty-evidence"):
            gate.check(source, self.root / "out")
        with self.assertRaisesRegex(gate.Rejected, "output-inside-input"):
            gate.check(source, source / "out")
        self.rejected(archive([]), "empty-evidence")

    def test_ustar_octal_padding_is_independent_of_writer(self):
        raw = bytearray(gzip.decompress(archive([("console.log", b"safe")])))
        for mode in (b"000644 \0", b"   0644\0"):
            for device in (bytes(8), b"0000000\0", b"000000 \0"):
                with self.subTest(mode=mode, device=device):
                    header = raw.copy()
                    header[100:108] = mode
                    header[329:337] = device
                    header[337:345] = device
                    header[148:156] = b"        "
                    header[148:156] = ("%06o\0 " % sum(header[:512])).encode()
                    inventory = gate.Inventory()
                    inventory.archive(gzip.compress(header, mtime=0))
                    self.assertEqual(gate.digest(b"safe"), inventory.members[0]["sha256"])
        for offset, value in ((102, b"a"), (108, b"0\x00007"), (20, b"hidden")):
            with self.subTest(offset=offset):
                header = raw.copy()
                header[offset:offset + len(value)] = value
                header[148:156] = b"        "
                header[148:156] = ("%06o\0 " % sum(header[:512])).encode()
                source = self.root / "input.tar.gz"
                source.write_bytes(gzip.compress(header, mtime=0))
                with contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(1, gate.main(["check", str(source), str(self.root / "out")]))

    def test_property_comment_and_unicode_line_boundaries(self):
        for comment in ("#note", "!note"):
            for newline in ("\n", "\r", "\r\n"):
                with self.subTest(comment=comment, newline=newline):
                    data = ("os.name=Linux" + newline + comment + "\\" + newline
                            + "java.version=\\u0070assword\\u003dsynthetic-value" + newline).encode()
                    self.rejected(archive([("manifest.properties", data)]), "credential-credential-assignment")
                    data = ("os.name=Linux" + newline + comment + "\\" + newline
                            + "unknown=synthetic" + newline).encode()
                    self.rejected(archive([("manifest.properties", data)]), "properties-key")
        for separator in ("\u0085", "\u2028", "\u2029"):
            data = ("os.name=Linux" + separator + "#\\u0070assword\\u003dsynthetic-value\n").encode()
            self.rejected(archive([("manifest.properties", data)]), "credential-credential-assignment")
        # A comment marker on an actual continuation line is part of the value.
        gate.content("manifest.properties", b"os.name=Linux\\\n  #continued\n")
        self.rejected(archive([("manifest.properties", b"\\\n#not-a-comment\n")]), "properties-syntax")

    def test_cli_argument_errors_do_not_echo_values(self):
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            self.assertEqual(1, gate.main(["synthetic-sensitive-invalid-mode"]))
        self.assertEqual("Evidence rejected: arguments\n", output.getvalue())

    def test_directory_links_cannot_be_followed_after_enumeration(self):
        source = self.root / "input"
        nested = source / "diagnostics"
        nested.mkdir(parents=True)
        (nested / "console.log").write_bytes(b"safe")
        outside = self.root / "outside"
        outside.mkdir()
        (outside / "console.log").write_bytes(b"password=synthetic-password")
        original_stat = os.stat
        replaced = False
        def replace_after_stat(path, *args, **kwargs):
            nonlocal replaced
            result = original_stat(path, *args, **kwargs)
            if path == "diagnostics" and kwargs.get("dir_fd") is not None and not replaced:
                replaced = True
                nested.rename(source / "old")
                nested.symlink_to(outside, target_is_directory=True)
            return result
        with patch.object(gate.os, "stat", side_effect=replace_after_stat):
            with self.assertRaises(OSError):
                gate.check(source, self.root / "out")
        self.assertTrue(replaced)
        self.assertFalse((self.root / "out").exists())

    def test_tar_unused_header_bytes_cannot_hide_metadata(self):
        raw = bytearray(gzip.decompress(archive([("console.log", b"safe")])))
        raw[500:504] = b"hide"
        raw[148:156] = b"        "
        checksum = sum(raw[:512])
        raw[148:156] = ("%06o\0 " % checksum).encode()
        self.rejected(gzip.compress(raw, mtime=0), "tar-header")

    def test_csv_row_width_properties_escapes_and_nested_directories(self):
        self.rejected(archive([("parity.csv", (gate.CSV_HEADERS["parity.csv"] + "\none,two\n").encode())]), "csv-row")
        self.rejected(archive([("manifest.properties", b"java.version=17\\")]), "properties-continuation")
        self.rejected(archive([("manifest.properties", b"java.version=\\uZZZZ")]), "properties-escape")
        source = self.root / "input"
        (source / "diagnostics").mkdir(parents=True)
        (source / "diagnostics" / EVENTS).write_bytes(encoded(events()))
        gate.check(source, self.root / "out")
        members = json.loads((self.root / "out/inventory.json").read_text())["members"]
        self.assertEqual(["directory", "jfr-events"], [m["format"] for m in members])

    def test_index_bytes_are_checked_even_when_worktree_is_safe(self):
        repo = self.root / "repository"
        repo.mkdir()
        def git(*args):
            return subprocess.run(["git", "-C", str(repo), *args], check=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        git("init")
        target = repo / gate.EVIDENCE_ROOT / "synthetic" / "evidence.tar.gz"
        target.parent.mkdir(parents=True)
        target.write_bytes(archive([("console.log", b"password=synthetic-password")]))
        git("add", ".")
        target.write_bytes(archive([("console.log", b"safe")]))
        previous = Path.cwd()
        os.chdir(repo)
        self.addCleanup(os.chdir, previous)
        with self.assertRaisesRegex(gate.Rejected, "credential-"):
            gate.repository(self.root / "reports", staged=True)
        self.assertFalse((self.root / "reports").exists())
        git("add", ".")
        gate.repository(self.root / "reports", staged=True)
        report = json.loads((self.root / "reports/0.json").read_text())
        self.assertEqual(gate.digest(target.read_bytes()), report["source"]["sha256"])
        self.assertEqual(git("rev-parse", ":" + str(target.relative_to(repo))).stdout.decode().strip(),
                         report["source"]["gitBlob"])


if __name__ == "__main__":
    unittest.main()
