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

import gzip
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import benchmark_evidence as gate

RESULT = "PROTOBUF_NATIVE-scalar-serialize-0.json"
EVENTS = "PROTOBUF_NATIVE-scalar-serialize-allocation-execution-events.json"
COMMAND = "PROTOBUF_NATIVE-scalar-serialize-command.json"
TOKEN = "ghp_" + "SyntheticSecret" * 3


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


def rejected(data, rule=None):
    with pytest.raises(gate.Rejected) as caught:
        gate.Inventory().archive(data)
    if rule:
        assert rule == str(caught.value)


def secret_cases():
    value = events()
    value["recording"]["events"][0]["values"]["eventThread"]["javaName"] = TOKEN
    return [pytest.param("console.log", ("token " + TOKEN).encode(), id="console-token"),
            pytest.param("invocation.txt", b"password=synthetic-password", id="invocation-password"),
            pytest.param("manifest.properties", b"java.version=password\\u003dsynthetic-password",
                         id="properties-escaped-assignment"),
            pytest.param(COMMAND, ('["' + TOKEN.replace("g", "\\u0067", 1) + '"]').encode(),
                         id="command-escaped-token"),
            pytest.param(COMMAND, b'[ {"pass\\u0077ord": "synthetic-password"} ]', id="command-escaped-key"),
            pytest.param(EVENTS, encoded(value), id="events-thread-token")]


def test_supported_formats():
    summary = {"PROTOBUF_NATIVE-scalar-serialize": {
        "allocation_sample_weight_top": [["example.Record", 1]],
        "execution_sample_top": [["example.Record.run", 1]],
        "allocation_stack_examples": {"example.Record": ["example.Record.run"]}}}
    cases = [(RESULT, encoded(jmh())), (EVENTS, encoded(events())),
             (COMMAND, encoded(["java", "-Xmx256m"])),
             ("allocation-and-execution-samples.json", encoded(summary)),
             ("manifest.properties", b"java.version=17\nsource.status=\n"),
             ("PROTOBUF_CHILL-opened.properties", b"lane=PROTOBUF_CHILL\nstatus=unsupported\nreason=synthetic\n"),
             ("PROTOBUF_DYNAMIC-plain.properties", b"lane=PROTOBUF_DYNAMIC\nstatus=supported\n"),
             ("PROTOBUF_DYNAMIC_FOREIGN-plain.properties", b"lane=PROTOBUF_DYNAMIC_FOREIGN\nstatus=supported\n"),
             ("measurement-source.patch", b"--- a/source\n+++ b/source\n"),
             ("console.log", b"Incomplete benchmark; synthetic failure\n"),
             ("vm-manifest.txt", b"gce.machine.type=c3-standard-8\n"),
             ("maintenance-events.log", b"2026-01-01T00:00:00Z NONE\n")]
    cases += [(name, (header + "\n").encode()) for name, header in gate.CSV_HEADERS.items()]
    inventory = gate.Inventory()
    inventory.archive(archive(cases))
    assert len(cases) == len(inventory.members)
    for item, (name, data) in zip(inventory.members, cases):
        assert (name, len(data), gate.digest(data)) == (item["path"], item["bytes"], item["sha256"])


@pytest.mark.parametrize("event_type", ["jdk.InitialEnvironmentVariable", "jdk.InitialSystemProperty",
                                        "jdk.JVMInformation"])
def test_jfr_rejects_other_events(event_type):
    value = events()
    value["recording"]["events"][0]["type"] = event_type
    rejected(archive([(EVENTS, encoded(value))]), "jfr-event")


def test_jfr_rejects_metadata_wrong_types_and_misnamed_recordings():
    for location in (lambda v: v, lambda v: v["recording"],
                     lambda v: v["recording"]["events"][0]["values"],
                     lambda v: v["recording"]["events"][0]["values"]["objectClass"]["classLoader"]):
        value = events()
        location(value)["environment"] = "synthetic metadata"
        rejected(archive([(EVENTS, encoded(value))]), "json-fields")
    value = events()
    value["recording"]["events"][0]["values"]["weight"] = "1"
    rejected(archive([(EVENTS, encoded(value))]), "json-type")
    rejected(archive([("console.log", encoded(events()))]), "misnamed-jfr")


@pytest.mark.parametrize(("name", "data"), secret_cases())
def test_compressed_secrets_and_decoded_escapes(name, data):
    with pytest.raises(gate.Rejected) as caught:
        gate.Inventory().archive(archive([(name, data)]))
    assert str(caught.value).startswith("credential-")
    assert TOKEN not in str(caught.value)


@pytest.mark.parametrize("value", ["-----BEGIN PRIVATE KEY-----", "github_pat_" + "a" * 30,
                                   "AKIA" + "A" * 16, "AIza" + "A" * 35, "ya29." + "a" * 30,
                                   "xoxb-" + "1" * 20, "Authorization: Bearer syntheticvalue",
                                   "client_secret=synthetic-value", "AWS_SECRET_ACCESS_KEY=synthetic-value",
                                   "ANTHROPIC_API_KEY=synthetic-value", "-Dpassword=synthetic-value"],
                         ids=["private-key", "github-pat", "aws-access-key", "google-api-key", "google-oauth-token",
                              "slack-token", "bearer-header", "client-secret", "aws-secret-key", "anthropic-key",
                              "password-property"])
def test_each_credential_rule_has_a_synthetic_positive_control(value):
    with pytest.raises(gate.Rejected):
        gate.scan(value)


@pytest.mark.parametrize("name", ["/console.log", "../console.log", "a/../console.log", "./console.log",
                                  "a//console.log", "C:/console.log", "a\\console.log", "console.log\n",
                                  "console.log/child"])
def test_unsafe_paths(name):
    rejected(archive([(name, b"safe")]))


def test_duplicates_and_collisions():
    rejected(archive([("console.log", b"a"), ("console.log", b"b")]), "duplicate-member")
    inventory = gate.Inventory()
    inventory.add("console.log", b"safe")
    with pytest.raises(gate.Rejected, match="member-path-collision"):
        inventory.add("console.log/child", b"safe")


@pytest.mark.parametrize("data", [b"FLR\x00recording", b"JAVA PROFILE 1.0\x00", b"PK\x03\x04zip",
                                  b"\x7fELFbinary", b"\x00binary", b"\xffinvalid utf8",
                                  archive([("console.log", b"safe")])],
                         ids=["jfr", "hprof", "zip", "elf", "nul", "invalid-utf8", "nested-gzip"])
def test_binary_and_nested_archives_even_with_text_names(tmp_path, data):
    source = tmp_path / "input.tar.gz"
    source.write_bytes(archive([("console.log", data)]))
    assert gate.main(["check", str(source), str(tmp_path / "out")]) == 1


def test_unknown_member_names():
    rejected(archive([("unknown.bin", b"innocent text")]), "member-inventory")
    rejected(archive([("unexpected.log", b"innocent text")]), "member-inventory")


@pytest.mark.parametrize(("attribute", "value"), [("uid", 1), ("uname", "synthetic"), ("mtime", 1),
                                                  ("type", tarfile.SYMTYPE), ("type", tarfile.LNKTYPE),
                                                  ("type", tarfile.FIFOTYPE), ("type", tarfile.XHDTYPE)],
                         ids=["uid", "uname", "mtime", "symlink", "hardlink", "fifo", "pax-header"])
def test_archive_metadata_and_special_members(attribute, value):
    rejected(archive([("console.log", b"safe")], lambda info: setattr(info, attribute, value)))


def test_gzip_header_metadata():
    data = bytearray(archive([("console.log", b"safe")]))
    data[4] = 1
    rejected(bytes(data), "gzip-header")
    rejected(gzip.compress(gzip.decompress(data), mtime=1), "gzip-header")


def test_truncation_extra_streams_and_tar_trailers():
    data = archive([("console.log", b"safe")])
    for value in (data[:-1], data + b"extra", data + data):
        rejected(value, "gzip-stream")
    raw = gzip.decompress(data)
    rejected(gzip.compress(raw[:1024], mtime=0), "tar-trailer")
    rejected(gzip.compress(raw + bytes(511) + b"x", mtime=0), "tar-trailer")
    rejected(gzip.compress(raw[:-1], mtime=0), "tar-alignment")
    changed = bytearray(raw)
    changed[512 + len(b"safe")] = 1
    rejected(gzip.compress(changed, mtime=0), "tar-padding")


@pytest.mark.parametrize(("name", "data"), [(RESULT, b"[{"), (RESULT, b'[{"x":1,"x":2}]'),
                                            (RESULT, b"[NaN]"), (RESULT, b"[1e9999]"),
                                            (EVENTS, encoded({"recording": {"events": None}})),
                                            ("measurements.csv", b"unexpected,header\n"),
                                            ("manifest.properties", b"unknown=synthetic\n"),
                                            ("manifest.properties", b"java.version=17\njava.version=21\n")],
                         ids=["json-truncated", "json-duplicate-key", "json-nan", "json-overflow",
                              "jfr-null-events", "csv-header", "properties-unknown-key", "properties-duplicate-key"])
def test_json_csv_and_properties_fail_closed(tmp_path, name, data):
    source = tmp_path / "input.tar.gz"
    source.write_bytes(archive([(name, data)]))
    assert gate.main(["check", str(source), str(tmp_path / "out")]) == 1


def test_jmh_extra_fields_fail_closed():
    value = jmh()
    value[0]["systemProperties"] = {}
    rejected(archive([(RESULT, encoded(value))]), "json-fields")


def test_limits_are_enforced_before_acceptance(monkeypatch):
    with monkeypatch.context() as patched:
        patched.setattr(gate, "MAX_MEMBERS", 1)
        rejected(archive([("console.log", b"a"), ("build.log", b"b")]), "member-count")
    with monkeypatch.context() as patched:
        patched.setattr(gate, "MAX_FILE", 3)
        rejected(archive([("console.log", b"four")]), "member-size")
    with monkeypatch.context() as patched:
        patched.setattr(gate, "MAX_TOTAL", 6)
        inventory = gate.Inventory()
        inventory.add("console.log", b"four")
        with pytest.raises(gate.Rejected, match="total-size"):
            inventory.add("build.log", b"four")
    with monkeypatch.context() as patched:
        patched.setattr(gate, "MAX_DEPTH", 2)
        with pytest.raises(gate.Rejected, match="json-depth"):
            gate.parse_json('[[[0]]]')
    with monkeypatch.context() as patched:
        patched.setattr(gate, "MAX_TOTAL", 1)
        patched.setattr(gate, "MAX_MEMBERS", 1)
        # Compressed bytes fit under a separate input bound in ordinary operation;
        # directly exercise the decompression cap with a compact expansion bomb.
        bomb = gzip.compress(bytes(100_000), mtime=0)
        with monkeypatch.context() as inner:
            inner.setattr(gate, "MAX_TOTAL", len(bomb))
            rejected(bomb, "gzip-stream")


def test_directory_copy_and_manifest_bind_exact_bytes(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    original = b"synthetic incomplete run\n"
    (source / "console.log").write_bytes(original)
    output = tmp_path / "output"
    gate.check(source, output)
    (source / "console.log").write_bytes(b"changed after validation")
    assert original == (output / "evidence/console.log").read_bytes()
    report = json.loads((output / "inventory.json").read_text())
    assert gate.digest(original) == report["members"][0]["sha256"]
    assert report["humanReviewRequired"]
    assert gate.digest(Path(gate.__file__).read_bytes()) == report["validatorSha256"]
    with pytest.raises(gate.Rejected, match="output-exists"):
        gate.check(source, output)


def test_archive_copy_is_not_repacked(tmp_path):
    source = tmp_path / "evidence.tar.gz"
    data = archive([("console.log", b"safe")])
    source.write_bytes(data)
    output = tmp_path / "output"
    gate.check(source, output)
    assert data == (output / "evidence/evidence.tar.gz").read_bytes()
    assert gate.digest(data) == json.loads((output / "inventory.json").read_text())["source"]["sha256"]


def test_failures_leave_no_output_or_sensitive_diagnostics(tmp_path, capsys):
    sentinel = "ghp_" + "NotARealCredential" * 3
    source = tmp_path / "input.tar.gz"
    source.write_bytes(archive([("console.log", sentinel.encode())]))
    status = gate.main(["check", str(source), str(tmp_path / "out")])
    captured = capsys.readouterr()
    assert status == 1
    assert sentinel not in captured.out + captured.err
    assert str(source) not in captured.err
    assert not (tmp_path / "out").exists()
    assert list(tmp_path.iterdir()) == [source]
    source.write_bytes(archive([("console.log", b"safe"), ("build.log", b"\xff")]))
    assert gate.main(["check", str(source), str(tmp_path / "out")]) == 1
    assert not (tmp_path / "out").exists()


def test_symlinks_hardlinks_special_files_and_unreadable_inputs(tmp_path, monkeypatch, capsys):
    source = tmp_path / "input"
    source.mkdir()
    external = tmp_path / "external"
    external.write_bytes(b"safe")
    member = source / "console.log"
    member.symlink_to(external)
    with pytest.raises(gate.Rejected, match="input-symlink"):
        gate.check(source, tmp_path / "out")
    member.unlink()
    os.link(external, member)
    with pytest.raises(gate.Rejected, match="input-not-regular"):
        gate.check(source, tmp_path / "out")
    member.unlink()
    os.mkfifo(member)
    with pytest.raises(gate.Rejected, match="input-not-regular"):
        gate.check(source, tmp_path / "out")
    member.unlink()
    member.write_bytes(b"safe")

    def unreadable(*args, **kwargs):
        raise PermissionError("sensitive detail")

    monkeypatch.setattr(gate, "regular_bytes", unreadable)
    capsys.readouterr()
    assert gate.main(["check", str(source), str(tmp_path / "out")]) == 1
    assert "sensitive detail" not in capsys.readouterr().err


def test_empty_input_and_output_inside_input(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    with pytest.raises(gate.Rejected, match="empty-evidence"):
        gate.check(source, tmp_path / "out")
    with pytest.raises(gate.Rejected, match="output-inside-input"):
        gate.check(source, source / "out")
    rejected(archive([]), "empty-evidence")


@pytest.mark.parametrize("device", [bytes(8), b"0000000\0", b"000000 \0"],
                         ids=["device-nul", "device-nul-terminated", "device-space"])
@pytest.mark.parametrize("mode", [b"000644 \0", b"   0644\0"], ids=["mode-trailing-space", "mode-leading-spaces"])
def test_ustar_octal_padding_is_independent_of_writer(mode, device):
    header = bytearray(gzip.decompress(archive([("console.log", b"safe")])))
    header[100:108] = mode
    header[329:337] = device
    header[337:345] = device
    header[148:156] = b"        "
    header[148:156] = ("%06o\0 " % sum(header[:512])).encode()
    inventory = gate.Inventory()
    inventory.archive(gzip.compress(header, mtime=0))
    assert gate.digest(b"safe") == inventory.members[0]["sha256"]


@pytest.mark.parametrize(("offset", "value"), [(102, b"a"), (108, b"0\x00007"), (20, b"hidden")],
                         ids=["mode-non-octal", "uid-embedded-nul", "name-after-nul"])
def test_ustar_noncanonical_fields_are_rejected(tmp_path, offset, value):
    header = bytearray(gzip.decompress(archive([("console.log", b"safe")])))
    header[offset:offset + len(value)] = value
    header[148:156] = b"        "
    header[148:156] = ("%06o\0 " % sum(header[:512])).encode()
    source = tmp_path / "input.tar.gz"
    source.write_bytes(gzip.compress(header, mtime=0))
    assert gate.main(["check", str(source), str(tmp_path / "out")]) == 1


@pytest.mark.parametrize("newline", ["\n", "\r", "\r\n"], ids=["lf", "cr", "crlf"])
@pytest.mark.parametrize("comment", ["#note", "!note"], ids=["hash", "bang"])
def test_property_comment_line_boundaries(comment, newline):
    data = ("os.name=Linux" + newline + comment + "\\" + newline
            + "java.version=\\u0070assword\\u003dsynthetic-value" + newline).encode()
    rejected(archive([("manifest.properties", data)]), "credential-credential-assignment")
    data = ("os.name=Linux" + newline + comment + "\\" + newline
            + "unknown=synthetic" + newline).encode()
    rejected(archive([("manifest.properties", data)]), "properties-key")


def test_property_unicode_line_boundaries_and_continuations():
    for separator in ("\u0085", "\u2028", "\u2029"):
        data = ("os.name=Linux" + separator + "#\\u0070assword\\u003dsynthetic-value\n").encode()
        rejected(archive([("manifest.properties", data)]), "credential-credential-assignment")
    # A comment marker on an actual continuation line is part of the value.
    gate.content("manifest.properties", b"os.name=Linux\\\n  #continued\n")
    rejected(archive([("manifest.properties", b"\\\n#not-a-comment\n")]), "properties-syntax")


def test_cli_argument_errors_do_not_echo_values(capsys):
    assert gate.main(["synthetic-sensitive-invalid-mode"]) == 1
    assert capsys.readouterr().err == "Evidence rejected: arguments\n"


def test_directory_links_cannot_be_followed_after_enumeration(tmp_path, monkeypatch):
    source = tmp_path / "input"
    nested = source / "diagnostics"
    nested.mkdir(parents=True)
    (nested / "console.log").write_bytes(b"safe")
    outside = tmp_path / "outside"
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
    with monkeypatch.context() as patched:
        patched.setattr(gate.os, "stat", replace_after_stat)
        with pytest.raises(OSError):
            gate.check(source, tmp_path / "out")
    assert replaced
    assert not (tmp_path / "out").exists()


def test_tar_unused_header_bytes_cannot_hide_metadata():
    raw = bytearray(gzip.decompress(archive([("console.log", b"safe")])))
    raw[500:504] = b"hide"
    raw[148:156] = b"        "
    checksum = sum(raw[:512])
    raw[148:156] = ("%06o\0 " % checksum).encode()
    rejected(gzip.compress(raw, mtime=0), "tar-header")


def test_csv_row_width_properties_escapes_and_nested_directories(tmp_path):
    rejected(archive([("parity.csv", (gate.CSV_HEADERS["parity.csv"] + "\none,two\n").encode())]), "csv-row")
    rejected(archive([("manifest.properties", b"java.version=17\\")]), "properties-continuation")
    rejected(archive([("manifest.properties", b"java.version=\\uZZZZ")]), "properties-escape")
    source = tmp_path / "input"
    (source / "diagnostics").mkdir(parents=True)
    (source / "diagnostics" / EVENTS).write_bytes(encoded(events()))
    gate.check(source, tmp_path / "out")
    members = json.loads((tmp_path / "out/inventory.json").read_text())["members"]
    assert [m["format"] for m in members] == ["directory", "jfr-events"]


def test_index_bytes_are_checked_even_when_worktree_is_safe(tmp_path, monkeypatch):
    repo = tmp_path / "repository"
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
    monkeypatch.chdir(repo)
    with pytest.raises(gate.Rejected, match="credential-"):
        gate.repository(tmp_path / "reports", staged=True)
    assert not (tmp_path / "reports").exists()
    git("add", ".")
    gate.repository(tmp_path / "reports", staged=True)
    report = json.loads((tmp_path / "reports/0.json").read_text())
    assert gate.digest(target.read_bytes()) == report["source"]["sha256"]
    blob = git("rev-parse", ":" + str(target.relative_to(repo))).stdout.decode().strip()
    assert blob == report["source"]["gitBlob"]
