#!/usr/bin/env python3
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

"""Validate benchmark publication copies without extracting untrusted archives."""

import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tarfile
import tempfile
import zlib

MAX_MEMBERS = 10_000
MAX_FILE = 64 * 1024 * 1024
MAX_TOTAL = 256 * 1024 * 1024
MAX_DEPTH = 64
EVIDENCE_ROOT = "docs/validation/data"
LANE = r"(?:POJO|TUPLE|AVRO_GENERIC|AVRO_SPECIFIC|KRYO|PROTOBUF_NATIVE|PROTOBUF_CHILL|THRIFT_CHILL)"
WORKLOAD = r"(?:scalar|text|nested|collections|large)"
OPERATION = r"(?:serialize|deserialize|objectCopy|streamCopy|deserializeReuse|objectCopyReuse|serializeSameInstance|buildAndSerialize)"
CASE = LANE + "-" + WORKLOAD + "-" + OPERATION
RESULT = CASE + r"(?:-deterministic)?-[0-4]"
DIAGNOSTIC = r"PROTOBUF_(?:NATIVE|CHILL)-" + WORKLOAD + "-" + OPERATION
SUPPORT = LANE + r"-(?:plain|opened)"
TEXT_NAMES = {
    "build.log", "console.log", "classpath-sha256.txt", "classpath-verification-end.txt",
    "completed-results.txt", "end-utc.txt", "environment-note.txt", "invocation.txt",
    "load-after-build.log", "load-before-build.log", "measurement-source.patch",
    "power-end.txt", "power-event-audit.txt", "power-events.log", "power-observations.log",
    "power-start.txt", "source-base.txt", "start-utc.txt",
}
CSV_HEADERS = {
    "measurements.csv": "lane,workload,operation,deterministic,block,ns_per_op,derived_ops_per_second,allocated_bytes_per_op",
    "parity.csv": "workload,operation,native_over_chill,lower95,upper95,verdict",
    "summary.csv": "protobuf_major,lane,workload,operation,mean_ns_per_op,min_fork_ns,max_fork_ns,mean_allocated_bytes_per_op,corpus_encoded_mean_bytes",
}
MANIFEST_KEYS = set("benchmark.flink.version benchmark.library.jar benchmark.protobuf.version benchmark.protoc.version benchmark.thrift.compiler corpus.records corpus.seed cpu cpu.count java.vendor java.version java.vm.name java.vm.version jmh.version jvm.arguments machine memory.bytes os.arch os.name os.version source.revision source.status thrift.compiler.version timestamp.utc".split())
SUPPORT_KEYS = set("adapter.class adapter.source immutable jvm.arguments kryo.record.id kryo.references kryo.registrationRequired kryo.source lane reason record.source serializer.chain serializer.source status".split())
SUPPORT_KEYS.update(w + "." + key for w in ("scalar", "text", "nested", "collections", "large")
                    for key in "copyReuses deserializeReuses encoded.max encoded.mean encoded.min identityCopies records".split())
# These deliberately bounded patterns supplement human review, not arbitrary-secret detection.
CREDENTIAL_RULES = {
    "private-key": r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----",
    "github-token": r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b",
    "aws-access-key": r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b",
    "google-api-key": r"\bAIza[A-Za-z0-9_-]{35}\b",
    "google-oauth": r"\bya29\.[A-Za-z0-9_-]{20,}",
    "slack-token": r"\bxox[baprs]-[A-Za-z0-9-]{10,}",
    "authorization": r"(?i)\b(?:authorization\s*[:=]\s*|bearer\s+|basic\s+)[\"']?(?:bearer\s+|basic\s+)?[A-Za-z0-9+/_=.-]{8,}",
    "credential-assignment": r"(?i)(?:[\"']?(?:\b|-D)(?:[A-Za-z0-9]{1,64}[_.-]){0,8}(?:password|passwd|token|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|secret[_-]?access[_-]?key|private[_-]?key)\b[\"']?\s*[:=]\s*[\"']?)[^\s\"',;}]{4,}",
}
PATTERNS = [(name, re.compile(pattern)) for name, pattern in CREDENTIAL_RULES.items()]


class Rejected(Exception):
    """A fixed rule identifier and optional ordinal, never an input value."""

    member = None


def require(condition, rule):
    if not condition:
        raise Rejected(rule)


def scan(text):
    for name, pattern in PATTERNS:
        require(pattern.search(text) is None, "credential-" + name)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def safe_path(name):
    require(isinstance(name, str) and len(name) <= 240, "member-path")
    scan(name)
    require(re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", name), "member-path")
    require(all(p not in (".", "..") for p in name.split("/")), "member-path")
    return name


def fields(value, required, optional=()):
    require(type(value) is dict and set(required) <= value.keys()
            and value.keys() <= set(required) | set(optional), "json-fields")


def typed(value, kind):
    require(type(value) is kind, "json-type")


def numeric(value):
    require((type(value) in (int, float) and math.isfinite(value))
            or (type(value) is str and value in ("NaN", "-INF", "+INF")), "json-number")


def array(value, check):
    typed(value, list)
    for item in value:
        check(item)


def strings(value):
    array(value, lambda item: typed(item, str))


def parse_json(text):
    # Bound nesting before the decoder can recurse; escaped braces do not count.
    depth, quoted, escaped = 0, False, False
    for char in text:
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "[{":
            depth += 1
            require(depth <= MAX_DEPTH, "json-depth")
        elif char in "]}":
            depth -= 1
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "json-duplicate-key")
            result[key] = value
        return result
    def number(token):
        require(len(token) <= 100, "json-number")
        value = float(token) if any(c in token for c in ".eE") else int(token)
        require(math.isfinite(value), "json-number")
        return value
    def constant(_):
        raise Rejected("json-number")
    value = json.loads(text, object_pairs_hook=pairs, parse_int=number,
                       parse_float=number, parse_constant=constant)
    def inspect(item):
        if type(item) is str:
            scan(item)
        elif type(item) is dict:
            for key, child in item.items():
                scan(key)
                if type(child) in (str, int, float):
                    scan(key + "=" + str(child))
                inspect(child)
        elif type(item) is list:
            for child in item:
                inspect(child)
    inspect(value)
    return value


JFR_SCHEMAS = {
    "thread": {"osName": str, "osThreadId": int, "javaName": str, "javaThreadId": int, "group": "group"},
    "group": {"name": str, "parent": "group"},
    "class": {"classLoader": "loader", "name": str, "package": "package", "modifiers": int, "hidden": bool},
    "loader": {"type": "class", "name": str},
    "package": {"name": str, "module": "module", "exported": bool},
    "module": {"name": str, "version": str, "location": str, "classLoader": "loader"},
    "method": {"type": "class", "name": str, "descriptor": str, "modifiers": int, "hidden": bool},
    "frame": {"method": "method", "lineNumber": int, "bytecodeIndex": int, "type": str},
    "stack": {"truncated": bool, "frames": ["frame"]},
    "jdk.ObjectAllocationSample": {"startTime": str, "eventThread": "thread", "stackTrace": "stack", "objectClass": "class", "weight": int},
    "jdk.ExecutionSample": {"startTime": str, "sampledThread": "thread", "stackTrace": "stack", "state": str},
}


def jfr_node(value, schema, nullable=True):
    # JFR represents absent thread/class-loader/module references and strings as null.
    if value is None and nullable:
        return
    fields(value, JFR_SCHEMAS[schema])
    for key, kind in JFR_SCHEMAS[schema].items():
        child = value[key]
        if isinstance(kind, str):
            jfr_node(child, kind)
        elif isinstance(kind, list):
            array(child, lambda frame: jfr_node(frame, kind[0], False))
        elif child is None and kind is str:
            continue
        else:
            typed(child, kind)


def jfr(value):
    fields(value, ("recording",))
    fields(value["recording"], ("events",))
    def event(item):
        fields(item, ("type", "values"))
        require(item["type"] in ("jdk.ObjectAllocationSample", "jdk.ExecutionSample"), "jfr-event")
        jfr_node(item["values"], item["type"], False)
    array(value["recording"]["events"], event)


def metric(value):
    fields(value, ("score", "scoreError", "scoreConfidence", "scorePercentiles", "scoreUnit", "rawData"))
    numeric(value["score"])
    numeric(value["scoreError"])
    array(value["scoreConfidence"], numeric)
    require(len(value["scoreConfidence"]) == 2, "jmh-confidence")
    typed(value["scorePercentiles"], dict)
    for percentile, score in value["scorePercentiles"].items():
        require(re.fullmatch(r"(?:100|[0-9]{1,2})(?:\.[0-9]+)?", percentile), "jmh-percentile")
        numeric(score)
    typed(value["scoreUnit"], str)
    array(value["rawData"], lambda row: array(row, numeric))


def jmh(value):
    typed(value, list)
    require(bool(value), "jmh-empty")
    text_keys = "jmhVersion benchmark mode jvm jdkVersion vmName vmVersion warmupTime measurementTime".split()
    int_keys = "threads forks warmupIterations warmupBatchSize measurementIterations measurementBatchSize".split()
    for item in value:
        fields(item, text_keys + int_keys + ["jvmArgs", "params", "primaryMetric", "secondaryMetrics"])
        for key in text_keys:
            typed(item[key], str)
        for key in int_keys:
            typed(item[key], int)
        strings(item["jvmArgs"])
        fields(item["params"], ("deterministic", "lane", "workload"))
        for parameter in item["params"].values():
            typed(parameter, str)
        metric(item["primaryMetric"])
        fields(item["secondaryMetrics"], (), ("gc.alloc.rate", "gc.alloc.rate.norm", "gc.count", "gc.time", "jfr"))
        for secondary in item["secondaryMetrics"].values():
            metric(secondary)


def summary(value):
    typed(value, dict)
    require(bool(value), "summary-empty")
    for case, item in value.items():
        require(re.fullmatch(DIAGNOSTIC, case), "summary-case")
        fields(item, ("allocation_sample_weight_top", "execution_sample_top", "allocation_stack_examples"))
        for key in ("allocation_sample_weight_top", "execution_sample_top"):
            typed(item[key], list)
            for row in item[key]:
                require(type(row) is list and len(row) == 2, "summary-row")
                typed(row[0], str)
                typed(row[1], int)
        typed(item["allocation_stack_examples"], dict)
        for frames in item["allocation_stack_examples"].values():
            strings(frames)


def properties(text, allowed):
    # java.util.Properties.store uses continuations and escapes; decode before scanning.
    logical, pending = [], None
    # Java natural lines end only at CR/LF. Comments never continue onto another line.
    for natural in re.split(r"\r\n|\r|\n", text):
        stripped = natural.lstrip(" \t\f")
        if pending is None and (not stripped or stripped.startswith(("#", "!"))):
            continue
        line = pending + stripped if pending is not None else stripped
        slashes = len(line) - len(line.rstrip("\\"))
        if slashes % 2:
            pending = line[:-1]
        else:
            logical.append(line)
            pending = None
    require(pending is None, "properties-continuation")
    keys = set()
    def decode(value):
        def escape(match):
            token = match.group(1)
            return chr(int(token[1:], 16)) if token.startswith("u") else {"t": "\t", "r": "\r", "n": "\n", "f": "\f"}.get(token, token)
        require(re.search(r"\\u(?![0-9a-fA-F]{4})", value) is None, "properties-escape")
        return re.sub(r"\\(u[0-9a-fA-F]{4}|.)", escape, value)
    for line in logical:
        match = re.fullmatch(r"([^=:\s]+)=(.*)", line)
        require(match is not None, "properties-syntax")
        key, value = (decode(part) for part in match.groups())
        require(key in allowed and key not in keys, "properties-key")
        keys.add(key)
        scan(key + "=" + value)
    require(bool(keys), "properties-empty")


def content(name, data):
    safe_path(name)
    parts = name.split("/")
    require(len(parts) == 1 or (len(parts) == 2 and parts[0] in ("protobuf-3", "protobuf-4", "diagnostics")), "member-inventory")
    require(len(data) <= MAX_FILE, "member-size")
    # Check magic independently of filenames, before decoding text.
    require(not data.startswith((b"FLR\x00", b"JAVA PROFILE", b"PK\x03\x04", b"\x1f\x8b", b"BZh", b"\xfd7zXZ", b"7z\xbc\xaf", b"\x7fELF", b"\xca\xfe\xba\xbe")), "binary-or-archive")
    require(data[257:263] not in (b"ustar\x00", b"ustar "), "nested-archive")
    text = data.decode("utf-8", errors="strict")
    require(not any(ord(c) < 32 and c not in "\t\r\n" for c in text), "binary-control")
    scan(text)
    base = parts[-1]
    if base.endswith(".json"):
        value = parse_json(text)
        if re.fullmatch(DIAGNOSTIC + "-allocation-execution-events.json", base):
            jfr(value)
            return "jfr-events"
        if base == "allocation-and-execution-samples.json":
            summary(value)
            return "diagnostic-summary"
        if re.fullmatch(DIAGNOSTIC + "-command.json", base):
            strings(value)
            require(bool(value), "command-empty")
            return "recorded-command"
        require(re.fullmatch("(?:" + RESULT + "|" + DIAGNOSTIC + r")\.json", base), "member-inventory")
        jmh(value)
        return "jmh-result"
    # Renaming an exported JFR document to a log does not bypass its schema.
    require(re.search(r'"(?:recording|events)"\s*:|jdk\.(?:ObjectAllocationSample|ExecutionSample|InitialEnvironmentVariable|InitialSystemProperty|JVMInformation)', text) is None, "misnamed-jfr")
    if base in CSV_HEADERS:
        rows = csv.reader(io.StringIO(text), strict=True)
        expected = CSV_HEADERS[base].split(",")
        require(next(rows, None) == expected, "csv-header")
        for row in rows:
            require(len(row) == len(expected), "csv-row")
        return "csv-summary"
    if base == "manifest.properties" or re.fullmatch(SUPPORT + r"\.properties", base):
        properties(text, MANIFEST_KEYS if base == "manifest.properties" else SUPPORT_KEYS)
        return "properties"
    require(base in TEXT_NAMES or re.fullmatch("(?:" + RESULT + "|" + DIAGNOSTIC + "|" + SUPPORT + r")\.log", base), "member-inventory")
    return "reviewed-text"


def regular_bytes(path, limit, directory_fd=None):
    # O_NOFOLLOW closes the final-component symlink race; the copy is the upload source.
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd), "rb") as source:
        before = os.fstat(source.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1, "input-not-regular")
        require(before.st_size <= limit, "input-size")
        data = source.read(limit + 1)
        after = os.fstat(source.fileno())
    require(len(data) <= limit, "input-size")
    require((before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_size, after.st_mtime_ns, after.st_ctime_ns), "input-changed")
    return data


def directory_members(path, parent_fd=None, prefix=""):
    # Resolve children relative to open directory descriptors, including during races.
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
    try:
        names = []
        with os.scandir(descriptor) as entries:
            for entry in entries:
                require(len(names) < MAX_MEMBERS, "member-count")
                names.append(entry.name)
        for name in sorted(names):
            relative = prefix + name
            safe_path(relative)
            mode = os.stat(name, dir_fd=descriptor, follow_symlinks=False).st_mode
            require(not stat.S_ISLNK(mode), "input-symlink")
            if stat.S_ISDIR(mode):
                yield relative, b"", True
                yield from directory_members(name, descriptor, relative + "/")
            else:
                yield relative, regular_bytes(name, MAX_FILE, descriptor), False
    finally:
        os.close(descriptor)


def tar_header(block):
    """Validate raw field padding without depending on the writer's octal formatting."""
    for start, end in ((100, 108), (108, 116), (116, 124), (124, 136), (136, 148),
                       (148, 156), (329, 337), (337, 345)):
        require(re.fullmatch(rb" *[0-7]*[ \x00]*", block[start:end]), "tar-header")
    for start, end in ((0, 100), (157, 257), (265, 297), (297, 329), (345, 500)):
        _, _, padding = block[start:end].partition(b"\0")
        require(not any(padding), "tar-header")
    require(not any(block[500:512]), "tar-header")


class Inventory:
    def __init__(self):
        self.members = []
        self.paths = {}
        self.total = 0

    def add(self, name, data, directory=False):
        try:
            self._add(name, data, directory)
        except Rejected as error:
            error.member = len(self.members) + 1
            raise

    def _add(self, name, data, directory):
        safe_path(name)
        require(name not in self.paths, "duplicate-member")
        require(all(self.paths.get("/".join(name.split("/")[:i])) != "file"
                    for i in range(1, len(name.split("/")))), "member-path-collision")
        require(directory or not any(p.startswith(name + "/") for p in self.paths), "member-path-collision")
        require(len(self.members) < MAX_MEMBERS, "member-count")
        self.total += len(data)
        require(self.total <= MAX_TOTAL, "total-size")
        kind = "directory" if directory else content(name, data)
        if directory:
            require(name in ("protobuf-3", "protobuf-4", "diagnostics") and not data, "directory-inventory")
        self.paths[name] = "directory" if directory else "file"
        self.members.append({"path": name, "format": kind, "bytes": len(data), "sha256": digest(data)})

    def archive(self, data):
        require(len(data) <= MAX_TOTAL, "archive-size")
        # Normalized gzip: no filename, comment, extra headers, or original timestamp.
        require(len(data) >= 18 and data[:4] == b"\x1f\x8b\x08\x00"
                and data[4:8] == bytes(4), "gzip-header")
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        limit = MAX_TOTAL + MAX_MEMBERS * 1024 + 10240
        raw = decoder.decompress(data, limit + 1)
        require(len(raw) <= limit and decoder.eof and not decoder.unused_data
                and not decoder.unconsumed_tail, "gzip-stream")
        require(len(raw) % 512 == 0, "tar-alignment")
        offset = 0
        while offset < len(raw):
            block = raw[offset:offset + 512]
            if block == bytes(512):
                require(len(raw) - offset >= 1024 and not any(raw[offset:]), "tar-trailer")
                break
            require(block[257:265] == b"ustar\x0000", "tar-format")
            member = tarfile.TarInfo.frombuf(block, "utf-8", "strict")
            require(member.type in (tarfile.REGTYPE, tarfile.DIRTYPE), "tar-member-type")
            tar_header(block)
            require(member.uid == member.gid == member.mtime == 0
                    and not member.uname and not member.gname and not member.linkname
                    and not member.devmajor and not member.devminor, "tar-metadata")
            require(0 <= member.size <= MAX_FILE, "member-size")
            end = offset + 512 + member.size
            padded = (end + 511) // 512 * 512
            require(padded <= len(raw) and not any(raw[end:padded]), "tar-padding")
            self.add(member.name.rstrip("/") if member.isdir() else member.name,
                     raw[offset + 512:end], member.isdir())
            offset = padded
        else:
            raise Rejected("tar-trailer")
        require(any(p == "file" for p in self.paths.values()), "empty-evidence")


def report(inventory, source):
    return {"formatVersion": 1, "policyVersion": 1,
            "validatorSha256": digest(Path(__file__).read_bytes()),
            "automatedChecksPassed": True, "humanReviewRequired": True,
            "source": source, "members": sorted(inventory.members, key=lambda m: m["path"])}


def write_report(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def check(source, destination):
    source, destination = Path(source), Path(destination)
    require(not destination.exists() and not destination.is_symlink(), "output-exists")
    require(not source.is_symlink(), "input-symlink")
    if source.is_dir():
        require(source.resolve() not in destination.resolve().parents, "output-inside-input")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".evidence-check-", dir=destination.parent) as temporary:
        stage = Path(temporary)
        payload = stage / "evidence"
        payload.mkdir()
        inventory = Inventory()
        if source.is_dir():
            for relative, data, directory in directory_members(source):
                inventory.add(relative, data, directory)
                if directory:
                    (payload / relative).mkdir()
                else:
                    (payload / relative).write_bytes(data)
            require(any(p == "file" for p in inventory.paths.values()), "empty-evidence")
            origin = {"kind": "directory"}
        else:
            safe_path(source.name)
            require(source.name.endswith(".tar.gz"), "input-format")
            data = regular_bytes(source, MAX_TOTAL)
            inventory.archive(data)
            (payload / source.name).write_bytes(data)
            origin = {"kind": "tar.gz", "path": source.name, "bytes": len(data), "sha256": digest(data)}
        write_report(stage / "inventory.json", report(inventory, origin))
        # No destination exists until the entire input has passed; failures remove the private copy.
        require(not destination.exists(), "output-exists")
        stage.rename(destination)


def git(*args):
    return subprocess.run(["git", *args], check=True, stdout=subprocess.PIPE,
                          stderr=subprocess.DEVNULL).stdout


def repository(destination, staged=False):
    """Check retained evidence from Git blobs, independent of working-tree substitutions."""
    require(not Path(destination).exists(), "output-exists")
    records = git("ls-files", "--stage", "-z", "--", EVIDENCE_ROOT).split(b"\0") if staged else git("ls-tree", "-rz", "HEAD", "--", EVIDENCE_ROOT).split(b"\0")
    archives = []
    for record in filter(None, records):
        metadata, raw_name = record.split(b"\t", 1)
        mode, second, third = metadata.decode("ascii").split()
        require(mode == "100644", "repository-file-mode")
        if staged:
            require(third == "0", "unmerged-index")
            blob = second
        else:
            require(second == "blob", "repository-object")
            blob = third
        name = raw_name.decode("utf-8")
        safe_path(name)
        if name.endswith("/README.md"):
            continue
        require(name.endswith(".tar.gz"), "repository-inventory")
        archives.append((name, blob))
    require(bool(archives), "repository-empty")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".evidence-repository-", dir=destination.parent) as temporary:
        stage = Path(temporary)
        for index, (name, blob) in enumerate(archives):
            require(int(git("cat-file", "-s", blob)) <= MAX_TOTAL, "archive-size")
            data = git("cat-file", "blob", blob)
            inventory = Inventory()
            inventory.archive(data)
            write_report(stage / (str(index) + ".json"), report(inventory, {
                "kind": "git-index" if staged else "git-head", "path": name,
                "gitBlob": blob, "bytes": len(data), "sha256": digest(data)}))
        require(not destination.exists(), "output-exists")
        stage.rename(destination)


def main(argv=None):
    class Arguments(argparse.ArgumentParser):
        def error(self, message):
            raise Rejected("arguments")
    parser = Arguments(description=__doc__)
    parser.add_argument("mode", choices=("check", "repository", "staged"))
    parser.add_argument("paths", nargs="+")
    try:
        args = parser.parse_args(argv)
        require(len(args.paths) == (2 if args.mode == "check" else 1), "arguments")
        if args.mode == "check":
            check(*args.paths)
        else:
            repository(args.paths[0], args.mode == "staged")
    except Rejected as error:
        location = " (member " + str(error.member) + ")" if error.member else ""
        print("Evidence rejected: " + str(error) + location, file=sys.stderr)
        return 1
    except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError,
            csv.Error, tarfile.TarError, zlib.error, subprocess.SubprocessError):
        # Decoder and filesystem exception messages can echo sensitive values or paths.
        print("Evidence rejected: unreadable-or-malformed-input", file=sys.stderr)
        return 1
    print("Evidence checks passed; human content review remains required.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
