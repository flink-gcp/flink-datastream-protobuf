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

"""Read, review, assemble, and verify a milestone's retained development bundles for its pre-release.

The work directory holds bundles/<bundle>/ from baseline-capture and README.md from the maintainer.
`read` writes verification/, `review` writes PUBLICATION-REVIEW.json for the reviewer to complete,
`assemble` writes release/, and `verify` compares the published assets with release/.
"""

import argparse
import bz2
import datetime
import fnmatch
import gzip
import hashlib
import importlib.util
import io
import json
import lzma
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
import zlib


def sibling(name):
    """Loads a sibling tool by path, which keeps the scripts runnable without making them a package."""
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# The retrieval tool defines the published layout and verifies it; this tool reuses its names.
fetch = sibling("baseline_fetch")
Rejected, require, sha256 = fetch.Rejected, fetch.require, fetch.sha256
REPOSITORY, BUNDLES, CHECKSUMS = fetch.REPOSITORY, fetch.BUNDLES, fetch.CHECKSUMS
DESIGNATION, INVENTORY = fetch.DESIGNATION, fetch.INVENTORY
# The benchmark evidence gate's bounded credential rules, compiled here for raw bytes.
CREDENTIAL_RULES = sibling("benchmark_evidence").CREDENTIAL_RULES
REVIEW = "PUBLICATION-REVIEW.json"
TAG = re.compile(r"v0\.[0-9]+\.[0-9]+")
TOOL = "io.github.flink.gcp.protobuf.BaselineTool"
# The Flink versions and JDKs that each artifact line is read with, as BaselineTool accepts them.
READERS = {"flink2": (("2.2.1", "2.3.0"), (17, 21)), "flink1": (("1.20.5",), (17,))}
# Each control must fail with its BaselineTool message before creating output.
NEGATIVE_CONTROLS = (
    ("negative-unsupported-flink", "flink2-pb3", "1.20.5", 17, "Unsupported Flink runtime",
     "Unsupported Flink runtime rejected before creating reader output"),
    ("negative-lts-jdk21", "flink1-pb3", "1.20.5", 21, "Unsupported JDK for artifact line",
     "JDK 21 rejected for the Flink 1.20 artifact line before creating reader output"),
)
# JVM diagnostics, credential files, and compressed files that the scan cannot open, none of which a
# retained input needs.
SENSITIVE_NAMES = ("hs_err_pid*", "*.hprof*", "*.jfr", "java_pid*", "*.dump", "core", "core.*", ".netrc",
                   "*.pem", "*.p12", "*.pfx", "*.key", "*.jks", "*.keystore", "credentials*.json", "settings.xml",
                   "*.env", "*.zst", "*.7z")
# Single-stream compression that the scan opens; tar archives inside them are opened as well.
STREAMS = ((b"\x1f\x8b", gzip.decompress), (b"BZh", bz2.decompress), (b"\xfd7zXZ\x00", lzma.decompress))
# Nested archives deeper than this are reported for review instead of being searched.
MAX_ARCHIVE_DEPTH = 4


def properties(path, unescape=True):
    """Reads the flat key=value files that BaselineTool writes, optionally unescaping colons."""
    values = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value.replace("\\:", ":") if unescape else value
    return values


def files_under(root, prefix):
    return [{"path": prefix + "/" + path.relative_to(root).as_posix(), "bytes": path.stat().st_size,
             "sha256": sha256(path)}
            for path in sorted(root.rglob("*")) if path.is_file() and not path.is_symlink()]


def bundle_directory(work, bundle):
    path = work / "bundles" / bundle
    require(path.is_dir() and not path.is_symlink(), "Missing captured bundle: " + str(path))
    return path


def run(command, cwd, log):
    """Runs a retained instrument without credentials in its environment and returns its exit status."""
    environment = {key: value for key, value in os.environ.items() if key not in fetch.CREDENTIALS}
    with open(log, "wb") as stream:
        return subprocess.run(command, cwd=cwd, stdin=subprocess.DEVNULL, stdout=stream,
                              stderr=subprocess.STDOUT, env=environment).returncode


def output(command, cwd=fetch.ROOT):
    result = subprocess.run(command, cwd=cwd, stdin=subprocess.DEVNULL, capture_output=True, text=True)
    require(result.returncode == 0, "Command failed: " + " ".join(command) + ": " + result.stderr.strip())
    return result.stdout


def reader_names(bundle, line):
    """Yields (name, Flink version, JDK) for each combination that the bundle's line is read with."""
    versions, jdks = READERS[line]
    for version in versions:
        for jdk in jdks:
            yield "read-%s-%s-jdk%d" % (bundle, version, jdk), version, jdk


def read(work, checkout, java17, java21):
    """Reads every bundle on each claimed combination from the work directory, then the controls.

    The capture checkout must no longer hold its build output, so a reader cannot pass by finding it.
    """
    require((checkout / ".git").exists(), "Not a capture checkout: " + str(checkout))
    require(not os.path.lexists(checkout / "target"),
            "Move the capture build output out of the checkout before reading: " + str(checkout / "target"))
    head = output(["git", "rev-parse", "HEAD"], cwd=checkout).strip()
    verification = work / "verification"
    require(not os.path.lexists(verification), "Verification output already exists: " + str(verification))
    javas = {17: java17, 21: java21}
    plan = []
    for bundle in BUNDLES:
        line = properties(bundle_directory(work, bundle) / "manifest.properties")["line"]
        plan += [(name, bundle, version, jdk, None, None) for name, version, jdk in reader_names(bundle, line)]
    plan += NEGATIVE_CONTROLS
    verification.mkdir()
    outcomes = []
    for name, bundle, version, jdk, message, label in plan:
        directory = bundle_directory(work, bundle)
        log = verification / (name + ".log")
        status = run([javas[jdk], "-cp", str(directory / "tools.jar"), TOOL, "read", str(directory), version,
                      str(verification / name)], cwd=work, log=log)
        outcomes.append({"name": name, "bundle": bundle, "flink_version": version, "jdk_major": jdk,
                         "exit": status, "output_created": (verification / name).exists(),
                         "negative_control": label})
        print("%s exit=%d" % (name, status))
    record = {"capture_checkout": {"path": str(checkout), "head": head}, "outcomes": outcomes}
    (verification / "readers.json").write_text(json.dumps(record, indent=1) + "\n", encoding="utf-8")
    checked_controls(verification, outcomes)
    failed = [o["name"] for o in outcomes if not o["negative_control"] and o["exit"] != 0]
    require(not failed, "Failed reader: " + ", ".join(failed))


def checked_controls(verification, outcomes):
    """Requires each control to have failed with its own message and without creating output."""
    by_name = {outcome["name"]: outcome for outcome in outcomes}
    for name, _, _, _, message, _ in NEGATIVE_CONTROLS:
        control = by_name.get(name, {})
        log = verification / (name + ".log")
        require(control.get("exit", 0) != 0 and control.get("output_created") is False
                and not os.path.lexists(verification / name)
                and log.is_file() and message in log.read_text(encoding="utf-8", errors="replace"),
                "Negative control did not reject with its message before creating output: " + name)


def members(name, data, findings, depth=0, whole=None):
    """Yields (name, searchable bytes) for a file and, recursively, for what its archives and streams hold.

    A file's bytes other than its archive members' contents, such as a launcher prefix, local headers,
    the central directory, a comment, or trailing data, are searched as the file itself, and its members
    as their own entries. An archive, stream, or member that cannot be read, or one nested too deeply to
    search, becomes a finding identified by its container's bytes. Like the evidence gate's rules, this
    supports human review of bytes this project produced; it does not detect deliberately hidden content.
    When given, whole(name, data) receives every file and member with its complete bytes, contents included.
    """
    if whole is not None:
        whole(name, data)
    expanded = expand(name, data, findings, depth >= MAX_ARCHIVE_DEPTH)
    if expanded is None:
        yield name, data
        return
    outside, nested = expanded
    yield name, outside
    for entry, body in nested:
        yield from members(entry, body, findings, depth + 1, whole)


def tar_header(data):
    """Returns whether data starts with a tar header whose checksum is valid, as every tar member has."""
    if len(data) < 512 or not data[148:156].strip(b" \0"):
        return False
    try:
        recorded = int(data[148:156].strip(b" \0").decode("ascii"), 8)
    except ValueError:
        return False
    return recorded == sum(data[:148]) + 8 * ord(" ") + sum(data[156:512])


def expand(name, data, findings, too_deep):
    """Returns the bytes outside the contents and the named contents of a stream, tar, or ZIP/JAR archive.

    Returns None for other data and for data that cannot be opened. An archive at the depth limit is
    reported instead of being opened.
    """
    stream = next((decompress for magic, decompress in STREAMS if data.startswith(magic)), None)
    tar = stream is None and tar_header(data)
    # zipfile also opens archives with prepended data, such as self-extracting launchers.
    archive = stream is None and not tar and zipfile.is_zipfile(io.BytesIO(data))
    if not (stream or tar or archive):
        if data[:4] == b"PK\x03\x04":
            findings.append(finding(name, "unreadable-archive", data))
        return None
    if too_deep:
        findings.append(finding(name, "archive-too-deep", data))
        return None
    if stream:
        try:
            content = stream(data)
        except (OSError, EOFError, ValueError, lzma.LZMAError, zlib.error):
            findings.append(finding(name, "unreadable-archive", data))
            return None
        # Name the content after its file, so that a compressed diagnostic keeps its own name.
        base = re.split(r"[/:]", name)[-1]
        for suffix, replacement in ((".tgz", ".tar"), (".gz", ""), (".bz2", ""), (".xz", "")):
            if base.endswith(suffix) and len(base) > len(suffix):
                return data, [(name + ":" + base[:-len(suffix)] + replacement, content)]
        # The compressed bytes stay searchable, since the decompressors ignore some trailing data.
        return data, [(name + ":<stream>", content)]
    if tar:
        try:
            with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
                entries = [(name + ":" + entry.name, archive.extractfile(entry).read())
                           for entry in archive if entry.isfile()]
                end = archive.offset
        except (tarfile.TarError, OSError, EOFError, ValueError):
            findings.append(finding(name, "unreadable-archive", data))
            return None
        return data[end:], entries
    try:
        opened = zipfile.ZipFile(io.BytesIO(data))
    except (zipfile.BadZipFile, NotImplementedError, RuntimeError, EOFError, zlib.error):
        findings.append(finding(name, "unreadable-archive", data))
        return None
    entries, contents = [], []
    with opened:
        for info in opened.infolist():
            # The member's compressed content follows its local header and is searched once decompressed.
            header = data[info.header_offset:info.header_offset + 30]
            if len(header) == 30 and header[:4] == b"PK\x03\x04":
                start = info.header_offset + 30 + int.from_bytes(header[26:28], "little") \
                    + int.from_bytes(header[28:30], "little")
                contents.append((start, start + info.compress_size))
            if info.is_dir():
                continue
            try:
                entries.append((name + ":" + info.filename, opened.read(info)))
            except (zipfile.BadZipFile, NotImplementedError, RuntimeError, EOFError, zlib.error):
                findings.append(finding(name + ":" + info.filename, "unreadable-archive-member", data))
    outside, position = bytearray(), 0
    for start, stop in sorted(contents):
        outside += data[position:max(position, start)]
        position = max(position, stop)
    return bytes(outside + data[position:]), entries


def finding(path, kind, data):
    """Identifies a finding by the bytes it was found in, so a review cannot outlive a change to them."""
    return {"path": path, "kind": kind, "sha256": hashlib.sha256(data).hexdigest()}


def patterns():
    return {name: re.compile(pattern.encode()) for name, pattern in CREDENTIAL_RULES.items()}


def scan(work):
    """Returns the files with sensitive names and the findings in every bundle file and reader output.

    Each flagged file is identified by its name and the SHA-256 of its complete bytes, including the
    contents of an archive, like a finding.
    """
    verification = work / "verification"
    require(verification.is_dir() and not verification.is_symlink(), "Run read before review: " + str(verification))
    rules = patterns()
    roots = [(bundle_directory(work, bundle), bundle) for bundle in BUNDLES] + [(verification, "verification")]
    flagged, findings = [], []

    def sensitive(name):
        return any(fnmatch.fnmatch(re.split(r"[/:]", name)[-1], pattern) for pattern in SENSITIVE_NAMES)

    def flag(name, data):
        if sensitive(name):
            flagged.append(finding(name, "sensitive-name", data))

    for root, prefix in roots:
        for path in sorted(root.rglob("*")):
            # A link could expose or, when the evidence is staged, overwrite a file outside the work directory.
            require(not path.is_symlink(), "Symbolic links are not retained inputs: " + str(path))
            if not path.is_file():
                continue
            relative = prefix + "/" + path.relative_to(root).as_posix()
            for name, data in members(relative, path.read_bytes(), findings, whole=flag):
                findings += [finding(name, kind, data) for kind, pattern in rules.items() if pattern.search(data)]
    # A member that cannot be read keeps its name, which is flagged by the bytes of its archive.
    flagged += [dict(f, kind="sensitive-name") for f in findings
                if f["kind"] == "unreadable-archive-member" and sensitive(f["path"])]
    return flagged, findings


def review(work):
    """Writes the publication review with every finding unresolved, for the reviewer to classify."""
    target = work / REVIEW
    require(not os.path.lexists(target), "Publication review already exists: " + str(target))
    flagged, findings = scan(work)
    record = {"scope": "All retained bundle files and nested ZIP/JAR members, plus reader outputs",
              "raw_jvm_diagnostics_or_credential_files": flagged, "reviewed_files": [],
              "reviewed_matches": [], "unresolved_matches": findings}
    target.write_text(json.dumps(record, indent=1) + "\n", encoding="utf-8")
    print("%d flagged files and %d unresolved matches in %s" % (len(flagged), len(findings), target))
    print("Inspect each entry; move a harmless file to reviewed_files and a harmless match to reviewed_matches,")
    print("each with a disposition, before assemble.")


def checked_review(work, readme):
    """Rescans the work directory and requires the review to explain exactly what the scan finds now.

    A flagged file and a finding are explained only by an entry with the same path, rule, and SHA-256 and a
    disposition. Only a flagged member of a bundle's source archive, which the tagged revision already
    publishes, can be explained; a diagnostic or credential file elsewhere always blocks assembly. The README
    and the dispositions are published too, so they must not match a credential rule.
    """
    record = json.loads((work / REVIEW).read_text(encoding="utf-8"))
    require(record.get("raw_jvm_diagnostics_or_credential_files") == []
            and record.get("unresolved_matches") == [],
            "Publication review has flagged files or unresolved matches: " + str(work / REVIEW))
    reviewed, files = record.get("reviewed_matches", []), record.get("reviewed_files", [])
    require(all(entry.get("disposition", "").strip() for entry in reviewed + files),
            "Every reviewed match or file needs a disposition")
    require(all(entry.get("path", "").partition("/source.zip:")[0] in BUNDLES
                and ":" not in entry.get("path", "").partition("/source.zip:")[2] for entry in files),
            "Only members of a bundle's source archive can be reviewed files")
    published = [readme.encode()] + [entry.get("disposition", "").encode() for entry in reviewed + files]
    require(not any(pattern.search(text) for text in published for pattern in patterns().values()),
            "README.md or a disposition matches a credential rule")
    flagged, findings = scan(work)

    def identities(entries):
        return sorted((entry.get("path"), entry.get("kind"), entry.get("sha256")) for entry in entries)

    require(identities(flagged) == identities(files),
            "Sensitive files do not match the reviewed files: " + ", ".join(f["path"] for f in flagged[:3]))
    require(identities(findings) == identities(reviewed),
            "Publication review does not match the current files; run review again")
    return record


def tar_directory(source, name, target):
    """Archives a directory under one top-level name, with sorted members and no owner names."""
    with tarfile.open(target, "x:gz", format=tarfile.PAX_FORMAT) as tar:
        for path in [source] + sorted(source.rglob("*")):
            member = name if path == source else name + "/" + path.relative_to(source).as_posix()
            # Retrieval rejects members whose names it does not accept, so assembly refuses them first.
            fetch.relative(member)
            info = tar.gettarinfo(str(path), arcname=member)
            require(info.isfile() or info.isdir(), "Unsupported file type: " + str(path))
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            if info.isfile():
                with open(path, "rb") as data:
                    tar.addfile(info, data)
            else:
                tar.addfile(info)


def clean_verify_tests(directory):
    """Counts the tests of the capture build's surefire summaries, which must all have passed."""
    log = (directory / "build.log").read_text(encoding="utf-8", errors="replace")
    require("BUILD SUCCESS" in log and not any(re.search(r"(Failures|Errors|Skipped): [1-9]", line)
                                              for line in log.splitlines() if "Tests run:" in line),
            "Capture build log does not report a successful build without failed or skipped tests: "
            + str(directory))
    total = sum(int(count) for count in re.findall(r"^\[INFO\] Tests run: (\d+), Failures: 0, Errors: 0, "
                                                    r"Skipped: 0$", log, re.MULTILINE))
    require(total > 0, "Capture build log reports no tests: " + str(directory))
    return total


def github_run(run_id):
    return json.loads(output(["gh", "run", "view", str(run_id), "--repo", REPOSITORY, "--json",
                              "conclusion,headSha,headBranch,event,workflowName,jobs,url"]))


def job_log(run_id, job_id):
    return output(["gh", "run", "view", str(run_id), "--repo", REPOSITORY, "--job", str(job_id), "--log"])


def checked_ci(run_id, revision):
    """Requires the run to be the main-branch CI push run that tested the tagged commit, with its gate passed.

    A push run checks out the branch tip, which can be newer than the commit that triggered it, so the
    tested commit is read from the checkout's log in the job that freezes the source revision.
    """
    ci = github_run(run_id)
    require(ci.get("workflowName") == "CI" and ci.get("event") == "push" and ci.get("headBranch") == "main"
            and ci.get("headSha") == revision and ci.get("conclusion") == "success"
            and any(job["name"] == "CI passed" and job["conclusion"] == "success" for job in ci.get("jobs", [])),
            "Run " + str(run_id) + " is not a successful main-branch CI push run of " + revision)
    freeze = [job for job in ci["jobs"] if job["name"] == "Freeze source revision"]
    require(len(freeze) == 1, "Run " + str(run_id) + " has no single source-freezing job")
    tested = re.findall(r"git log -1 --format=%H[ \t]*\n[^\n]*?\b([0-9a-f]{40})[ \t]*$",
                        job_log(run_id, freeze[0]["databaseId"]), re.MULTILINE)
    require(tested == [revision], "Run " + str(run_id) + " did not test " + revision)
    return ci


def tag_identity(tag, cwd=fetch.ROOT):
    require(output(["git", "cat-file", "-t", "refs/tags/" + tag], cwd).strip() == "tag",
            "Tag is not annotated: " + tag)
    return (output(["git", "rev-parse", "refs/tags/" + tag], cwd).strip(),
            output(["git", "rev-parse", "refs/tags/" + tag + "^{commit}"], cwd).strip())


def bundle_record(work, bundle, revision):
    """Checks a captured bundle and returns what the designation records about it."""
    directory = bundle_directory(work, bundle)
    fetch.baseline_check(directory)
    raw = properties(directory / "manifest.properties", unescape=False)
    values = properties(directory / "manifest.properties")
    require(values.get("source.revision") == revision and values.get("milestone.final") == "false"
            and values.get("status") == "development-tooling-baseline",
            "Bundle was not captured as a development baseline of " + revision + ": " + bundle)
    require(bundle == "%s-pb%s" % (values.get("line"), values.get("protobuf.major")),
            "Bundle name does not match its artifact line and Protobuf profile: " + bundle)
    return {"directory": directory, "name": bundle, "manifest": raw, "values": values,
            "inventory_sha256": sha256(directory / CHECKSUMS), "files": files_under(directory, bundle),
            "clean_verify_tests": clean_verify_tests(directory),
            "captures": [{"flink_version": path.parent.name.removeprefix("capture-"),
                          "cases": int(properties(path)["case.count"])}
                         for path in sorted(directory.glob("capture-*/result.properties"))]}


def checked_readers(work, bundles, revision):
    """Checks that each claimed combination passed with the retained jars and each control failed."""
    verification = work / "verification"
    record = json.loads((verification / "readers.json").read_text(encoding="utf-8"))
    require(record["capture_checkout"]["head"] == revision,
            "Readers did not run with the build output of the tagged capture checkout moved away")
    outcomes = {o["name"]: o for o in record["outcomes"]}
    results = []
    for bundle, recorded in bundles.items():
        cases = {capture["flink_version"]: capture["cases"] for capture in recorded["captures"]}
        for name, version, jdk in reader_names(bundle, recorded["values"]["line"]):
            require(outcomes.get(name, {}).get("exit") == 0, "Missing or failed reader: " + name)
            result = properties(verification / name / "result.properties")
            require(result.get("status") == "passed" and result.get("flink.version") == version
                    and result.get("java.version", "").split(".")[0].split("+")[0] == str(jdk)
                    and result.get("library.sha256") == recorded["values"]["library.sha256"]
                    and result.get("consumer.sha256") == recorded["values"]["consumer.sha256"]
                    and result.get("case.count") == str(cases.get(version)),
                    "Reader result does not match its retained bundle: " + name)
            results.append({"bundle": bundle, "flink_version": version, "jdk_major": jdk, "result": result})
    checked_controls(verification, record["outcomes"])
    return results


def designation(tag, revision, tag_object, source, bundles, readers, ci, verification):
    return {
        "format_version": 1,
        "milestone": tag,
        "milestone_final": True,
        "source_revision": revision,
        "annotated_tag_object": tag_object,
        "source_archive_sha256": source,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "retention_url": "https://github.com/%s/releases/tag/%s" % (REPOSITORY, tag),
        "maven_central_publication": False,
        "embedded_artifact_version": bundles[BUNDLES[0]]["values"]["writer.artifact"].rsplit(":", 1)[-1],
        "original_bundle_manifests_unchanged": True,
        "designation": "This external manifest selects the exact retained tooling bundles as the final"
                       " milestone baseline. Embedded milestone.final=false fields remain unchanged.",
        "ci_url": ci["url"],
        "build_outputs_absent_during_readers": True,
        "bundles": [{key: recorded[key] for key in ("name", "asset", "manifest", "inventory_sha256")}
                    | {"file_count": len(recorded["files"]),
                       "uncompressed_bytes": sum(f["bytes"] for f in recorded["files"]),
                       "clean_verify_tests": recorded["clean_verify_tests"], "captures": recorded["captures"]}
                    for recorded in bundles.values()],
        "readers": readers,
        "negative_controls": [label for *_, label in NEGATIVE_CONTROLS],
        "verification_asset": {"file": verification.name, "bytes": verification.stat().st_size,
                               "sha256": sha256(verification)},
    }


def evidence_directory(work, target, bundles, ci, review_record):
    """Writes the verification archive's contents: reader outputs, audits, the CI jobs, and the review."""
    shutil.copytree(work / "verification", target, symlinks=True)
    for bundle, recorded in bundles.items():
        (target / ("audit-%s.json" % bundle)).write_text(json.dumps({"files": recorded["files"]}, indent=1) + "\n",
                                                        encoding="utf-8")
    reader_files = []
    for directory in sorted((work / "verification").glob("read-*")):
        if directory.is_dir():
            reader_files += files_under(directory, directory.name)
    (target / "audit-readers.json").write_text(json.dumps({"files": reader_files}, indent=1) + "\n",
                                               encoding="utf-8")
    jobs = [{"conclusion": job["conclusion"], "name": job["name"], "url": job["url"]} for job in ci["jobs"]]
    (target / "ci.json").write_text(json.dumps({"conclusion": ci["conclusion"], "headSha": ci["headSha"],
                                                "jobs": jobs}, separators=(",", ":")) + "\n", encoding="utf-8")
    (target / REVIEW).write_text(json.dumps(review_record, indent=1) + "\n", encoding="utf-8")


def assemble(tag, work, run_id):
    require(TAG.fullmatch(tag), "Expected a development milestone tag such as v0.2.0: " + tag)
    release = work / "release"
    require(not os.path.lexists(release), "Release output already exists: " + str(release))
    require(not (work / "README.md").is_symlink() and not (work / REVIEW).is_symlink(),
            "README.md and the review must be files in the work directory")
    readme = (work / "README.md").read_text(encoding="utf-8")
    tag_object, revision = tag_identity(tag)
    require(tag in readme and revision in readme and tag_object in readme,
            "README.md must name the tag, its commit, and its tag object")
    review_record = checked_review(work, readme)
    bundles = {bundle: bundle_record(work, bundle, revision) for bundle in BUNDLES}
    sources = {sha256(recorded["directory"] / "source.zip") for recorded in bundles.values()}
    require(len(sources) == 1, "Bundles retain different source archives")
    readers = checked_readers(work, bundles, revision)
    ci = checked_ci(run_id, revision)
    # Stage beside the output so a failure leaves no partial release to mistake for a complete one.
    stage = Path(tempfile.mkdtemp(prefix=".release.", dir=work))
    claimed = False
    try:
        assets = stage / "assets"
        assets.mkdir()
        for bundle, recorded in bundles.items():
            archive = assets / fetch.archive_name(tag, bundle)
            tar_directory(recorded["directory"], bundle, archive)
            recorded["asset"] = {"file": archive.name, "bytes": archive.stat().st_size, "sha256": sha256(archive)}
        evidence_directory(work, stage / "verification", bundles, ci, review_record)
        verification = assets / ("%s-verification.tar.gz" % tag)
        tar_directory(stage / "verification", "verification", verification)
        (assets / INVENTORY).write_text(json.dumps({b: r["files"] for b, r in bundles.items()}, indent=1) + "\n",
                                        encoding="utf-8")
        final = designation(tag, revision, tag_object, sources.pop(), bundles, readers, ci, verification)
        (assets / DESIGNATION).write_text(json.dumps(final, indent=1) + "\n", encoding="utf-8")
        (assets / "README.md").write_text(readme, encoding="utf-8")
        sums = "".join("%s  %s\n" % (sha256(path), path.name) for path in sorted(assets.iterdir()))
        (assets / CHECKSUMS).write_text(sums, encoding="utf-8")
        # Claim the name first, because rename silently replaces an empty directory created meanwhile.
        try:
            os.mkdir(release)
        except FileExistsError:
            raise Rejected("Release output appeared during assembly: " + str(release)) from None
        claimed = True
        os.rename(assets, release)
    except BaseException:
        if claimed:
            try:
                os.rmdir(release)
            except OSError:
                pass
        raise
    finally:
        shutil.rmtree(stage, ignore_errors=True)
    print("Assembled %s; pin %s for %s in scripts/baseline_fetch.py" % (release, sha256(release / CHECKSUMS), tag))


def verify(tag, work):
    """Downloads every published asset and requires the names and bytes to equal release/."""
    release = work / "release"
    local = {path.name: sha256(path) for path in release.iterdir() if path.is_file()}
    with tempfile.TemporaryDirectory() as temporary:
        # Without patterns, gh downloads every asset of the release.
        fetch.download(tag, [], Path(temporary))
        published = {path.name: sha256(path) for path in Path(temporary).iterdir()}
    require(published == local, "Published assets differ from " + str(release))
    print("Verified %d published assets of %s against %s" % (len(published), tag, release))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    reading = commands.add_parser("read", help="read every bundle from the work directory")
    reading.add_argument("work")
    reading.add_argument("--capture-checkout", required=True, help="the tagged worktree that captured the bundles")
    reading.add_argument("--java17", required=True, help="java executable of JDK 17")
    reading.add_argument("--java21", required=True, help="java executable of JDK 21")
    commands.add_parser("review", help="write the publication review candidates").add_argument("work")
    assembling = commands.add_parser("assemble", help="write the release assets")
    assembling.add_argument("tag")
    assembling.add_argument("work")
    assembling.add_argument("ci_run", help="the successful main-branch CI run of the tagged commit")
    verifying = commands.add_parser("verify", help="compare the published assets with release/")
    verifying.add_argument("tag")
    verifying.add_argument("work")
    args = parser.parse_args(argv)
    work = Path(args.work).absolute()
    try:
        require(work.is_dir(), "Work directory does not exist: " + str(work))
        if args.command == "read":
            read(work, Path(args.capture_checkout).absolute(), args.java17, args.java21)
        elif args.command == "review":
            review(work)
        elif args.command == "assemble":
            assemble(args.tag, work, args.ci_run)
        else:
            verify(args.tag, work)
    except Rejected as error:
        print("baseline-release: " + str(error), file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, TypeError, tarfile.TarError) as error:
        print("baseline-release: unreadable or malformed input: " + repr(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
