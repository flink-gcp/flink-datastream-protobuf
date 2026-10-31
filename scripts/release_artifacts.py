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

"""Rehearse and inspect the Central bundles of both artifact lines (ADR-0006).

Usage: release_artifacts.py <version> <new-output> [--unsigned]

Exports the committed HEAD, runs ``just release-stage`` for ``<version>`` and
``<version>-1.20`` with ``-DskipUpload=true``, keeps each bundle, checks that the
release-line guard rejects a mislabelled build, and inspects both bundles against the
exported sources. It reads no Central credentials and uploads nothing.
"""

import hashlib
import io
import os
import posixpath
import re
import shutil
import subprocess
import sys
import tarfile
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter
from pathlib import Path

GROUP = "io.github.flink-gcp"
ARTIFACT = "flink-datastream-protobuf"
LTS_SUFFIX = "-1.20"
VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
STAGED_BUNDLE = Path("target/central-publishing/central-bundle.zip")
GUARD_MESSAGE = "The release version and toolchain disagree"
REQUIRED_CHECKSUMS = (".md5", ".sha1")
CHECKSUMS = {".md5": "md5", ".sha1": "sha1", ".sha256": "sha256", ".sha512": "sha512"}
SIGNATURE = ".asc"
CLASS_MAJOR = 61  # Java release 17
NOTICES = ("LICENSE", "NOTICE")
POM_NAMESPACE = {"m": "http://maven.apache.org/POM/4.0.0"}
CONSUMER_SCOPES = {"provided", "test"}
# Internal names that would put Kryo, Chill, or Flink's generic fallback on a production path; every
# UTF-8 constant is searched, so a literal in internal slash form also fails, but dotted names do not.
FORBIDDEN_TYPES = re.compile(
    r"com/esotericsoftware/|com/twitter/chill/|org/apache/flink/api/java/typeutils/runtime/kryo/"
    r"|org/apache/flink/api/java/typeutils/GenericTypeInfo")
# Flink members such as ExecutionConfig.registerKryoType (1.20) or SerializerConfig.isForceKryoEnabled
# configure or inspect the Kryo fallback.
FORBIDDEN_FLINK_MEMBER = re.compile(r"kryo", re.IGNORECASE)
CONSTANT_SIZES = {3: 4, 4: 4, 5: 8, 6: 8, 7: 2, 8: 2, 9: 4, 10: 4, 11: 4, 12: 4, 15: 3, 16: 2,
                  17: 4, 18: 4, 19: 2, 20: 2}


def line_compat(version):
    """The production adapter that the given release version must contain."""
    return "flink1" if version.endswith(LTS_SUFFIX) else "flink2"


def production_sources(source_root, version):
    """Map each production source path to its bytes for the version's artifact line."""
    sources = {}
    for root in (Path(source_root) / "src/main/java",
                 Path(source_root) / f"src/main/java-{line_compat(version)}"):
        for path in sorted(root.rglob("*.java")):
            sources[path.relative_to(root).as_posix()] = path.read_bytes()
    if not sources:
        raise SystemExit(f"No production sources under {source_root}")
    return sources


def class_references(data):
    """Return the class-file major version and the forbidden references.

    UTF-8 constants hold class names, descriptors, and signatures, including those of declared
    members outside the constant pool's own references, so all of them are searched.
    """
    if data[:4] != b"\xca\xfe\xba\xbe":
        raise ValueError("not a class file")
    major = int.from_bytes(data[6:8], "big")
    count = int.from_bytes(data[8:10], "big")
    utf8, constants, offset, index = {}, {}, 10, 1
    while index < count:
        tag = data[offset]
        if tag == 1:
            length = int.from_bytes(data[offset + 1:offset + 3], "big")
            utf8[index] = data[offset + 3:offset + 3 + length].decode("utf-8", "replace")
            offset += 3 + length
        elif tag in CONSTANT_SIZES:
            body = data[offset + 1:offset + 1 + CONSTANT_SIZES[tag]]
            if len(body) != CONSTANT_SIZES[tag]:
                raise ValueError("truncated constant pool")
            constants[index] = (tag, [int.from_bytes(body[i:i + 2], "big") for i in range(0, len(body) - 1, 2)])
            offset += 1 + CONSTANT_SIZES[tag]
        else:
            raise ValueError(f"unknown constant tag {tag}")
        index += 2 if tag in (5, 6) else 1
    found = {value for value in utf8.values() if FORBIDDEN_TYPES.search(value)}
    for tag, refs in constants.values():
        if tag in (9, 10, 11):
            owner = utf8.get(constants.get(refs[0], (0, [0]))[1][0], "")
            member = utf8.get(constants.get(refs[1], (0, [0]))[1][0], "")
            if owner.startswith("org/apache/flink/") and FORBIDDEN_FLINK_MEMBER.search(member):
                found.add(f"{owner}.{member}")
    return major, sorted(found)


def read_zip(data, label, errors):
    """Read an archive's file entries, reporting an unreadable archive or a repeated name."""
    try:
        with zipfile.ZipFile(data if isinstance(data, Path) else io.BytesIO(data)) as archive:
            infos = [info for info in archive.infolist() if not info.filename.endswith("/")]
            entries = {info.filename: archive.read(info) for info in infos}
    except zipfile.BadZipFile:
        errors.append(f"{label} is not a readable archive")
        return {}
    for name, count in sorted(Counter(info.filename for info in infos).items()):
        if count > 1:
            errors.append(f"{label} repeats entry {name}")
    return entries


def notice_entry(name):
    return name in (f"META-INF/{notice}" for notice in NOTICES)


def check_notices(entries, label, source_root, errors):
    """Require the tracked LICENSE and NOTICE bytes under META-INF."""
    for name in NOTICES:
        packaged = entries.get(f"META-INF/{name}")
        if packaged is None:
            errors.append(f"{label} has no META-INF/{name}")
        elif packaged != (Path(source_root) / name).read_bytes():
            errors.append(f"{label} META-INF/{name} differs from the tracked {name}")


def metadata_entry(name):
    """Whether an archive entry is manifest or Maven metadata that every jar may carry."""
    return name == "META-INF/MANIFEST.MF" or name in (
        f"META-INF/maven/{GROUP}/{ARTIFACT}/pom.xml", f"META-INF/maven/{GROUP}/{ARTIFACT}/pom.properties")


def inspect_jar(entries, sources, source_root, errors):
    """Check that the main jar holds only production classes and the notices."""
    expected = {path.removesuffix(".java") for path in sources}
    found = set()  # owners whose own class file, not only a nested one, is present
    for name, data in sorted(entries.items()):
        if metadata_entry(name) or notice_entry(name):
            continue
        # Classes are owned by the source file of their outermost class, named in the last path segment.
        directory, _, base = name.rpartition("/")
        owner = f"{directory}/{base.removesuffix('.class').split('$', 1)[0]}"
        if not name.endswith(".class") or owner not in expected:
            errors.append(f"jar has non-production entry {name}")
            continue
        if name == owner + ".class":
            found.add(owner)
        try:
            major, forbidden = class_references(data)
        except (ValueError, IndexError) as error:
            errors.append(f"jar entry {name} is unreadable: {error}")
            continue
        if major != CLASS_MAJOR:
            errors.append(f"{name} has class-file major {major}, not {CLASS_MAJOR} (Java 17)")
        if forbidden:
            errors.append(f"{name} references {', '.join(forbidden)}")
    for owner in sorted(expected - found):
        errors.append(f"jar has no class for {owner}.java")
    check_notices(entries, "jar", source_root, errors)


def inspect_sources(entries, sources, source_root, errors):
    """Check that the sources jar holds exactly the line's production sources."""
    for name, data in sorted(entries.items()):
        if metadata_entry(name) or notice_entry(name):
            continue
        if name not in sources:
            errors.append(f"sources jar has non-production entry {name}")
        elif data != sources[name]:
            errors.append(f"sources jar entry {name} differs from the exported source")
    for name in sorted(set(sources) - set(entries)):
        errors.append(f"sources jar has no {name}")
    check_notices(entries, "sources jar", source_root, errors)


def inspect_javadoc(entries, errors):
    """Require the generated API index and package summary."""
    for name in ("index.html", "io/github/flink/gcp/protobuf/package-summary.html"):
        if name not in entries:
            errors.append(f"javadoc jar has no {name}")


def inspect_pom(data, version, errors):
    """Check coordinates, Central metadata, and the dependency scopes consumers see."""
    try:
        project = ET.fromstring(data)
    except ET.ParseError as error:
        errors.append(f"POM is not readable XML: {error}")
        return

    def text(node, path):
        found = node.find(path, POM_NAMESPACE)
        return None if found is None or found.text is None else found.text.strip()

    for path, expected in (("m:groupId", GROUP), ("m:artifactId", ARTIFACT), ("m:version", version)):
        if text(project, path) != expected:
            errors.append(f"POM {path[2:]} is {text(project, path)!r}, expected {expected!r}")
    if text(project, "m:packaging") not in (None, "jar"):
        errors.append(f"POM packaging is {text(project, 'm:packaging')!r}")
    for path in ("m:name", "m:description", "m:url", "m:licenses/m:license/m:name",
                 "m:developers/m:developer/m:name", "m:scm/m:url", "m:scm/m:connection"):
        if not text(project, path):
            errors.append(f"POM has no {path.replace('m:', '')}")
    dependencies = project.findall("m:dependencies/m:dependency", POM_NAMESPACE)
    for profile in project.findall("m:profiles/m:profile", POM_NAMESPACE):
        dependencies += profile.findall("m:dependencies/m:dependency", POM_NAMESPACE)
    for dependency in dependencies:
        scope = text(dependency, "m:scope") or "compile"
        if scope not in CONSUMER_SCOPES:
            coordinates = f"{text(dependency, 'm:groupId')}:{text(dependency, 'm:artifactId')}"
            errors.append(f"POM dependency {coordinates} has scope {scope}, not provided or test")


def verify_checksums(name, files, errors, required):
    """Check every present checksum of one file, requiring MD5 and SHA-1 if asked."""
    for suffix, algorithm in CHECKSUMS.items():
        recorded = files.get(name + suffix)
        if recorded is None:
            if required and suffix in REQUIRED_CHECKSUMS:
                errors.append(f"{name} has no {suffix} checksum")
            continue
        actual = hashlib.new(algorithm, files[name]).hexdigest()
        if recorded.decode("ascii", "replace").split()[0:1] != [actual]:
            errors.append(f"{name}{suffix} does not match the file")


def inspect_bundle(bundle, version, source_root, unsigned=False):
    """Return the defects of one Central bundle for the given release version."""
    errors = []
    files = read_zip(Path(bundle), "bundle", errors)
    directory = f"{GROUP.replace('.', '/')}/{ARTIFACT}/{version}"
    base = f"{directory}/{ARTIFACT}-{version}"
    primary = {"pom": base + ".pom", "jar": base + ".jar",
               "sources": base + "-sources.jar", "javadoc": base + "-javadoc.jar"}
    signed = [] if unsigned else [name + SIGNATURE for name in primary.values()]
    allowed = set(primary.values()) | {name + SIGNATURE for name in primary.values()}
    allowed |= {name + suffix for name in set(allowed) for suffix in CHECKSUMS}
    for name in sorted(set(files) - allowed):
        errors.append(f"bundle has unexpected file {name}")
    # Central requires checksums of the artifacts, not of their signatures.
    for name in [*primary.values(), *signed]:
        if name not in files:
            errors.append(f"bundle has no {posixpath.basename(name)}")
        else:
            verify_checksums(name, files, errors, required=not name.endswith(SIGNATURE))
    if any(name not in files for name in primary.values()):
        return errors

    sources = production_sources(source_root, version)
    inspect_pom(files[primary["pom"]], version, errors)
    inspect_jar(read_zip(files[primary["jar"]], "jar", errors), sources, source_root, errors)
    inspect_sources(read_zip(files[primary["sources"]], "sources jar", errors), sources, source_root, errors)
    inspect_javadoc(read_zip(files[primary["javadoc"]], "javadoc jar", errors), errors)
    return errors


def export_head(source):
    """Extract the committed HEAD, which the release workflow builds from its tag."""
    revision = subprocess.run(["git", "rev-parse", "HEAD"], check=True, capture_output=True,
                              text=True).stdout.strip()
    archive = subprocess.run(["git", "archive", "--format=tar", revision], check=True,
                             capture_output=True).stdout
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(source, filter="data")
    return revision


def stage(source, line, extra):
    """Run one release-stage build in the exported tree, failing with the line named."""
    result = subprocess.run(["just", "release-stage", line, *extra], cwd=source, check=False)
    if result.returncode:
        raise SystemExit(f"release-stage {line} failed with exit status {result.returncode}")


def check_guard(source, version, extra):
    """Require the release-line guard to reject the LTS version built with the 2.x adapter."""
    # The last -D wins over the recipe's flink1 selection; skipUpload stays last.
    result = subprocess.run(["just", "release-stage", version + LTS_SUFFIX, "-Dflink.compat=flink2", *extra],
                            cwd=source, check=False, capture_output=True, text=True)
    if result.returncode == 0 or GUARD_MESSAGE not in result.stdout + result.stderr:
        print(result.stdout + result.stderr, file=sys.stderr)
        raise SystemExit("The release-line guard did not reject a -1.20 version built with flink2")


def rehearse(version, output, unsigned):
    """Stage both lines from the committed HEAD without uploading, then inspect them."""
    if not VERSION.fullmatch(version):
        raise SystemExit("Rehearse a bare X.Y.Z version; the X.Y.Z-1.20 line is staged with it")
    output = Path(output)
    if output.exists():
        raise SystemExit(f"{output} exists; choose a new output directory")
    if not unsigned and not (os.environ.get("MAVEN_GPG_KEY") and os.environ.get("MAVEN_GPG_PASSPHRASE")):
        raise SystemExit("Signing needs MAVEN_GPG_KEY and MAVEN_GPG_PASSPHRASE; pass --unsigned to skip it")
    source, bundles = output / "source", output / "bundles"
    source.mkdir(parents=True)
    bundles.mkdir()
    revision = export_head(source)
    print(f"Rehearsing {version} and {version}{LTS_SUFFIX} from {revision}", flush=True)
    # skipUpload comes last; the build reads no Central credentials and uploads nothing.
    extra = [*(["-Dgpg.skip=true"] if unsigned else []), "-DskipUpload=true"]
    defects = []
    for line in (version, version + LTS_SUFFIX):
        staged = source / STAGED_BUNDLE
        staged.unlink(missing_ok=True)
        stage(source, line, extra)
        if not staged.is_file():
            raise SystemExit(f"The {line} build wrote no {STAGED_BUNDLE}")
        kept = bundles / f"central-bundle-{line}.zip"
        shutil.copyfile(staged, kept)
        defects += [f"{line}: {error}" for error in inspect_bundle(kept, line, source, unsigned)]
    check_guard(source, version, extra)
    summary = (f"Rehearsed `{version}` and `{version}{LTS_SUFFIX}` from {revision}"
               f"{' without signatures' if unsigned else ''}: {len(defects)} defects.\n")
    summary += "".join(f"- {defect}\n" for defect in defects)
    print(summary, end="")
    if destination := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(destination, "a", encoding="utf-8") as stream:
            stream.write(f"### Central bundle rehearsal\n\n{summary}")
    return 1 if defects else 0


def main(argv):
    if len(argv) in (2, 3) and argv[2:] in ([], ["--unsigned"]):
        return rehearse(argv[0], argv[1], unsigned=len(argv) == 3)
    print("usage: release_artifacts.py <version> <new-output> [--unsigned]", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
