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

import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path


def version(value):
    """Parse a released Flink 2.x version for comparison."""
    if not re.fullmatch(r"2\.\d+\.\d+", value):
        sys.exit("Binary compatibility requires released Flink 2.x versions")
    return tuple(map(int, value.split(".")))


def fingerprint(reports, major):
    """Collect successful test identities, requiring the reports' Protobuf runtime to have the given major."""
    cases = []
    for report in sorted(reports.glob("TEST-*.xml")):
        suite = ET.parse(report).getroot()
        runtime = {p.get("name"): p.get("value") for p in suite.iter("property")}
        if not runtime.get("protobuf.test.protobuf.version", "").startswith(f"{major}."):
            sys.exit(f"{report} did not run with Protobuf {major}")
        for case in suite.iter("testcase"):
            if any(case.find(tag) is not None for tag in ("failure", "error", "skipped")):
                sys.exit(f"Unsuccessful test in {report}")
            cases.append((case.attrib["classname"], case.attrib["name"]))
    if not cases:
        sys.exit("Missing test inventory")
    return sorted(cases)


def bytecode(jar):
    """Hash the packaged jar and compiled production and test classes."""
    files = [jar]
    variants = sorted(path.name for path in Path("src/test/runtime-app/proto").iterdir() if path.is_dir())
    for root in ("target/classes", "target/test-classes",
                 *(f"target/runtime-app/{variant}/classes" for variant in variants)):
        classes = sorted(Path(root).rglob("*.class"))
        if not classes:
            sys.exit(f"Missing compiled classes in {root}")
        files.extend(classes)
    for variant in variants:
        application = Path(f"target/runtime-app/application-{variant}.jar")
        if not application.is_file():
            sys.exit(f"Missing isolated application jar: {application}")
        files.append(application)
        files.append(Path(f"target/runtime-app/{variant}/schema.pb"))
    dynamic = Path("target/runtime-app/application-dynamic.jar")
    if not dynamic.is_file():
        sys.exit(f"Missing isolated application jar: {dynamic}")
    files.append(dynamic)
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}


def run(command, flink, jar, major, expected):
    """Run the compiled tests on the given Flink runtime without recompiling them, then compare."""
    reports = Path("target/surefire-reports")
    shutil.rmtree(reports, ignore_errors=True)
    before = bytecode(jar)
    subprocess.run(
        command + [
            f"-Dflink.version={flink}",
            f"-Dtest.production.classes={jar}",
            "surefire:test@default-test",
            "surefire:test@integration-tests",
        ],
        check=True,
    )
    if fingerprint(reports, major) != expected:
        sys.exit(f"Test inventory on Flink {flink} with Protobuf {major} differs from the Protobuf 4 build")
    if bytecode(jar) != before:
        sys.exit(f"Library jar or compiled classes changed during the run on Flink {flink} with Protobuf {major}")


def main():
    """Build a line's jar with Protobuf 4 and verify it unchanged on the 2.x ceiling and Protobuf 3."""
    line, ceiling = (sys.argv[1:] + [""])[:2]
    pom = ET.parse("pom.xml").getroot()
    if line == "flink2":
        compile_version = pom.findtext("{*}properties/{*}flink.version")
        if version(ceiling) <= version(compile_version):
            sys.exit("Expected a ceiling above the POM floor")
        runtimes = [compile_version, ceiling]
    elif line == "flink1" and not ceiling:
        compile_version = next(p.findtext("{*}properties/{*}flink.version")
                               for p in pom.iterfind("{*}profiles/{*}profile") if p.findtext("{*}id") == "flink1")
        runtimes = [compile_version]
    else:
        sys.exit("Expected flink2 with a 2.x ceiling, or flink1 without one")

    protobuf4 = ["./mvnw", "-ntp", "-Pprotobuf4", f"-Dflink.compat={line}"]
    subprocess.run(protobuf4 + ["clean", "verify"], check=True)
    jars = list(Path("target").glob("*.jar"))
    if len(jars) != 1:
        sys.exit("Expected exactly one packaged library jar after clean verify")
    cases = fingerprint(Path("target/surefire-reports"), 4)
    # Every rerun loads this copy, which stays outside target/ while the Protobuf 3 build cleans it.
    with tempfile.TemporaryDirectory(prefix="binary-compat-") as temporary:
        jar = Path(temporary) / jars[0].name
        shutil.copy2(jars[0], jar)
        library = hashlib.sha256(jar.read_bytes()).hexdigest()
        for flink in runtimes[1:]:
            run(protobuf4, flink, jar, 4, cases)
        # Regenerate and recompile the tests and runtime applications with Protobuf 3 once.
        protobuf3 = ["./mvnw", "-ntp", "-Pprotobuf3", f"-Dflink.compat={line}"]
        subprocess.run(protobuf3 + ["clean", "process-test-classes"], check=True)
        for flink in runtimes:
            run(protobuf3, flink, jar, 3, cases)
        if hashlib.sha256(jar.read_bytes()).hexdigest() != library:
            sys.exit("The Protobuf 4 library jar changed during the runs")
    Path("target/binary-compat-tests.json").write_text(json.dumps(cases, indent=2) + "\n")
    print(f"Verified the unchanged Protobuf 4 {line} jar on Flink {' and '.join(runtimes)}"
          f" with Protobuf 4 and 3: {len(cases)} tests")


if __name__ == "__main__":
    main()
