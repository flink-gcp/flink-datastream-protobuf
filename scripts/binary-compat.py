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
import xml.etree.ElementTree as ET
from pathlib import Path


def version(value):
    """Parse a released Flink 2.x version for comparison."""
    if not re.fullmatch(r"2\.\d+\.\d+", value):
        sys.exit("Binary compatibility requires released Flink 2.x versions")
    return tuple(map(int, value.split(".")))


def fingerprint(reports):
    """Collect successful test identities from the current Surefire reports."""
    cases = []
    for report in sorted(reports.glob("TEST-*.xml")):
        suite = ET.parse(report).getroot()
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
    for root in ("target/classes", "target/test-classes",
                 "target/runtime-app/original/classes", "target/runtime-app/changed/classes"):
        classes = sorted(Path(root).rglob("*.class"))
        if not classes:
            sys.exit(f"Missing compiled classes in {root}")
        files.extend(classes)
    for variant in ("original", "changed"):
        application = Path(f"target/runtime-app/application-{variant}.jar")
        if not application.is_file():
            sys.exit(f"Missing isolated application jar: {application}")
        files.append(application)
        files.append(Path(f"target/runtime-app/{variant}/schema.pb"))
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}


def main():
    """Build at the floor and verify the unchanged jar on the ceiling runtime."""
    ceiling, major = sys.argv[1:]
    floor = ET.parse("pom.xml").findtext("{*}properties/{*}flink.version")
    if version(ceiling) <= version(floor) or major not in ("3", "4"):
        sys.exit("Expected a ceiling above the POM floor and Protobuf major 3 or 4")

    reports = Path("target/surefire-reports")
    command = ["./mvnw", "-ntp", f"-Pprotobuf{major}", "-Dflink.compat=flink2"]
    subprocess.run(command + [f"-Dflink.version={floor}", "clean", "verify"], check=True)

    jars = list(Path("target").glob("*.jar"))
    if len(jars) != 1:
        sys.exit("Expected exactly one packaged library jar after clean verify")
    jar = jars[0].resolve()

    floor_cases, floor_bytes = fingerprint(reports), bytecode(jar)
    Path("target/floor-tests.json").write_text(json.dumps(floor_cases, indent=2) + "\n")

    shutil.rmtree(reports)
    subprocess.run(
        command + [
            f"-Dflink.version={ceiling}",
            f"-Dtest.production.classes={jar}",
            "surefire:test@default-test",
            "surefire:test@integration-tests",
        ],
        check=True,
    )
    ceiling_cases = fingerprint(reports)
    Path("target/ceiling-tests.json").write_text(json.dumps(ceiling_cases, indent=2) + "\n")
    if ceiling_cases != floor_cases:
        sys.exit("Floor and ceiling test inventories differ")
    if bytecode(jar) != floor_bytes:
        sys.exit("Packaged jar or compiled classes changed during the runtime-only rerun")
    print(f"Verified unchanged Flink {floor} jar on {ceiling}: {len(floor_cases)} tests, Protobuf {major}")


if __name__ == "__main__":
    main()
