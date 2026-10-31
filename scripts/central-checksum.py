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

"""Compare a resolved library jar and POM with the SHA-1 checksums that Maven Central publishes.

Usage: central-checksum.py <maven-repository> <version>

Maven settings can add repositories ahead of Central, so an empty local repository alone does
not prove that a published version came from Central.
"""

import hashlib
import sys
import urllib.error
import urllib.request
from pathlib import Path

CENTRAL = "https://repo1.maven.org/maven2"
PATH = "io/github/flink-gcp/flink-datastream-protobuf/{version}/flink-datastream-protobuf-{version}"


def mismatches(repository, version, base=CENTRAL):
    """Return the resolved files whose SHA-1 differs from Central's, or that are missing."""
    path = PATH.format(version=version)
    problems = []
    for suffix in (".jar", ".pom"):
        resolved = Path(repository) / (path + suffix)
        if not resolved.is_file():
            problems.append(f"{path}{suffix} was not resolved")
            continue
        try:
            with urllib.request.urlopen(f"{base}/{path}{suffix}.sha1", timeout=60) as response:
                expected = response.read().decode("ascii", "replace").split()[0:1]
        except urllib.error.URLError as error:
            problems.append(f"{path}{suffix}.sha1 is not available from {base}: {error}")
            continue
        if [hashlib.sha1(resolved.read_bytes()).hexdigest()] != expected:
            problems.append(f"The resolved {path}{suffix} differs from {base}")
    return problems


def main(argv):
    if len(argv) != 2:
        print("usage: central-checksum.py <maven-repository> <version>", file=sys.stderr)
        return 2
    problems = mismatches(*argv)
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
