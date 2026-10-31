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

"""Names, download, and checks shared by the tools that read retained bundles from a GitHub pre-release or release."""

import hashlib
import os
import re
import subprocess

REPOSITORY = "flink-gcp/flink-datastream-protobuf"
BUNDLES = ("flink2-pb3", "flink2-pb4", "flink1-pb3", "flink1-pb4")
CHECKSUMS = "SHA256SUMS"
DESIGNATION = "FINAL-BASELINE.json"
INVENTORY = "INVENTORY.json"
COMPONENT = re.compile(r"[A-Za-z0-9_.-]+")
# Bundle code runs after verification, but it never needs the download credentials.
CREDENTIALS = ("GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN")


class Rejected(Exception):
    """A verification failure; the destination is left absent."""


def require(condition, message):
    if not condition:
        raise Rejected(message)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for block in iter(lambda: file.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def relative(name):
    parts = name.split("/")
    require(all(COMPONENT.fullmatch(part) and part not in (".", "..") for part in parts),
            "Unsafe path: " + repr(name))
    return parts


def archive_name(tag, bundle):
    return tag + "-" + bundle + ".tar.gz"


def download(tag, names, directory):
    command = ["gh", "release", "download", tag, "--repo", REPOSITORY, "--dir", str(directory)]
    for name in names:
        command += ["--pattern", name]
    result = subprocess.run(command, stdin=subprocess.DEVNULL)
    require(result.returncode == 0, "gh release download failed for " + tag)


def baseline_check(bundle):
    environment = {key: value for key, value in os.environ.items() if key not in CREDENTIALS}
    command = ["java", "-cp", str(bundle / "tools.jar"), "io.github.flink.gcp.protobuf.BaselineTool",
               "check", str(bundle)]
    result = subprocess.run(command, stdin=subprocess.DEVNULL, env=environment)
    require(result.returncode == 0, "baseline-check failed for " + bundle.name)
