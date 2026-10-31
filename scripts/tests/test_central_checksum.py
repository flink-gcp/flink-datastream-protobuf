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

"""A local HTTP server stands in for Maven Central."""

import functools
import hashlib
import http.server
from pathlib import Path
import threading

import pytest
from conftest import load_script

central_checksum = load_script("central-checksum.py")

VERSION = "1.0.0"
PATH = central_checksum.PATH.format(version=VERSION)


@pytest.fixture
def central(tmp_path):
    root = tmp_path / "central"
    root.mkdir()
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(root))
    handler.log_message = lambda *args: None
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield root, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def publish(root, suffix, data):
    target = root / (PATH + suffix + ".sha1")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(hashlib.sha1(data).hexdigest() + "  " + Path(PATH).name + suffix + "\n")


def resolve(repository, suffix, data):
    target = repository / (PATH + suffix)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


def test_accepts_files_matching_central(tmp_path, central):
    root, base = central
    for suffix, data in ((".jar", b"jar"), (".pom", b"<project/>")):
        publish(root, suffix, data)
        resolve(tmp_path / "repository", suffix, data)
    assert central_checksum.mismatches(tmp_path / "repository", VERSION, base) == []


def test_rejects_a_jar_from_another_repository(tmp_path, central):
    root, base = central
    publish(root, ".jar", b"central jar")
    publish(root, ".pom", b"<project/>")
    resolve(tmp_path / "repository", ".jar", b"locally built jar")
    resolve(tmp_path / "repository", ".pom", b"<project/>")
    assert central_checksum.mismatches(tmp_path / "repository", VERSION, base) == [
        f"The resolved {PATH}.jar differs from {base}"]


def test_reports_missing_files_and_checksums(tmp_path, central):
    root, base = central
    publish(root, ".jar", b"jar")
    resolve(tmp_path / "repository", ".pom", b"<project/>")
    problems = central_checksum.mismatches(tmp_path / "repository", VERSION, base)
    assert problems[0] == f"{PATH}.jar was not resolved"
    assert problems[1].startswith(f"{PATH}.pom.sha1 is not available from {base}")


def test_cli_usage():
    assert central_checksum.main([]) == 2
