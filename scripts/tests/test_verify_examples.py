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

"""Argument checks of verify-examples.sh's published mode, which fail before any Maven run."""

from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/verify-examples.sh"
OTHER_LINE = "Use an X.Y.Z-1.20 version with Flink 1.20 and an X.Y.Z version with Flink 2.x"
NOT_RELEASED = "Expected a released X.Y.Z or X.Y.Z-1.20 version"


def run(tmp_path, flink, major, published, repository="repository"):
    return subprocess.run(["bash", str(SCRIPT), flink, major, str(tmp_path / repository), published],
                          cwd=ROOT, capture_output=True, text=True, check=False)


@pytest.mark.parametrize("flink, published, message", [
    ("2.2.1", "1.0.0-1.20", OTHER_LINE),
    ("2.3.0", "1.0.0-1.20", OTHER_LINE),
    ("1.20.5", "1.0.0", OTHER_LINE),
    ("1.20.5", "1.0.0-1.20.1", OTHER_LINE),
    ("2.2.1", "1.0", NOT_RELEASED),
    ("2.2.1", "v1.0.0", NOT_RELEASED),
    ("2.2.1", "1.0.0-SNAPSHOT", NOT_RELEASED),
    ("1.20.5", "01.0.0-1.20", NOT_RELEASED),
])
def test_rejects_versions_outside_the_runtime_line(tmp_path, flink, published, message):
    result = run(tmp_path, flink, "4", published)
    assert result.returncode == 1
    assert result.stderr.strip().endswith(message)
    assert not (tmp_path / "repository").exists()


def test_rejects_a_repository_that_could_hold_a_local_build(tmp_path):
    (tmp_path / "repository").mkdir()
    (tmp_path / "repository/io").mkdir()
    result = run(tmp_path, "2.2.1", "3", "1.0.0")
    assert result.returncode == 1
    assert result.stderr.strip().endswith("Use a new or empty Maven repository for a published version")
