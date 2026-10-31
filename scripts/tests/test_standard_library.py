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

"""Import every repository Python tool without site-packages, where the test-only packages live."""

from pathlib import Path
import subprocess
import sys

import pytest

SCRIPTS = Path(__file__).resolve().parents[1]
TOOLS = sorted([*SCRIPTS.glob("*.py"), *SCRIPTS.glob("lib/*.py")])
# Isolated mode omits the script directory from the path, which the executables need for scripts/lib/.
LOAD = """
import importlib.util, sys
sys.path.insert(0, sys.argv[2])
spec = importlib.util.spec_from_file_location("tool", sys.argv[1])
spec.loader.exec_module(importlib.util.module_from_spec(spec))
"""


def test_finds_the_tools():
    assert TOOLS


@pytest.mark.parametrize("tool", TOOLS, ids=lambda tool: tool.relative_to(SCRIPTS).as_posix())
def test_tools_import_only_the_standard_library(tool):
    result = subprocess.run([sys.executable, "-I", "-S", "-c", LOAD, str(tool), str(SCRIPTS)],
                            stdin=subprocess.DEVNULL, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
