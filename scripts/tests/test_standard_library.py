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
import unittest

SCRIPTS = Path(__file__).resolve().parents[1]
LOAD = """
import importlib.util, sys
spec = importlib.util.spec_from_file_location("tool", sys.argv[1])
spec.loader.exec_module(importlib.util.module_from_spec(spec))
"""


class StandardLibraryTest(unittest.TestCase):
    def test_tools_import_only_the_standard_library(self):
        tools = sorted(SCRIPTS.glob("*.py"))
        self.assertTrue(tools)
        for tool in tools:
            with self.subTest(tool=tool.name):
                result = subprocess.run([sys.executable, "-I", "-S", "-c", LOAD, str(tool)],
                                        stdin=subprocess.DEVNULL, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
