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

# Capture an unpublished development bundle from a clean committed checkout on JDK 17.
[positional-arguments]
baseline-capture line major output:
    java src/test/java/io/github/flink/gcp/protobuf/BaselineTool.java capture "$1" "$2" "$3"

# Check a retained bundle without Maven, recompilation, or network access.
[positional-arguments]
baseline-check bundle:
    java -cp "$1/tools.jar" io.github.flink.gcp.protobuf.BaselineTool check "$1"

# Run retained bytecode and state under the selected runtime and the current JDK.
[positional-arguments]
baseline-read bundle version output:
    java -cp "$1/tools.jar" io.github.flink.gcp.protobuf.BaselineTool read "$1" "$2" "$3"
