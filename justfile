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

import 'dev-tools.just'
import 'benchmarks.just'

set shell := ["bash", "-euo", "pipefail", "-c"]

dev_tools_revision := "c3784bce1f95dc75848b1fc2de0f370e5c0a09ea"

mvn := "./mvnw -ntp"

# Show the available development commands.
default:
    @just --list

# Refresh the four shared workflow skills from the reviewed upstream commit.
skills-sync: (install-skills dev_tools_revision)

# Run Maven verification; arguments precede the final lifecycle phase.
[positional-arguments]
verify *args:
    {{ mvn }} "$@" verify

# Clean when switching generated code and runtime between Protobuf majors.
[positional-arguments]
verify-protobuf major *args:
    test "{{ major }}" = 3 || test "{{ major }}" = 4
    {{ mvn }} clean -Pprotobuf{{ major }} "${@:2}" verify

# Select the matching source adapter and clean the paired Protobuf build.
[positional-arguments]
verify-flink version major="3":
    bash scripts/verify-flink.sh "$1" "$2"

# Run the unchanged floor jar and compiled test inventory on a newer 2.x runtime.
[positional-arguments]
binary-compat ceiling major="3":
    python3 scripts/binary-compat.py "$1" "$2"

# Opt in to the isolated Chill comparison as well as the native probes.
probe-chill:
    {{ mvn }} clean -Pchill -Dtest.excluded.groups= verify

# Apply the Java formatter before committing.
format:
    {{ mvn }} spotless:apply

# Lint workflows, shell scripts, and Markdown with the same tool versions as CI.
lint:
    mise x actionlint shellcheck -- actionlint
    mise x shellcheck -- shellcheck scripts/*.sh
    mise x npm:markdownlint-cli2 -- markdownlint-cli2

# Pin new or updated GitHub Actions to commit SHAs.
pin-actions:
    mise x pinact -- pinact run
