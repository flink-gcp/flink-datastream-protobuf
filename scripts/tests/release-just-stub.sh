#!/usr/bin/env bash
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

# Stands in for `just release-stage` in test_release_artifacts.py: records each call, answers the
# guard check as STUB_GUARD_STATUS and STUB_GUARD_OUTPUT say, and otherwise installs the prepared
# bundle for the requested version where the plugin would write it.
set -euo pipefail
printf '%s\n' "$*" >> "$STUB_CALLS"
test "$1" = release-stage
version=$2
if [[ "$version" == *-1.20 && " $* " == *" -Dflink.compat=flink2 "* ]]; then
  echo "$STUB_GUARD_OUTPUT"
  exit "$STUB_GUARD_STATUS"
fi
mkdir -p target/central-publishing
cp "$STUB_BUNDLES/central-bundle-$version.zip" target/central-publishing/central-bundle.zip
