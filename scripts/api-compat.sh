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

# Build the candidate with the bundle's adapter line, Protobuf profile, and compile Flink version,
# then compare its annotated API with the retained library.
set -euo pipefail
bundle=$(CDPATH='' cd -- "$1" && pwd -P)
# The current tree's checker validates the manifest's line and Protobuf profile.
java src/test/java/io/github/flink/gcp/protobuf/BaselineTool.java check "$bundle"
value() { sed -n "s/^$1=//p" "$bundle/manifest.properties"; }
./mvnw -ntp clean "-Pprotobuf$(value protobuf.major),api-compat" "-Dflink.compat=$(value line)" \
  "-Dflink.version=$(value compile.flink)" -DskipTests "-Dapi.baseline.jar=$bundle/library.jar" verify
