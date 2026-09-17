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

set -euo pipefail
compat="$1"; major="$2"
test "$compat" = flink1 || test "$compat" = flink2
test "$major" = 3 || test "$major" = 4
./mvnw -ntp "-Dflink.compat=$compat" "-Pprotobuf$major" -DskipTests clean compile dependency:build-classpath -Dmdep.outputFile=target/javadoc-classpath
"$JAVA_HOME/bin/javadoc" --release 17 -Xdoclint:all -Werror -quiet -d target/apidocs -classpath "$(cat target/javadoc-classpath)" -sourcepath "src/main/java:src/main/java-$compat" -subpackages io.github.flink.gcp.protobuf
