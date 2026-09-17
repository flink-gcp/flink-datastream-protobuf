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
version="$1"; major="$2"
case "$version" in
  1.20.*) compat=flink1 ;;
  2.*) compat=flink2 ;;
  *) echo 'Expected Flink 1.20.x or 2.x' >&2; exit 1 ;;
esac
test "$major" = 3 || test "$major" = 4
repository=$(python3 -c 'from pathlib import Path; import sys; p=Path(sys.argv[1]).resolve(); r=Path.cwd().resolve(); sys.exit("Use an external Maven repository") if p == r or r in p.parents else print(p)' "$3")
mkdir -p "$repository"
identity="$compat-protobuf$major"
marker="$repository/.protobuf-example-build"
if [[ -f "$marker" && "$(cat "$marker")" != "$identity" ]]; then
  echo 'Use a separate Maven repository for each Flink adapter and Protobuf major' >&2
  exit 1
fi
printf '%s\n' "$identity" > "$marker"
library_version=$(python3 -c 'import xml.etree.ElementTree as E; print(E.parse("pom.xml").getroot().findtext("{*}version"))')
protobuf_version=$(python3 -c 'import xml.etree.ElementTree as E, sys; r=E.parse("pom.xml").getroot(); print(r.findtext("{*}properties/{*}protobuf.version") if sys.argv[1]=="3" else next(p.findtext("{*}properties/{*}protobuf.version") for p in r.findall("{*}profiles/{*}profile") if p.findtext("{*}id")=="protobuf4"))' "$major")
# The 2.x library is always compiled at the floor, including a ceiling consumer run.
./mvnw -ntp "-Pprotobuf$major" "-Dflink.compat=$compat" -DskipTests clean package
./mvnw -ntp org.apache.maven.plugins:maven-install-plugin:3.1.3:install-file "-Dfile=target/flink-datastream-protobuf-$library_version.jar" -DpomFile=pom.xml "-DlocalRepositoryPath=$repository"
consumer=(./mvnw -ntp -f examples/datastream/pom.xml "-Dmaven.repo.local=$repository" "-Dprotobuf.integration.version=$library_version" "-Dflink.version=$version" "-Dprotobuf.version=$protobuf_version")
"${consumer[@]}" clean verify
for entrypoint in explicit registered stateful; do
  "${consumer[@]}" "exec:exec@$entrypoint"
done
