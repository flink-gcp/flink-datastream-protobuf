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
version="$1"; major="$2"; published="${4:-}"
case "$version" in
  1.20.*) compat=flink1 ;;
  2.*) compat=flink2 ;;
  *) echo 'Expected Flink 1.20.x or 2.x' >&2; exit 1 ;;
esac
test "$major" = 3 || test "$major" = 4
# The checks use only the standard library of the project's Python, without test packages.
python=(uv run --locked --isolated --no-dev python)
repository=$("${python[@]}" -c 'from pathlib import Path; import sys; p=Path(sys.argv[1]).resolve(); r=Path.cwd().resolve(); sys.exit("Use an external Maven repository") if p == r or r in p.parents else print(p)' "$3")
if [[ -n "$published" ]]; then
  # A published version resolves from Maven Central into an empty repository, never from a local build.
  case "$compat:$published" in
    flink1:*-1.20) ;;
    flink1:* | flink2:*-1.20) echo 'Use an X.Y.Z-1.20 version with Flink 1.20 and an X.Y.Z version with Flink 2.x' >&2; exit 1 ;;
  esac
  [[ "$published" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-1\.20)?$ ]] || { echo 'Expected a released X.Y.Z or X.Y.Z-1.20 version' >&2; exit 1; }
  if [[ -e "$repository" && -n "$(ls -A "$repository")" ]]; then
    echo 'Use a new or empty Maven repository for a published version' >&2
    exit 1
  fi
fi
mkdir -p "$repository"
identity="$compat-protobuf$major"
marker="$repository/.protobuf-example-build"
if [[ -f "$marker" && "$(cat "$marker")" != "$identity" ]]; then
  echo 'Use a separate Maven repository for each Flink adapter and Protobuf major' >&2
  exit 1
fi
printf '%s\n' "$identity" > "$marker"
library_version=${published:-$("${python[@]}" -c 'import xml.etree.ElementTree as E; print(E.parse("pom.xml").getroot().findtext("{*}version"))')}
protobuf_version=$("${python[@]}" -c 'import xml.etree.ElementTree as E, sys; r=E.parse("pom.xml").getroot(); print(r.findtext("{*}properties/{*}protobuf.version") if sys.argv[1]=="4" else next(p.findtext("{*}properties/{*}protobuf.version") for p in r.findall("{*}profiles/{*}profile") if p.findtext("{*}id")=="protobuf3"))' "$major")
if [[ -z "$published" ]]; then
  # The library is always compiled with Protobuf 4, and the 2.x library at the floor, as for a release.
  ./mvnw -ntp -Pprotobuf4 "-Dflink.compat=$compat" -DskipTests clean package
  ./mvnw -ntp org.apache.maven.plugins:maven-install-plugin:3.1.3:install-file "-Dfile=target/flink-datastream-protobuf-$library_version.jar" -DpomFile=pom.xml "-DlocalRepositoryPath=$repository"
fi
consumer=(./mvnw -ntp -f examples/datastream/pom.xml "-Dmaven.repo.local=$repository" "-Dprotobuf.integration.version=$library_version" "-Dflink.version=$version" "-Dprotobuf.version=$protobuf_version")
"${consumer[@]}" clean verify
if [[ -n "$published" ]]; then
  # Settings can add repositories ahead of Central, so compare what Maven resolved with Central's checksums.
  "${python[@]}" scripts/central-checksum.py "$repository" "$published"
fi
for entrypoint in explicit registered stateful dynamic; do
  "${consumer[@]}" "exec:exec@$entrypoint"
done
