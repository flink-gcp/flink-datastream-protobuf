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
case "$1" in
  1.20.*) compat=flink1 ;;
  2.*) compat=flink2 ;;
  *) echo 'Expected Flink 1.20.x or 2.x' >&2; exit 1 ;;
esac
just verify-protobuf "$2" "-Dflink.version=$1" "-Dflink.compat=$compat"
