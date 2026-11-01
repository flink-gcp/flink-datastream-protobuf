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

# Stands in for `gh` in test_release_workflow.py: appends one argument per line and a separator
# to $STUB_GH_CALLS, then exits with $STUB_GH_EXIT if it is set. Otherwise an attempt's jobs query
# prints $STUB_JOBS/<attempt>, the conclusion that gh's --jq filter would print, or nothing when
# that file is absent; `release view` prints $STUB_RELEASE_UNSTABLE and exits with $STUB_RELEASE_VIEW;
# `release create` exits with $STUB_RELEASE_CREATE.
set -euo pipefail
{ printf '%s\n' "$@"; printf '%s\n' '----'; } >> "$STUB_GH_CALLS"
if [[ -n "${STUB_GH_EXIT:-}" ]]; then
  exit "$STUB_GH_EXIT"
fi
case "$1 ${2:-}" in
  "api --paginate")
    attempt=${3%/jobs}
    attempt=${attempt##*/}
    if [[ -f "$STUB_JOBS/$attempt" ]]; then
      cat "$STUB_JOBS/$attempt"
    fi
    ;;
  "release view")
    if [[ "${STUB_RELEASE_VIEW:-1}" == 0 ]]; then
      echo "$STUB_RELEASE_UNSTABLE"
    fi
    exit "${STUB_RELEASE_VIEW:-1}"
    ;;
  "release create")
    exit "${STUB_RELEASE_CREATE:-0}"
    ;;
  *)
    echo "release-gh-stub: unexpected command: $*" >&2
    exit 64
    ;;
esac
