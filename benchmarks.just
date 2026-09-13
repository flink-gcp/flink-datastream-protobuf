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

# Benchmark tools are opt-in; the production build never needs Thrift or CMake.
benchmark_thrift := env_var_or_default("BENCHMARK_THRIFT_COMPILER", "thrift")

# Build the pinned compiler in an explicit external cache. Requires CMake, Flex, Bison 3.8+ and C++.
[positional-arguments]
benchmark-tools cache:
    #!/usr/bin/env bash
    set -euo pipefail
    cache="$1"
    mkdir -p "$cache"
    archive="$cache/thrift-0.23.0.tar.gz"
    expected=1859d932d2ae1f13d16c5a196931208c116310a5ff50f2bfd11d3db03be8f46f
    if [[ ! -f "$archive" ]] || [[ "$(shasum -a 256 "$archive" | awk '{print $1}')" != "$expected" ]]; then
        download=$(mktemp "$cache/thrift-0.23.0.tar.gz.XXXXXX")
        trap 'rm -f "$download"' EXIT
        curl --fail --location --retry 3 --retry-delay 2 --output "$download" https://archive.apache.org/dist/thrift/0.23.0/thrift-0.23.0.tar.gz
        actual=$(shasum -a 256 "$download" | awk '{print $1}')
        [[ "$actual" == "$expected" ]]
        mv "$download" "$archive"
        trap - EXIT
    fi
    if [[ ! -f "$cache/thrift-0.23.0/.benchmark-extracted" ]]; then
        tar -xf "$archive" -C "$cache"
        touch "$cache/thrift-0.23.0/.benchmark-extracted"
    fi
    cmake_args=()
    if [[ "$(uname -s)" == Darwin ]]; then
        sdk=$(xcrun --show-sdk-path)
        cmake_args+=("-DCMAKE_OSX_SYSROOT=$sdk" "-DCMAKE_CXX_FLAGS=-isystem $sdk/usr/include/c++/v1")
    fi
    cmake -S "$cache/thrift-0.23.0" -B "$cache/build" "${cmake_args[@]}" -DBUILD_COMPILER=ON -DBUILD_LIBRARIES=OFF -DBUILD_TESTING=OFF -DBUILD_TUTORIALS=OFF -DCMAKE_BUILD_TYPE=Release
    cmake --build "$cache/build" --target thrift-compiler --parallel 2
    "$cache/build/compiler/cpp/bin/thrift" --version

# Correctness and short JMH probes; no timing thresholds.
benchmark-smoke version major output:
    @just benchmark-execute smoke '{{version}}' '{{major}}' '{{output}}'

# Five independent blocks with allocation profiling and a parity report.
benchmark-run version major output:
    @just benchmark-execute run '{{version}}' '{{major}}' '{{output}}'

# Full corpus correctness without JMH measurements.
benchmark-check version major output:
    @just benchmark-execute check '{{version}}' '{{major}}' '{{output}}'

[private]
[positional-arguments]
benchmark-execute mode version major output:
    #!/usr/bin/env bash
    set -euo pipefail
    mode="$1"; version="$2"; major="$3"; output="$4"
    [[ "$major" == 3 || "$major" == 4 ]]
    [[ ! -e "$output" ]]
    case "$version" in
        1.20.4) adapter=flink1 ;;
        2.2.1|2.3.0) adapter=flink2 ;;
        *) printf 'Unsupported benchmark Flink version: %s\n' "$version" >&2; exit 1 ;;
    esac
    args=(-Pbenchmarks "-Pprotobuf$major" "-Dflink.compat=$adapter" "-Dflink.version=$version" "-Dbenchmark.thrift.compiler={{benchmark_thrift}}")
    ./mvnw -ntp clean "${args[@]}" -Dtest=BenchmarkContractTest package
    ./mvnw -ntp "${args[@]}" "-Dbenchmark.mode=$mode" "-Dbenchmark.output=$output" exec:exec@run-benchmarks
