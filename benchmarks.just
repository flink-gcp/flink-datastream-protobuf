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
benchmark_thrift_version := "0.25.0"
benchmark_thrift_sha256 := "66da4707214c54c94bac082103dc67adaf9e08925662700f269170a7b534b214"

# Build the pinned compiler in an explicit external cache. Requires CMake, Flex, Bison 3.8+ and C++.
[positional-arguments]
benchmark-tools cache:
    #!/usr/bin/env bash
    set -euo pipefail
    cache="$1"
    mkdir -p "$cache"
    version='{{benchmark_thrift_version}}'
    archive="$cache/thrift-$version.tar.gz"
    expected='{{benchmark_thrift_sha256}}'
    if [[ ! -f "$archive" ]] || [[ "$(shasum -a 256 "$archive" | awk '{print $1}')" != "$expected" ]]; then
        download=$(mktemp "$archive.XXXXXX")
        trap 'rm -f "$download"' EXIT
        curl --fail --location --retry 3 --retry-delay 2 --output "$download" "https://archive.apache.org/dist/thrift/$version/thrift-$version.tar.gz"
        actual=$(shasum -a 256 "$download" | awk '{print $1}')
        [[ "$actual" == "$expected" ]]
        mv "$download" "$archive"
        trap - EXIT
    fi
    if [[ ! -f "$cache/thrift-$version/.benchmark-extracted" ]]; then
        tar -xf "$archive" -C "$cache"
        touch "$cache/thrift-$version/.benchmark-extracted"
    fi
    cmake_args=()
    if [[ "$(uname -s)" == Darwin ]]; then
        sdk=$(xcrun --show-sdk-path)
        cmake_args+=("-DCMAKE_OSX_SYSROOT=$sdk" "-DCMAKE_CXX_FLAGS=-isystem $sdk/usr/include/c++/v1")
    fi
    # CMake refuses a build directory configured from another source tree, such as an earlier pinned version.
    if [[ -f "$cache/build/CMakeCache.txt" ]] && ! grep -q "^CMAKE_HOME_DIRECTORY:INTERNAL=.*/thrift-$version\$" "$cache/build/CMakeCache.txt"; then
        rm -rf "$cache/build"
    fi
    cmake -S "$cache/thrift-$version" -B "$cache/build" "${cmake_args[@]}" -DBUILD_COMPILER=ON -DBUILD_LIBRARIES=OFF -DBUILD_TESTING=OFF -DBUILD_TUTORIALS=OFF -DCMAKE_BUILD_TYPE=Release
    cmake --build "$cache/build" --target thrift-compiler --parallel 2
    just benchmark-tools-check "$cache/build/compiler/cpp/bin/thrift"

# Reject a compiler that is not the pinned version or cannot generate the benchmark corpus.
[positional-arguments]
benchmark-tools-check compiler:
    #!/usr/bin/env bash
    set -euo pipefail
    compiler="$1"
    if [[ ! -f "$compiler" || ! -x "$compiler" ]]; then
        printf 'Benchmark compiler is not an executable file: %s\n' "$compiler" >&2
        exit 1
    fi
    actual=$("$compiler" --version)
    if [[ "$actual" != 'Thrift version {{benchmark_thrift_version}}' ]]; then
        printf 'Benchmark generation requires Thrift {{benchmark_thrift_version}}; got %s\n' "$actual" >&2
        exit 1
    fi
    output=$(mktemp -d)
    trap 'rm -rf "$output"' EXIT
    "$compiler" --gen java:generated_annotations=suppress -out "$output" src/benchmark/thrift/benchmark.thrift
    if [[ -z "$(find "$output" -name '*.java' -type f -print -quit)" ]]; then
        printf 'Benchmark compiler generated no Java sources: %s\n' "$compiler" >&2
        exit 1
    fi
    printf '%s\n' "$actual"

# Print the CI cache key suffix for a compiler that benchmark-tools would build on this host.
[private]
benchmark-tools-identity:
    #!/usr/bin/env bash
    set -euo pipefail
    case "$(uname -s)" in
        Linux) release=$(. /etc/os-release && printf '%s-%s' "$ID" "$VERSION_ID") ;;
        Darwin) release="macos-$(sw_vers -productVersion)" ;;
        *) printf 'Unsupported benchmark compiler host: %s\n' "$(uname -s)" >&2; exit 1 ;;
    esac
    digest() { shasum -a 256 | cut -c 1-16; }
    recipe=$(just --justfile '{{justfile()}}' --show benchmark-tools | digest)
    toolchain=$("${CXX:-c++}" --version | digest)
    printf 'thrift-%s-%s-recipe-%s-%s-%s-cxx-%s\n' '{{benchmark_thrift_version}}' "$(printf %s '{{benchmark_thrift_sha256}}' | cut -c 1-16)" "$recipe" "$release" "$(uname -m)" "$toolchain"

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
    if [[ "$major" != 3 && "$major" != 4 ]]; then
        printf 'Unsupported benchmark Protobuf major: %s\n' "$major" >&2
        exit 1
    fi
    if [[ -e "$output" ]]; then
        printf 'Benchmark output already exists; choose a new directory: %s\n' "$output" >&2
        exit 1
    fi
    case "$version" in
        1.20.4) adapter=flink1 ;;
        2.2.1|2.3.0) adapter=flink2 ;;
        *) printf 'Unsupported benchmark Flink version: %s\n' "$version" >&2; exit 1 ;;
    esac
    args=(-Pbenchmarks "-Pprotobuf$major" "-Dflink.compat=$adapter" "-Dflink.version=$version" "-Dbenchmark.thrift.compiler={{benchmark_thrift}}")
    ./mvnw -ntp clean "${args[@]}" -Dtest=BenchmarkContractTest package
    ./mvnw -ntp "${args[@]}" "-Dbenchmark.mode=$mode" "-Dbenchmark.output=$output" exec:exec@run-benchmarks

# Validate a directory or normalized tar.gz and retain the exact checked copy and inventory.
[positional-arguments]
benchmark-evidence-check input output:
    python3 scripts/benchmark_evidence.py check "$1" "$2"

# Inspect retained evidence from HEAD without copying the original archives to upload paths.
[positional-arguments]
benchmark-evidence-repository output:
    python3 scripts/benchmark_evidence.py repository "$1"

# Inspect the exact staged evidence blobs immediately before an evidence commit.
[positional-arguments]
benchmark-evidence-staged output:
    python3 scripts/benchmark_evidence.py staged "$1"

# Exercise the content gate with synthetic safe and unsafe inputs only.
benchmark-evidence-test:
    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -v
