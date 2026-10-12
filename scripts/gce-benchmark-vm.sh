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

# Runs one benchmark plan on a temporary VM created without a service account.
# scripts/gce_benchmark.py uploads the inputs and starts this script as a root systemd unit.
set -euo pipefail
# Command substitutions such as source=$(checkout ...) must also stop on the first failure.
shopt -s inherit_errexit

work=/srv/benchmark
input="$work/input"
state="$work/state"
results="$work/results"
# shellcheck source=/dev/null
source "$input/run.env"

# An interruption is final: later progress updates must not replace it.
status() {
    if [[ -f "$state/interrupted" && $1 != INTERRUPTED ]]; then return 0; fi
    printf '%s\n' "$*" > "$state/status"
    printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$state/events.log"
}

# Stop at the next step boundary when the maintenance watcher has invalidated the run.
check_interrupted() {
    if [[ -f "$state/interrupted" ]]; then return 1; fi
}

metadata() {
    curl --fail --silent --show-error --max-time 10 -H 'Metadata-Flavor: Google' \
        "http://metadata.google.internal/computeMetadata/v1/instance/$1"
}

cpu_counters() {
    awk '/^cpu / { idle = $5 + $6; total = 0; for (i = 2; i <= NF; i++) total += $i; print idle, total }' /proc/stat
}

vm_counter() {
    awk -v key="$1" '$1 == key { print $2 }' /proc/vmstat
}

# Wait until ten consecutive observations, about 30 s apart and each measuring CPU idle over two
# seconds, average >= 90% idle with none below 85%, load1 < 10, no swap-out, and at most 1 MiB
# swap-in; fail after GATE_TIMEOUT_MINUTES so a noisy VM cannot bill indefinitely.
gate() {
    local out=$1 window=() idle0 total0 idle1 total1 idle load ins outs prev_in prev_out ok mean minimum
    local deadline=$((SECONDS + ${GATE_TIMEOUT_MINUTES:-60} * 60))
    prev_in=$(vm_counter pswpin)
    prev_out=$(vm_counter pswpout)
    : > "$out"
    while true; do
        read -r idle0 total0 < <(cpu_counters)
        sleep 2
        read -r idle1 total1 < <(cpu_counters)
        idle=$(awk -v i=$((idle1 - idle0)) -v t=$((total1 - total0)) 'BEGIN { printf "%.2f", (t > 0 ? 100 * i / t : 0) }')
        load=$(awk '{ print $1 }' /proc/loadavg)
        ins=$(vm_counter pswpin)
        outs=$(vm_counter pswpout)
        ok=1
        awk -v l="$load" 'BEGIN { exit !(l < 10) }' || ok=0
        [[ $((outs - prev_out)) -eq 0 && $(((ins - prev_in) * 4096)) -le 1048576 ]] || ok=0
        printf '%s idle=%s load1=%s swapin_bytes=%s swapouts=%s sample_ok=%s\n' \
            "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$idle" "$load" $(((ins - prev_in) * 4096)) \
            $((outs - prev_out)) "$ok" >> "$out"
        prev_in=$ins
        prev_out=$outs
        if [[ $ok -eq 1 ]]; then window+=("$idle"); else window=(); fi
        if ((${#window[@]} > 10)); then window=("${window[@]:1}"); fi
        if ((${#window[@]} == 10)); then
            read -r mean minimum < <(printf '%s\n' "${window[@]}" |
                awk 'NR == 1 { m = $1 } { s += $1; if ($1 < m) m = $1 } END { print s / NR, m }')
            if awk -v s="$mean" -v m="$minimum" 'BEGIN { exit !(s >= 90 && m >= 85) }'; then
                # The root-owned gate survives the watcher; do not report success after an event.
                check_interrupted
                printf '%s gate passed mean_idle=%s min_idle=%s\n' \
                    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$mean" "$minimum" >> "$out"
                return 0
            fi
        fi
        if ((SECONDS >= deadline)); then
            printf '%s gate timed out\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$out"
            return 1
        fi
        check_interrupted
        sleep 28
    done
}

# GCE announces host maintenance before a TERMINATE-policy VM stops; any event invalidates the run.
# Hanging requests pass the last ETag so a change between two requests is not missed; a failed
# request is logged and retried rather than treated as an event.
watch_maintenance() {
    local event query etag='' headers="$state/maintenance-headers"
    while true; do
        query=maintenance-event
        if [[ -n $etag ]]; then query="maintenance-event?wait_for_change=true&last_etag=$etag"; fi
        if event=$(curl --fail --silent --show-error -D "$headers" -H 'Metadata-Flavor: Google' \
            "http://metadata.google.internal/computeMetadata/v1/instance/$query"); then
            etag=$(awk 'tolower($1) == "etag:" { gsub(/\r/, "", $2); print $2 }' "$headers")
        else
            printf '%s UNAVAILABLE\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$state/maintenance-events.log"
            sleep 5
            continue
        fi
        printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$event" >> "$state/maintenance-events.log"
        if [[ $event != NONE ]]; then
            touch "$state/interrupted"
            status INTERRUPTED "maintenance-event $event"
            # Keep stopping benchmark processes, so work the root runner launches after a check
            # cannot run; the runner's EXIT trap ends this loop.
            while true; do
                pkill -TERM -u benchmark || true
                sleep 1
            done
        fi
    done
}

tool_path="$work/.local/bin:/usr/local/bin:/usr/bin:/bin"
java_home=

# Run a command as the unprivileged benchmark user with a minimal environment.
# Checkouts never activate mise configuration; Java and just come from the pinned installs.
as_benchmark() {
    (cd "$work" && runuser -u benchmark -- env -i HOME="$work" LANG=C.UTF-8 PATH="$tool_path" \
        ${java_home:+JAVA_HOME="$java_home"} \
        BENCHMARK_THRIFT_COMPILER="$work/tools/build/compiler/cpp/bin/thrift" "$@")
}

prepare() {
    status PREPARING
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -q > "$state/apt.log" 2>&1
    apt-get install -y -q --no-install-recommends build-essential cmake flex bison git curl \
        ca-certificates python3 >> "$state/apt.log" 2>&1
    id benchmark > /dev/null 2>&1 || useradd --system --home-dir "$work" --shell /bin/bash benchmark
    mkdir -p "$work/.local/bin" "$results"
    chown -R benchmark: "$work"
    chown -R root: "$input" "$state"
    local binary="mise-v$MISE_VERSION-linux-x64" expected actual
    curl --fail --location --silent --show-error --retry 3 -o "$state/SHASUMS256.txt" \
        "https://github.com/jdx/mise/releases/download/v$MISE_VERSION/SHASUMS256.txt"
    curl --fail --location --silent --show-error --retry 3 -o "$work/.local/bin/mise" \
        "https://github.com/jdx/mise/releases/download/v$MISE_VERSION/$binary"
    expected=$(awk -v name="./$binary" '$2 == name || $2 == substr(name, 3) { print $1 }' "$state/SHASUMS256.txt")
    actual=$(sha256sum "$work/.local/bin/mise" | awk '{ print $1 }')
    [[ -n $expected && $expected == "$actual" ]]
    chmod 755 "$work/.local/bin/mise"
    chown benchmark: "$work/.local/bin/mise"
    printf '%s\n' "$actual" > "$state/mise.sha256"
    as_benchmark mise install "java@$JAVA_VERSION" "just@$JUST_VERSION" > "$state/tools.log" 2>&1
    java_home=$(as_benchmark mise where "java@$JAVA_VERSION")
    local just
    just=$(as_benchmark mise x "just@$JUST_VERSION" -- bash -c 'command -v just')
    tool_path="$java_home/bin:$(dirname "$just"):$tool_path"
}

checkout() {
    local variant=$1
    local source="$work/src/$variant"
    rm -rf "$source"
    # The bundle carries a non-branch ref, which a plain clone would not fetch.
    as_benchmark git init --quiet "$source"
    as_benchmark git -C "$source" fetch --quiet "$input/source.bundle" '+refs/*:refs/bundle/*'
    as_benchmark git -C "$source" -c advice.detachedHead=false checkout --quiet --detach "$SOURCE_REVISION"
    if [[ -f "$input/patches/$variant.patch" ]]; then
        # --index tracks added files, so the retained source patch includes them.
        as_benchmark git -C "$source" apply --index --whitespace=nowarn "$input/patches/$variant.patch"
    fi
    printf '%s\n' "$source"
}

# Each value is resolved by an assignment first, so a failed lookup stops the run under errexit
# instead of recording an empty field.
manifest() {
    local out=$1 variant=$2 key value
    local -a entries=()
    entry() { entries+=("$1=$2"); }
    value=$(metadata zone); entry gce.zone "${value##*/}"
    value=$(metadata machine-type); entry gce.machine.type "${value##*/}"
    value=$(metadata cpu-platform); entry gce.cpu.platform "$value"
    value=$(metadata image); entry gce.image "${value##*/}"
    value=$(metadata scheduling/on-host-maintenance); entry gce.scheduling "$value"
    value=$(awk -F= '$1 == "PRETTY_NAME" { gsub(/"/, "", $2); print $2 }' /etc/os-release); entry os.release "$value"
    value=$(uname -srm); entry kernel "$value"
    value=$(awk -F': ' '/^model name/ { print $2; exit }' /proc/cpuinfo); entry cpu.model "$value"
    value=$(nproc); entry cpu.count "$value"
    value=$(awk '/^MemTotal/ { print $2 }' /proc/meminfo); entry memory.kib "$value"
    value=$(cat "$state/mise.sha256"); entry mise.sha256 "$value"
    value=$(as_benchmark java -version 2>&1); entry java.version "${value%%$'\n'*}"
    value=$(as_benchmark just --version); entry just.version "$value"
    value=$(as_benchmark "$work/tools/build/compiler/cpp/bin/thrift" --version); entry thrift.version "$value"
    value=$(sha256sum "$0"); entry runner.sha256 "${value%% *}"
    value=
    if [[ -f "$input/patches/$variant.patch" ]]; then
        value=$(sha256sum "$input/patches/$variant.patch")
        value=${value%% *}
    fi
    entry variant.patch.sha256 "$value"
    value=$(cat /proc/sys/kernel/random/boot_id); entry boot.id "$value"
    {
        printf 'run.id=%s\nvariant=%s\nbenchmark.mode=%s\nflink.version=%s\nprotobuf.major=%s\n' \
            "$RUN_ID" "$variant" "$BENCHMARK_MODE" "$FLINK_VERSION" "$PROTOBUF_MAJOR"
        printf 'source.revision=%s\nsource.base=%s\nmise.version=%s\njava.tool=%s\n' \
            "$SOURCE_REVISION" "$SOURCE_BASE" "$MISE_VERSION" "${java_home##*/}"
        for key in "${entries[@]}"; do printf '%s\n' "$key"; done
    } > "$out/vm-manifest.txt"
}

measure() {
    local variant=$1 source out
    out="$results/protobuf-$PROTOBUF_MAJOR-$variant"
    status "RUNNING $variant"
    source=$(checkout "$variant")
    mkdir -p "$out"
    chown benchmark: "$out"
    as_benchmark git -C "$source" diff --cached "$SOURCE_BASE" > "$out/measurement-source.patch"
    printf '%s\n' "$SOURCE_BASE" > "$out/source-base.txt"
    local -a just=(just --justfile "$source/justfile" --working-directory "$source")
    local -a build=(benchmark-build "$FLINK_VERSION" "$PROTOBUF_MAJOR")
    local -a run=(benchmark-measure "$BENCHMARK_MODE" "$FLINK_VERSION" "$PROTOBUF_MAJOR" "$out/benchmark")
    {
        printf 'cd <checkout of %s with the %s variant>\n' "$SOURCE_REVISION" "$variant"
        printf 'just %s\n' "${build[*]}" "${run[*]/#$results/<results>}"
        printf 'JAVA_HOME=<mise java@%s>, BENCHMARK_THRIFT_COMPILER=<pinned compiler>\n' "$JAVA_VERSION"
    } > "$out/invocation.txt"
    if [[ ! -x "$work/tools/build/compiler/cpp/bin/thrift" ]]; then
        as_benchmark "${just[@]}" benchmark-tools "$work/tools" > "$state/thrift.log" 2>&1
    fi
    manifest "$out" "$variant"
    gate "$out/load-before-build.log"
    as_benchmark "${just[@]}" "${build[@]}" > "$out/build.log" 2>&1
    check_interrupted
    gate "$out/load-after-build.log"
    date -u +%Y-%m-%dT%H:%M:%SZ > "$out/start-utc.txt"
    as_benchmark "${just[@]}" "${run[@]}" > "$out/console.log" 2>&1
    date -u +%Y-%m-%dT%H:%M:%SZ > "$out/end-utc.txt"
    check_interrupted
    [[ "$(cat /proc/sys/kernel/random/boot_id)" == "$(cat "$state/boot-id")" ]]
}

current=prepare
watcher=

# Record a failure for the step in progress; collection reads results directly.
finish() {
    local code=$?
    set +e
    if [[ -n $watcher ]]; then kill "$watcher" 2> /dev/null; fi
    if [[ $code -ne 0 ]]; then status FAILED "$current"; fi
}

run() {
    mkdir -p "$state"
    cat /proc/sys/kernel/random/boot_id > "$state/boot-id"
    watch_maintenance &
    watcher=$!
    trap finish EXIT
    prepare
    check_interrupted
    local variant
    for variant in $VARIANTS; do
        current=$variant
        measure "$variant"
    done
    status SUCCEEDED
}

case "${1:-}" in
    run) run ;;
    *) printf 'Usage: %s run\n' "$0" >&2; exit 2 ;;
esac
