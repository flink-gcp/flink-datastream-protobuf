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

# Command stubs for test_gce_benchmark_vm.py, which places a one-line wrapper at each replaced
# command's location that runs: bash gce-benchmark-vm-stubs.sh <stub directory> <command> <args>.

stubs=$1
command=$2
shift 2
# The test writes the run's paths to config.sh; declaring them lets shellcheck catch misspellings.
# shellcheck source=/dev/null
source "$stubs/config.sh"
readonly state system boot_id control java_home just_home real_sleep real_curl real_mv

record() { printf '%s\n' "$*" >> "$stubs/calls.log"; }

await() {
    local _
    for _ in $(seq 600); do
        if "$@"; then return 0; fi
        "$real_sleep" 0.05
    done
    record "timed out waiting: $*"
    return 1
}

# Announce a host-maintenance event, then continue once the watcher has written the interruption
# status, which it does after creating its marker, or only the marker when the test holds the watcher.
announce() {
    "$real_curl" --fail --silent "$control/control/maintenance" > /dev/null
    if [[ -f $stubs/hold-watcher ]]; then
        await test -f "$state/interrupted"
    else
        await grep -q ' INTERRUPTED ' "$state/events.log"
    fi
}

logged() {
    [[ -f $state/maintenance-events.log ]] && (($(wc -l < "$state/maintenance-events.log") >= $1))
}

# Perform the action the test configured for this step, preferring a variant-specific one.
act() {
    local action=ok
    if [[ -f $stubs/actions/$1.${2:-} ]]; then
        action=$(cat "$stubs/actions/$1.$2")
    elif [[ -f $stubs/actions/$1 ]]; then
        action=$(cat "$stubs/actions/$1")
    fi
    case $action in
        ok) ;;
        fail) exit 1 ;;
        # Finish normally despite the watcher's signal, like a step that completes just after the event.
        event) trap '' TERM; announce ;;
        # Model a runner status update that passed its check before the marker existed and replaced
        # the watcher's status file, or followed its interruption in the event log.
        event-status-replaced) trap '' TERM; announce; printf 'RUNNING %s\n' "$2" > "$state/status" ;;
        event-logged-late) trap '' TERM; announce; printf 'later RUNNING %s\n' "$2" >> "$state/events.log" ;;
        # Keep running until the watcher's signal stops this process, like a long benchmark.
        event-block) announce; exec "$real_sleep" 600 ;;
        block) exec "$real_sleep" 600 ;;
        reboot) printf 'rebooted\n' > "$boot_id" ;;
        watched) await logged "$(cat "$stubs/watched")" ;;
    esac
}

# Commands that the runner finds on its own PATH.

# Run the command as a child whose PID the pkill stub can signal, as runuser does for the user.
stub_runuser() {
    if [[ $1 != -u || $2 != benchmark || $3 != -- ]]; then record "runuser $*"; exit 97; fi
    shift 3
    "$@" &
    local child=$! code=0
    : > "$stubs/benchmark-processes/$child"
    wait "$child" || code=$?
    rm -f "$stubs/benchmark-processes/$child"
    exit "$code"
}

stub_pkill() {
    if [[ $* != "-TERM -u benchmark" ]]; then record "pkill $*"; exit 97; fi
    local process found=1
    for process in "$stubs"/benchmark-processes/*; do
        if [[ -e $process ]] && kill -TERM "${process##*/}" 2> /dev/null; then found=0; fi
    done
    exit "$found"
}

stub_apt_get() {
    record apt-get "$1"
    act apt-get "$1"
}

stub_useradd() { record useradd "${@: -1}"; }

stub_chown() { record chown "$@"; }

# Optionally hold the watcher after it creates its marker, before it records the interruption.
stub_mv() {
    "$real_mv" "$@"
    if [[ -f $stubs/hold-watcher && $2 == "$state/interrupted" ]]; then exec "$real_sleep" 600; fi
}

# One load-gate observation: advance the counters by the next sample, or by an idle default.
# Half of the idle time is I/O wait and half of the busy time is steal, which GCE reports.
stub_sleep() {
    if [[ $1 != 2 ]]; then exec "$real_sleep" 0.02; fi
    local observation sample idle load swapin swapout idle_ticks busy_ticks pswpin pswpout
    observation=$(($(cat "$stubs/observations") + 1))
    printf '%s\n' "$observation" > "$stubs/observations"
    sample=$(sed -n "${observation}p" "$stubs/samples")
    read -r idle load swapin swapout <<< "${sample:-99 0.50 0 0}"
    read -r idle_ticks busy_ticks pswpin pswpout < "$stubs/counters"
    idle_ticks=$((idle_ticks + idle * 10))
    busy_ticks=$((busy_ticks + 1000 - idle * 10))
    pswpin=$((pswpin + swapin))
    pswpout=$((pswpout + swapout))
    printf '%s %s %s %s\n' "$idle_ticks" "$busy_ticks" "$pswpin" "$pswpout" > "$stubs/counters"
    printf 'cpu  %s 0 0 %s %s 0 0 %s 0 0\n' $((busy_ticks / 2)) $((idle_ticks / 2)) $((idle_ticks / 2)) \
        $((busy_ticks / 2)) > "$system/proc/stat"
    printf '%s 0.10 0.05 1/100 1000\n' "$load" > "$system/proc/loadavg"
    printf 'pswpin %s\npswpout %s\n' "$pswpin" "$pswpout" > "$system/proc/vmstat"
    if [[ $(cat "$stubs/event-at" 2> /dev/null) == "$observation" ]]; then announce; fi
}

# Commands that the benchmark user runs: the downloaded mise and the tools it reports.

stub_mise() {
    record mise "$1"
    case $1 in
        install) act mise-install ;;
        where) printf '%s\n' "$java_home" ;;
        x) shift 3; PATH="$just_home/bin:$PATH" exec "$@" ;;
        *) exit 97 ;;
    esac
}

stub_java() { printf 'openjdk version "17.0.16" 2025-07-15\nOpenJDK Runtime Environment\n' >&2; }

# Record each recipe with the variant directory and its payload, which a variant patch changes.
stub_just() {
    if [[ $1 == --version ]]; then printf 'just 1.40.0\n'; exit 0; fi
    if [[ $1 != --justfile || $3 != --working-directory ]]; then record "just $*"; exit 97; fi
    local checkout=$4 variant
    shift 4
    variant=${checkout##*/}
    record just "$1" "$variant" "$(cat "$checkout/payload.txt")"
    case $1 in
        benchmark-tools)
            act benchmark-tools "$variant"
            mkdir -p "$2/build/compiler/cpp/bin"
            printf '#!/bin/bash\nprintf "Thrift version 0.25.0\\n"\n' > "$2/build/compiler/cpp/bin/thrift"
            chmod 755 "$2/build/compiler/cpp/bin/thrift" ;;
        benchmark-build) act benchmark-build "$variant" ;;
        benchmark-measure)
            act benchmark-measure "$variant"
            mkdir -p "$5"
            printf '[]\n' > "$5/result.json" ;;
        *) exit 97 ;;
    esac
}

"stub_${command//-/_}" "$@"
