---
title: "CI compiler reuse and job budgets"
bookHidden: true
---
<!--
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# CI compiler reuse and job budgets

This page records the GitHub runner durations behind the Verify job limits and the benchmark compiler cache from [#46](https://github.com/flink-gcp/flink-datastream-protobuf/issues/46).
The [development guide](../development.md#ci-compiler-reuse-and-job-budgets) describes the current procedure, and [ADR-0002](../adr/0002-build-and-development-workflow.md) records the decision and the rejected alternatives.
These are budget measurements on shared runners, not benchmark results; no CI check compares them with a threshold.

## Method

Durations come from the `started_at` and `completed_at` timestamps of jobs and steps in the GitHub Actions jobs API.
Only successful jobs are counted, including successful jobs in runs where another job failed or the run was cancelled; every step value is that step's own duration.
All jobs ran on `ubuntu-latest` standard runners; the measured runs after the change reported Ubuntu 24.04 on x86_64 in the compiler cache key.
Medians of an even number of jobs are the mean of the two middle values.
GitHub [announced](https://github.com/actions/runner-images/issues/14748) that `ubuntu-latest` moves to Ubuntu 26 from 2026-10-19; that changes the distribution release in the key, and these durations do not describe the new image.
A matrix job is one of the ten runtime/JDK/Protobuf builds; the binary compatibility and development bundle jobs are reported as whole jobs.

## Before compiler reuse

The baseline covers every CI run and scheduled Verify run from 2026-09-19 to 2026-10-05 whose Verify workflow matched commit `f5063f7`, apart from the pinned version of the action that installs `just`.
These 23 runs are [35434366511](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35434366511), [35435340746](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35435340746), [35440618065](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35440618065), [35441056894](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35441056894), [35442533077](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35442533077), [35443577622](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35443577622), [35443918300](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35443918300), [35444041169](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35444041169), [35447057022](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35447057022), [35447360960](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35447360960), [35448219934](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35448219934), [35524618918](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35524618918), [35524623214](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35524623214), [35524628581](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35524628581), [35524639121](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35524639121), [35524641946](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35524641946), [35524647119](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35524647119), [35600464462](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/35600464462), [36335473491](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/36335473491), [36429785484](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/36429785484), [37219064508](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/37219064508), [37219079057](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/37219079057), [37323878801](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/37323878801).
Every matrix job built the compiler from source, so each of these runs was cold for the compiler.

| Matrix step | Jobs | Min s | Median s | Max s |
|---|---:|---:|---:|---:|
| Native verification | 189 | 59 | 87 | 228 |
| Standalone examples | 189 | 48 | 68 | 78 |
| API Javadoc | 189 | 7 | 24 | 31 |
| Compiler preparation | 189 | 51 | 82 | 182 |
| Benchmark smoke | 189 | 57 | 82 | 89 |

| Job | Jobs | Min s | Median s | Max s |
|---|---:|---:|---:|---:|
| Matrix | 189 | 239 | 355 | 501 |
| Binary compatibility | 45 | 125 | 162 | 315 |
| Development bundle | 88 | 113 | 182 | 345 |

Compiler preparation took about a quarter of a median matrix job and repeated in all ten jobs of each run.
One sampled pull request log split it into about 11 seconds of package installation, 5 seconds of download, extraction, and configuration, and 42 seconds of C++ build.
The archive download varied more than the build.
The slowest preparation steps, from 146 to 182 seconds, came from the three scheduled runs, and in the sampled scheduled job the download from `archive.apache.org` alone took about 75 seconds.
The slowest native verification steps, from 171 to 228 seconds, occurred in three runs whose commits changed the POM: in all ten jobs of the `main` push that merged [PR #51](https://github.com/flink-gcp/flink-datastream-protobuf/pull/51), and in some jobs of two dependency-update PRs.
Their logs had expired when this page was written, so the cause of those slower steps is not established.

## After compiler reuse

Two manual runs on the PR branch at commit `c4bc8dc` measured the cold and warm cache paths.
That commit copied the built compiler into a separate cached directory and checked it a second time; the final layout caches the built file in place and checks it once, which removes less than a second from a cold job.
The cold run [38119339196](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/38119339196) found no entry; one job saved the checked compiler (about 2 MB), and the other nine logged that another job was creating the entry, without a warning annotation.
The warm run [38119813575](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/38119813575) restored that entry in every matrix job and skipped package installation, download, and the build.

| Matrix step | Cold median s | Cold max s | Warm median s | Warm max s |
|---|---:|---:|---:|---:|
| Compiler identity, restore, and save | 1 | 2 | 1 | 2 |
| Compiler preparation and check | 85.5 | 103 | 0 | 0 |
| Benchmark smoke | 82.5 | 89 | 71 | 84 |
| Whole matrix job | 362.5 | 380 | 247.5 | 286 |

The binary compatibility and development bundle jobs do not use the compiler; their maxima were 165 and 204 seconds across the two runs.
A warm run removes the compiler preparation, a median of about 80 seconds, from each matrix job, or about 13 runner-minutes per CI run, and no longer depends on the archive download.
The longest warm matrix job took 286 seconds, compared with 380 seconds in the cold run.
A cold run costs the same compiler build as before, plus at most 2 seconds of identity, restore, and save steps; the API reports whole seconds.

A pull request run can restore entries saved on its own merge ref or on `main`, but not on the PR's head branch.
The first PR run [38120594823](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/38120594823) found no entry for its merge ref: four matrix jobs built the compiler, the first of them to save created the entry, and the six jobs that reached the restore step afterwards used that entry.
Later runs of the same PR restore that entry, and new PRs restore an entry once a `main` run has saved it.

## Job limits

The rule in the [development guide](../development.md#ci-compiler-reuse-and-job-budgets) gives these limits from the slowest successful durations above.

| Limit | Slowest measured | Previous | Current |
|---|---:|---:|---:|
| Matrix job | 501 s | 20 min | 20 min |
| Binary compatibility job | 365 s | 15 min | 15 min |
| Development bundle job | 345 s | 30 min | 15 min |
| Benchmark smoke command | 89 s | none | 5 min |
| Retained upgrade job | 474 s | 15 min | 20 min |

[Issue #141](https://github.com/flink-gcp/flink-datastream-protobuf/issues/141) changed the binary compatibility job from one cell per Protobuf profile to one per artifact line, each building the jar with Protobuf 4 and also running it with Protobuf 3 tests.
In pull-request run [40918680497](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/40918680497), the 2.x cell, which runs the suite four times, took 365 seconds, and the LTS cell took 242 seconds.
That run still used a full `verify` for the Protobuf 3 build, which the final recipe replaces with a build through `process-test-classes`; twice its 2.x duration stays within the 15-minute limit.

The retained upgrade job fetches one bundle and restores its state with a clean Maven build; its ten jobs in the first pull-request run, [39451689508](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/39451689508), took 59 to 122 seconds.
The job then also runs the API compatibility gate, which rebuilds the candidate cleanly, and the retained compiled consumers; with those steps its ten jobs in run [39518523442](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/39518523442) took 110 to 196 seconds.
Twice the new maximum rounds up to the same 10-minute limit.
Those ten cells were then folded into one job per bundle that runs each Flink version and JDK in turn and the API gate once.
In five pull-request runs from [39630362526](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/39630362526) to [39639022949](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/39639022949), its LTS jobs took 132 to 159 seconds and its 2.x jobs, which cover four combinations each, took 246 to 322 seconds.
Twice that maximum rounds up to a 15-minute limit.
For [issue #23](https://github.com/flink-gcp/flink-datastream-protobuf/issues/23), each job also fetches and checks the v0.2.0 bundle of its name, whose format adds evolved and enum fixtures.
In pull-request run [41087953257](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/41087953257), which downloaded the v0.2.0 archives into empty cache entries, the LTS jobs took 179 and 187 seconds and the 2.x jobs 413 and 474 seconds, so twice that maximum rounds up to a 20-minute limit.
The matrix limit keeps the cold-compiler maximum, because a key change or an evicted entry returns every job to the cold path.
No measured job combined the slowest step of each kind, but even the sum of the per-step maxima, about 630 seconds including setup steps, stays within the 20-minute limit.
