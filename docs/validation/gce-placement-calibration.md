---
title: "Variant placement across temporary GCE VMs"
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

# Variant placement across temporary GCE VMs

This report calibrates the comparison of benchmark variants that run on different VMs of one plan, for [#109](https://github.com/flink-gcp/flink-datastream-protobuf/issues/109).
The [temporary GCE procedure](../development.md#temporary-gce-execution) measures every variant of a profile sequentially on one VM, so a plan's wall-clock time grows with its variant count: each `just benchmark-pair` variant takes about 70 minutes.
Placing each variant on its own VM would make that time independent of the variant count, but it compares variants across hosts.
The [ADR-0004](../adr/0004-serializer-benchmarks.md) native/Chill protocol controls host speed within each block, and this calibration measures how much of the between-VM difference that control leaves.

## Method

Plan `issue109-calibration-2` measures one unchanged tree on several VMs.
Each Protobuf profile runs on three `c3-standard-8` VMs in `asia-northeast1-b`, from one image, with Temurin 17 and Flink 2.3.0, and the six VMs start together.
Each VM runs the unchanged tree twice in sequence with `just benchmark-pair`, which keeps the ADR-0004 forks, warmups, five alternating native/Chill blocks, and correctness gates.
Every run has the same source, so any difference between two runs is measurement variation.

Two runs are compared per workload/operation cell as in the earlier GCE reports: the ratio of their geometric-mean native/Chill time ratios, with a pooled two-sample Student-t interval on the five log ratios of each run (8 degrees of freedom).
Each profile has 40 cells.

- **Within-VM comparison**: the first and second run on one VM, which is the comparison the sequential procedure makes; 3 VM × 40 cells give 120 comparisons per profile.
- **Between-VM comparison**: runs at the same sequential position on two different VMs, which ran at about the same time; 3 VM pairs × 2 positions × 40 cells give 240 comparisons per profile.

The decision rule was fixed before the plan ran: [pull request #111](https://github.com/flink-gcp/flink-datastream-protobuf/pull/111) published this method on 2026-10-20 at 16:00 UTC, before either plan below executed.
For a profile, separate-VM placement passes when both conditions hold:

1. At least 95% of the between-VM point estimates lie within 0.95 to 1.05.
2. The share of between-VM 95% intervals that exclude 1 is at most the within-VM share plus 5 percentage points.

Separate-VM placement is adopted only if both profiles pass; otherwise every compared variant of a profile stays on one VM.
The report also gives, without a decision role, the spread of each cell's native/Chill ratio across the six runs of a profile and the agreement of their ADR-0004 verdicts.

The calibration covers one machine type, zone, image, and JDK, at one time of day, in the paired mode.
It does not establish placement for other machine series, for the cross-format `run` mode, or for comparisons between plans, which remain unsupported.

## Results

Plan `issue109-calibration-2` ran on 2026-10-21 from 03:57 to 06:59 UTC; each run's measurement took about 70 minutes, and the six runners started within three minutes of each other.
An earlier attempt, `issue109-calibration-1`, stopped while creating its fifth VM on the project-wide `CPUS_ALL_REGIONS` quota, deleted the four VMs it had created, and measured nothing.
The plans' resource ledgers stay outside the repository; the pull request's review records their events.
All twelve runs used source `844fd45e700040dbe036acea9f98f95922897d3e` unchanged, on an Intel Xeon Platinum 8481C (Sapphire Rapids, 8 vCPUs, 32 GiB) from image `ubuntu-2404-noble-amd64-v20261016`, kernel 7.0.0-1014-gcp, with Temurin 17.0.20.1, Flink 2.3.0, and Protobuf/protoc 3.25.9 or 4.33.6.
Every VM recorded `TERMINATE` host maintenance and `NONE` as the only maintenance event, passed the load gate before every build and measurement, and reported the same machine type, CPU platform, image, kernel, and JDK.

| Profile | Comparison | Point estimate within 0.95 to 1.05 | Interval excludes 1 |
|---|---|---:|---:|
| Protobuf 4 | within-VM | 112 of 120 (93.3%) | 3 of 120 (2.5%) |
| Protobuf 4 | between-VM | 221 of 240 (92.1%) | 18 of 240 (7.5%) |
| Protobuf 3 | within-VM | 103 of 120 (85.8%) | 4 of 120 (3.3%) |
| Protobuf 3 | between-VM | 210 of 240 (87.5%) | 21 of 240 (8.75%) |

Each ratio puts the first run of a VM over its second, and between VMs the run of the earlier group over the later (a over b, a over c, b over c).
The rule did not fix this direction, and the band is not symmetric under inversion: reversing every ratio moves the Protobuf 3 between-VM band count to 212 of 240 (88.3%) and changes no other count, so neither outcome depends on it.

Neither profile passes the rule.
The between-VM point estimates fall short of the 95% band on both profiles.
The share of between-VM intervals that exclude 1 sits exactly at the limit on Protobuf 4, 7.5% against 2.5% plus 5 points, and exceeds it on Protobuf 3, 8.75% against 8.33%.
Separate-VM placement is therefore not adopted; [ADR-0004](../adr/0004-serializer-benchmarks.md) keeps every compared variant of a profile on one VM.

The two conditions fail for different reasons.
The band condition fails within one VM as well: two runs of the same tree on the same host already place 6.7% of Protobuf 4 and 14.2% of Protobuf 3 point estimates outside 0.95 to 1.05, against 7.9% and 12.5% between VMs.
Most of those estimates come from cells whose five block ratios scatter widely, such as Protobuf 3 text `serialize`, `buildAndSerialize`, and `streamCopy` and Protobuf 4 collections `objectCopy` and `serializeSameInstance`, where the median standard deviation of a run's five log ratios is 0.06 to 0.34 against about 0.03 for the median cell; the widest between-VM estimate is 1.549 (1.042, 2.302) for Protobuf 3 text `serialize`, and the widest within-VM estimate is 1.368 (0.834, 2.244) for the same cell.
The band therefore reflects run-to-run variation on these cells, which two sequential runs on one host already show, more than a difference between hosts.

The interval condition, by contrast, separates the placements.
The excess between-VM exclusions come mostly from large-record cells, which have the narrowest intervals: 12 of the 18 Protobuf 4 exclusions and 11 of the 21 Protobuf 3 exclusions, against 0 and 2 within one VM.
Their point estimates range from 0.979 to 1.036 on Protobuf 4 and from 0.984 to 1.016 on Protobuf 3, apart from large `objectCopy`, an identity copy of about 1.4 ns against Chill's 16 µs copy, where a difference of a tenth of a nanosecond moves the ratio by several percent.
The per-block Chill control leaves host offsets of 1% to 4% on these cells, which a five-block interval resolves as a difference between identical trees.

Across the six runs of a profile, the largest-to-smallest geometric-mean native/Chill ratio of a cell has a median of 1.032 on Protobuf 4 and 1.028 on Protobuf 3.
All six runs give the same ADR-0004 verdict in every Protobuf 4 cell and in 37 of 40 Protobuf 3 cells; the exceptions are text `serialize`, `serializeSameInstance`, and `buildAndSerialize`, which change between established and inconclusive.
The [retained archives](data/gce-placement-calibration-v0.2.0/README.md) contain the measurements and run records behind these counts.
