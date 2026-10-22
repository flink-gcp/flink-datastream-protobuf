---
title: "Sizing traversal allocation in the native serializer"
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

# Sizing traversal allocation in the native serializer

This report measures the safe-sizing traversal investigated in [#43](https://github.com/flink-gcp/flink-datastream-protobuf/issues/43).
After the [buffer-allocation change](serializer-buffer-allocation.md), scalar and nested `serialize` and `serializeSameInstance` still took 6.2 to 7.0 times Chill's time and allocated 1.9 to 2.2 KB/op; the 0.1.0 diagnostics had sampled allocation sites in the safe-sizing traversal.
Two plans measure the change on temporary GCE VMs: one compares the final change with the unchanged serializer, and an earlier one separates the traversal bookkeeping, the descriptor work, and the remaining sizing cost from the output work with controlled variants.
The measurement contract is [ADR-0004](../adr/0004-serializer-benchmarks.md); the comparison does not establish whole-job throughput or state compatibility.

## Change under test

Before the change, every write sized the record by collecting each message's populated fields with `getAllFields()`, which builds a sorted map, and by allocating a traversal frame, a child list, an edge record per child, and a summary per completed value.
A per-call identity map held every summary, including those of empty unknown-field sets, and each message asked its descriptor for its field list and per-field properties.

The changed sizing iterates over field layouts that each payload class computes once for every message type it reaches and caches with its other `ClassValue` metadata.
A layout records each field's type, tag size, repetition, packing, presence, whether reflection copies its list (maps and enums), whether it is a raw proto2 string, the required fields, and the layout of its message type.
Traversal frames are cursors over those layouts; each serializer instance reuses up to 64 of them and clears their record references after every call.
Within one call, only a subtree whose sizing examined more than 64 content units (declared fields, elements, unknown values, and string characters) is summarized by identity; smaller shared subtrees are sized again at each occurrence, which bounds that repeated work.
A singular field without presence outside map entries, such as a proto3 scalar, is read once and compared with its default, which is how the runtime's `hasField` decides its presence, instead of being read by both `hasField` and `getField`.
Empty unknown-field sets and empty repeated enums are skipped, packed fixed-width fields are sized from their count, and packed varint fields of `Internal.LongList` or `Internal.IntList` are read without boxing.
Checked arithmetic, size and depth limits, UTF-8 and raw proto2 string handling, unknown fields, and the exact-size check after writing are unchanged; [ADR-0001](../adr/0001-native-protobuf-type-integration.md) records the amended traversal.

`ProtobufMessageSizeTest` compares the calculated size with the runtime size for random graphs with shared children, fresh and memoized records, packed blocks with two- and three-byte length prefixes, implicit-presence fields at their defaults and at values that only resemble them (negative zero, NaN, and undeclared enum numbers), a map entry with a default key and value, shared unknown-field sets below and above the summary bound, and a value whose descriptor differs from the layout passed in.
It also checks shared children above and below the summary bound and a summarized unknown-field set at deeper paths, rejects a 64-level doubling graph without exponential traversal, reuses one instance after failed checks, and confirms that no checked record stays reachable from the instance, including siblings pending in a repeated field, a map, or an unknown group when a check fails.
The existing serializer tests continue to cover overflow above 4 GiB, depth limits through maps and unknown groups, raw strings, and inconsistent writers.

## Method

Both plans used the [temporary GCE procedure](../development.md#temporary-gce-execution) in project `flink-gcp`.
Each Protobuf profile ran on its own `c3-standard-8` VM in `asia-northeast1-b` (Intel Xeon Platinum 8481C, Sapphire Rapids, 8 vCPUs, 32 GiB) from image `ubuntu-2404-noble-amd64-v20261016`, kernel 7.0.0-1014-gcp, with Temurin 17.0.20.1, Flink 2.3.0, and Protobuf/protoc 3.25.9 or 4.33.6.
Every VM recorded `TERMINATE` host maintenance and `NONE` as the only maintenance event, passed the load gate before every build and measurement, and reported the same machine type, CPU platform, image, kernel, and JDK as the other VMs of its plan.
`just benchmark-pair` ran the ADR-0004 long-run protocol for the native and Chill Protobuf lanes, as in the buffer-allocation report, and each variant took about 70 minutes.

| Plan | Source and base | Variants, in run order | Time (UTC) |
|---|---|---|---|
| `issue43-sizing-2` | `1e3a6c1b97e41f520b3ff1d9d1f0ea2db416e2a7` on `aefae28` | baseline, candidate | 2026-10-20, 00:17 to 02:48 |
| `issue43-sizing-1` | `dcd061701678409cfb2c137445d5346012557227` on `eef658f` | baseline, first candidate, descriptors, floor | 2026-10-19, 13:03 to 18:14 |

In both plans, the baseline is the previous sizing and its tests, identical to the base under `src/`, and the candidate is the source commit unchanged.
The candidate of `issue43-sizing-2` is the change described above.
The first candidate of `issue43-sizing-1` lacks the single read of fields without presence and the count check of empty repeated enums; the variants that separate the sources are patches to it:

| Variant | Sizing under test |
|---|---|
| descriptors | The new traversal, reading the field list and every field property from the descriptor at each use |
| floor | No safe sizing: the frame length comes from the runtime's unchecked `getSerializedSize()`, and depth is not checked |

The floor is a diagnostic lower bound, not a supported configuration; it shows the output work that remains without safe sizing.
The two plans are not compared with each other, because they ran at different times on different VMs.

Comparisons between variants of one plan use the Chill lane as a per-block control, as in the buffer-allocation report: the ratio of two variants' geometric-mean native/Chill time ratios, with a pooled two-sample Student-t interval on the five log ratios of each run (8 degrees of freedom).
Below 1 favors the first variant.
Allocation is the five-fork mean of JMH's normalized allocation, in bytes per operation.
These intervals are per cell, without a simultaneous claim across cells.

## Results

### The change against the previous sizing

Each variant cell is native time in ns/op and allocation in B/op, as five-fork means, from plan `issue43-sizing-2`.
The last three columns give the candidate's time relative to the baseline and each variant's native/Chill interval and ADR-0004 verdict.

Protobuf 4:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | serialize | 688 / 1988 | 279 / 346 | 0.404 (0.384, 0.426) | 7.100 (6.883, 7.323) missed | 2.870 (2.719, 3.029) missed |
| scalar | serializeSameInstance | 585 / 1856 | 227 / 280 | 0.349 (0.296, 0.411) | 6.826 (6.480, 7.190) missed | 2.381 (1.970, 2.879) missed |
| scalar | buildAndSerialize | 990 / 2705 | 483 / 1010 | 0.486 (0.475, 0.498) | 3.232 (3.189, 3.276) missed | 1.572 (1.532, 1.614) missed |
| text | serialize | 1761 / 2119 | 1286 / 346 | 0.721 (0.620, 0.838) | 2.184 (1.960, 2.433) missed | 1.574 (1.361, 1.820) missed |
| text | serializeSameInstance | 1677 / 1970 | 1065 / 312 | 0.605 (0.555, 0.660) | 2.222 (2.120, 2.329) missed | 1.345 (1.225, 1.476) missed |
| text | buildAndSerialize | 2576 / 3841 | 1871 / 2034 | 0.724 (0.684, 0.765) | 1.755 (1.703, 1.808) missed | 1.270 (1.196, 1.348) missed |
| nested | serialize | 855 / 2150 | 321 / 357 | 0.364 (0.342, 0.388) | 7.044 (6.897, 7.194) missed | 2.567 (2.388, 2.759) missed |
| nested | serializeSameInstance | 689 / 1923 | 260 / 312 | 0.376 (0.365, 0.387) | 6.377 (6.177, 6.583) missed | 2.399 (2.363, 2.434) missed |
| nested | buildAndSerialize | 1200 / 2798 | 571 / 1021 | 0.471 (0.453, 0.489) | 3.274 (3.150, 3.402) missed | 1.540 (1.504, 1.578) missed |
| collections | serialize | 5324 / 12919 | 2186 / 660 | 0.412 (0.392, 0.432) | 5.924 (5.729, 6.126) missed | 2.440 (2.327, 2.557) missed |
| collections | serializeSameInstance | 4968 / 13022 | 1953 / 600 | 0.378 (0.346, 0.412) | 6.094 (6.050, 6.138) missed | 2.301 (2.069, 2.558) missed |
| collections | buildAndSerialize | 7289 / 15281 | 3668 / 3986 | 0.514 (0.503, 0.525) | 3.955 (3.907, 4.004) missed | 2.033 (1.986, 2.081) missed |
| large | serialize | 4122 / 6207 | 3553 / 4490 | 0.855 (0.828, 0.883) | 0.392 (0.381, 0.403) established | 0.335 (0.326, 0.343) established |
| large | serializeSameInstance | 3048 / 6096 | 2699 / 4456 | 0.885 (0.867, 0.904) | 0.310 (0.306, 0.314) established | 0.274 (0.268, 0.280) established |
| large | buildAndSerialize | 12782 / 72505 | 11932 / 70679 | 0.931 (0.921, 0.940) | 0.690 (0.683, 0.697) established | 0.642 (0.638, 0.646) established |

Protobuf 3:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | serialize | 676 / 2081 | 269 / 346 | 0.400 (0.379, 0.421) | 6.864 (6.672, 7.062) missed | 2.743 (2.594, 2.900) missed |
| scalar | serializeSameInstance | 569 / 1920 | 229 / 272 | 0.403 (0.385, 0.422) | 6.523 (6.424, 6.624) missed | 2.628 (2.493, 2.771) missed |
| scalar | buildAndSerialize | 961 / 2769 | 476 / 1010 | 0.494 (0.477, 0.511) | 3.139 (3.070, 3.208) missed | 1.549 (1.496, 1.604) missed |
| text | serialize | 1800 / 2193 | 1273 / 346 | 0.694 (0.662, 0.726) | 1.059 (1.034, 1.084) established | 0.734 (0.699, 0.772) established |
| text | serializeSameInstance | 1579 / 2072 | 1158 / 312 | 0.730 (0.703, 0.759) | 0.951 (0.921, 0.981) established | 0.694 (0.671, 0.718) established |
| text | buildAndSerialize | 2464 / 3905 | 1913 / 2034 | 0.674 (0.473, 0.959) | 0.871 (0.571, 1.329) inconclusive | 0.587 (0.558, 0.618) established |
| nested | serialize | 819 / 2205 | 324 / 357 | 0.397 (0.385, 0.409) | 6.755 (6.626, 6.887) missed | 2.681 (2.599, 2.767) missed |
| nested | serializeSameInstance | 674 / 2016 | 266 / 312 | 0.396 (0.383, 0.410) | 6.181 (6.011, 6.356) missed | 2.450 (2.376, 2.526) missed |
| nested | buildAndSerialize | 1142 / 2927 | 582 / 1021 | 0.518 (0.502, 0.533) | 3.104 (3.039, 3.171) missed | 1.606 (1.560, 1.654) missed |
| collections | serialize | 5328 / 12772 | 2336 / 666 | 0.449 (0.408, 0.494) | 5.634 (5.058, 6.276) missed | 2.529 (2.429, 2.633) missed |
| collections | serializeSameInstance | 4838 / 12850 | 1994 / 619 | 0.410 (0.383, 0.439) | 5.796 (5.579, 6.021) missed | 2.377 (2.211, 2.554) missed |
| collections | buildAndSerialize | 7408 / 15345 | 3938 / 4231 | 0.530 (0.501, 0.562) | 3.827 (3.787, 3.868) missed | 2.030 (1.897, 2.173) missed |
| large | serialize | 4085 / 6281 | 3562 / 4490 | 0.870 (0.850, 0.890) | 0.382 (0.377, 0.387) established | 0.332 (0.324, 0.341) established |
| large | serializeSameInstance | 3076 / 6160 | 2715 / 4456 | 0.884 (0.870, 0.898) | 0.308 (0.305, 0.312) established | 0.273 (0.268, 0.277) established |
| large | buildAndSerialize | 13037 / 72569 | 12106 / 70690 | 0.936 (0.926, 0.947) | 0.693 (0.685, 0.701) established | 0.649 (0.644, 0.653) established |

The candidate takes 0.35 to 0.53 times the baseline's time for scalar, nested, and collections writes on both profiles, 0.60 to 0.73 for text writes, and 0.85 to 0.94 for large writes, and every write interval lies below 1.
It removes 1.6 to 1.9 KB/op from every scalar, text, nested, and large write and 11.1 to 12.4 KB/op from collections writes, leaving 272 to 357 B/op for previously encoded scalar and nested records and 600 to 666 B/op for previously encoded collections records.
The reduction is similar in each size-cache state, so it does not come from generated size memoization; `buildAndSerialize` keeps the allocation of building the record.
Text and large writes improve less in relative terms because UTF-8 counting or payload output dominates their time.

Small writes still miss the native/Chill tolerance, but closer: scalar and nested `serialize` and `serializeSameInstance` move from 6.2 to 7.1 times Chill's time to 2.4 to 2.9 times, collections writes from 3.8 to 6.1 to 2.0 to 2.5, and `buildAndSerialize` of scalar and nested records from 3.1 to 3.3 to 1.5 to 1.6.
Protobuf 4 text writes move from 1.8 to 2.2 to 1.3 to 1.6.
The [generated-size report](serializer-generated-size.md) measures the later change for these writes.

| Profile | Variant | Established | Inconclusive | Missed |
|---|---|---:|---:|---:|
| Protobuf 4, 40 cells | baseline | 26 | 0 | 14 |
| Protobuf 4, 40 cells | candidate | 26 | 0 | 14 |
| Protobuf 3, 40 cells | baseline | 28 | 1 | 11 |
| Protobuf 3, 40 cells | candidate | 29 | 0 | 11 |

The only verdict change is Protobuf 3 text `buildAndSerialize`, from inconclusive to established.

### Separating the sources

Plan `issue43-sizing-1` measured the first candidate with the descriptors and floor variants.
The baseline-to-descriptors step measures the traversal bookkeeping and field enumeration, the descriptors-to-first-candidate step measures per-use descriptor work, and the floor-to-first-candidate step measures what safe sizing still cost.

Protobuf 4 write time and allocation:

| Workload | Operation | Baseline | Descriptors | First candidate | Floor | First candidate/baseline | First candidate native/Chill |
|---|---|---:|---:|---:|---:|---|---|
| scalar | serialize | 691 / 1978 | 412 / 553 | 345 / 409 | 44 / 136 | 0.490 (0.426, 0.565) | 3.473 (2.935, 4.108) missed |
| scalar | serializeSameInstance | 574 / 1864 | 340 / 448 | 271 / 320 | 31 / 96 | 0.473 (0.450, 0.496) | 3.164 (2.998, 3.339) missed |
| scalar | buildAndSerialize | 994 / 2705 | 648 / 1217 | 532 / 1073 | 211 / 733 | 0.549 (0.520, 0.580) | 1.730 (1.684, 1.776) missed |
| text | serialize | 1789 / 2129 | 1419 / 585 | 1327 / 441 | 534 / 136 | 0.707 (0.633, 0.791) | 1.485 (1.365, 1.615) missed |
| text | serializeSameInstance | 1707 / 1970 | 1273 / 520 | 1180 / 376 | 489 / 136 | 0.696 (0.666, 0.727) | 1.563 (1.537, 1.589) missed |
| text | buildAndSerialize | 2564 / 3841 | 2153 / 2273 | 1918 / 2129 | 1158 / 1768 | 0.746 (0.715, 0.779) | 1.275 (1.227, 1.324) missed |
| nested | serialize | 837 / 2122 | 531 / 608 | 399 / 464 | 67 / 136 | 0.484 (0.474, 0.494) | 3.296 (3.222, 3.371) missed |
| nested | serializeSameInstance | 682 / 1904 | 428 / 504 | 324 / 360 | 55 / 136 | 0.476 (0.456, 0.498) | 2.992 (2.860, 3.131) missed |
| nested | buildAndSerialize | 1220 / 2864 | 793 / 1272 | 650 / 1128 | 257 / 744 | 0.537 (0.509, 0.567) | 1.771 (1.737, 1.805) missed |
| collections | serialize | 5297 / 12702 | 2858 / 2019 | 2373 / 716 | 849 / 283 | 0.451 (0.423, 0.480) | 2.647 (2.498, 2.805) missed |
| collections | serializeSameInstance | 4872 / 12798 | 2645 / 1928 | 2176 / 664 | 743 / 142 | 0.442 (0.415, 0.470) | 2.652 (2.484, 2.831) missed |
| collections | buildAndSerialize | 7298 / 15270 | 4380 / 5345 | 3834 / 4038 | 1642 / 1736 | 0.526 (0.508, 0.544) | 2.093 (2.021, 2.167) missed |
| large | serialize | 4145 / 6207 | 3843 / 4713 | 3701 / 4569 | 3211 / 4280 | 0.895 (0.872, 0.918) | 0.352 (0.350, 0.353) established |
| large | serializeSameInstance | 3087 / 6086 | 2859 / 4648 | 2759 / 4504 | 2361 / 4280 | 0.895 (0.873, 0.917) | 0.282 (0.277, 0.287) established |
| large | buildAndSerialize | 12914 / 72505 | 12239 / 70913 | 12071 / 70758 | 11678 / 70413 | 0.928 (0.917, 0.939) | 0.650 (0.644, 0.657) established |

Protobuf 4 decomposition:

| Workload | Operation | Descriptors/baseline | First candidate/descriptors | Floor/first candidate |
|---|---|---|---|---|
| scalar | serialize | 0.593 (0.567, 0.620) | 0.827 (0.716, 0.955) | 0.129 (0.112, 0.148) |
| scalar | serializeSameInstance | 0.599 (0.583, 0.615) | 0.789 (0.752, 0.829) | 0.115 (0.110, 0.120) |
| scalar | buildAndSerialize | 0.674 (0.638, 0.711) | 0.815 (0.792, 0.839) | 0.391 (0.376, 0.407) |
| text | serialize | 0.800 (0.698, 0.917) | 0.884 (0.779, 1.003) | 0.398 (0.361, 0.439) |
| text | serializeSameInstance | 0.730 (0.685, 0.778) | 0.953 (0.907, 1.001) | 0.410 (0.376, 0.447) |
| text | buildAndSerialize | 0.848 (0.813, 0.884) | 0.880 (0.843, 0.919) | 0.608 (0.573, 0.646) |
| nested | serialize | 0.641 (0.630, 0.654) | 0.755 (0.736, 0.773) | 0.168 (0.165, 0.172) |
| nested | serializeSameInstance | 0.626 (0.609, 0.644) | 0.760 (0.730, 0.792) | 0.170 (0.162, 0.178) |
| nested | buildAndSerialize | 0.659 (0.618, 0.701) | 0.816 (0.785, 0.848) | 0.393 (0.380, 0.407) |
| collections | serialize | 0.542 (0.510, 0.577) | 0.831 (0.777, 0.888) | 0.341 (0.290, 0.401) |
| collections | serializeSameInstance | 0.546 (0.523, 0.571) | 0.809 (0.760, 0.861) | 0.345 (0.326, 0.364) |
| collections | buildAndSerialize | 0.601 (0.588, 0.614) | 0.875 (0.848, 0.903) | 0.427 (0.410, 0.444) |
| large | serialize | 0.923 (0.895, 0.952) | 0.970 (0.953, 0.987) | 0.860 (0.850, 0.870) |
| large | serializeSameInstance | 0.930 (0.909, 0.951) | 0.962 (0.946, 0.979) | 0.850 (0.835, 0.865) |
| large | buildAndSerialize | 0.935 (0.920, 0.949) | 0.993 (0.978, 1.008) | 0.963 (0.952, 0.974) |

Traversal bookkeeping and field enumeration are the larger source.
On both profiles, the descriptors variant takes 0.52 to 0.67 times the baseline's time for scalar, nested, and collections writes and removes 1.4 to 1.6 KB/op from scalar and nested writes and 9.4 to 10.9 KB/op from collections writes.
Precomputed layouts then take a further 0.75 to 0.90 of the descriptors variant's time for those writes and remove 120 to 144 B/op from scalar and nested writes and 1.3 to 1.5 KB/op from collections writes, whose map entries are messages with their own descriptors.
Only Protobuf 4 is tabulated for this plan; the ranges in this section include the Protobuf 3 values, which can be computed from the retained measurements.

Safe sizing remained the largest part of a small write.
The floor took 0.11 to 0.20 of the first candidate's time for previously encoded scalar and nested records on both profiles and 0.32 to 0.36 for collections; between the first candidate and the floor, 224 to 574 B/op remained on every previously encoded record.
That remainder includes Protobuf's reflective field access, besides the traversal, depth checks, and UTF-8 counting; this report does not apportion it.
In the pinned runtimes, `hasField`, `getField`, and `getRepeatedFieldCount` call generated accessors through `Method.invoke`, which boxes primitive results; map fields go through the generated map-field accessor instead, and map and repeated-enum `getField` copy their elements into a new list.
The single read of fields without presence, added after this plan, removes one of those reflective calls for each such field with a non-default value; the plans are not compared, so its share of the difference between them is not measured.
Removing reflection entirely would require reproducing protoc's Java accessor names, which ADR-0001's public-API design avoids.
The floor met the native/Chill tolerance in 38 of 40 Protobuf 4 cells and 37 of 40 Protobuf 3 cells, missing only large `deserialize` and `deserializeReuse` and inconclusive for Protobuf 3 text `serialize`; the small-write gap to Chill is therefore safe sizing, not output or framing.

The first candidate's verdicts matched the baseline's except Protobuf 3 text `serialize`, from inconclusive to established.
Intervals involving the Protobuf 3 text write cells of this plan are wide, for example 0.573 (0.374, 0.878) for the first candidate relative to the baseline.
In the first-candidate and floor runs, the standard deviation of their five log native/Chill ratios is 0.27 to 0.54, against a median of about 0.02 for the other write cells; their native means still fall in the same variant order as on Protobuf 4.
In the later plan, only the baseline's Protobuf 3 text `buildAndSerialize` shows such a spread, at 0.34, which explains its wide interval and its inconclusive verdict.

### Possible regressions

Two of the 50 candidate-versus-baseline intervals outside the write operations in plan `issue43-sizing-2`, 25 per profile, lie entirely above 1: Protobuf 4 scalar `streamCopy` at 1.033 (1.009, 1.058) and Protobuf 3 text `deserializeReuse` at 1.010 (1.001, 1.019).
In plan `issue43-sizing-1`, five such intervals lay above 1 for the first candidate, at most 1.049 (1.031, 1.068) for Protobuf 3 nested `streamCopy`; none of them recurs in the later plan.
Reading and stream copy run identical code in both builds; the only other difference is that the first metadata resolution for each class also builds its layouts.
The buffer-allocation report found the same pattern of small, inconsistent intervals on unchanged paths, so these intervals are recorded as run-to-run drift between sequential runs and are not attributed to the change.

## Limitations

The variants of each plan ran sequentially on one VM per profile, over about two and a half hours in the later plan and five in the earlier one, so host drift can affect native times; the Chill lane controls part of it within each block.
Five blocks per run give limited information about the distribution of ratios.
Neither plan is compared with the buffer-allocation report, which used a different VM, image, and kernel.
The corpus has no shared children, deep nesting, unknown fields, enum fields, or messages with many declared fields; the summary bound and the release of frames beyond the 64 retained affect those shapes, which the unit tests check for correctness but these measurements do not time.
The corpus and warm setup do not cover first-use costs, other Flink or JDK versions, or record shapes outside the five workloads.

## Retained evidence

The [data inventory](data/serializer-sizing-allocation-v0.2.0/README.md) lists the twelve result archives of both plans, their checksums, and how to reconstruct each measured tree.
