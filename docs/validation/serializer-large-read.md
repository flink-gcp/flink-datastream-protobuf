---
title: "Aliased large-frame reads in the native serializer"
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

# Aliased large-frame reads in the native serializer

This report measures the large-frame read path changed for [#113](https://github.com/flink-gcp/flink-datastream-protobuf/issues/113).
After the [generated-size change](serializer-generated-size.md), large `deserialize` and `deserializeReuse` were the only cells that missed the ADR-0004 native/Chill tolerance on both profiles, at 1.26 to 1.27 times Chill's time with about 132 KB/op allocated for a 64 KiB payload.
One plan on temporary GCE VMs compares the change with the unchanged serializer.
The measurement contract is [ADR-0004](../adr/0004-serializer-benchmarks.md); the comparison does not establish whole-job throughput, heap retention, or state compatibility.

## Change under test

Before the change, a payload above 4 KiB was parsed by Protobuf's stream decoder, which in both pinned runtimes copies a `bytes` value larger than its 4 KiB buffer twice.
The changed serializer copies such a payload through its retained 4 KiB array into a new array, which starts at no more than 1 MiB and doubles until it holds the payload, and parses it with aliasing enabled, so decoded `bytes` fields, proto2 strings, and unknown length-delimited fields may share that array; proto3 strings are still decoded into new `String` values.
Payloads of at most 4 KiB keep the retained scratch array and the non-aliasing array decoder, and writes are unchanged.
[ADR-0001](../adr/0001-native-protobuf-type-integration.md#message-framing-version-1) records the amended read-side rule, the decoder it requires, its retention trade-off, and the alternatives.

The benchmark record has one `bytes` field, a `string`, a nested message, a packed repeated `int64`, and a map.
Only the large workload, whose `bytes` field holds 65,536 random bytes, has a payload above 4 KiB; the text workload's payload is below it, so the other workloads read through the unchanged scratch-array strategy, whose read loop and end-of-frame check this change simplifies.

## Method

The plan used the [temporary GCE procedure](../development.md#temporary-gce-execution) in project `flink-gcp`.
Each Protobuf profile ran on its own `c3-standard-8` VM in `asia-northeast1-b` (Intel Xeon Platinum 8481C, Sapphire Rapids, 8 vCPUs, 32 GiB) from image `ubuntu-2404-noble-amd64-v20261024`, kernel 7.0.0-1014-gcp, with Temurin 17.0.21, Flink 2.3.0, and Protobuf/protoc 3.25.9 or 4.33.6.
Both VMs recorded `TERMINATE` host maintenance and `NONE` as the only maintenance event, passed the load gate before every build and measurement, and reported the same machine type, CPU platform, image, kernel, and JDK.
`just benchmark-pair` ran the ADR-0004 long-run protocol for the native and Chill Protobuf lanes, and each variant took about 70 minutes.

| Plan | Source and base | Variants, in run order | Time (UTC) |
|---|---|---|---|
| `issue113-read-3` | `ed66955bf8e168759a540882cad661fcc934dc5c` on `ea2cae6` | baseline, candidate | 2026-10-25, 04:42 to 07:13 |

The baseline is the previous serializer and its tests, identical to the base under `src`; the candidate is the source commit unchanged.

Comparisons between the variants use the Chill lane as a per-block control, as in the earlier reports: the ratio of the two variants' geometric-mean native/Chill time ratios, with a pooled two-sample Student-t interval on the five log ratios of each run (8 degrees of freedom).
Below 1 favors the candidate.
Allocation is the five-fork mean of JMH's normalized allocation, in bytes per operation.
These intervals are per cell, without a simultaneous claim across cells.

## Results

### Reading and copying

Each variant cell is native time in ns/op and allocation in B/op, as five-fork means.
The last three columns give the candidate's time relative to the baseline and each variant's native/Chill interval and ADR-0004 verdict.

Protobuf 4:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | deserialize | 111 / 352 | 108 / 352 | 0.952 (0.886, 1.022) | 0.660 (0.626, 0.696) established | 0.628 (0.588, 0.671) established |
| scalar | deserializeReuse | 118 / 352 | 109 / 352 | 0.929 (0.890, 0.970) | 0.665 (0.633, 0.698) established | 0.618 (0.607, 0.629) established |
| scalar | streamCopy | 15 / 0 | 15 / 0 | 1.007 (0.964, 1.052) | 0.041 (0.039, 0.042) established | 0.041 (0.039, 0.043) established |
| scalar | objectCopy | 1 / 0 | 1 / 0 | 0.986 (0.937, 1.037) | 0.006 (0.005, 0.006) established | 0.005 (0.005, 0.006) established |
| scalar | objectCopyReuse | 2 / 0 | 2 / 0 | 1.017 (0.994, 1.042) | 0.008 (0.008, 0.008) established | 0.008 (0.008, 0.009) established |
| text | deserialize | 1237 / 4104 | 1229 / 4104 | 0.987 (0.971, 1.003) | 0.819 (0.805, 0.833) established | 0.808 (0.801, 0.815) established |
| text | deserializeReuse | 1232 / 4104 | 1234 / 4104 | 1.001 (0.990, 1.011) | 0.811 (0.805, 0.817) established | 0.811 (0.803, 0.819) established |
| text | streamCopy | 50 / 0 | 51 / 0 | 1.021 (0.999, 1.044) | 0.018 (0.018, 0.018) established | 0.018 (0.018, 0.019) established |
| text | objectCopy | 1 / 0 | 1 / 0 | 0.951 (0.909, 0.995) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| text | objectCopyReuse | 2 / 0 | 2 / 0 | 0.998 (0.971, 1.026) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| nested | deserialize | 170 / 512 | 167 / 512 | 0.985 (0.950, 1.022) | 0.710 (0.697, 0.723) established | 0.699 (0.672, 0.728) established |
| nested | deserializeReuse | 182 / 512 | 173 / 512 | 0.941 (0.882, 1.005) | 0.745 (0.727, 0.764) established | 0.701 (0.651, 0.756) established |
| nested | streamCopy | 16 / 0 | 15 / 0 | 0.964 (0.948, 0.980) | 0.031 (0.031, 0.031) established | 0.030 (0.029, 0.030) established |
| nested | objectCopy | 1 / 0 | 1 / 0 | 1.031 (0.942, 1.128) | 0.004 (0.003, 0.004) established | 0.004 (0.003, 0.004) established |
| nested | objectCopyReuse | 2 / 0 | 2 / 0 | 0.999 (0.967, 1.032) | 0.006 (0.005, 0.006) established | 0.006 (0.005, 0.006) established |
| collections | deserialize | 1492 / 3792 | 1494 / 3792 | 0.946 (0.874, 1.024) | 0.955 (0.878, 1.038) established | 0.903 (0.864, 0.944) established |
| collections | deserializeReuse | 1494 / 3792 | 1494 / 3792 | 0.979 (0.953, 1.007) | 0.921 (0.909, 0.933) established | 0.902 (0.875, 0.930) established |
| collections | streamCopy | 25 / 0 | 25 / 0 | 1.001 (0.955, 1.048) | 0.008 (0.008, 0.008) established | 0.008 (0.008, 0.008) established |
| collections | objectCopy | 1 / 0 | 1 / 0 | 1.051 (0.936, 1.181) | 0.001 (0.000, 0.001) established | 0.001 (0.001, 0.001) established |
| collections | objectCopyReuse | 2 / 0 | 2 / 0 | 0.955 (0.886, 1.028) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| large | deserialize | 20843 / 132056 | 9120 / 65969 | 0.436 (0.434, 0.438) | 1.253 (1.248, 1.257) missed | 0.546 (0.543, 0.549) established |
| large | deserializeReuse | 20896 / 132056 | 9115 / 65974 | 0.434 (0.426, 0.443) | 1.255 (1.244, 1.265) missed | 0.545 (0.533, 0.557) established |
| large | streamCopy | 2980 / 0 | 2963 / 0 | 0.988 (0.968, 1.009) | 0.110 (0.108, 0.112) established | 0.109 (0.107, 0.110) established |
| large | objectCopy | 1 / 0 | 1 / 0 | 0.984 (0.924, 1.048) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |
| large | objectCopyReuse | 2 / 0 | 2 / 0 | 0.992 (0.987, 0.997) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |

Protobuf 3:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | deserialize | 109 / 376 | 103 / 376 | 0.951 (0.924, 0.979) | 0.650 (0.631, 0.670) established | 0.618 (0.607, 0.630) established |
| scalar | deserializeReuse | 117 / 376 | 111 / 376 | 0.942 (0.915, 0.969) | 0.676 (0.664, 0.688) established | 0.637 (0.618, 0.656) established |
| scalar | streamCopy | 14 / 0 | 14 / 0 | 1.011 (0.993, 1.030) | 0.039 (0.039, 0.040) established | 0.040 (0.039, 0.040) established |
| scalar | objectCopy | 1 / 0 | 1 / 0 | 0.987 (0.900, 1.083) | 0.006 (0.006, 0.006) established | 0.006 (0.005, 0.006) established |
| scalar | objectCopyReuse | 2 / 0 | 2 / 0 | 0.988 (0.966, 1.012) | 0.008 (0.008, 0.009) established | 0.008 (0.008, 0.008) established |
| text | deserialize | 1263 / 4128 | 1263 / 4128 | 0.999 (0.991, 1.006) | 0.811 (0.805, 0.817) established | 0.810 (0.806, 0.814) established |
| text | deserializeReuse | 1274 / 4128 | 1271 / 4128 | 1.000 (0.991, 1.008) | 0.813 (0.807, 0.819) established | 0.813 (0.807, 0.819) established |
| text | streamCopy | 50 / 0 | 50 / 0 | 1.042 (1.013, 1.072) | 0.011 (0.010, 0.011) established | 0.011 (0.011, 0.011) established |
| text | objectCopy | 1 / 0 | 1 / 0 | 1.019 (0.966, 1.075) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |
| text | objectCopyReuse | 2 / 0 | 2 / 0 | 1.068 (0.906, 1.260) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| nested | deserialize | 176 / 536 | 172 / 536 | 0.971 (0.940, 1.003) | 0.743 (0.717, 0.771) established | 0.722 (0.712, 0.732) established |
| nested | deserializeReuse | 181 / 536 | 179 / 536 | 0.984 (0.947, 1.023) | 0.738 (0.718, 0.759) established | 0.726 (0.700, 0.754) established |
| nested | streamCopy | 16 / 0 | 15 / 0 | 0.971 (0.949, 0.995) | 0.031 (0.030, 0.032) established | 0.030 (0.030, 0.030) established |
| nested | objectCopy | 1 / 0 | 1 / 0 | 0.961 (0.909, 1.017) | 0.004 (0.004, 0.004) established | 0.004 (0.003, 0.004) established |
| nested | objectCopyReuse | 2 / 0 | 2 / 0 | 0.995 (0.952, 1.039) | 0.005 (0.005, 0.006) established | 0.005 (0.005, 0.006) established |
| collections | deserialize | 1466 / 3816 | 1463 / 3816 | 1.015 (0.938, 1.099) | 0.934 (0.890, 0.980) established | 0.948 (0.874, 1.030) established |
| collections | deserializeReuse | 1458 / 3816 | 1410 / 3611 | 0.999 (0.922, 1.084) | 0.922 (0.898, 0.947) established | 0.921 (0.839, 1.012) established |
| collections | streamCopy | 24 / 0 | 25 / 0 | 1.023 (0.980, 1.067) | 0.008 (0.008, 0.008) established | 0.008 (0.008, 0.008) established |
| collections | objectCopy | 1 / 0 | 1 / 0 | 0.958 (0.858, 1.070) | 0.001 (0.001, 0.001) established | 0.001 (0.000, 0.001) established |
| collections | objectCopyReuse | 2 / 0 | 2 / 0 | 1.010 (0.958, 1.066) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| large | deserialize | 20774 / 132080 | 8991 / 65988 | 0.433 (0.428, 0.437) | 1.257 (1.251, 1.264) missed | 0.544 (0.538, 0.550) established |
| large | deserializeReuse | 20818 / 132080 | 9041 / 65988 | 0.435 (0.433, 0.437) | 1.261 (1.258, 1.264) missed | 0.549 (0.545, 0.552) established |
| large | streamCopy | 3011 / 0 | 2966 / 0 | 0.985 (0.967, 1.003) | 0.112 (0.110, 0.115) established | 0.111 (0.110, 0.111) established |
| large | objectCopy | 1 / 0 | 1 / 0 | 0.978 (0.892, 1.073) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |
| large | objectCopyReuse | 2 / 0 | 2 / 0 | 0.999 (0.988, 1.010) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |

On both profiles, large `deserialize` and `deserializeReuse` take 0.433 to 0.436 times the baseline's time, with every interval between 0.426 and 0.443.
They allocate 65,969 to 65,988 B/op, against 132,056 to 132,080 B/op before; what remains is about the 64 KiB frame array plus the decoded message.
Their native/Chill ratios move from 1.253 to 1.261, missed, to 0.544 to 0.549, established.
Every other reading and copying cell stays established, and the small-frame reads allocate the same in both variants except Protobuf 3 collections `deserializeReuse`, at 3,816 and 3,611 B/op, within the 3,587 to 3,816 B/op that the [generated-size report](serializer-generated-size.md#reading-and-copying) recorded for the same scratch-array reads.

### Writes

Writes run identical code in both builds.

Protobuf 4:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | serialize | 58 / 136 | 58 / 136 | 0.996 (0.958, 1.035) | 0.602 (0.587, 0.617) established | 0.599 (0.576, 0.623) established |
| scalar | serializeSameInstance | 46 / 96 | 46 / 96 | 1.000 (0.973, 1.028) | 0.537 (0.528, 0.545) established | 0.537 (0.521, 0.553) established |
| scalar | buildAndSerialize | 227 / 733 | 226 / 744 | 0.993 (0.975, 1.010) | 0.741 (0.732, 0.750) established | 0.736 (0.723, 0.749) established |
| text | serialize | 559 / 136 | 548 / 136 | 0.985 (0.861, 1.127) | 0.679 (0.605, 0.762) established | 0.669 (0.597, 0.749) established |
| text | serializeSameInstance | 651 / 136 | 567 / 136 | 0.844 (0.692, 1.029) | 0.837 (0.787, 0.892) established | 0.707 (0.562, 0.889) established |
| text | buildAndSerialize | 1202 / 1768 | 1247 / 1768 | 1.050 (0.978, 1.127) | 0.793 (0.736, 0.854) established | 0.832 (0.798, 0.869) established |
| nested | serialize | 81 / 136 | 81 / 136 | 0.999 (0.982, 1.017) | 0.668 (0.658, 0.678) established | 0.668 (0.657, 0.678) established |
| nested | serializeSameInstance | 68 / 136 | 69 / 136 | 1.008 (0.990, 1.027) | 0.630 (0.622, 0.638) established | 0.635 (0.624, 0.647) established |
| nested | buildAndSerialize | 271 / 744 | 270 / 744 | 1.003 (0.981, 1.024) | 0.735 (0.721, 0.750) established | 0.737 (0.725, 0.750) established |
| collections | serialize | 835 / 136 | 824 / 142 | 0.944 (0.806, 1.105) | 0.894 (0.797, 1.003) established | 0.844 (0.726, 0.981) established |
| collections | serializeSameInstance | 757 / 142 | 763 / 136 | 1.054 (0.940, 1.182) | 0.895 (0.781, 1.027) established | 0.943 (0.925, 0.962) established |
| collections | buildAndSerialize | 1719 / 1736 | 1662 / 1736 | 0.968 (0.901, 1.040) | 0.925 (0.865, 0.990) established | 0.896 (0.848, 0.946) established |
| large | serialize | 3615 / 4506 | 3654 / 4506 | 1.006 (0.990, 1.023) | 0.336 (0.330, 0.341) established | 0.338 (0.334, 0.342) established |
| large | serializeSameInstance | 2737 / 4472 | 2751 / 4472 | 1.011 (0.997, 1.025) | 0.268 (0.265, 0.272) established | 0.271 (0.269, 0.273) established |
| large | buildAndSerialize | 12267 / 70706 | 12307 / 70684 | 0.998 (0.990, 1.006) | 0.645 (0.640, 0.650) established | 0.644 (0.641, 0.647) established |

Protobuf 3:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | serialize | 61 / 136 | 61 / 136 | 1.007 (0.973, 1.041) | 0.617 (0.598, 0.635) established | 0.621 (0.604, 0.638) established |
| scalar | serializeSameInstance | 48 / 96 | 47 / 96 | 0.977 (0.941, 1.015) | 0.547 (0.527, 0.567) established | 0.534 (0.520, 0.549) established |
| scalar | buildAndSerialize | 228 / 744 | 227 / 744 | 1.000 (0.986, 1.013) | 0.744 (0.738, 0.749) established | 0.743 (0.733, 0.754) established |
| text | serialize | 961 / 136 | 1138 / 136 | 1.053 (0.448, 2.473) | 0.580 (0.247, 1.362) inconclusive | 0.611 (0.345, 1.082) established |
| text | serializeSameInstance | 949 / 136 | 940 / 136 | 0.853 (0.448, 1.625) | 0.515 (0.277, 0.955) established | 0.439 (0.274, 0.702) established |
| text | buildAndSerialize | 1903 / 1768 | 1901 / 1768 | 0.978 (0.475, 2.011) | 0.526 (0.290, 0.956) established | 0.515 (0.274, 0.967) established |
| nested | serialize | 81 / 136 | 105 / 136 | 1.274 (1.099, 1.477) | 0.674 (0.668, 0.679) established | 0.859 (0.719, 1.026) established |
| nested | serializeSameInstance | 79 / 136 | 70 / 136 | 0.888 (0.741, 1.063) | 0.719 (0.580, 0.891) established | 0.638 (0.618, 0.659) established |
| nested | buildAndSerialize | 315 / 744 | 300 / 744 | 0.951 (0.778, 1.163) | 0.856 (0.718, 1.020) established | 0.814 (0.689, 0.963) established |
| collections | serialize | 845 / 136 | 853 / 149 | 1.049 (0.953, 1.154) | 0.886 (0.791, 0.991) established | 0.929 (0.908, 0.951) established |
| collections | serializeSameInstance | 787 / 155 | 791 / 136 | 1.090 (0.954, 1.244) | 0.872 (0.745, 1.021) established | 0.951 (0.927, 0.975) established |
| collections | buildAndSerialize | 1823 / 2120 | 1848 / 2120 | 1.007 (0.900, 1.127) | 0.938 (0.833, 1.057) established | 0.945 (0.886, 1.008) established |
| large | serialize | 3659 / 4506 | 3627 / 4506 | 0.980 (0.954, 1.006) | 0.340 (0.336, 0.345) established | 0.333 (0.324, 0.343) established |
| large | serializeSameInstance | 2716 / 4464 | 2744 / 4472 | 1.004 (0.986, 1.022) | 0.271 (0.268, 0.273) established | 0.272 (0.266, 0.277) established |
| large | buildAndSerialize | 12257 / 70695 | 12211 / 70706 | 0.992 (0.981, 1.003) | 0.649 (0.644, 0.654) established | 0.644 (0.637, 0.651) established |

Every write cell of both variants is established, except Protobuf 3 text `serialize` in the baseline run, which is inconclusive at 0.580 (0.247, 1.362).
The standard deviation of the five log native/Chill ratios is 0.38 to 0.69 for the three Protobuf 3 text write cells in both variants, against at most 0.19 for every other write cell; the earlier reports recorded the same spread for these cells.
Write allocations differ between the variants by at most 22 B/op; the code is identical, so these differences are not attributed to the change.

| Profile | Variant | Established | Inconclusive | Missed |
|---|---|---:|---:|---:|
| Protobuf 4, 40 cells | baseline | 38 | 0 | 2 |
| Protobuf 4, 40 cells | candidate | 40 | 0 | 0 |
| Protobuf 3, 40 cells | baseline | 37 | 1 | 2 |
| Protobuf 3, 40 cells | candidate | 40 | 0 | 0 |

Every candidate cell meets the tolerance on both profiles.

### Possible regressions

Two candidate-versus-baseline intervals outside the large reads lie entirely above 1, both on code that runs identically in both builds.
Protobuf 3 nested `serialize` is at 1.274 (1.099, 1.477): four of the candidate's five blocks ran at 0.91 to 0.92 times Chill's time and one at 0.66, where all five baseline blocks ran at 0.67 to 0.68.
Protobuf 3 text `streamCopy`, which never parses the payload, is at 1.042 (1.013, 1.072).
As in the earlier reports, such intervals on unchanged paths are recorded as run-to-run drift between sequential runs and are not attributed to the change.

## Limitations

The two variants of each profile ran sequentially on one VM, over about two and a half hours, so host drift can affect native times; the Chill lane controls part of it within each block.
Five blocks per run give limited information about the distribution of ratios.
The plan is not compared with the earlier reports, which used other VMs, images, or JDK builds.
The corpus has one large shape, a single 64 KiB `bytes` field; large frames dominated by proto3 strings, many small fields, unknown fields, or proto2 strings use the changed path but are not timed here, and the unit tests check only their correctness.
Frames above 1 MiB, whose array grows by doubling, are tested for correctness but not timed.
The benchmarks measure per-record time and allocation, not how long applications keep decoded values; the retention of frame arrays through kept `ByteString` values is a documented trade-off, not a measured cost.
The corpus and warm setup do not cover first-use costs, other Flink or JDK versions, or record shapes outside the five workloads.
Two earlier plans, `issue113-read-1` and `issue113-read-2`, measured intermediate sources before the review changes, including one that read directly into the array that decoded values share; their results are not retained, and this report does not use them.

## Retained evidence

The [data inventory](data/serializer-large-read-v0.2.0/README.md) lists the four result archives, their checksums, and how to reconstruct each measured tree.
