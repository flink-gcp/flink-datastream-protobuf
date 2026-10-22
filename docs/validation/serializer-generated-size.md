---
title: "Generated sizes for small writes in the native serializer"
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

# Generated sizes for small writes in the native serializer

This report measures the generated-size path added for [#112](https://github.com/flink-gcp/flink-datastream-protobuf/issues/112).
After the [sizing-traversal change](serializer-sizing-allocation.md), scalar and nested `serialize` and `serializeSameInstance` still took 2.4 to 2.9 times Chill's time, collections writes 2.0 to 2.5 times, and scalar and nested `buildAndSerialize` 1.5 to 1.6 times, while a diagnostic variant without safe sizing met the ADR-0004 tolerance in almost every cell.
One plan on temporary GCE VMs compares the change with the unchanged serializer.
The measurement contract is [ADR-0004](../adr/0004-serializer-benchmarks.md); the comparison does not establish whole-job throughput or state compatibility.

## Change under test

Before the change, every write sized the record with the iterative traversal, which reads each field through Protobuf's reflective accessors.
The changed serializer first decides whether the payload type can use the runtime's memoized `getSerializedSize()`.
That requires an acyclic reachable type graph whose static height, the greatest parser depth a value can reach without unknown groups, is within the recursion limit.
For such a type, the serializer reads only the value's message fields to find unknown groups and values reporting another descriptor, and its repeated strings and string-valued maps to bound their wire size from below.
It then uses the generated size if that size is at most 4 KiB and the size limit, and confirms it by encoding into the existing 4 KiB scratch array; a size that wrapped around `int` implies at least 2 GiB of output, so that encoding fails and the traversal reports the exceeded limit.
Every other value, including every payload above 4 KiB, uses the traversal as before.
[ADR-0001](../adr/0001-native-protobuf-type-integration.md) records the conditions and the amended write-side rule; `ProtobufGeneratedSizeTest` compares the path's decisions with the traversal's.

The benchmark record is acyclic with static height 1: one nested message field without message fields of its own, and a map with `int64` values.
Its small workloads therefore use the generated size with one reflective read per write, and the large workload, whose payload exceeds 4 KiB, uses the traversal after that read.

## Method

The plan used the [temporary GCE procedure](../development.md#temporary-gce-execution) in project `flink-gcp`.
Each Protobuf profile ran on its own `c3-standard-8` VM in `asia-northeast1-b` (Intel Xeon Platinum 8481C, Sapphire Rapids, 8 vCPUs, 32 GiB) from image `ubuntu-2404-noble-amd64-v20261021`, kernel 7.0.0-1014-gcp, with Temurin 17.0.20.1, Flink 2.3.0, and Protobuf/protoc 3.25.9 or 4.33.6.
Both VMs recorded `TERMINATE` host maintenance and `NONE` as the only maintenance event, passed the load gate before every build and measurement, and reported the same machine type, CPU platform, image, kernel, and JDK.
`just benchmark-pair` ran the ADR-0004 long-run protocol for the native and Chill Protobuf lanes, and each variant took about 70 minutes.

| Plan | Source and base | Variants, in run order | Time (UTC) |
|---|---|---|---|
| `issue112-sizing-2` | `ee63f1c8da68cdcad4c961afb71b7754acfbbd3d` on `0c40c0b` | baseline, candidate | 2026-10-22, 03:21 to 05:53 |

The baseline is the previous serializer and its tests, identical to the base under `src/main`; the candidate is the source commit unchanged.

Comparisons between the variants use the Chill lane as a per-block control, as in the earlier reports: the ratio of the two variants' geometric-mean native/Chill time ratios, with a pooled two-sample Student-t interval on the five log ratios of each run (8 degrees of freedom).
Below 1 favors the candidate.
Allocation is the five-fork mean of JMH's normalized allocation, in bytes per operation.
These intervals are per cell, without a simultaneous claim across cells.

## Results

### Writes

Each variant cell is native time in ns/op and allocation in B/op, as five-fork means.
The last three columns give the candidate's time relative to the baseline and each variant's native/Chill interval and ADR-0004 verdict.

Protobuf 4:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | serialize | 271 / 346 | 58 / 136 | 0.217 (0.204, 0.230) | 2.774 (2.591, 2.968) missed | 0.601 (0.586, 0.616) established |
| scalar | serializeSameInstance | 231 / 280 | 46 / 96 | 0.200 (0.190, 0.210) | 2.693 (2.555, 2.839) missed | 0.538 (0.524, 0.552) established |
| scalar | buildAndSerialize | 487 / 1010 | 226 / 733 | 0.463 (0.449, 0.477) | 1.576 (1.536, 1.617) missed | 0.730 (0.712, 0.748) established |
| text | serialize | 1254 / 346 | 549 / 136 | 0.414 (0.381, 0.449) | 1.591 (1.524, 1.662) missed | 0.658 (0.602, 0.720) established |
| text | serializeSameInstance | 1058 / 312 | 611 / 136 | 0.562 (0.477, 0.663) | 1.409 (1.328, 1.495) missed | 0.792 (0.655, 0.958) established |
| text | buildAndSerialize | 1903 / 2034 | 1217 / 1768 | 0.636 (0.587, 0.689) | 1.295 (1.209, 1.389) missed | 0.823 (0.770, 0.881) established |
| nested | serialize | 326 / 357 | 81 / 136 | 0.247 (0.241, 0.253) | 2.670 (2.592, 2.751) missed | 0.660 (0.657, 0.662) established |
| nested | serializeSameInstance | 260 / 312 | 68 / 136 | 0.264 (0.256, 0.272) | 2.364 (2.283, 2.448) missed | 0.624 (0.617, 0.631) established |
| nested | buildAndSerialize | 575 / 1021 | 270 / 744 | 0.476 (0.462, 0.491) | 1.554 (1.508, 1.601) missed | 0.740 (0.725, 0.755) established |
| collections | serialize | 2201 / 666 | 838 / 136 | 0.363 (0.325, 0.405) | 2.461 (2.276, 2.660) missed | 0.893 (0.803, 0.992) established |
| collections | serializeSameInstance | 1960 / 600 | 760 / 142 | 0.388 (0.363, 0.415) | 2.416 (2.241, 2.605) missed | 0.937 (0.913, 0.963) established |
| collections | buildAndSerialize | 3663 / 3980 | 1690 / 1736 | 0.465 (0.446, 0.485) | 1.974 (1.925, 2.023) missed | 0.918 (0.880, 0.959) established |
| large | serialize | 3586 / 4490 | 3633 / 4506 | 1.018 (0.997, 1.040) | 0.338 (0.331, 0.347) established | 0.345 (0.341, 0.348) established |
| large | serializeSameInstance | 2695 / 4456 | 2703 / 4472 | 1.011 (0.989, 1.032) | 0.271 (0.268, 0.275) established | 0.274 (0.268, 0.280) established |
| large | buildAndSerialize | 12016 / 70695 | 12006 / 70706 | 0.998 (0.985, 1.010) | 0.648 (0.640, 0.657) established | 0.647 (0.642, 0.652) established |

Protobuf 3:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | serialize | 273 / 346 | 61 / 136 | 0.222 (0.212, 0.232) | 2.777 (2.710, 2.844) missed | 0.616 (0.588, 0.646) established |
| scalar | serializeSameInstance | 232 / 288 | 47 / 96 | 0.203 (0.192, 0.215) | 2.657 (2.489, 2.836) missed | 0.540 (0.528, 0.552) established |
| scalar | buildAndSerialize | 483 / 1010 | 229 / 750 | 0.475 (0.463, 0.487) | 1.573 (1.537, 1.611) missed | 0.747 (0.732, 0.761) established |
| text | serialize | 1273 / 346 | 971 / 136 | 0.687 (0.420, 1.124) | 0.747 (0.689, 0.810) established | 0.514 (0.286, 0.923) established |
| text | serializeSameInstance | 1147 / 312 | 1120 / 136 | 1.031 (0.524, 2.028) | 0.674 (0.605, 0.750) established | 0.694 (0.310, 1.558) inconclusive |
| text | buildAndSerialize | 1952 / 2039 | 1541 / 1768 | 0.719 (0.482, 1.071) | 0.598 (0.561, 0.636) established | 0.429 (0.267, 0.692) established |
| nested | serialize | 329 / 357 | 87 / 136 | 0.260 (0.222, 0.304) | 2.744 (2.608, 2.888) missed | 0.713 (0.595, 0.855) established |
| nested | serializeSameInstance | 266 / 312 | 74 / 136 | 0.278 (0.241, 0.319) | 2.449 (2.408, 2.491) missed | 0.680 (0.575, 0.804) established |
| nested | buildAndSerialize | 577 / 1021 | 299 / 744 | 0.519 (0.450, 0.598) | 1.576 (1.533, 1.619) missed | 0.817 (0.690, 0.968) established |
| collections | serialize | 2292 / 666 | 863 / 136 | 0.366 (0.332, 0.403) | 2.473 (2.341, 2.612) missed | 0.905 (0.816, 1.003) established |
| collections | serializeSameInstance | 1960 / 613 | 776 / 136 | 0.398 (0.390, 0.405) | 2.352 (2.313, 2.392) missed | 0.936 (0.922, 0.950) established |
| collections | buildAndSerialize | 3952 / 4103 | 1856 / 2120 | 0.481 (0.444, 0.520) | 1.980 (1.858, 2.110) missed | 0.952 (0.888, 1.020) established |
| large | serialize | 3579 / 4490 | 3603 / 4506 | 1.020 (0.981, 1.061) | 0.325 (0.314, 0.337) established | 0.332 (0.322, 0.343) established |
| large | serializeSameInstance | 2738 / 4456 | 2747 / 4472 | 1.006 (0.986, 1.026) | 0.270 (0.265, 0.275) established | 0.271 (0.267, 0.276) established |
| large | buildAndSerialize | 12294 / 70679 | 12351 / 70695 | 1.003 (0.997, 1.009) | 0.647 (0.643, 0.651) established | 0.649 (0.646, 0.652) established |

On both profiles, the candidate takes 0.20 to 0.28 times the baseline's time for scalar and nested `serialize` and `serializeSameInstance`, 0.36 to 0.40 for collections `serialize` and `serializeSameInstance`, and 0.46 to 0.52 for scalar, nested, and collections `buildAndSerialize`, and each of those intervals lies below 1.
Protobuf 4 text writes take 0.41 to 0.64 times the baseline's time.
Previously encoded scalar, text, nested, and collections records now allocate 96 to 142 B/op, against 280 to 666 B/op before; `buildAndSerialize` keeps the allocation of building the record.

Every small write except Protobuf 3 text `serializeSameInstance`, which is inconclusive, now meets the native/Chill tolerance.
Scalar and nested `serialize` and `serializeSameInstance` take 0.54 to 0.71 times Chill's time, scalar and nested `buildAndSerialize` 0.73 to 0.82, collections writes 0.89 to 0.95, and Protobuf 4 text writes 0.66 to 0.82.

Large writes are unchanged within the intervals, all of which contain 1, and remain established at 0.27 to 0.65 times Chill's time.
They allocate 11 to 16 B/op more, which is consistent with the one reflective read before the traversal; this report does not profile it.

Protobuf 3 text writes vary more within each run than the other cells.
The standard deviation of the candidate's five log native/Chill ratios is 0.38 to 0.65 for them, against at most 0.15 for the other write cells, so their candidate-versus-baseline intervals are wide and contain 1, and `serializeSameInstance` is inconclusive at 0.694 (0.310, 1.558); the earlier reports recorded the same spread for these cells on Protobuf 3.

| Profile | Variant | Established | Inconclusive | Missed |
|---|---|---:|---:|---:|
| Protobuf 4, 40 cells | baseline | 26 | 0 | 14 |
| Protobuf 4, 40 cells | candidate | 38 | 0 | 2 |
| Protobuf 3, 40 cells | baseline | 29 | 0 | 11 |
| Protobuf 3, 40 cells | candidate | 37 | 1 | 2 |

The remaining misses on both profiles are large `deserialize` and `deserializeReuse`, at 1.255 to 1.274 times Chill's time, which [#113](https://github.com/flink-gcp/flink-datastream-protobuf/issues/113) addresses.

### Reading and copying

Reading, stream copy, and object copy run identical code in both builds.

Protobuf 4:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | deserialize | 110 / 352 | 112 / 352 | 1.039 (0.965, 1.118) | 0.640 (0.624, 0.657) established | 0.665 (0.611, 0.724) established |
| scalar | deserializeReuse | 117 / 352 | 117 / 352 | 0.991 (0.948, 1.036) | 0.670 (0.643, 0.698) established | 0.664 (0.642, 0.687) established |
| scalar | streamCopy | 15 / 0 | 14 / 0 | 0.958 (0.933, 0.984) | 0.041 (0.041, 0.041) established | 0.039 (0.038, 0.041) established |
| scalar | objectCopy | 1 / 0 | 1 / 0 | 1.025 (0.957, 1.099) | 0.006 (0.005, 0.006) established | 0.006 (0.006, 0.006) established |
| scalar | objectCopyReuse | 2 / 0 | 2 / 0 | 1.010 (0.995, 1.025) | 0.008 (0.008, 0.008) established | 0.008 (0.008, 0.009) established |
| text | deserialize | 1228 / 4104 | 1227 / 4104 | 1.000 (0.985, 1.015) | 0.823 (0.811, 0.835) established | 0.823 (0.813, 0.832) established |
| text | deserializeReuse | 1226 / 4104 | 1225 / 4104 | 0.995 (0.983, 1.007) | 0.814 (0.808, 0.821) established | 0.810 (0.800, 0.820) established |
| text | streamCopy | 50 / 0 | 50 / 0 | 1.013 (0.994, 1.034) | 0.018 (0.017, 0.018) established | 0.018 (0.018, 0.018) established |
| text | objectCopy | 1 / 0 | 1 / 0 | 0.934 (0.872, 1.001) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| text | objectCopyReuse | 2 / 0 | 2 / 0 | 0.994 (0.962, 1.026) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| nested | deserialize | 169 / 512 | 172 / 512 | 1.006 (0.950, 1.066) | 0.712 (0.690, 0.735) established | 0.717 (0.674, 0.763) established |
| nested | deserializeReuse | 180 / 512 | 179 / 512 | 1.004 (0.958, 1.053) | 0.741 (0.709, 0.775) established | 0.745 (0.718, 0.772) established |
| nested | streamCopy | 16 / 0 | 16 / 0 | 0.960 (0.934, 0.988) | 0.032 (0.031, 0.032) established | 0.030 (0.029, 0.031) established |
| nested | objectCopy | 1 / 0 | 1 / 0 | 0.990 (0.910, 1.077) | 0.004 (0.003, 0.004) established | 0.004 (0.003, 0.004) established |
| nested | objectCopyReuse | 2 / 0 | 2 / 0 | 0.988 (0.971, 1.006) | 0.006 (0.006, 0.006) established | 0.006 (0.005, 0.006) established |
| collections | deserialize | 1441 / 3587 | 1488 / 3792 | 1.047 (0.943, 1.164) | 0.882 (0.816, 0.954) established | 0.924 (0.836, 1.021) established |
| collections | deserializeReuse | 1487 / 3792 | 1472 / 3792 | 1.012 (0.984, 1.042) | 0.898 (0.876, 0.921) established | 0.909 (0.887, 0.932) established |
| collections | streamCopy | 27 / 0 | 25 / 0 | 0.913 (0.807, 1.034) | 0.009 (0.007, 0.010) established | 0.008 (0.008, 0.008) established |
| collections | objectCopy | 1 / 0 | 1 / 0 | 1.013 (0.870, 1.180) | 0.001 (0.000, 0.001) established | 0.001 (0.000, 0.001) established |
| collections | objectCopyReuse | 2 / 0 | 2 / 0 | 0.996 (0.944, 1.051) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| large | deserialize | 20321 / 132056 | 20237 / 132056 | 1.000 (0.994, 1.006) | 1.255 (1.246, 1.263) missed | 1.255 (1.253, 1.257) missed |
| large | deserializeReuse | 20296 / 132056 | 20268 / 132056 | 1.001 (0.996, 1.006) | 1.254 (1.248, 1.259) missed | 1.256 (1.250, 1.261) missed |
| large | streamCopy | 2984 / 0 | 2957 / 0 | 0.994 (0.966, 1.024) | 0.113 (0.112, 0.114) established | 0.112 (0.109, 0.116) established |
| large | objectCopy | 1 / 0 | 1 / 0 | 1.070 (0.978, 1.171) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |
| large | objectCopyReuse | 2 / 0 | 2 / 0 | 1.002 (0.993, 1.012) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |

Protobuf 3:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | deserialize | 112 / 376 | 109 / 376 | 0.971 (0.946, 0.996) | 0.667 (0.662, 0.672) established | 0.648 (0.629, 0.667) established |
| scalar | deserializeReuse | 117 / 376 | 117 / 376 | 0.995 (0.974, 1.015) | 0.669 (0.656, 0.681) established | 0.665 (0.654, 0.676) established |
| scalar | streamCopy | 15 / 0 | 14 / 0 | 0.980 (0.945, 1.016) | 0.040 (0.039, 0.040) established | 0.039 (0.037, 0.041) established |
| scalar | objectCopy | 1 / 0 | 1 / 0 | 1.059 (1.006, 1.115) | 0.005 (0.005, 0.006) established | 0.006 (0.005, 0.006) established |
| scalar | objectCopyReuse | 2 / 0 | 2 / 0 | 0.979 (0.948, 1.010) | 0.008 (0.008, 0.009) established | 0.008 (0.008, 0.008) established |
| text | deserialize | 1275 / 4128 | 1272 / 4128 | 0.998 (0.991, 1.005) | 0.814 (0.807, 0.820) established | 0.812 (0.809, 0.815) established |
| text | deserializeReuse | 1280 / 4128 | 1282 / 4128 | 1.001 (0.993, 1.008) | 0.812 (0.807, 0.816) established | 0.812 (0.806, 0.818) established |
| text | streamCopy | 50 / 0 | 51 / 0 | 1.217 (0.919, 1.611) | 0.011 (0.011, 0.011) established | 0.013 (0.010, 0.019) established |
| text | objectCopy | 1 / 0 | 1 / 0 | 1.048 (0.992, 1.107) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |
| text | objectCopyReuse | 2 / 0 | 2 / 0 | 0.869 (0.713, 1.060) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| nested | deserialize | 175 / 536 | 177 / 536 | 1.007 (0.975, 1.039) | 0.732 (0.710, 0.754) established | 0.737 (0.719, 0.754) established |
| nested | deserializeReuse | 184 / 536 | 184 / 536 | 1.003 (0.965, 1.042) | 0.751 (0.721, 0.782) established | 0.753 (0.736, 0.769) established |
| nested | streamCopy | 16 / 0 | 15 / 0 | 0.954 (0.927, 0.982) | 0.030 (0.029, 0.031) established | 0.029 (0.028, 0.029) established |
| nested | objectCopy | 1 / 0 | 1 / 0 | 1.109 (0.992, 1.241) | 0.003 (0.003, 0.004) established | 0.004 (0.004, 0.004) established |
| nested | objectCopyReuse | 2 / 0 | 2 / 0 | 1.027 (0.997, 1.057) | 0.005 (0.005, 0.005) established | 0.006 (0.005, 0.006) established |
| collections | deserialize | 1473 / 3816 | 1440 / 3662 | 1.003 (0.929, 1.084) | 0.950 (0.918, 0.983) established | 0.953 (0.874, 1.038) established |
| collections | deserializeReuse | 1459 / 3816 | 1451 / 3739 | 1.018 (0.944, 1.097) | 0.939 (0.899, 0.980) established | 0.955 (0.882, 1.033) established |
| collections | streamCopy | 25 / 0 | 26 / 0 | 1.029 (0.976, 1.084) | 0.008 (0.008, 0.008) established | 0.008 (0.008, 0.009) established |
| collections | objectCopy | 1 / 0 | 1 / 0 | 0.947 (0.856, 1.048) | 0.001 (0.001, 0.001) established | 0.001 (0.000, 0.001) established |
| collections | objectCopyReuse | 2 / 0 | 2 / 0 | 0.974 (0.938, 1.011) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| large | deserialize | 21106 / 132080 | 21082 / 132080 | 0.998 (0.987, 1.010) | 1.270 (1.255, 1.285) missed | 1.268 (1.259, 1.277) missed |
| large | deserializeReuse | 21095 / 132080 | 21155 / 132080 | 1.005 (0.995, 1.015) | 1.267 (1.254, 1.281) missed | 1.274 (1.268, 1.280) missed |
| large | streamCopy | 3020 / 0 | 2995 / 0 | 0.979 (0.961, 0.998) | 0.112 (0.111, 0.113) established | 0.110 (0.108, 0.112) established |
| large | objectCopy | 1 / 0 | 1 / 0 | 0.981 (0.903, 1.066) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |
| large | objectCopyReuse | 2 / 0 | 2 / 0 | 1.002 (0.994, 1.010) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |

### Possible regressions

One of the 50 candidate-versus-baseline intervals outside the write operations, 25 per profile, lies entirely above 1: Protobuf 3 scalar `objectCopy` at 1.059 (1.006, 1.115), an operation that returns its input reference in about 1 ns.
No write interval lies entirely above 1.
As in the earlier reports, such small intervals on unchanged paths are recorded as run-to-run drift between sequential runs and are not attributed to the change.

## Limitations

The two variants of each profile ran sequentially on one VM, over about two and a half hours, so host drift can affect native times; the Chill lane controls part of it within each block.
Five blocks per run give limited information about the distribution of ratios.
The plan is not compared with the earlier reports, which used other VMs, images, or kernels.
The corpus has no recursive types, unknown fields, repeated strings, string-valued maps, proto2 strings, or values above 64 nested values; those use the traversal or its added checks, which the unit tests check for correctness but these measurements do not time.
The corpus and warm setup do not cover first-use costs, other Flink or JDK versions, or record shapes outside the five workloads.
An earlier plan, `issue112-sizing-1`, measured an intermediate source before the review changes; its results are not retained, and this report does not use them.

## Retained evidence

The [data inventory](data/serializer-generated-size-v0.2.0/README.md) lists the four result archives, their checksums, and how to reconstruct each measured tree.
