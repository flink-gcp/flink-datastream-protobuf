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

# Reused coded stream for large-frame writes in the native serializer

This report measures the large-frame write path changed for [#176](https://github.com/flink-gcp/flink-datastream-protobuf/issues/176).
In the [1.0.0 benchmark report](serializer-benchmarks-v1.0.0.md), native `serialize` of the 64 KiB `large` workload allocated 4,506 to 4,519 B/op, most of it the 4,096-byte buffer of a coded stream created for each record.
One plan on temporary GCE VMs compares the change with the unchanged serializer.
The measurement contract is [ADR-0004](../adr/0004-serializer-benchmarks.md); the comparison does not establish whole-job throughput or state compatibility.

## Change under test

Before the change, every payload above 4 KiB was written through `CodedOutputStream.newInstance(OutputStream)`, which in both pinned runtimes allocates a 4,096-byte buffer for each call.
The changed serializer creates one such stream with a 4 KiB buffer on its first large frame, over an output view that it points at each frame's target and expected size, and reuses both for later large frames.
A failed write discards the stream, because bytes left in its buffer must not reach a later frame.
Payloads of at most 4 KiB keep the retained scratch array, and reads are unchanged.
[ADR-0001](../adr/0001-native-protobuf-type-integration.md#message-framing-version-1) records the amended buffering rule.

Only the large workload, whose `bytes` field holds 65,536 random bytes, has a payload above 4 KiB, so the other workloads write through code that is identical in both builds.

## Method

The plan used the [temporary GCE procedure](../development.md#temporary-gce-execution) in project `flink-gcp`.
Each Protobuf profile ran on its own `c3-standard-8` VM in `asia-northeast1-b` (Intel Xeon Platinum 8481C, Sapphire Rapids, 8 vCPUs, 32 GiB) from image `ubuntu-2404-noble-amd64-v20261105`, kernel 7.0.0-1017-gcp, with Temurin 17.0.21, Flink 2.3.0, and Protobuf/protoc 3.25.9 or 4.36.2.
Both VMs recorded `TERMINATE` host maintenance and `NONE` as the only maintenance event, passed the load gate before every build and measurement, and reported the same machine type, CPU platform, image, kernel, and JDK.
`just benchmark-pair` ran the ADR-0004 long-run protocol for the native and Chill Protobuf lanes, and each variant took about 70 minutes.

| Plan | Source and base | Variants, in run order | Time (UTC) |
|---|---|---|---|
| `issue176-write-1` | `7c84fa0df9d65dfaf61d8c768084239ff3720f0f` on `fc64b42` | baseline, candidate | 2026-11-08, 06:54 to 09:25 |

The baseline is the previous serializer and its tests, identical to the base under `src`; the candidate is the source commit unchanged.

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
| scalar | serialize | 55 / 149 | 55 / 149 | 0.982 (0.942, 1.023) | 0.617 (0.599, 0.635) established | 0.606 (0.582, 0.631) established |
| scalar | serializeSameInstance | 37 / 96 | 37 / 96 | 1.053 (0.889, 1.247) | 0.455 (0.372, 0.557) established | 0.479 (0.470, 0.489) established |
| scalar | buildAndSerialize | 227 / 661 | 228 / 661 | 1.001 (0.985, 1.017) | 0.767 (0.764, 0.771) established | 0.768 (0.753, 0.782) established |
| text | serialize | 803 / 1957 | 806 / 1957 | 1.002 (0.987, 1.017) | 0.722 (0.716, 0.728) established | 0.723 (0.712, 0.735) established |
| text | serializeSameInstance | 790 / 1912 | 794 / 1920 | 1.006 (0.974, 1.040) | 0.698 (0.675, 0.722) established | 0.703 (0.688, 0.717) established |
| text | buildAndSerialize | 1395 / 3578 | 1399 / 3589 | 1.017 (0.991, 1.043) | 0.805 (0.782, 0.828) established | 0.818 (0.810, 0.827) established |
| nested | serialize | 88 / 245 | 88 / 245 | 1.011 (0.990, 1.033) | 0.710 (0.696, 0.725) established | 0.718 (0.706, 0.729) established |
| nested | serializeSameInstance | 64 / 192 | 64 / 192 | 0.993 (0.953, 1.033) | 0.598 (0.583, 0.613) established | 0.594 (0.569, 0.619) established |
| nested | buildAndSerialize | 287 / 757 | 350 / 757 | 1.203 (1.005, 1.440) | 0.788 (0.716, 0.869) established | 0.948 (0.781, 1.151) inconclusive |
| collections | serialize | 771 / 533 | 772 / 533 | 1.003 (0.980, 1.027) | 0.864 (0.855, 0.874) established | 0.867 (0.844, 0.890) established |
| collections | serializeSameInstance | 714 / 520 | 710 / 520 | 0.997 (0.973, 1.021) | 0.892 (0.882, 0.901) established | 0.889 (0.865, 0.913) established |
| collections | buildAndSerialize | 1641 / 2029 | 1656 / 2029 | 1.015 (0.979, 1.052) | 0.885 (0.869, 0.902) established | 0.898 (0.864, 0.934) established |
| large | serialize | 3629 / 4519 | 3201 / 335 | 0.881 (0.871, 0.890) | 0.344 (0.340, 0.348) established | 0.303 (0.302, 0.305) established |
| large | serializeSameInstance | 2755 / 4472 | 2150 / 288 | 0.785 (0.769, 0.802) | 0.279 (0.273, 0.285) established | 0.219 (0.217, 0.221) established |
| large | buildAndSerialize | 12088 / 70708 | 10860 / 66531 | 0.903 (0.895, 0.912) | 0.651 (0.645, 0.657) established | 0.588 (0.583, 0.593) established |

Protobuf 3:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | serialize | 63 / 136 | 63 / 136 | 1.001 (0.954, 1.050) | 0.649 (0.623, 0.676) established | 0.650 (0.623, 0.677) established |
| scalar | serializeSameInstance | 49 / 96 | 50 / 96 | 1.019 (0.965, 1.075) | 0.554 (0.530, 0.579) established | 0.564 (0.538, 0.592) established |
| scalar | buildAndSerialize | 231 / 744 | 233 / 750 | 1.002 (0.983, 1.020) | 0.758 (0.751, 0.766) established | 0.759 (0.745, 0.774) established |
| text | serialize | 977 / 136 | 976 / 136 | 1.014 (0.519, 1.983) | 0.514 (0.295, 0.894) established | 0.521 (0.290, 0.937) established |
| text | serializeSameInstance | 928 / 136 | 598 / 136 | 0.626 (0.387, 1.012) | 0.576 (0.324, 1.026) established | 0.361 (0.350, 0.372) established |
| text | buildAndSerialize | 1597 / 1768 | 1565 / 1768 | 1.322 (0.558, 3.133) | 0.460 (0.287, 0.736) established | 0.608 (0.241, 1.536) inconclusive |
| nested | serialize | 101 / 136 | 95 / 136 | 0.928 (0.736, 1.170) | 0.833 (0.688, 1.009) established | 0.773 (0.631, 0.947) established |
| nested | serializeSameInstance | 83 / 136 | 88 / 136 | 1.087 (0.845, 1.397) | 0.745 (0.596, 0.931) established | 0.809 (0.660, 0.993) established |
| nested | buildAndSerialize | 333 / 744 | 320 / 744 | 0.951 (0.776, 1.165) | 0.901 (0.780, 1.040) established | 0.857 (0.703, 1.043) established |
| collections | serialize | 848 / 136 | 861 / 155 | 1.009 (0.979, 1.040) | 0.935 (0.905, 0.965) established | 0.943 (0.926, 0.960) established |
| collections | serializeSameInstance | 785 / 136 | 789 / 136 | 0.961 (0.864, 1.069) | 0.950 (0.943, 0.957) established | 0.913 (0.804, 1.038) established |
| collections | buildAndSerialize | 1778 / 1992 | 1729 / 1736 | 0.955 (0.866, 1.053) | 0.938 (0.859, 1.025) established | 0.896 (0.828, 0.969) established |
| large | serialize | 3627 / 4506 | 3244 / 322 | 0.893 (0.882, 0.904) | 0.346 (0.343, 0.349) established | 0.309 (0.305, 0.313) established |
| large | serializeSameInstance | 2735 / 4472 | 2167 / 288 | 0.794 (0.784, 0.805) | 0.281 (0.277, 0.285) established | 0.223 (0.221, 0.225) established |
| large | buildAndSerialize | 11972 / 70695 | 10840 / 66522 | 0.906 (0.899, 0.914) | 0.650 (0.644, 0.656) established | 0.589 (0.586, 0.591) established |

On both profiles, large `serialize` takes 0.881 to 0.893 times the baseline's time, `serializeSameInstance` 0.785 to 0.794 times, and `buildAndSerialize` 0.903 to 0.906 times, with every interval between 0.769 and 0.914.
Each of these cells allocates 4,173 to 4,184 B/op less than the baseline: `serialize` and `serializeSameInstance` fall from 4,472 to 4,519 B/op to 288 to 335 B/op, and `buildAndSerialize` from about 70.7 KB/op to about 66.5 KB/op, which still includes building the 64 KiB message.
Their native/Chill ratios fall from 0.344 to 0.346 to 0.303 to 0.309 for `serialize`, from 0.279 to 0.281 to 0.219 to 0.223 for `serializeSameInstance`, and from 0.650 to 0.651 to 0.588 to 0.589 for `buildAndSerialize`.

### Reads

Reads run identical code in both builds.

Protobuf 4:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | deserialize | 105 / 352 | 104 / 352 | 0.967 (0.921, 1.015) | 0.622 (0.615, 0.629) established | 0.602 (0.568, 0.637) established |
| scalar | deserializeReuse | 109 / 352 | 113 / 352 | 1.032 (0.990, 1.077) | 0.619 (0.607, 0.632) established | 0.639 (0.610, 0.670) established |
| scalar | streamCopy | 15 / 0 | 14 / 0 | 0.951 (0.919, 0.983) | 0.039 (0.038, 0.041) established | 0.037 (0.036, 0.038) established |
| scalar | objectCopy | 1 / 0 | 1 / 0 | 0.965 (0.924, 1.008) | 0.006 (0.006, 0.006) established | 0.005 (0.005, 0.006) established |
| scalar | objectCopyReuse | 2 / 0 | 2 / 0 | 0.994 (0.975, 1.013) | 0.008 (0.008, 0.008) established | 0.008 (0.008, 0.008) established |
| text | deserialize | 1234 / 4104 | 1238 / 4104 | 1.013 (0.996, 1.030) | 0.809 (0.799, 0.819) established | 0.819 (0.807, 0.832) established |
| text | deserializeReuse | 1241 / 4104 | 1242 / 4104 | 1.001 (0.987, 1.015) | 0.817 (0.807, 0.827) established | 0.818 (0.809, 0.828) established |
| text | streamCopy | 50 / 0 | 51 / 0 | 1.010 (1.000, 1.020) | 0.016 (0.016, 0.017) established | 0.017 (0.016, 0.017) established |
| text | objectCopy | 1 / 0 | 1 / 0 | 1.005 (0.991, 1.020) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| text | objectCopyReuse | 2 / 0 | 2 / 0 | 1.005 (0.995, 1.014) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| nested | deserialize | 167 / 512 | 168 / 512 | 1.009 (0.974, 1.045) | 0.701 (0.688, 0.714) established | 0.707 (0.680, 0.735) established |
| nested | deserializeReuse | 175 / 512 | 175 / 512 | 0.987 (0.935, 1.041) | 0.712 (0.678, 0.748) established | 0.702 (0.673, 0.733) established |
| nested | streamCopy | 15 / 0 | 16 / 0 | 1.022 (0.993, 1.051) | 0.030 (0.029, 0.030) established | 0.030 (0.030, 0.031) established |
| nested | objectCopy | 1 / 0 | 1 / 0 | 1.070 (0.975, 1.174) | 0.003 (0.003, 0.004) established | 0.004 (0.003, 0.004) established |
| nested | objectCopyReuse | 2 / 0 | 2 / 0 | 0.989 (0.945, 1.036) | 0.005 (0.005, 0.006) established | 0.005 (0.005, 0.006) established |
| collections | deserialize | 1435 / 3792 | 1420 / 3792 | 0.986 (0.940, 1.034) | 0.884 (0.869, 0.901) established | 0.872 (0.825, 0.920) established |
| collections | deserializeReuse | 1434 / 3792 | 1439 / 3792 | 1.009 (0.965, 1.056) | 0.883 (0.838, 0.931) established | 0.892 (0.879, 0.904) established |
| collections | streamCopy | 26 / 0 | 25 / 0 | 0.976 (0.945, 1.007) | 0.008 (0.008, 0.008) established | 0.008 (0.008, 0.008) established |
| collections | objectCopy | 1 / 0 | 1 / 0 | 1.023 (0.973, 1.077) | 0.001 (0.000, 0.001) established | 0.001 (0.001, 0.001) established |
| collections | objectCopyReuse | 2 / 0 | 2 / 0 | 1.014 (0.971, 1.058) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| large | deserialize | 8809 / 65964 | 8865 / 65964 | 1.003 (0.991, 1.015) | 0.547 (0.540, 0.554) established | 0.549 (0.545, 0.552) established |
| large | deserializeReuse | 8820 / 65964 | 8877 / 65964 | 1.005 (0.993, 1.016) | 0.547 (0.542, 0.551) established | 0.549 (0.543, 0.556) established |
| large | streamCopy | 3028 / 0 | 2997 / 0 | 0.982 (0.944, 1.021) | 0.115 (0.112, 0.119) established | 0.113 (0.109, 0.118) established |
| large | objectCopy | 1 / 0 | 1 / 0 | 0.999 (0.914, 1.091) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |
| large | objectCopyReuse | 2 / 0 | 2 / 0 | 0.977 (0.943, 1.012) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |

Protobuf 3:

| Workload | Operation | Baseline | Candidate | Candidate/baseline | Baseline native/Chill | Candidate native/Chill |
|---|---|---:|---:|---|---|---|
| scalar | deserialize | 103 / 376 | 103 / 376 | 1.011 (0.963, 1.060) | 0.616 (0.591, 0.642) established | 0.622 (0.598, 0.648) established |
| scalar | deserializeReuse | 110 / 376 | 109 / 376 | 1.012 (0.971, 1.055) | 0.631 (0.611, 0.651) established | 0.638 (0.614, 0.663) established |
| scalar | streamCopy | 15 / 0 | 14 / 0 | 0.978 (0.937, 1.021) | 0.040 (0.039, 0.042) established | 0.040 (0.039, 0.040) established |
| scalar | objectCopy | 1 / 0 | 1 / 0 | 0.985 (0.902, 1.077) | 0.006 (0.005, 0.006) established | 0.006 (0.005, 0.006) established |
| scalar | objectCopyReuse | 2 / 0 | 2 / 0 | 1.032 (0.999, 1.067) | 0.008 (0.008, 0.008) established | 0.008 (0.008, 0.009) established |
| text | deserialize | 1257 / 4128 | 1255 / 4128 | 0.998 (0.991, 1.006) | 0.811 (0.806, 0.816) established | 0.810 (0.804, 0.816) established |
| text | deserializeReuse | 1265 / 4128 | 1257 / 4128 | 0.993 (0.981, 1.006) | 0.813 (0.804, 0.823) established | 0.808 (0.800, 0.816) established |
| text | streamCopy | 50 / 0 | 50 / 0 | 1.095 (0.762, 1.575) | 0.012 (0.009, 0.016) established | 0.013 (0.009, 0.018) established |
| text | objectCopy | 1 / 0 | 1 / 0 | 1.026 (0.803, 1.310) | 0.000 (0.000, 0.001) established | 0.000 (0.000, 0.001) established |
| text | objectCopyReuse | 2 / 0 | 2 / 0 | 0.930 (0.790, 1.095) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| nested | deserialize | 172 / 536 | 171 / 536 | 0.980 (0.939, 1.022) | 0.717 (0.703, 0.732) established | 0.703 (0.671, 0.736) established |
| nested | deserializeReuse | 178 / 536 | 176 / 536 | 0.995 (0.964, 1.026) | 0.718 (0.700, 0.737) established | 0.715 (0.696, 0.734) established |
| nested | streamCopy | 16 / 0 | 16 / 0 | 1.011 (0.973, 1.052) | 0.030 (0.029, 0.032) established | 0.031 (0.030, 0.031) established |
| nested | objectCopy | 1 / 0 | 1 / 0 | 1.043 (0.998, 1.089) | 0.004 (0.003, 0.004) established | 0.004 (0.004, 0.004) established |
| nested | objectCopyReuse | 2 / 0 | 2 / 0 | 1.010 (0.977, 1.045) | 0.005 (0.005, 0.006) established | 0.005 (0.005, 0.006) established |
| collections | deserialize | 1368 / 3457 | 1453 / 3816 | 1.071 (0.994, 1.155) | 0.899 (0.847, 0.955) established | 0.963 (0.901, 1.030) established |
| collections | deserializeReuse | 1464 / 3816 | 1437 / 3739 | 1.033 (0.957, 1.115) | 0.947 (0.931, 0.964) established | 0.978 (0.894, 1.071) established |
| collections | streamCopy | 26 / 0 | 25 / 0 | 0.941 (0.888, 0.997) | 0.009 (0.008, 0.009) established | 0.008 (0.008, 0.008) established |
| collections | objectCopy | 1 / 0 | 1 / 0 | 0.962 (0.863, 1.073) | 0.001 (0.000, 0.001) established | 0.000 (0.000, 0.001) established |
| collections | objectCopyReuse | 2 / 0 | 2 / 0 | 1.018 (0.972, 1.066) | 0.001 (0.001, 0.001) established | 0.001 (0.001, 0.001) established |
| large | deserialize | 8757 / 65988 | 8794 / 65988 | 1.002 (0.992, 1.011) | 0.543 (0.538, 0.548) established | 0.544 (0.541, 0.548) established |
| large | deserializeReuse | 8751 / 65988 | 8798 / 65988 | 1.005 (0.995, 1.015) | 0.541 (0.538, 0.545) established | 0.544 (0.539, 0.550) established |
| large | streamCopy | 2979 / 0 | 3009 / 0 | 1.008 (0.979, 1.038) | 0.114 (0.110, 0.117) established | 0.115 (0.113, 0.117) established |
| large | objectCopy | 1 / 0 | 1 / 0 | 0.960 (0.881, 1.046) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |
| large | objectCopyReuse | 2 / 0 | 2 / 0 | 1.001 (0.991, 1.011) | 0.000 (0.000, 0.000) established | 0.000 (0.000, 0.000) established |

### Verdicts

| Profile | Variant | Established | Inconclusive | Missed |
|---|---|---:|---:|---:|
| Protobuf 4, 40 cells | baseline | 40 | 0 | 0 |
| Protobuf 4, 40 cells | candidate | 39 | 1 | 0 |
| Protobuf 3, 40 cells | baseline | 40 | 0 | 0 |
| Protobuf 3, 40 cells | candidate | 39 | 1 | 0 |

Both inconclusive cells write payloads below 4 KiB through code that is identical in both builds.
Protobuf 4 nested `buildAndSerialize` is at 0.948 (0.781, 1.151): the candidate's five blocks ran at 0.77 to 1.20 times Chill's time, where the baseline's ran at 0.75 to 0.90.
Protobuf 3 text `buildAndSerialize` is at 0.608 (0.241, 1.536): one candidate block ran at 1.96 times Chill's time and the others at 0.36 to 0.84.
The standard deviation of the five log native/Chill ratios is 0.38 to 0.75 for the three Protobuf 3 text write cells in both variants except candidate `serializeSameInstance`, at 0.02; every other cell is at most 0.28, the largest being Protobuf 3 text `streamCopy` at 0.22 and 0.28; the earlier reports recorded the same spread for these cells, and [#178](https://github.com/flink-gcp/flink-datastream-protobuf/issues/178) investigates it.

### Possible regressions

Two candidate-versus-baseline intervals outside the large writes lie entirely above 1, both on code that runs identically in both builds.
Protobuf 4 nested `buildAndSerialize`, the inconclusive cell above, is at 1.203 (1.005, 1.440).
Protobuf 4 text `streamCopy`, which never encodes a payload, is at 1.010 (1.000, 1.020), with an unrounded lower bound of 1.0004.
As in the earlier reports, such intervals on unchanged paths are recorded as run-to-run drift between sequential runs and are not attributed to the change.

## Limitations

The two variants of each profile ran sequentially on one VM, over about two and a half hours, so host drift can affect native times; the Chill lane controls part of it within each block.
Five blocks per run give limited information about the distribution of ratios.
`just benchmark-pair` measures only the native and Chill Protobuf lanes, so this plan does not compare the change with the Kryo lane that the 1.0.0 report cited, and the plan is not compared with earlier reports, which used other VMs, images, JDK builds, or Protobuf 4 releases.
The corpus has one large shape, a single 64 KiB `bytes` field; large frames dominated by strings, many small fields, or unknown fields use the changed path but are not timed here, and the unit tests check only their correctness.
The corpus and warm setup do not cover first-use costs, other Flink or JDK versions, or record shapes outside the five workloads.

## Retained evidence

The [data inventory](data/serializer-large-write-v1.1.0/README.md) lists the four result archives, their checksums, and how to reconstruct each measured tree.
