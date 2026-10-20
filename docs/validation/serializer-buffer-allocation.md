---
title: "Coded-stream buffer allocation in the native serializer"
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

# Coded-stream buffer allocation in the native serializer

This report measures the per-record coded-stream buffers investigated in [#42](https://github.com/flink-gcp/flink-datastream-protobuf/issues/42).
It separates the output and input buffers with controlled variants of one source tree and compares each with the unchanged serializer on the same VM.
The measurement contract is [ADR-0004](../adr/0004-serializer-benchmarks.md); the comparison does not establish whole-job throughput or state compatibility.

## Change under test

Before the change, every `serialize` call wrapped the target view in `CodedOutputStream.newInstance(OutputStream)`, and every `deserialize` call wrapped a bounded frame stream in `CodedInputStream.newInstance(InputStream)`.
In the pinned Protobuf 3.25.9 and 4.33.6 runtimes, both factories allocate a 4,096-byte buffer for each call.

Each serializer instance now retains one lazily allocated 4 KiB array, which stream copy already used.
A payload of at most 4 KiB is encoded into that array by an array encoder and then written with its length prefix.
On reading, such a payload is read completely into the array and parsed by an array decoder without aliasing; decoded bytes and strings are copies.
Larger payloads keep the previous streaming paths and their per-call buffers.
Framing, deterministic writing, snapshots, settings, and public defaults are unchanged; [ADR-0001](../adr/0001-native-protobuf-type-integration.md) records the amended buffering rule.

The merged serializer differs from the measured tree only after a generated writer disagrees with the calculated size, where it now reports an overrun of a small payload as such rather than as short output, and in Javadoc and comments.
Successful writes and reads perform the same operations as the measured tree.

Small frames gain one behavioral difference: a generated writer whose output disagrees with the calculated size now leaves the target untouched, because the frame is completed in the array first.
Size and depth limits, overflow checks, malformed-input failures, adjacent-frame boundaries, and thread confinement keep their existing tests; `ProtobufTypeSerializerTest` adds boundary sizes around 4 KiB, fresh and repeatedly encoded values, no aliasing after the array is overwritten, concurrent duplicates, and both inconsistent-writer paths.

## Method

The measurement used the [temporary GCE procedure](../development.md#temporary-gce-execution) with plan `issue42-ablation-2` in project `flink-gcp`.
Each Protobuf profile ran on its own `c3-standard-8` VM in `asia-northeast1-b` (Intel Xeon Platinum 8481C, Sapphire Rapids, 8 vCPUs, 32 GiB) from image `ubuntu-2404-noble-amd64-v20260918`, kernel 7.0.0-1011-gcp, with Temurin 17.0.20.1, Flink 2.3.0, and Protobuf/protoc 3.25.9 or 4.33.6.
The plan requested standard provisioning, and every VM manifest records `TERMINATE` host maintenance; every result recorded `NONE` as the only maintenance event and passed the load gate before its build and before measurement.
Both VMs reported the same machine type, CPU platform, image, kernel, and JDK.

`just benchmark-pair` ran the ADR-0004 long-run protocol for the native and Chill Protobuf lanes only: all five workloads and eight operations, five independent blocks with alternating native/Chill order, five one-second warmups and measurements per fork, one thread, `-Xms1g -Xmx1g -XX:+UseG1GC`, and the JMH GC profiler.
Correctness gates for every lane ran before timing.

The source commit was `2b9d952880df15d45ad6e85178f0fc647093924d`, which adds the candidate serializer and the paired mode to the GCE tooling.
Variants are patches to that commit:

| Variant | Serializer under test |
|---|---|
| baseline | The previous serializer and its tests |
| output | Candidate writing; previous streaming read path |
| input | Candidate reading; previous streaming write path |
| candidate | The change described above |

Protobuf 4 ran baseline, candidate, output, and input sequentially on one VM; Protobuf 3 ran baseline and candidate on the other.
Each run took about 70 minutes, between 09:55 and 15:06 UTC on 2026-10-11.
The first attempt, `issue42-ablation-1`, failed to compile its baseline tests and produced no measurements.

For each variant, the native/Chill tolerance verdict follows ADR-0004.
Comparisons with the baseline use the Chill lane as a per-block control: the ratio of a variant's geometric-mean native/Chill time ratio to the baseline's, with a pooled two-sample Student-t interval on the five log ratios of each run (8 degrees of freedom).
Below 1 favors the variant.
Allocation is the five-fork mean of JMH's normalized allocation, in bytes per operation.
These intervals are per cell, without a simultaneous claim across the 40 cells of each profile or the 80 in total.

## Results

### Protobuf 4: separating output and input buffers

Each cell is native time in ns/op and allocation in B/op, as five-fork means.
The last two columns describe the candidate: its native/Chill interval and verdict, and its time relative to the baseline.

| Workload | Operation | Baseline | Output | Input | Candidate | Candidate native/Chill | Candidate/baseline |
|---|---|---:|---:|---:|---:|---|---|
| scalar | serialize | 1345 / 6113 | 685 / 1988 | 1338 / 6142 | 684 / 1988 | 7.023 (6.923, 7.124) missed | 0.509 (0.494, 0.524) |
| text | serialize | 2403 / 6273 | 1793 / 2119 | 2411 / 6273 | 1771 / 2129 | 2.091 (1.858, 2.353) missed | 0.733 (0.638, 0.842) |
| nested | serialize | 1489 / 6251 | 844 / 2107 | 1476 / 6261 | 839 / 2093 | 6.888 (6.741, 7.038) missed | 0.564 (0.552, 0.577) |
| collections | serialize | 5953 / 17050 | 5380 / 13143 | 5912 / 16820 | 5393 / 13150 | 6.005 (5.823, 6.192) missed | 0.903 (0.851, 0.958) |
| large | serialize | 4183 / 6217 | 4172 / 6217 | 4169 / 6198 | 4181 / 6198 | 0.393 (0.389, 0.397) established | 0.995 (0.984, 1.007) |
| scalar | deserialize | 741 / 4496 | 723 / 4496 | 111 / 352 | 109 / 352 | 0.646 (0.627, 0.666) established | 0.147 (0.139, 0.157) |
| text | deserialize | 1844 / 8248 | 1848 / 8248 | 1224 / 4104 | 1221 / 4104 | 0.810 (0.797, 0.822) established | 0.666 (0.655, 0.677) |
| nested | deserialize | 746 / 4656 | 750 / 4656 | 170 / 512 | 168 / 512 | 0.715 (0.697, 0.733) established | 0.227 (0.220, 0.234) |
| collections | deserialize | 2024 / 7782 | 2022 / 7859 | 1477 / 3792 | 1484 / 3792 | 0.903 (0.883, 0.923) established | 0.737 (0.714, 0.761) |
| large | deserialize | 20868 / 132056 | 20769 / 132056 | 20884 / 132056 | 20905 / 132056 | 1.272 (1.267, 1.276) missed | 1.002 (0.997, 1.006) |
| scalar | serializeSameInstance | 1157 / 6056 | 581 / 1864 | 1145 / 6040 | 600 / 1856 | 7.025 (6.407, 7.703) missed | 0.515 (0.474, 0.560) |
| collections | serializeSameInstance | 5573 / 17160 | 4876 / 13022 | 5451 / 16699 | 4936 / 12798 | 6.105 (5.811, 6.415) missed | 0.884 (0.838, 0.932) |
| scalar | buildAndSerialize | 1610 / 6849 | 995 / 2705 | 1619 / 6867 | 1002 / 2713 | 3.263 (3.123, 3.409) missed | 0.626 (0.602, 0.651) |
| collections | buildAndSerialize | 7905 / 19414 | 7269 / 15270 | 7813 / 19414 | 7293 / 15281 | 3.977 (3.848, 4.111) missed | 0.912 (0.881, 0.943) |

The output variant removes 3.9 to 4.2 KB per write, and the input variant 3.9 to 4.2 KB per read, on every workload whose payload fits in 4 KiB; collections cells are at the low end.
Neither variant moves the other direction's allocation by more than 461 B/op.
Their time intervals for the other direction contain 1 in 23 of 25 cells. The exceptions are the input variant's collections `buildAndSerialize`, at 0.971 (0.946, 0.996), and large `serializeSameInstance`, at 1.013 (1.000, 1.025); both measure the unchanged write path.
The candidate's effect on each operation matches the variant that changes that direction.

The write-side reduction is the same in each size-cache state.
`serialize` cycles over previously encoded records, `serializeSameInstance` repeats one encoded record, and `buildAndSerialize` writes a fresh record whose memoized sizes the generated writer computes during that write.
All three lose 3.9 to 4.4 KB/op, so the removed allocation is serializer buffering, not generated size caching.

Payloads above 4 KiB take the unchanged streaming paths. Every candidate and output `large` interval relative to the baseline contains 1 on Protobuf 4; Protobuf 4 input `serializeSameInstance` and Protobuf 3 candidate `deserialize` lie just above 1 and are discussed below.
Large deserialization still allocates about 132 KB/op, about twice its 64 KiB payload, through runtime byte copying that this change does not address.

### Protobuf 3: baseline and candidate

| Workload | Operation | Baseline | Candidate | Candidate native/Chill | Candidate/baseline |
|---|---|---:|---:|---|---|
| scalar | serialize | 1325 / 6196 | 665 / 2071 | 6.861 (6.562, 7.172) missed | 0.506 (0.486, 0.526) |
| text | serialize | 2417 / 6337 | 1798 / 2193 | 1.051 (1.016, 1.087) established | 0.652 (0.477, 0.891) |
| nested | serialize | 1476 / 6339 | 806 / 2186 | 6.653 (6.474, 6.838) missed | 0.547 (0.533, 0.562) |
| collections | serialize | 6014 / 17140 | 5347 / 12977 | 5.540 (5.001, 6.138) missed | 0.847 (0.777, 0.924) |
| large | serialize | 4148 / 6281 | 4125 / 6281 | 0.386 (0.381, 0.391) established | 0.992 (0.972, 1.013) |
| scalar | deserialize | 735 / 4520 | 111 / 376 | 0.665 (0.642, 0.689) established | 0.152 (0.147, 0.157) |
| text | deserialize | 1890 / 8272 | 1266 / 4128 | 0.808 (0.801, 0.815) established | 0.671 (0.664, 0.678) |
| nested | deserialize | 767 / 4680 | 174 / 536 | 0.741 (0.717, 0.766) established | 0.230 (0.222, 0.239) |
| collections | deserialize | 2033 / 7883 | 1439 / 3739 | 0.980 (0.905, 1.060) established | 0.739 (0.690, 0.791) |
| large | deserialize | 20876 / 132080 | 20870 / 132080 | 1.271 (1.268, 1.275) missed | 1.004 (1.000, 1.008) |

### Tolerance verdicts

| Profile | Variant | Established | Inconclusive | Missed |
|---|---|---:|---:|---:|
| Protobuf 4, 40 cells | baseline | 18 | 0 | 22 |
| Protobuf 4, 40 cells | candidate | 26 | 0 | 14 |
| Protobuf 3, 40 cells | baseline | 18 | 1 | 21 |
| Protobuf 3, 40 cells | candidate | 27 | 2 | 11 |

Every verdict change is from missed to established or inconclusive: `deserialize` and `deserializeReuse` for the four small workloads in both profiles, Protobuf 3 text `serialize` (established), and Protobuf 3 text `serializeSameInstance` (inconclusive).
The candidate still misses the tolerance for scalar, nested, and collections writes in both profiles; scalar and nested `serialize` and `serializeSameInstance` remain at 6.2 to 7.0 times Chill's time and allocate 1.9 to 2.2 KB/op.
Protobuf 3 text writes are established for `serialize` and inconclusive for the other two write operations; Protobuf 4 text writes miss.
The [0.1.0 diagnostics](serializer-benchmarks-v0.1.0.md#allocation-and-optimization-candidates) sampled the remaining allocation sites in the safe-sizing traversal, which [#43](https://github.com/flink-gcp/flink-datastream-protobuf/issues/43) owns; this report does not attribute the remaining time to it.
The [sizing-allocation report](serializer-sizing-allocation.md) measures that traversal separately.
Chill's Protobuf 4 text writes are about twice as fast as its Protobuf 3 writes in both runs, so the text verdict differs between profiles without a native difference of that size.

### Possible regressions

Four of the 80 candidate-versus-baseline intervals, 40 per profile, lie entirely above 1:

| Profile | Workload | Operation | Candidate/baseline |
|---|---|---|---|
| 3 | scalar | streamCopy | 1.053 (1.019, 1.088) |
| 3 | scalar | objectCopyReuse | 1.023 (1.003, 1.044) |
| 3 | large | deserialize | 1.004 (1.000, 1.008) |
| 4 | nested | streamCopy | 1.036 (1.021, 1.051) |

Object copy runs identical code in both builds, and a 64 KiB `deserialize` takes the unchanged streaming path, so those two intervals show the run-to-run drift between sequential runs on one VM.
Stream copy changed only in how it obtains the same 4 KiB array; the two stream-copy cells are within about 5% and are not consistent across workloads or profiles.
They are recorded as observed and are not attributed to the change.

## Limitations

The runs were sequential on one VM per profile, so host drift over about five hours on the Protobuf 4 VM and two and a half on the Protobuf 3 VM can affect the native times; the Chill lane controls part of it within each block.
Five blocks per run give limited information about the distribution of ratios.
The VM results are not compared with the [0.1.0 Mac baseline](serializer-benchmarks-v0.1.0.md), which used a different machine.
The corpus and warm setup do not cover first-use costs, other Flink or JDK versions, or record shapes outside the five workloads.

## Retained evidence

The [data inventory](data/serializer-buffer-allocation-v0.2.0/README.md) lists the six result archives, their checksums, and how to reconstruct each measured tree.
