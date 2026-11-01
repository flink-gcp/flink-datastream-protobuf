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

# Serializer benchmarks for the 1.0.0 release candidate

This report is the release-candidate performance evidence for 1.0.0 that [#45](https://github.com/flink-gcp/flink-datastream-protobuf/issues/45) requires.
It repeats the long serializer benchmarks of the [0.1.0 report](serializer-benchmarks-v0.1.0.md) on temporary Google Compute Engine VMs, with the same corpus, lanes, operations, fork and iteration settings, paired blocks, and practical tolerance, all defined in [ADR-0004](../adr/0004-serializer-benchmarks.md).
It adds the two DynamicMessage lanes that ADR-0004 describes.
The results describe the recorded VMs and runtimes; they do not rank serializers universally and do not establish whole-job throughput or saved-state compatibility.

## Reproduce

The plan measured commit `9e7d175341fc0e418a97f199168d63c1a0f8cb33` with this specification:

```json
{"runId": "issue45-rc-1", "mode": "run", "source": "9e7d175341fc0e418a97f199168d63c1a0f8cb33",
 "profiles": [{"protobuf": 4}, {"protobuf": 3}]}
```

```sh
mise x -- just gce-benchmark plan rc.json /tmp/issue45-gce/plan
mise x -- just gce-benchmark execute /tmp/issue45-gce/plan --approve
mise x -- just gce-benchmark collect /tmp/issue45-gce/plan /tmp/issue45-gce/results
```

[Temporary GCE execution](../development.md#temporary-gce-execution) describes the commands, the load gate, maintenance handling, and cleanup.
Each VM ran `just benchmark-build 2.3.0 <major>` and `just benchmark-measure run 2.3.0 <major>` on a checkout of the source commit, as each archive's `invocation.txt` records.
A rerun needs a new run ID and new output directories, and its VMs are not comparable causally with these.

## Candidate and hosts

The source commit is the merge of [#173](https://github.com/flink-gcp/flink-datastream-protobuf/pull/173), which set the version to `1.0.0-SNAPSHOT`.
The commits that follow it on `main` up to `ed4db42` change no production source, POM, or dependency, so they do not change the measured library.
A later change to production code or a dependency before the `v1.0.0` tag requires a rerun or an explicit invalidation of this evidence.

`just benchmark-build` packages the library for each Protobuf profile, with `-Pprotobuf<major>` at Flink 2.3.0.
The measured jars have SHA-256 `cdc10744cc934f9d8c427aa09de066fce26ec6dcbbd8b2eca4bb732a925ff80e` (Protobuf 4) and `0b47620f7758467cee99e9662d92d487ff960e567b9ce184fb405bb2a6c5fc5a` (Protobuf 3).
The release builds each artifact line once with Protobuf 4, at Flink 2.2.1 for the 2.x line, and ships that jar for both runtimes.
A local rebuild of the source commit with Temurin 17.0.20 in the release configuration and in both benchmark configurations produced byte-identical class files and resources, which differed only in zip entry timestamps.
The measured classes are therefore those of the release configuration, as Temurin 17 compiles them; the archives do not retain the VM jars, so this comparison covers the local rebuild only.
[#177](https://github.com/flink-gcp/flink-datastream-protobuf/issues/177) tracks measuring the release-configuration jar directly.

| Item | Value |
|---|---|
| Plan, zone | `issue45-rc-1`, `asia-northeast1-b` |
| Machine | `c3-standard-8`, Intel Sapphire Rapids (Xeon Platinum 8481C), 8 vCPUs, 32 GB |
| Image and kernel | `ubuntu-2404-noble-amd64-v20261024` (Ubuntu 24.04.5 LTS), Linux 7.0.0-1014-gcp |
| JDK | Temurin 17.0.21+9, `-Xms1g -Xmx1g -XX:+UseG1GC` |
| Flink | 2.3.0 |
| Protobuf runtime and protoc | 4.36.2 and 3.25.9, one per VM |
| Comparators | Avro 1.11.4, Thrift runtime and compiler 0.25.0, Chill 0.10.0, Kryo 5.6.2 |
| Harness | JMH 1.37, corpus seed `30012026`, 64 records per workload |

| Profile | VM | Measurement start (UTC) | Measurement end (UTC) | Accepted JMH results |
|---|---|---|---|---:|
| Protobuf 4.36.2 | `issue45-rc-1-pb4` | 2026-10-31 17:25:37 | 2026-10-31 23:14:10 | 2,005 |
| Protobuf 3.25.9 | `issue45-rc-1-pb3` | 2026-10-31 17:26:04 | 2026-10-31 23:14:21 | 2,005 |

Each profile holds 2,000 primary forks, ten lanes by five workloads by eight operations by five blocks, and the five deterministic supplements.
Both VMs ran concurrently, one profile each, from the same image, machine type, and CPU platform.
Their maintenance-event logs record only `NONE`, and both load gates passed before and after the build with a mean CPU idle of at least 99.95% and a minimum of at least 99.88%.
Recomputing every native/Chill interval from `measurements.csv` reproduces `parity.csv` within floating-point rounding.
The collection deleted both VMs and their disks, and a later cleanup check found no instance or disk labelled with the run.

## Native and Chill time tolerance

| Profile | Established | Inconclusive | Missed |
|---|---:|---:|---:|
| Protobuf 3, all 40 cells | 38 | 2 | 0 |
| Protobuf 4, all 40 cells | 40 | 0 | 0 |
| Protobuf 3, 25 encoding, decoding, and construction cells | 23 | 2 | 0 |
| Protobuf 4, 25 encoding, decoding, and construction cells | 25 | 0 | 0 |

No cell misses the 10% tolerance in either profile.
All 15 copy cells per profile establish it, with upper limits of at most 0.114; their copy semantics differ from Chill's, as the 0.1.0 report explains.
The table shows the paired geometric native/Chill time ratio with its 95% interval for the encoding cells, next to the 0.1.0 point estimate; lower ratios favor native Protobuf.
The archives retain every cell.

| Profile | Workload | Operation | 0.1.0 native/Chill | 1.0.0 native/Chill (95% interval) | 1.0.0 verdict |
|---|---|---|---:|---:|---|
| 3 | scalar | serialize | 9.332 | 0.643 (0.623, 0.664) | established |
| 3 | scalar | deserialize | 2.883 | 0.603 (0.576, 0.630) | established |
| 3 | scalar | buildAndSerialize | 4.671 | 0.761 (0.753, 0.770) | established |
| 3 | text | serialize | 1.107 | 0.519 (0.294, 0.915) | established |
| 3 | text | deserialize | 1.043 | 0.812 (0.807, 0.816) | established |
| 3 | text | buildAndSerialize | 1.012 | 0.627 (0.245, 1.604) | inconclusive |
| 3 | nested | serialize | 8.606 | 0.736 (0.619, 0.876) | established |
| 3 | nested | deserialize | 2.242 | 0.705 (0.673, 0.739) | established |
| 3 | nested | buildAndSerialize | 4.554 | 0.868 (0.713, 1.057) | established |
| 3 | collections | serialize | 5.413 | 0.945 (0.931, 0.960) | established |
| 3 | collections | deserialize | 1.267 | 0.915 (0.864, 0.969) | established |
| 3 | collections | buildAndSerialize | 4.677 | 0.929 (0.879, 0.983) | established |
| 3 | large | serialize | 0.358 | 0.338 (0.329, 0.347) | established |
| 3 | large | deserialize | 1.091 | 0.543 (0.542, 0.544) | established |
| 3 | large | buildAndSerialize | 0.631 | 0.649 (0.644, 0.654) | established |
| 4 | scalar | serialize | 10.097 | 0.611 (0.588, 0.634) | established |
| 4 | scalar | deserialize | 3.013 | 0.619 (0.602, 0.636) | established |
| 4 | scalar | buildAndSerialize | 4.614 | 0.779 (0.769, 0.790) | established |
| 4 | text | serialize | 2.586 | 0.724 (0.711, 0.737) | established |
| 4 | text | deserialize | 1.071 | 0.812 (0.798, 0.825) | established |
| 4 | text | buildAndSerialize | 2.031 | 0.818 (0.792, 0.845) | established |
| 4 | nested | serialize | 9.258 | 0.714 (0.707, 0.720) | established |
| 4 | nested | deserialize | 2.132 | 0.706 (0.686, 0.727) | established |
| 4 | nested | buildAndSerialize | 4.697 | 0.815 (0.698, 0.953) | established |
| 4 | collections | serialize | 5.574 | 0.880 (0.865, 0.894) | established |
| 4 | collections | deserialize | 1.212 | 0.881 (0.830, 0.936) | established |
| 4 | collections | buildAndSerialize | 4.907 | 0.886 (0.864, 0.908) | established |
| 4 | large | serialize | 0.366 | 0.337 (0.334, 0.339) | established |
| 4 | large | deserialize | 1.099 | 0.539 (0.534, 0.543) | established |
| 4 | large | buildAndSerialize | 0.651 | 0.647 (0.641, 0.653) | established |

The two inconclusive Protobuf 3 cells have point estimates below the tolerance.
Text `buildAndSerialize`, at 0.627 (0.245, 1.604), belongs to the Protobuf 3 text write cells whose JVM forks take one of two times: native text `serialize` forks take 639 to 642 or 1,467 to 1,469 ns/op, and Chill `buildAndSerialize` forks 1,496 or 3,193 to 3,359 ns/op.
The standard deviation of the five log ratios is 0.39 to 0.76 for text `serialize`, `serializeSameInstance`, and `buildAndSerialize`, against at most 0.16 for every other cell; the [sizing-allocation](serializer-sizing-allocation.md), [generated-size](serializer-generated-size.md), and [large-read](serializer-large-read.md) reports recorded the same spread, and [#178](https://github.com/flink-gcp/flink-datastream-protobuf/issues/178) investigates it.
Collections `serializeSameInstance`, at 0.945 (0.794, 1.125), crosses the tolerance because one native fork and one Chill fork each took about 20% longer than the other four of its lane.

## Changes since the 0.1.0 baseline

ADR-0004 does not compare a VM result causally with the Mac baseline, so this section describes differences without attributing them.
Between the two runs the following changed:

- Host: an Apple M1 Pro Mac with macOS 26.6.2 became a `c3-standard-8` VM with Ubuntu 24.04.5.
- JDK: Temurin 17.0.20+8 became 17.0.21+9.
- Protobuf 4 profile: protobuf-java and protoc 4.33.6 became 4.36.2; the Protobuf 3 profile kept 3.25.9.
- Thrift: runtime and compiler 0.23.0 became 0.25.0. Flink 2.3.0, Avro, Chill, Kryo, JMH, and the corpus are unchanged.
- Library: the per-record buffer change of [#42](serializer-buffer-allocation.md), the sizing changes of [#43](serializer-sizing-allocation.md) and [#112](serializer-generated-size.md), the large-frame read of [#113](serializer-large-read.md), DynamicMessage support, and the 1.0.0 persisted-format restart.

The unchanged Chill comparator took 1.06 to 2.66 times as long on the VM as on the Mac for `serialize` and `deserialize`, so absolute times do not compare across the two runs.
The native/Chill ratio divides out what the host change does equally to both lanes, but on Protobuf 4 it also contains the comparator's runtime change.
Of the 33 encoding cells that missed the tolerance in 0.1.0, 32 establish it now, and Protobuf 3 collections `serializeSameInstance` is inconclusive.
The large-payload `serialize` and `buildAndSerialize` ratios stayed within 0.03 of their 0.1.0 values, while `deserialize` and `deserializeReuse` fell from 1.09–1.10 to 0.54–0.55; the [large-read report](serializer-large-read.md) measured that change of #113 against a baseline on the same VM.

Encoded sizes are unchanged for both Protobuf lanes in both profiles, for example 23.9 bytes for scalar, 498.1 for collections, and 65,563.9 for large native records.
Native allocation, the mean of five forks, fell in every cell of this table:

| Workload | Operation | Protobuf 3, 0.1.0 → 1.0.0 (B/op) | Protobuf 4, 0.1.0 → 1.0.0 (B/op) |
|---|---|---:|---:|
| scalar | serialize | 6,152.8 → 136.0 | 6,088.8 → 149.1 |
| scalar | deserialize | 4,519.8 → 375.8 | 4,495.8 → 351.8 |
| scalar | buildAndSerialize | 6,861.6 → 744.0 | 6,752.8 → 661.1 |
| text | serialize | 6,296.8 → 136.0 | 6,210.4 → 1,957.1 |
| text | deserialize | 8,271.8 → 4,127.8 | 8,247.8 → 4,103.8 |
| text | buildAndSerialize | 7,994.4 → 1,768.0 | 7,901.6 → 3,577.9 |
| nested | serialize | 6,305.6 → 136.0 | 6,222.4 → 245.1 |
| nested | deserialize | 4,679.8 → 535.8 | 4,655.8 → 511.8 |
| nested | buildAndSerialize | 6,987.2 → 744.0 | 6,947.2 → 757.1 |
| collections | serialize | 16,666.4 → 148.8 | 17,050.4 → 533.1 |
| collections | deserialize | 7,883.0 → 3,739.0 | 7,859.0 → 3,791.8 |
| collections | buildAndSerialize | 19,488.8 → 1,992.0 | 19,413.6 → 2,029.1 |
| large | serialize | 6,208.8 → 4,506.1 | 6,144.8 → 4,519.3 |
| large | deserialize | 132,080.0 → 65,988.1 | 132,056.0 → 65,964.1 |
| large | buildAndSerialize | 72,461.6 → 70,695.0 | 72,410.4 → 70,708.1 |

The [buffer-allocation](serializer-buffer-allocation.md) and [sizing-allocation](serializer-sizing-allocation.md) reports measured the main reductions against baselines on the same VM.
The remaining Protobuf 4 write allocation grows with the number of string fields: text `serialize` allocates 1,957.1 B/op on Protobuf 4 against 136.0 on Protobuf 3.
Chill and DynamicMessage carry the same increase on Protobuf 4, and Chill's own text `serialize` allocation rose from 1,814.5 B/op with 4.33.6 in 0.1.0 to 3,642.0 B/op with 4.36.2, so it comes from the protobuf-java 4.36.2 runtime rather than from this library.

## Cross-format encoding costs

Each entry is the arithmetic mean of five fork scores for ordinary corpus-cycling `serialize`; time is ns/op, allocation is B/op from the GC profiler, and encoded bytes are the corpus mean including serializer framing.
The two values in each cell are profile 3 / profile 4, measured on different VMs of one plan.
These are descriptive means; the paired intervals above and the per-fork data carry the uncertainty.

| Workload | Lane | Time (ns/op), 3 / 4 | Allocation (B/op), 3 / 4 | Encoded bytes, 3 / 4 |
|---|---|---:|---:|---:|
| scalar | POJO | 111.5 / 111.4 | 23.6 / 23.6 | 46.9 / 46.9 |
| scalar | TUPLE | 71.8 / 71.9 | 0.0 / 0.0 | 35.2 / 35.2 |
| scalar | AVRO_SPECIFIC | 120.3 / 120.9 | 132.8 / 132.8 | 39.9 / 39.9 |
| scalar | AVRO_GENERIC | 139.5 / 137.6 | 109.1 / 109.1 | 39.9 / 39.9 |
| scalar | PROTOBUF_NATIVE | 63.8 / 54.5 | 136.0 / 149.1 | 23.9 / 23.9 |
| scalar | PROTOBUF_DYNAMIC | 94.6 / 90.3 | 40.0 / 53.1 | 23.9 / 23.9 |
| scalar | PROTOBUF_DYNAMIC_FOREIGN | 93.9 / 89.3 | 40.0 / 53.1 | 23.9 / 23.9 |
| scalar | PROTOBUF_CHILL | 99.2 / 89.3 | 174.5 / 187.6 | 22.9 / 22.9 |
| scalar | THRIFT_CHILL | 1,040.5 / 1,039.7 | 157.0 / 157.0 | 81.8 / 81.8 |
| scalar | KRYO | 256.8 / 255.0 | 0.0 / 0.0 | 27.2 / 27.2 |
| text | POJO | 1,398.3 / 1,383.0 | 23.6 / 23.6 | 1681.8 / 1681.8 |
| text | TUPLE | 1,356.1 / 1,271.3 | 0.0 / 0.0 | 1670.1 / 1670.1 |
| text | AVRO_SPECIFIC | 834.8 / 833.0 | 1,924.8 / 1,924.8 | 1673.7 / 1673.7 |
| text | AVRO_GENERIC | 853.1 / 851.9 | 1,901.1 / 1,901.1 | 1673.7 / 1673.7 |
| text | PROTOBUF_NATIVE | 971.6 / 808.1 | 136.0 / 1,957.1 | 1663.8 / 1663.8 |
| text | PROTOBUF_DYNAMIC | 724.2 / 854.6 | 40.0 / 1,861.1 | 1663.8 / 1663.8 |
| text | PROTOBUF_DYNAMIC_FOREIGN | 728.4 / 854.9 | 40.0 / 1,861.1 | 1663.8 / 1663.8 |
| text | PROTOBUF_CHILL | 1,721.8 / 1,116.3 | 1,814.5 / 3,642.0 | 1663.8 / 1663.8 |
| text | THRIFT_CHILL | 1,865.6 / 1,864.9 | 3,586.0 / 3,586.0 | 1716.7 / 1716.7 |
| text | KRYO | 1,022.0 / 1,020.9 | 0.0 / 0.0 | 1664.0 / 1664.0 |
| nested | POJO | 133.1 / 133.4 | 34.9 / 34.9 | 64.8 / 64.8 |
| nested | TUPLE | 95.9 / 98.3 | 0.0 / 0.0 | 53.1 / 53.1 |
| nested | AVRO_SPECIFIC | 146.5 / 152.3 | 224.0 / 224.0 | 57.7 / 57.7 |
| nested | AVRO_GENERIC | 163.1 / 165.9 | 189.1 / 189.1 | 57.7 / 57.7 |
| nested | PROTOBUF_NATIVE | 89.9 / 88.2 | 136.0 / 245.1 | 46.4 / 46.4 |
| nested | PROTOBUF_DYNAMIC | 154.7 / 141.0 | 40.0 / 149.1 | 46.4 / 46.4 |
| nested | PROTOBUF_DYNAMIC_FOREIGN | 144.8 / 140.7 | 40.0 / 149.1 | 46.4 / 46.4 |
| nested | PROTOBUF_CHILL | 121.1 / 123.7 | 198.5 / 307.6 | 45.4 / 45.4 |
| nested | THRIFT_CHILL | 1,050.7 / 1,053.0 | 258.0 / 258.0 | 99.7 / 99.7 |
| nested | KRYO | 298.7 / 303.7 | 0.0 / 0.0 | 46.9 / 46.9 |
| collections | POJO | 384.8 / 388.6 | 23.6 / 23.6 | 420.9 / 420.9 |
| collections | TUPLE | 309.2 / 309.9 | 0.0 / 0.0 | 409.2 / 409.2 |
| collections | AVRO_SPECIFIC | 544.1 / 548.6 | 516.8 / 516.8 | 447.9 / 447.9 |
| collections | AVRO_GENERIC | 561.7 / 587.6 | 493.1 / 518.7 | 447.9 / 447.9 |
| collections | PROTOBUF_NATIVE | 861.4 / 777.3 | 148.8 / 533.1 | 498.1 / 498.1 |
| collections | PROTOBUF_DYNAMIC | 1,541.2 / 1,350.8 | 136.0 / 437.1 | 498.1 / 498.1 |
| collections | PROTOBUF_DYNAMIC_FOREIGN | 1,544.3 / 1,351.8 | 136.0 / 437.1 | 498.1 / 498.1 |
| collections | PROTOBUF_CHILL | 911.5 / 883.5 | 675.7 / 1,047.3 | 498.1 / 498.1 |
| collections | THRIFT_CHILL | 2,676.4 / 2,678.0 | 1,045.0 / 1,061.0 | 488.8 / 488.8 |
| collections | KRYO | 1,198.6 / 1,206.3 | 0.0 / 0.0 | 433.0 / 433.0 |
| large | POJO | 2,763.5 / 2,774.5 | 23.6 / 23.6 | 65582.9 / 65582.9 |
| large | TUPLE | 2,673.1 / 2,696.8 | 0.0 / 0.0 | 65571.2 / 65571.2 |
| large | AVRO_SPECIFIC | 2,884.8 / 2,847.6 | 132.8 / 132.8 | 65575.9 / 65575.9 |
| large | AVRO_GENERIC | 2,908.3 / 2,892.2 | 109.1 / 109.1 | 65575.9 / 65575.9 |
| large | PROTOBUF_NATIVE | 3,583.1 / 3,632.8 | 4,506.1 / 4,519.3 | 65563.9 / 65563.9 |
| large | PROTOBUF_DYNAMIC | 3,690.3 / 3,644.8 | 4,184.0 / 4,197.1 | 65563.9 / 65563.9 |
| large | PROTOBUF_DYNAMIC_FOREIGN | 3,663.1 / 3,686.5 | 4,184.0 / 4,197.1 | 65563.9 / 65563.9 |
| large | PROTOBUF_CHILL | 10,602.4 / 10,792.0 | 65,716.3 / 65,729.4 | 65564.9 / 65564.9 |
| large | THRIFT_CHILL | 13,600.7 / 13,778.6 | 65,693.1 / 65,693.1 | 65619.8 / 65619.8 |
| large | KRYO | 3,302.6 / 3,314.2 | 0.0 / 0.0 | 65565.2 / 65565.2 |

Native scalar serialization is the fastest of all lanes, at 54.5 to 63.8 ns/op.
Collections have the largest native cost relative to the fixed-layout lanes: native `serialize` takes 777 to 861 ns/op, 0.88 to 0.95 times Chill's time but 2.5 to 2.8 times Tuple's and 2.0 to 2.2 times POJO's.
Protobuf writes each of the 16 map entries as a length-delimited message and each long as a varint, while Flink's POJO and Tuple serializers write longs as fixed eight-byte values and no per-entry length; this report does not measure how much each difference contributes.
Native large serialization takes 0.34 times Chill's time, but 1.08 to 1.10 times Kryo's, and allocates about 4.5 KB per record for the stream encoder buffer that [#176](https://github.com/flink-gcp/flink-datastream-protobuf/issues/176) addresses.
Generated-message `serialize` allocates 96 B/op more than DynamicMessage `serialize` of the same content in the scalar, text, and nested workloads of both profiles; [#175](https://github.com/flink-gcp/flink-datastream-protobuf/issues/175) investigates that fixed allocation.

## DynamicMessage lanes

`PROTOBUF_DYNAMIC` values carry the serializer's own descriptors, taken from a value it decoded, and `PROTOBUF_DYNAMIC_FOREIGN` values carry an equal-content descriptor from another instance, which the serializer accepts through its cache of accepted descriptor pairs.
Both are measured in the same blocks as the generated and Chill lanes, and ADR-0004 gives their ratios as descriptive results without a tolerance or verdict.
The table shows the encoding operations; `dynamic.csv` in each archive holds all 40 cells.

| Profile | Workload | Operation | Dynamic/generated (95% interval) | Foreign/own (95% interval) |
|---|---|---|---:|---:|
| 3 | scalar | serialize | 1.484 (1.468, 1.500) | 0.992 (0.959, 1.027) |
| 3 | scalar | deserialize | 6.319 (6.124, 6.521) | 1.000 (0.988, 1.011) |
| 3 | scalar | buildAndSerialize | 2.960 (2.932, 2.988) | 1.022 (1.009, 1.035) |
| 3 | text | serialize | 0.811 (0.465, 1.415) | 1.006 (0.983, 1.029) |
| 3 | text | deserialize | 1.568 (1.525, 1.614) | 0.977 (0.950, 1.005) |
| 3 | text | buildAndSerialize | 1.063 (0.593, 1.908) | 1.007 (0.982, 1.033) |
| 3 | nested | serialize | 1.728 (1.336, 2.235) | 0.937 (0.773, 1.136) |
| 3 | nested | deserialize | 5.841 (5.688, 5.998) | 1.000 (0.986, 1.013) |
| 3 | nested | buildAndSerialize | 3.161 (2.576, 3.879) | 0.983 (0.913, 1.059) |
| 3 | collections | serialize | 1.789 (1.754, 1.825) | 1.002 (0.968, 1.037) |
| 3 | collections | deserialize | 5.338 (5.245, 5.433) | 1.009 (0.991, 1.027) |
| 3 | collections | buildAndSerialize | 5.659 (5.163, 6.203) | 1.001 (0.988, 1.014) |
| 3 | large | serialize | 1.030 (0.999, 1.062) | 0.993 (0.963, 1.023) |
| 3 | large | deserialize | 1.111 (1.107, 1.115) | 0.998 (0.991, 1.005) |
| 3 | large | buildAndSerialize | 1.048 (1.045, 1.051) | 1.001 (0.993, 1.009) |
| 4 | scalar | serialize | 1.655 (1.596, 1.716) | 0.989 (0.927, 1.055) |
| 4 | scalar | deserialize | 6.344 (6.276, 6.413) | 0.994 (0.984, 1.005) |
| 4 | scalar | buildAndSerialize | 2.918 (2.887, 2.949) | 1.008 (1.000, 1.017) |
| 4 | text | serialize | 1.058 (1.050, 1.066) | 1.000 (0.988, 1.013) |
| 4 | text | deserialize | 1.622 (1.600, 1.644) | 0.994 (0.972, 1.016) |
| 4 | text | buildAndSerialize | 1.430 (1.401, 1.460) | 1.008 (0.993, 1.024) |
| 4 | nested | serialize | 1.598 (1.576, 1.620) | 0.998 (0.988, 1.007) |
| 4 | nested | deserialize | 6.108 (5.853, 6.375) | 1.002 (0.985, 1.020) |
| 4 | nested | buildAndSerialize | 3.197 (2.751, 3.716) | 1.006 (0.994, 1.018) |
| 4 | collections | serialize | 1.738 (1.705, 1.771) | 1.001 (0.982, 1.020) |
| 4 | collections | deserialize | 5.760 (5.008, 6.626) | 0.999 (0.817, 1.220) |
| 4 | collections | buildAndSerialize | 5.582 (5.423, 5.747) | 1.013 (0.990, 1.036) |
| 4 | large | serialize | 1.003 (0.976, 1.031) | 1.011 (0.992, 1.031) |
| 4 | large | deserialize | 1.114 (1.096, 1.132) | 1.002 (0.987, 1.018) |
| 4 | large | buildAndSerialize | 1.050 (1.037, 1.063) | 1.003 (0.994, 1.011) |

Decoding a DynamicMessage takes 5.3 to 6.3 times as long as decoding the generated message for the scalar, nested, and collections records, 1.6 times for text, and 1.1 times for large.
The serializer decodes both through the parser of the value's type, so the difference is the cost of protobuf-java's DynamicMessage parsing; DynamicMessage collections decoding also allocates 9.5 to 12.6 KB per record against 3.7 to 3.8 KB.
Writing takes 1.5 to 1.8 times as long for the scalar, nested, and collections records and 0.81 to 1.06 times as long for text and large, where the Protobuf 3 text interval, 0.465 to 1.415, is wide.
`buildAndSerialize` also includes building the DynamicMessage value with its builder, which is the benchmark's conversion, not serializer work.

The foreign/own point estimates lie between 0.91 and 1.05 across all 80 cells.
Eight of their intervals exclude 1, by at most 3%, and four of those are copy or stream-copy cells, which run the same code for both lanes; such differences are therefore within the variation between adjacent lanes.
Values with descriptors from another instance thus show no measurable cost from the write-acceptance cache beyond that variation.

## Remaining gaps and release decision

- Collections encoding remains the narrowest native margin over Chill, 0.88 to 0.95, and costs several times the fixed-layout lanes.
- Scalar and nested encoding are now faster than Chill in every operation, against 2.1 to 10.1 times Chill's time in 0.1.0.
- On Protobuf 4.36.2, every Protobuf lane allocates an additional amount per string field when writing.
- Protobuf 3 text write cells stay bimodal across forks, which leaves one of them inconclusive ([#178](https://github.com/flink-gcp/flink-datastream-protobuf/issues/178)).
- DynamicMessage decoding costs 5.3 to 6.3 times generated decoding for small records, which applications choosing DynamicMessage pay to protobuf-java's parser.
- [#175](https://github.com/flink-gcp/flink-datastream-protobuf/issues/175) and [#176](https://github.com/flink-gcp/flink-datastream-protobuf/issues/176) track the two library allocations above, and [#177](https://github.com/flink-gcp/flink-datastream-protobuf/issues/177) the measured jar, all for 1.1.0.

No cell misses the tolerance, and the only encoding ratio above its 0.1.0 estimate, Protobuf 3 large `buildAndSerialize`, rose from 0.631 to 0.649 across the host and runtime changes listed above.
The measurements therefore show no material regression and raise no performance objection to releasing this candidate as 1.0.0.
None of the follow-ups requires an API, state-format, or default change.

## Retained evidence

The [retained evidence](data/serializer-benchmarks-v1.0.0/README.md) holds both complete result directories with their GCE run records, checksums, and inventories.
