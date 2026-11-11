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

# Serializer benchmarks for the 1.1.0 release candidate

This report is the release-candidate performance evidence for 1.1.0 that [#213](https://github.com/flink-gcp/flink-datastream-protobuf/issues/213) requires.
It repeats the long serializer benchmarks of the [1.0.0 report](serializer-benchmarks-v1.0.0.md) on temporary Google Compute Engine VMs, with the corpus, lanes, operations, fork and iteration settings, paired blocks, and practical tolerance defined in [ADR-0004](../adr/0004-serializer-benchmarks.md).
It is the first long run that measures the release-configuration library jar ([#177](https://github.com/flink-gcp/flink-datastream-protobuf/issues/177)), pins the compilation of the Protobuf 3 text write cells ([#178](https://github.com/flink-gcp/flink-datastream-protobuf/issues/178)), and runs on Temurin 21 ([#214](https://github.com/flink-gcp/flink-datastream-protobuf/issues/214)).
Earlier long runs, those of the 0.1.0 and 1.0.0 reports and the paired comparisons of the other serializer reports, used Temurin 17.
The results describe the recorded VMs and runtimes; they do not rank serializers universally and do not establish whole-job throughput or saved-state compatibility.

## Reproduce

The plan measured commit `85657151ecb69a7a8c44ffae651d8150aef99dcf` with this specification:

```json
{"runId": "issue213-rc-1", "mode": "run", "source": "85657151ecb69a7a8c44ffae651d8150aef99dcf",
 "profiles": [{"protobuf": 4}, {"protobuf": 3}]}
```

```sh
mise x -- just gce-benchmark plan rc.json /tmp/issue213-gce/plan
mise x -- just gce-benchmark execute /tmp/issue213-gce/plan --approve
mise x -- just gce-benchmark collect /tmp/issue213-gce/plan /tmp/issue213-gce/results
```

[Temporary GCE execution](../development.md#temporary-gce-execution) describes the commands, the load gate, maintenance handling, and cleanup.
Each VM ran `just benchmark-build 2.3.0 <major>` and `just benchmark-measure run 2.3.0 <major>` on a checkout of the source commit, as each archive's `invocation.txt` records.
A rerun needs a new run ID and new output directories, and its VMs are not comparable causally with these.

## Candidate and hosts

The source commit is the merge of [#216](https://github.com/flink-gcp/flink-datastream-protobuf/pull/216), which made Temurin 21 the default JDK.
Since the 1.0.0 candidate, the only change to production source is the large-frame write of [#176](serializer-large-write.md); the other changes touch the build, the benchmark harness, documentation, and tooling.
A later change to production code or a dependency before the `v1.1.0` tag requires a rerun or an explicit invalidation of this evidence.

`just benchmark-build` packages the library as the release does, with the default Protobuf 4 profile at the line's Flink floor, and both profiles measured that jar.
Both profiles record SHA-256 `c9d34c4800046faecc7e591fe81d6349a1a0b52d70654a0f23c554f068977ab8` for it, built with Temurin 21.0.13 on each VM.
The `Release` workflow also builds with JDK 21.
Every other jar on the measurement classpath has the hash that the 1.0.0 run recorded.

| Item | Value |
|---|---|
| Plan, zone | `issue213-rc-1`, `asia-northeast1-b` |
| Machine | `c3-standard-8`, Intel Sapphire Rapids (Xeon Platinum 8481C), 8 vCPUs, 32 GB |
| Image and kernel | `ubuntu-2404-noble-amd64-v20261105` (Ubuntu 24.04.5 LTS), Linux 7.0.0-1017-gcp |
| JDK | Temurin 21.0.13+9, `-Xms1g -Xmx1g -XX:+UseG1GC` |
| Flink | 2.3.0 |
| Protobuf runtime and protoc | 4.36.2 and 3.25.9, one per VM |
| Comparators | Avro 1.11.4, Thrift runtime and compiler 0.25.0, Chill 0.10.0, Kryo 5.6.2 |
| Harness | JMH 1.37, corpus seed `30012026`, 64 records per workload |

| Profile | VM | Measurement start (UTC) | Measurement end (UTC) | Accepted JMH results |
|---|---|---|---|---:|
| Protobuf 4.36.2 | `issue213-rc-1-pb4` | 2026-11-10 16:16:31 | 2026-11-10 22:06:20 | 2,005 |
| Protobuf 3.25.9 | `issue213-rc-1-pb3` | 2026-11-10 16:16:44 | 2026-11-10 22:05:48 | 2,005 |

Each profile holds 2,000 primary forks, ten lanes by five workloads by eight operations by five blocks, and the five deterministic supplements.
Both VMs ran concurrently, one profile each, from the same image, machine type, and CPU platform.
Their maintenance-event logs record only `NONE`, and both load gates passed before and after the build with a mean CPU idle of at least 99.94% and a minimum of 99.88%.
Recomputing every native/Chill interval from `measurements.csv` reproduces `parity.csv` within floating-point rounding.
The operator's local plan directory was lost while the VMs were measuring, so collection used a plan regenerated from the same specification, whose source, image, machine shape, and VM names match the VMs' manifests.
The VMs therefore stayed up for about 2.8 hours after their runners finished, which affected cost but not the measurements.
The collection deleted both VMs and their disks and found no instance or disk labelled with the run.

## Native and Chill time tolerance

| Profile | Established | Inconclusive | Missed |
|---|---:|---:|---:|
| Protobuf 3, all 40 cells | 40 | 0 | 0 |
| Protobuf 4, all 40 cells | 40 | 0 | 0 |
| Protobuf 3, 25 encoding, decoding, and construction cells | 25 | 0 | 0 |
| Protobuf 4, 25 encoding, decoding, and construction cells | 25 | 0 | 0 |

Every cell establishes the 10% tolerance in both profiles.
All 15 copy cells per profile establish it with upper limits of at most 0.117; their copy semantics differ from Chill's, as the [0.1.0 report](serializer-benchmarks-v0.1.0.md) explains.
The table shows the paired geometric native/Chill time ratio with its 95% interval for the encoding cells, next to the 1.0.0 point estimate; lower ratios favor native Protobuf.
The archives retain every cell.

| Profile | Workload | Operation | 1.0.0 native/Chill | 1.1.0 native/Chill (95% interval) | 1.1.0 verdict |
|---|---|---|---:|---:|---|
| 3 | scalar | serialize | 0.643 | 0.611 (0.595, 0.629) | established |
| 3 | scalar | deserialize | 0.603 | 0.680 (0.653, 0.708) | established |
| 3 | scalar | buildAndSerialize | 0.761 | 0.708 (0.699, 0.718) | established |
| 3 | text | serialize | 0.519 | 0.745 (0.693, 0.802) | established |
| 3 | text | deserialize | 0.812 | 0.811 (0.801, 0.821) | established |
| 3 | text | buildAndSerialize | 0.627 | 0.773 (0.750, 0.796) | established |
| 3 | nested | serialize | 0.736 | 0.774 (0.767, 0.781) | established |
| 3 | nested | deserialize | 0.705 | 0.749 (0.722, 0.777) | established |
| 3 | nested | buildAndSerialize | 0.868 | 0.787 (0.745, 0.832) | established |
| 3 | collections | serialize | 0.945 | 0.960 (0.946, 0.975) | established |
| 3 | collections | deserialize | 0.915 | 0.907 (0.794, 1.036) | established |
| 3 | collections | buildAndSerialize | 0.929 | 0.900 (0.863, 0.938) | established |
| 3 | large | serialize | 0.338 | 0.306 (0.297, 0.315) | established |
| 3 | large | deserialize | 0.543 | 0.542 (0.537, 0.548) | established |
| 3 | large | buildAndSerialize | 0.649 | 0.585 (0.581, 0.589) | established |
| 4 | scalar | serialize | 0.611 | 0.616 (0.602, 0.630) | established |
| 4 | scalar | deserialize | 0.619 | 0.631 (0.618, 0.645) | established |
| 4 | scalar | buildAndSerialize | 0.779 | 0.732 (0.704, 0.760) | established |
| 4 | text | serialize | 0.724 | 0.712 (0.708, 0.717) | established |
| 4 | text | deserialize | 0.812 | 0.821 (0.805, 0.838) | established |
| 4 | text | buildAndSerialize | 0.818 | 0.729 (0.531, 1.002) | established |
| 4 | nested | serialize | 0.714 | 0.717 (0.707, 0.727) | established |
| 4 | nested | deserialize | 0.706 | 0.714 (0.699, 0.729) | established |
| 4 | nested | buildAndSerialize | 0.815 | 0.839 (0.710, 0.991) | established |
| 4 | collections | serialize | 0.880 | 0.874 (0.813, 0.940) | established |
| 4 | collections | deserialize | 0.881 | 0.904 (0.857, 0.953) | established |
| 4 | collections | buildAndSerialize | 0.886 | 0.854 (0.830, 0.878) | established |
| 4 | large | serialize | 0.337 | 0.305 (0.298, 0.312) | established |
| 4 | large | deserialize | 0.539 | 0.544 (0.539, 0.548) | established |
| 4 | large | buildAndSerialize | 0.647 | 0.586 (0.577, 0.595) | established |

The widest interval, Protobuf 4 text `buildAndSerialize` at 0.729 (0.531, 1.002), comes from one Chill fork that took 2,919 ns/op while the other four took 1,652 to 1,686; the five log ratios of that cell have a standard deviation of 0.256, against at most 0.134 for every other Protobuf 4 cell.

## Protobuf 3 text write cells

Under the [ADR-0004](../adr/0004-serializer-benchmarks.md) rule, the 60 forks of the four Protobuf lanes in the Protobuf 3 text `serialize`, `serializeSameInstance`, and `buildAndSerialize` cells each recorded exactly the four pinned `-XX:CompileCommand=inline` flags in their JMH `jvmArgs`, and no other fork of either profile recorded a `CompileCommand`.
Every fork ran on JDK 21.0.13.

| Operation | Native/Chill (95% interval) | Verdict | Standard deviation of the log ratios |
|---|---:|---|---:|
| `serialize` | 0.745 (0.693, 0.802) | established | 0.059 |
| `serializeSameInstance` | 0.727 (0.718, 0.737) | established | 0.010 |
| `buildAndSerialize` | 0.773 (0.750, 0.796) | established | 0.024 |

These intervals describe the pinned compilation, which is not the one that every JVM reaches on its own.
Without forced inlining, the [diagnosis](protobuf3-text-write-modes.md) found that the Chill lane reached the slow compilation in 7 of 8 JDK 21 forks of text `serialize`, against none of 16 native forks, and on JDK 17 in 11 of 12 forks of the three cells, against 9 of 48 native `serialize` forks.
ADR-0004 therefore states that the pin lowers Chill times in these cells more than native times; whether a Flink TaskManager reaches the slow compilations as often as a benchmark JVM was not measured.

| Lane | `serialize` (ns/op) | `serializeSameInstance` (ns/op) | `buildAndSerialize` (ns/op) |
|---|---|---|---|
| Native | 642, 642, 717, 721, 721 | 684 to 686 | 1,110 to 1,187 |
| Chill | 918 to 925 | 928 to 951 | 1,474 to 1,540 |
| DynamicMessage | 722, 789 to 819 | 543 to 576, 721 | 1,484 to 1,543 |
| Foreign-descriptor DynamicMessage | 718 to 722, 790, 793 | 562 to 569, 722, 727 | 1,469 to 1,589 |

Times are the five forks of each lane.
No fork took the two to three times longer that the slow compilations of the diagnosis produce.
The five Chill `serializeSameInstance` forks took 928 to 951 ns/op; this run has no JDK 17 forks, so it does not show whether the second mode that this cell kept on JDK 17 under the rule recurs.
Forks of one lane still differ by up to 1.33 times within these cells, most clearly in native `serialize` and in both DynamicMessage lanes; the diagnosis did not examine differences of this size.

The largest standard deviation of the other Protobuf 3 cells is 0.246, for text `streamCopy`, whose Chill forks took 2,626 to 2,734 or 4,113 to 4,278 ns/op; Chill text `objectCopy` and `objectCopyReuse` forks also fall into two groups, 2,056 to 2,095 and 2,832 to 2,840 ns/op.
These copy cells are outside the pinned operations, and their ratios stay below 0.021.

## Changes since the 1.0.0 candidate

The two runs used different plans and VMs, so this section describes differences without attributing them.
Between the two runs the following changed:

- JDK: Temurin 17.0.21+9 became 21.0.13+9.
- Image and kernel: `v20261024` with Linux 7.0.0-1014-gcp became `v20261105` with 7.0.0-1017-gcp; machine type and CPU platform are the same.
- Measured jar: each profile's own build became the release-configuration jar, identical for both profiles.
- Protobuf 3 text write cells: the pinned compilation above.
- Library: the large-frame write of #176.
- Unchanged: Flink, Protobuf, Avro, Thrift, Chill, Kryo, JMH, and the corpus.

The two cells that were inconclusive in 1.0.0, Protobuf 3 text `buildAndSerialize` and collections `serializeSameInstance`, establish the tolerance now; the latter is at 0.957 (0.937, 0.978).
The pinned Protobuf 3 text write ratios rose, `serialize` from 0.519 to 0.745, `serializeSameInstance` from 0.435 to 0.727, and `buildAndSerialize` from 0.627 to 0.773, with both their compilation and the JDK changed, and the standard deviations of their log ratios fell from 0.39 to 0.76 to 0.010 to 0.059.
In those cells Chill text `serialize` averaged 922.6 ns/op against 1,721.8 in the 1.0.0 run, and native 688.7 against 971.6.
Across the other 47 non-copy cells, no ratio rose by more than 0.077, the rise of Protobuf 3 scalar `deserialize` from 0.603 to 0.680.
Large `serialize` fell from 0.337 and 0.338 to 0.305 and 0.306, and `buildAndSerialize` from 0.647 and 0.649 to 0.585 and 0.586; the [large-write report](serializer-large-write.md) measured the time and allocation change of #176 against a baseline on the same VM.

Encoded sizes are unchanged for every lane and workload.
Native allocation, the mean of five forks, changed as follows:

| Workload | Operation | Protobuf 3, 1.0.0 → 1.1.0 (B/op) | Protobuf 4, 1.0.0 → 1.1.0 (B/op) |
|---|---|---:|---:|
| scalar | serialize | 136.0 → 40.0 | 149.1 → 53.1 |
| scalar | deserialize | 375.8 → 375.8 | 351.8 → 351.8 |
| scalar | buildAndSerialize | 744.0 → 536.0 | 661.1 → 453.1 |
| text | serialize | 136.0 → 40.0 | 1,957.1 → 1,861.1 |
| text | deserialize | 4,127.8 → 4,127.8 | 4,103.8 → 4,103.8 |
| text | buildAndSerialize | 1,768.0 → 1,560.0 | 3,577.9 → 3,381.1 |
| nested | serialize | 136.0 → 40.0 | 245.1 → 149.1 |
| nested | deserialize | 535.8 → 535.8 | 511.8 → 511.8 |
| nested | buildAndSerialize | 744.0 → 536.0 | 757.1 → 549.1 |
| collections | serialize | 148.8 → 68.8 | 533.1 → 480.3 |
| collections | deserialize | 3,739.0 → 3,414.2 | 3,791.8 → 3,799.8 |
| collections | buildAndSerialize | 1,992.0 → 1,502.4 | 2,029.1 → 1,797.1 |
| large | serialize | 4,506.1 → 23.6 | 4,519.3 → 36.8 |
| large | deserialize | 65,988.1 → 65,988.1 | 65,964.1 → 65,964.1 |
| large | buildAndSerialize | 70,695.0 → 66,079.7 | 70,708.1 → 66,190.4 |

Large `serialize` no longer allocates the per-record stream buffer, as #176 intended.
The other write reductions have no corresponding library change.
The 1.0.0 report attributed 96 B/op of generated-message writes to the code that protoc generates for the empty map field `attributes` ([#175](https://github.com/flink-gcp/flink-datastream-protobuf/issues/175)).
In this run, `serialize` of the scalar, text, and nested records allocates 96 B/op less in both profiles, the Chill lane, which writes the same generated messages, allocates 96 to 102 B/op less, and generated-message `serialize` now allocates as much as DynamicMessage `serialize`.
This report does not examine whether the JDK change accounts for that difference.

## Cross-format encoding costs

Each entry is the arithmetic mean of five fork scores for ordinary corpus-cycling `serialize`; time is ns/op, allocation is B/op from the GC profiler, and encoded bytes are the corpus mean including serializer framing.
The two values in each cell are profile 3 / profile 4, measured on different VMs of one plan.
These are descriptive means; the paired intervals above and the per-fork data carry the uncertainty.

| Workload | Lane | Time (ns/op), 3 / 4 | Allocation (B/op), 3 / 4 | Encoded bytes, 3 / 4 |
|---|---|---:|---:|---:|
| scalar | POJO | 123.3 / 124.6 | 23.6 / 23.6 | 46.9 / 46.9 |
| scalar | TUPLE | 69.0 / 69.8 | 0.0 / 0.0 | 35.2 / 35.2 |
| scalar | AVRO_SPECIFIC | 123.3 / 124.4 | 68.8 / 68.8 | 39.9 / 39.9 |
| scalar | AVRO_GENERIC | 126.9 / 127.4 | 45.1 / 45.1 | 39.9 / 39.9 |
| scalar | PROTOBUF_NATIVE | 61.2 / 50.8 | 40.0 / 53.1 | 23.9 / 23.9 |
| scalar | PROTOBUF_DYNAMIC | 89.5 / 90.9 | 40.0 / 53.1 | 23.9 / 23.9 |
| scalar | PROTOBUF_DYNAMIC_FOREIGN | 90.6 / 88.1 | 40.0 / 53.1 | 23.9 / 23.9 |
| scalar | PROTOBUF_CHILL | 100.1 / 82.6 | 78.5 / 91.6 | 22.9 / 22.9 |
| scalar | THRIFT_CHILL | 1,041.2 / 1,039.4 | 157.0 / 157.0 | 81.8 / 81.8 |
| scalar | KRYO | 254.0 / 258.6 | 0.0 / 0.0 | 27.2 / 27.2 |
| text | POJO | 1,206.4 / 1,205.7 | 23.6 / 23.6 | 1681.8 / 1681.8 |
| text | TUPLE | 1,145.9 / 1,054.1 | 0.0 / 0.0 | 1670.1 / 1670.1 |
| text | AVRO_SPECIFIC | 846.9 / 852.0 | 1,860.8 / 1,860.8 | 1673.7 / 1673.7 |
| text | AVRO_GENERIC | 840.9 / 851.9 | 1,837.1 / 1,837.1 | 1673.7 / 1673.7 |
| text | PROTOBUF_NATIVE | 688.7 / 812.9 | 40.0 / 1,861.1 | 1663.8 / 1663.8 |
| text | PROTOBUF_DYNAMIC | 783.4 / 880.8 | 40.0 / 1,861.1 | 1663.8 / 1663.8 |
| text | PROTOBUF_DYNAMIC_FOREIGN | 748.7 / 881.8 | 40.0 / 1,861.1 | 1663.8 / 1663.8 |
| text | PROTOBUF_CHILL | 922.6 / 1,141.4 | 1,718.5 / 3,539.6 | 1663.8 / 1663.8 |
| text | THRIFT_CHILL | 1,891.5 / 1,899.9 | 3,589.2 / 3,586.0 | 1716.7 / 1716.7 |
| text | KRYO | 1,008.6 / 1,001.3 | 0.0 / 0.0 | 1664.0 / 1664.0 |
| nested | POJO | 170.5 / 170.0 | 34.9 / 34.9 | 64.8 / 64.8 |
| nested | TUPLE | 111.6 / 113.5 | 0.0 / 0.0 | 53.1 / 53.1 |
| nested | AVRO_SPECIFIC | 144.6 / 148.9 | 160.0 / 160.0 | 57.7 / 57.7 |
| nested | AVRO_GENERIC | 146.1 / 145.8 | 125.1 / 125.1 | 57.7 / 57.7 |
| nested | PROTOBUF_NATIVE | 106.1 / 82.9 | 40.0 / 149.1 | 46.4 / 46.4 |
| nested | PROTOBUF_DYNAMIC | 154.8 / 140.5 | 40.0 / 149.1 | 46.4 / 46.4 |
| nested | PROTOBUF_DYNAMIC_FOREIGN | 155.5 / 137.6 | 40.0 / 149.1 | 46.4 / 46.4 |
| nested | PROTOBUF_CHILL | 137.1 / 115.7 | 102.5 / 211.6 | 45.4 / 45.4 |
| nested | THRIFT_CHILL | 1,052.3 / 1,053.2 | 258.0 / 258.0 | 99.7 / 99.7 |
| nested | KRYO | 303.4 / 301.2 | 0.0 / 0.0 | 46.9 / 46.9 |
| collections | POJO | 369.5 / 372.1 | 23.6 / 23.6 | 420.9 / 420.9 |
| collections | TUPLE | 292.5 / 292.6 | 0.0 / 0.0 | 409.2 / 409.2 |
| collections | AVRO_SPECIFIC | 525.7 / 540.8 | 452.8 / 452.8 | 447.9 / 447.9 |
| collections | AVRO_GENERIC | 526.6 / 524.4 | 429.1 / 429.1 | 447.9 / 447.9 |
| collections | PROTOBUF_NATIVE | 812.5 / 798.1 | 68.8 / 480.3 | 498.1 / 498.1 |
| collections | PROTOBUF_DYNAMIC | 1,515.3 / 1,315.1 | 136.0 / 437.1 | 498.1 / 498.1 |
| collections | PROTOBUF_DYNAMIC_FOREIGN | 1,505.2 / 1,315.1 | 136.0 / 437.1 | 498.1 / 498.1 |
| collections | PROTOBUF_CHILL | 846.1 / 913.7 | 582.9 / 980.1 | 498.1 / 498.1 |
| collections | THRIFT_CHILL | 2,652.9 / 2,657.5 | 1,022.6 / 1,016.2 | 488.8 / 488.8 |
| collections | KRYO | 1,200.1 / 1,198.3 | 0.0 / 0.0 | 433.0 / 433.0 |
| large | POJO | 2,815.0 / 2,819.6 | 23.6 / 23.6 | 65582.9 / 65582.9 |
| large | TUPLE | 2,683.8 / 2,715.9 | 0.0 / 0.0 | 65571.2 / 65571.2 |
| large | AVRO_SPECIFIC | 2,788.4 / 2,749.0 | 68.8 / 68.8 | 65575.9 / 65575.9 |
| large | AVRO_GENERIC | 2,829.7 / 2,801.2 | 45.1 / 45.1 | 65575.9 / 65575.9 |
| large | PROTOBUF_NATIVE | 3,189.0 / 3,192.8 | 23.6 / 36.8 | 65563.9 / 65563.9 |
| large | PROTOBUF_DYNAMIC | 3,096.8 / 3,073.5 | 0.0 / 13.1 | 65563.9 / 65563.9 |
| large | PROTOBUF_DYNAMIC_FOREIGN | 3,153.8 / 3,096.3 | 0.0 / 13.1 | 65563.9 / 65563.9 |
| large | PROTOBUF_CHILL | 10,414.4 / 10,465.0 | 65,620.3 / 65,633.4 | 65564.9 / 65564.9 |
| large | THRIFT_CHILL | 13,479.8 / 13,430.1 | 65,693.1 / 65,693.1 | 65619.8 / 65619.8 |
| large | KRYO | 3,326.0 / 3,319.1 | 0.0 / 0.0 | 65565.2 / 65565.2 |

Native scalar serialization is the fastest of all lanes, at 50.8 to 61.2 ns/op.
Collections have the largest native cost relative to the fixed-layout lanes: native `serialize` takes 798 to 813 ns/op, 0.87 to 0.96 times Chill's time but 2.7 to 2.8 times Tuple's and 2.1 to 2.2 times POJO's.
Native large serialization takes 0.31 times Chill's time and 0.96 times Kryo's, and allocates 23.6 to 36.8 B/op.
On Protobuf 4.36.2, writing still allocates in proportion to the string fields: native text `serialize` allocates 1,861.1 B/op on Protobuf 4 against 40.0 on Protobuf 3, and the DynamicMessage and Chill lanes show the same difference.

## DynamicMessage lanes

`PROTOBUF_DYNAMIC` values carry the serializer's own descriptors, taken from a value it decoded, and `PROTOBUF_DYNAMIC_FOREIGN` values carry an equal-content descriptor from another instance, which the serializer accepts through its cache of accepted descriptor pairs.
ADR-0004 gives their ratios as descriptive results without a tolerance or verdict.
The table shows the encoding operations; `dynamic.csv` in each archive holds all 40 cells.

| Profile | Workload | Operation | Dynamic/generated (95% interval) | Foreign/own (95% interval) |
|---|---|---|---:|---:|
| 3 | scalar | serialize | 1.463 (1.453, 1.474) | 1.011 (0.995, 1.028) |
| 3 | scalar | deserialize | 2.415 (2.320, 2.515) | 1.015 (0.982, 1.050) |
| 3 | scalar | buildAndSerialize | 2.350 (2.286, 2.415) | 1.023 (0.993, 1.053) |
| 3 | text | serialize | 1.138 (1.009, 1.284) | 0.955 (0.885, 1.031) |
| 3 | text | deserialize | 1.173 (1.147, 1.199) | 0.994 (0.963, 1.026) |
| 3 | text | buildAndSerialize | 1.301 (1.273, 1.330) | 1.008 (0.966, 1.051) |
| 3 | nested | serialize | 1.459 (1.447, 1.471) | 1.004 (0.991, 1.018) |
| 3 | nested | deserialize | 2.483 (2.343, 2.631) | 1.020 (0.990, 1.051) |
| 3 | nested | buildAndSerialize | 2.107 (2.053, 2.162) | 1.014 (0.990, 1.038) |
| 3 | collections | serialize | 1.865 (1.830, 1.900) | 0.993 (0.978, 1.009) |
| 3 | collections | deserialize | 2.964 (2.658, 3.306) | 1.006 (0.986, 1.027) |
| 3 | collections | buildAndSerialize | 3.636 (3.418, 3.868) | 1.012 (0.920, 1.114) |
| 3 | large | serialize | 0.971 (0.924, 1.021) | 1.018 (0.971, 1.068) |
| 3 | large | deserialize | 1.057 (1.046, 1.068) | 0.991 (0.980, 1.003) |
| 3 | large | buildAndSerialize | 1.021 (1.018, 1.023) | 0.996 (0.991, 1.001) |
| 4 | scalar | serialize | 1.787 (1.732, 1.843) | 0.970 (0.952, 0.988) |
| 4 | scalar | deserialize | 3.159 (3.096, 3.222) | 1.009 (0.992, 1.025) |
| 4 | scalar | buildAndSerialize | 2.598 (2.520, 2.678) | 1.002 (0.981, 1.023) |
| 4 | text | serialize | 1.084 (1.073, 1.094) | 1.001 (0.988, 1.015) |
| 4 | text | deserialize | 1.256 (1.233, 1.279) | 1.002 (0.984, 1.019) |
| 4 | text | buildAndSerialize | 1.296 (1.277, 1.315) | 0.997 (0.976, 1.018) |
| 4 | nested | serialize | 1.694 (1.640, 1.751) | 0.979 (0.934, 1.026) |
| 4 | nested | deserialize | 3.134 (3.035, 3.236) | 0.990 (0.976, 1.004) |
| 4 | nested | buildAndSerialize | 2.556 (2.207, 2.960) | 1.016 (0.994, 1.039) |
| 4 | collections | serialize | 1.648 (1.606, 1.692) | 1.000 (0.987, 1.013) |
| 4 | collections | deserialize | 2.934 (2.765, 3.112) | 1.024 (0.951, 1.103) |
| 4 | collections | buildAndSerialize | 3.396 (3.370, 3.423) | 0.976 (0.939, 1.014) |
| 4 | large | serialize | 0.963 (0.939, 0.987) | 1.007 (1.000, 1.015) |
| 4 | large | deserialize | 1.065 (1.054, 1.076) | 1.007 (0.997, 1.018) |
| 4 | large | buildAndSerialize | 1.019 (1.010, 1.028) | 1.001 (0.993, 1.008) |

Decoding a DynamicMessage takes 2.4 to 3.2 times as long as decoding the generated message for the scalar, nested, and collections records, 1.2 to 1.3 times for text, and 1.1 times for large.
In the 1.0.0 run the first group took 5.3 to 6.3 times as long.
The serializer decodes both through the parser of the value's type, so the difference is the cost of protobuf-java's DynamicMessage parsing.
The own-descriptor DynamicMessage decoding times of those records fell to 41% to 53% of their 1.0.0 values while the generated ones changed by at most 8%, and no library change between the two candidates touches decoding.
Writing takes 1.5 to 1.9 times as long for the scalar, nested, and collections records and 0.96 to 1.14 times as long for text and large.
`buildAndSerialize` also includes building the DynamicMessage value with its builder, which is the benchmark's conversion, not serializer work.

The foreign/own point estimates lie between 0.91 and 1.07 across all 80 cells.
Eight of their intervals exclude 1.
Five are copy or stream-copy cells, which run the same code for both lanes, and the farthest from 1 is Protobuf 4 text `objectCopy`, at 0.908 (0.871, 0.946); the other three exclude 1 by at most 1.2%, and one of them, Protobuf 4 large `serialize`, has a lower limit that rounds to 1.000.
Values with descriptors from another instance thus show no consistent cost from the write-acceptance cache.

## Remaining gaps and release decision

- Collections encoding remains the narrowest native margin over Chill, 0.87 to 0.96, and costs several times the fixed-layout lanes.
- On Protobuf 4.36.2, every Protobuf lane allocates an additional amount per string field when writing.
- The pinned Protobuf 3 text write cells describe one compilation, and the Chill lane reaches the slow one far more often without the pin.
- DynamicMessage decoding costs 2.4 to 3.2 times generated decoding for small records, which applications choosing DynamicMessage pay to protobuf-java's parser.
- The 96 B/op that the 1.0.0 report attributed to protoc's generated code for empty map fields is not allocated in this run, and this report does not examine whether the JDK accounts for that; [#200](https://github.com/flink-gcp/flink-datastream-protobuf/issues/200) still waits for a protobuf-java release with the generated-code change.

No cell misses or leaves the tolerance undecided, and no ratio of a non-copy cell rose by more than 0.077 apart from the pinned Protobuf 3 text write cells, whose compilation and JDK both changed.
The measurements therefore show no material regression and raise no performance objection to releasing this candidate as 1.1.0.
None of the follow-ups requires an API, state-format, or default change.

## Retained evidence

The [retained evidence](data/serializer-benchmarks-v1.1.0/README.md) holds both complete result directories with their GCE run records, checksums, and inventories.
