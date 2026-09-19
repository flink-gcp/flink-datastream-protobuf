---
title: "Serializer benchmarks for the 0.1.0 development milestone"
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

# Serializer benchmarks for the 0.1.0 development milestone

This report compares equivalent logical records through Flink serializers.
The benchmark is development tooling and makes no production API or state-compatibility promise.
The measurement contract is [ADR-0004](../adr/0004-serializer-benchmarks.md).

## Reproduce

The ordinary build does not require benchmark tools.
For opt-in generation, install CMake, Flex, Bison 3.8 or newer, and a C++ compiler; build the pinned Thrift 0.23.0 compiler in an external cache:

```sh
mise x -- just benchmark-tools /tmp/flink-protobuf-benchmark-tools
export BENCHMARK_THRIFT_COMPILER=/tmp/flink-protobuf-benchmark-tools/build/compiler/cpp/bin/thrift
mise x -- just benchmark-smoke 2.3.0 3 /tmp/flink-protobuf-smoke-3
mise x -- just benchmark-run 2.3.0 3 /tmp/flink-protobuf-baseline-3
mise x -- just benchmark-run 2.3.0 4 /tmp/flink-protobuf-baseline-4
```

Each output directory must be new.
The recipes clean between runtime/gencode combinations, generate all three schema representations, build the library, run the benchmark contract tests, check JAR isolation, and then execute independent validation and measurement JVMs.
`benchmark-check` performs the full correctness inventory without timing; `benchmark-smoke` additionally measures every operation and supported lane on the collections workload with one 100 ms warmup and one 100 ms measurement.
A full run measures all five workloads with five independent JVMs per cell, five one-second warmups and five one-second measurements per JVM.
It uses one benchmark thread, `-Xms1g -Xmx1g -XX:+UseG1GC`, and JMH's GC profiler.
Two complete Protobuf profile runs take several hours; run them sequentially without other builds or heavy work.
Keep the Mac connected to power and record interruptions or concurrent load.

On macOS, ensure the selected Bison is modern; `/usr/bin/bison` may be too old.
On macOS the recipe selects the installed SDK and its matching C++ include path through `xcrun`.
These toolchain settings do not change the Thrift source.
The compiler tarball is checked against SHA-256 `1859d932d2ae1f13d16c5a196931208c116310a5ff50f2bfd11d3db03be8f46f`.
Generation checks `thrift --version` before using it.

## Corpus and representation mapping

The seed is `30012026`, with 64 distinct records per workload.
Every representation carries the same nine fields: signed integer ID, long timestamp, boolean, name, optional note, bytes, a child record, a list of longs, and a string-to-long map.
All workloads use the same record schema; omitted payload categories retain their defined empty/default values, so fixed-field formats still encode those fields.
This measures populated shapes within one schema, not minimal separately optimized schemas for each format.

| Workload | Distinguishing values |
|---|---|
| scalar | Scalars and empty/default structured fields |
| text | ASCII, Japanese, and accented text plus 1 KiB pseudorandom bytes |
| nested | A populated child record with integer and Unicode string fields |
| collections | 16 long values and 16 string-to-long entries inserted in descending key order |
| large | 64 KiB pseudorandom bytes |

The optional note cycles through absent, present-empty, and present-nonempty.
Absent means POJO/Kryo null, Avro union null, Protobuf unset optional, Thrift unset optional, and Tuple `(false, "")`.
Tuple present values use `(true, value)`.
Empty strings, zero scalars, empty lists, empty maps, and a present default child remain valid values.
Top-level nulls and null collection entries are excluded.
The Protobuf schema uses proto3 and valid UTF-8, so it does not exercise the proto2 ambiguous-string counting path covered by the separate [implementation performance check](serializer-performance.md).
Maps compare by key/value equality, not iteration order; their initial insertion order is deterministic.
Byte arrays, ByteString, and ByteBuffer compare by payload bytes; Avro strings normalize to Java strings.

| Lane | Explicit serializer policy |
|---|---|
| POJO | PojoTypeInfo with explicit child, list, map, and primitive-array types; recursively reject Kryo serializers |
| TUPLE | Tuple9 with explicit field types and Tuple2 child/presence wrappers; recursively reject Kryo serializers |
| AVRO_SPECIFIC | AvroTypeInfo of the generated specific record and AvroSerializer |
| AVRO_GENERIC | GenericRecordAvroTypeInfo with the generated schema and AvroSerializer |
| PROTOBUF_NATIVE | Production ProtobufTypeInformation builder; default limits and nondeterministic writing |
| PROTOBUF_CHILL | Flink GenericTypeInfo with the generated record registered to Chill ProtobufSerializer |
| THRIFT_CHILL | Flink GenericTypeInfo, TBase default adapter, and registered generated root class |
| KRYO | A final-field record lacking a no-argument constructor; register root, child, ArrayList, LinkedHashMap, and byte array in that order |

The Kryo lanes retain Flink's reference tracking and `registrationRequired=false` policy.
The comparison inventory records the actual adapter, root registration ID, loaded Kryo/adapter artifacts, and serializer graph.
Avro's type discovery may log a generic ByteBuffer field; the selected runtime serializer is independently asserted to be AvroSerializer.
The POJO lane checks its actual nested serializers, rather than assuming the top-level type proves the entire route.

## Timed operations and interpretation

`serialize`, `deserialize`, `objectCopy`, and `streamCopy` each process one record per invocation, cycling over the corpus.
`deserializeReuse` and `objectCopyReuse` pass the previous result as the reuse candidate.
`serializeSameInstance` repeatedly writes corpus element zero.
`buildAndSerialize` constructs a fresh representation from a prebuilt logical value inside the timed method, including representation allocation and serialization; it does not include pseudorandom corpus generation.
String and scalar values are shared with the prebuilt logical corpus; each conversion creates fresh record/container objects and byte storage.
The logical-to-representation conversion is common instrumentation, not a production ingestion recommendation.

Correctness subprocesses and JMH forks load the packaged production library JAR, whose hash is retained alongside the test instrumentation hashes.
Each JMH fork asserts the library's loaded location against the packaged JAR before timing.
Trial setup constructs type information, creates serializers, validates round-trips, pre-encodes the input records, and warms copy/stream-copy storage.
Repeated serialization therefore measures previously encoded instances; fresh construction creates new generated messages without reusing their memoized sizes.
Native sizing traverses populated fields on every write; Chill uses `toByteArray`, which consults the generated runtime's memoized sizes.
The benchmark never resets private Protobuf caches or subtracts construction costs from measured results.
Initialization, descriptor discovery, and fresh-classloader startup are outside the timed regions.

Each thread owns its serializer, input view, output buffer, and reuse candidate.
The output buffer is preallocated to 128 KiB and cleared for each write; input views are reset to the exact encoded record.
Buffer resets and corpus indexing are included in the measured operation.
Outputs and returned objects escape through JMH to prevent dead-code elimination.
Allocation results include work performed by the timed method, including any internal adapter buffers.

Native Protobuf object copy returns its immutable input reference.
Chill's Protobuf adapter serializes and parses a new value even though Protobuf messages are immutable.
Other serializers' mutation independence and reuse identities are recorded by correctness checks.
Flink Kryo stream copy deserializes and serializes; native Protobuf stream copy transfers the framed bytes through reusable storage.
These are the actual implementations of the Flink operations and are not interchangeable amounts of copying work.
Do not interpret their ratios as general encoding-speed ratios.

Encoded size includes every byte emitted by the Flink TypeSerializer, including the native four-byte payload-length prefix and any Kryo/adapter framing.
It excludes Flink network envelopes, checkpoints, compression, and transport overhead.
The support inventory reports mean, minimum, and maximum encoded sizes over all 64 values.
Throughput in `measurements.csv` is explicitly derived as `1e9 / ns_per_op`; it is not a separate throughput-mode experiment.
Native deterministic writing is a separately labeled serialize/collections supplement; it does not change the primary comparison configuration.

## Correctness and performance matrices

Correctness checks decode all records, exercise both copy overloads and both deserialize overloads, mutate copies of mutable representations, and ensure a deserialize or stream copy consumes exactly one of two adjacent records.
JMH setup repeats value checks inside each measurement fork before timing.
A missing prerequisite, incorrect serializer, wrong value, or missing allocation metric fails the run.
Known adapter linkage or Java module-access failures are recorded as unsupported without substitution.

CI covers Flink 2.2.1 and 2.3.0 with JDK 17/21, and Flink 1.20.4 with JDK 17, each with Protobuf 3.25.9/protoc 3.25.9 and Protobuf 4.33.6/protoc 4.33.6.
The representative long-run matrix is Flink 2.3.0 on this Mac's Temurin 17 for both Protobuf profiles.
JMH is 1.37, Avro runtime/generator is 1.11.4, Thrift runtime/compiler is 0.23.0, and both Chill adapters are 0.10.0.
The report reader uses Jackson 2.14.3 through an explicit benchmark-only dependency.
Exact runtime JARs and hashes are retained in each run's classpath manifest.
Generic comparison JVMs on Flink 1.20 additionally try `--add-opens=java.base/java.util=ALL-UNNAMED` after an unsupported plain-JVM result.
Native tests and benchmarks never inherit that flag.

## Statistical contract and retained evidence

The practical goal is at most 10% additional native Protobuf time relative to Chill on the same workload, operation, and input policy.
Five measurement blocks place the native and Chill forks adjacent, reversing their order in alternating blocks.
The remaining lane groups use a fixed shuffled order within each workload/operation block.
For each block, average the five timed iterations, then compute the native/Chill log ratio.
Use the mean and Student-t interval of those five log ratios (four degrees of freedom, critical value 2.7764451051977987), and exponentiate the limits.
This retains fork-to-fork uncertainty instead of treating 25 iterations as independent JVM runs.

An upper 95% limit at or below 1.10 is `established`; a lower limit above 1.10 is `missed`; all other intervals are `inconclusive`.
The intervals are per comparison and assume approximately normal independent log ratios, not simultaneous confidence across every reported cell.
Identity-copy timings near the harness overhead deserve particular caution.
Noisy overlaps between independent JMH scores do not establish the tolerance.
Allocation and encoded size are reported separately without inventing an additional acceptance threshold.

Each run retains JMH JSON with per-iteration raw data and GC metrics, per-fork logs, completed result inventory, support properties, machine/JVM/configuration manifest, and SHA-256 hashes of the classpath and packaged library.
`measurements.csv` and `parity.csv` are reproducible summaries of that evidence.
The source revision identifies the measurement source; report-only commits may follow without relabeling its provenance.
Results apply to the recorded machine and runtime, do not rank serializers universally, and do not establish snapshot compatibility or whole-job throughput.

## Baseline results

The native serializer misses the predeclared time tolerance in several encoding and decoding cells on this host.
Large-payload serialization and the copy operations favor the native implementation, while small-record writes expose substantial allocation and sizing costs.
These measurements do not establish a universal fastest serializer or parity across the corpus.
The [retained evidence](data/serializer-benchmarks-v0.1.0/README.md) includes both complete runs, all per-cell results, archive hashes, and separate diagnostic event exports.

### Host and run validity

Both runs used the clean measurement source `f1fdd36137b8baa59202652da83a7a8a94d98c3f`, Flink 2.3.0, Temurin 17.0.20+8, an Apple M1 Pro with 10 logical CPUs and 32 GiB memory, and macOS 26.6.2 as reported by the JVM.
The system manifest separately reports Darwin 25.6.0 on arm64.
The JMH fork settings and comparison contract above were unchanged between profiles.
The version in the packaged development artifact is `0.1.0-SNAPSHOT`; 0.1.0 identifies a development milestone, not a published release.

| Profile | Start (JST) | Measurement completion (JST) | Accepted JMH results |
|---|---|---|---:|
| Protobuf/protoc 3.25.9 | 2026-09-12 17:29:29 | 2026-09-12 22:09:16 | 1,605 |
| Protobuf/protoc 4.33.6 | 2026-09-13 11:10:26 | 2026-09-13 15:50:11 | 1,605 |

Each profile contains 1,600 primary forks and five deterministic supplements.
An independent audit checked all 3,210 results for the expected inventory, source/JAR provenance, fork/iteration settings, finite timing and allocation data, and agreement of all 80 paired intervals with the report reader.

Before each build and after each build, the start gate required at least five minutes of sampled CPU idle averaging 90% or more, with an 85% minimum, one-minute load below 10, no observed swap-out, and at most 1 MiB of swap-in per observation.
The gate sampled two-second windows approximately every 30 seconds; it did not establish exclusive host access or continuous CPU isolation.
Its initial stricter CPU/page-in thresholds were refined before any retained timing, as recorded in the environment notes.
The profiles ran sequentially on different days, so ambient host conditions may differ.

An initial run under background load was stopped and excluded.
The first overnight Protobuf 4 attempt was excluded in full after the system recorded clamshell sleep on battery power at 2026-09-13 01:43:46 JST and wake on AC at 10:35:01.
No results from that attempt enter these tables.
The accepted replacement used closed-display operation with USB-C AC power, power observations every 30 seconds, and an external guard that invalidated power-source changes or observation gaps exceeding 120 seconds.
Its retained `power-events.log` contains no sleep events; the accepted Protobuf 3 interval also had no recorded sleep events.
The Protobuf 3 archive includes a separate `power-event-audit.txt`; the Protobuf 4 archive retains the event log and power observations without that separate audit summary.
These power checks address interruptions, not background CPU scheduling or thermal variation.

### Native and Chill time tolerance

| Profile | Established | Inconclusive | Missed |
|---|---:|---:|---:|
| Protobuf 3, all 40 cells | 20 | 5 | 15 |
| Protobuf 4, all 40 cells | 19 | 3 | 18 |
| Protobuf 3, 25 encoding/decoding/construction cells | 5 | 5 | 15 |
| Protobuf 4, 25 encoding/decoding/construction cells | 4 | 3 | 18 |

All 15 copy cells per profile establish the tolerance, with the different copy semantics described above.
They do not offset missed encoding/decoding cells.
The following selected cells show the paired geometric time ratio and its 95% interval; lower ratios favor native Protobuf.
The archives retain every cell, including reuse, same-instance, and copy results.

| Profile | Workload | Operation | Native/Chill (95% interval) | Verdict |
|---|---|---|---:|---|
| 3 | scalar | serialize | 9.332 (9.145, 9.523) | missed |
| 3 | scalar | deserialize | 2.883 (2.703, 3.075) | missed |
| 3 | scalar | buildAndSerialize | 4.671 (4.329, 5.039) | missed |
| 3 | text | serialize | 1.107 (1.077, 1.138) | inconclusive |
| 3 | text | deserialize | 1.043 (0.971, 1.120) | inconclusive |
| 3 | text | buildAndSerialize | 1.012 (0.673, 1.522) | inconclusive |
| 3 | nested | serialize | 8.606 (8.433, 8.783) | missed |
| 3 | nested | deserialize | 2.242 (2.022, 2.486) | missed |
| 3 | nested | buildAndSerialize | 4.554 (4.404, 4.710) | missed |
| 3 | collections | serialize | 5.413 (5.217, 5.615) | missed |
| 3 | collections | deserialize | 1.267 (1.174, 1.368) | missed |
| 3 | collections | buildAndSerialize | 4.677 (4.555, 4.801) | missed |
| 3 | large | serialize | 0.358 (0.343, 0.374) | established |
| 3 | large | deserialize | 1.091 (1.086, 1.095) | established |
| 3 | large | buildAndSerialize | 0.631 (0.619, 0.644) | established |
| 4 | scalar | serialize | 10.097 (9.583, 10.639) | missed |
| 4 | scalar | deserialize | 3.013 (2.863, 3.171) | missed |
| 4 | scalar | buildAndSerialize | 4.614 (4.500, 4.732) | missed |
| 4 | text | serialize | 2.586 (2.572, 2.599) | missed |
| 4 | text | deserialize | 1.071 (1.012, 1.134) | inconclusive |
| 4 | text | buildAndSerialize | 2.031 (2.009, 2.054) | missed |
| 4 | nested | serialize | 9.258 (8.984, 9.541) | missed |
| 4 | nested | deserialize | 2.132 (2.007, 2.264) | missed |
| 4 | nested | buildAndSerialize | 4.697 (4.498, 4.906) | missed |
| 4 | collections | serialize | 5.574 (5.459, 5.692) | missed |
| 4 | collections | deserialize | 1.212 (1.149, 1.279) | missed |
| 4 | collections | buildAndSerialize | 4.907 (4.871, 4.944) | missed |
| 4 | large | serialize | 0.366 (0.345, 0.388) | established |
| 4 | large | deserialize | 1.099 (1.085, 1.114) | inconclusive |
| 4 | large | buildAndSerialize | 0.651 (0.636, 0.667) | established |

Protobuf 3 text `buildAndSerialize` has a broad interval, so its point estimate is insufficient for an acceptance claim.
The narrower text `serialize` interval also crosses the 1.10 tolerance and is inconclusive.
Across profiles, Chill text serialization falls from 1,176.8 to 519.4 ns/op with the same 1,814.5 B/op, while native text serialization changes from 1,303.2 to 1,343.0 ns/op.
The changed comparator contributes to the different text verdicts; the profile totals do not establish a native-serializer regression between Protobuf versions.
The profiles differ in runtime/gencode and ran on different days, so these observations do not isolate a Protobuf implementation effect from host/run variation.
Five independent pairs give limited information about host drift and distribution shape.
The fixed corpus and warm setup also limit transfer to fresh application objects and first-use costs.
`serializeSameInstance` uses record zero while `serialize` cycles through all 64 values; their difference does not isolate memoization.

### Cross-format encoding costs

Each entry below is the arithmetic mean across five fork scores for ordinary corpus-cycling serialization.
Time is ns/op and allocation is B/op from the GC profiler; encoded bytes are the corpus mean including serializer framing.
The two values in each cell are Protobuf profile 3 / profile 4.
Every lane was rerun in each profile, so cross-profile timing differences can include host and run variation as well as the changed Protobuf runtime/gencode.
Use per-fork raw data for uncertainty; this table supplies descriptive means, not additional statistical acceptance tests.

| Workload | Lane | Time (ns/op), 3 / 4 | Allocation (B/op), 3 / 4 | Encoded bytes, 3 / 4 |
|---|---|---:|---:|---:|
| scalar | POJO | 96.1 / 97.8 | 23.6 / 23.6 | 46.9 / 46.9 |
| scalar | TUPLE | 68.8 / 70.3 | 0.0 / 0.0 | 35.2 / 35.2 |
| scalar | AVRO_SPECIFIC | 96.8 / 100.4 | 132.8 / 132.8 | 39.9 / 39.9 |
| scalar | AVRO_GENERIC | 101.7 / 105.6 | 109.1 / 109.1 | 39.9 / 39.9 |
| scalar | PROTOBUF_NATIVE | 680.7 / 735.9 | 6152.8 / 6088.8 | 23.9 / 23.9 |
| scalar | PROTOBUF_CHILL | 72.9 / 72.9 | 174.5 / 174.5 | 22.9 / 22.9 |
| scalar | THRIFT_CHILL | 440.3 / 451.6 | 157.0 / 157.0 | 81.8 / 81.8 |
| scalar | KRYO | 209.8 / 212.3 | 0.0 / 0.0 | 27.2 / 27.2 |
| text | POJO | 1161.5 / 1202.0 | 23.6 / 23.6 | 1681.8 / 1681.8 |
| text | TUPLE | 1136.4 / 1156.3 | 0.0 / 0.0 | 1670.1 / 1670.1 |
| text | AVRO_SPECIFIC | 525.2 / 524.2 | 1924.8 / 1924.8 | 1673.7 / 1673.7 |
| text | AVRO_GENERIC | 536.5 / 546.5 | 1901.1 / 1901.1 | 1673.7 / 1673.7 |
| text | PROTOBUF_NATIVE | 1303.2 / 1343.0 | 6296.8 / 6210.4 | 1663.8 / 1663.8 |
| text | PROTOBUF_CHILL | 1176.8 / 519.4 | 1814.5 / 1814.5 | 1663.8 / 1663.8 |
| text | THRIFT_CHILL | 933.5 / 943.6 | 3608.4 / 3586.0 | 1716.7 / 1716.7 |
| text | KRYO | 700.6 / 692.4 | 0.0 / 0.0 | 1664.0 / 1664.0 |
| nested | POJO | 117.7 / 121.6 | 34.9 / 34.9 | 64.8 / 64.8 |
| nested | TUPLE | 81.6 / 81.8 | 0.0 / 0.0 | 53.1 / 53.1 |
| nested | AVRO_SPECIFIC | 117.2 / 116.2 | 224.0 / 224.0 | 57.7 / 57.7 |
| nested | AVRO_GENERIC | 120.1 / 125.3 | 189.1 / 189.1 | 57.7 / 57.7 |
| nested | PROTOBUF_NATIVE | 774.6 / 816.2 | 6305.6 / 6222.4 | 46.4 / 46.4 |
| nested | PROTOBUF_CHILL | 90.0 / 88.2 | 198.5 / 198.5 | 45.4 / 45.4 |
| nested | THRIFT_CHILL | 452.2 / 451.9 | 258.0 / 258.0 | 99.7 / 99.7 |
| nested | KRYO | 240.2 / 242.2 | 0.0 / 0.0 | 46.9 / 46.9 |
| collections | POJO | 300.3 / 299.9 | 23.6 / 23.6 | 420.9 / 420.9 |
| collections | TUPLE | 264.8 / 260.2 | 0.0 / 0.0 | 409.2 / 409.2 |
| collections | AVRO_SPECIFIC | 423.5 / 420.8 | 516.8 / 516.8 | 447.9 / 447.9 |
| collections | AVRO_GENERIC | 428.1 / 419.6 | 493.1 / 493.1 | 447.9 / 447.9 |
| collections | PROTOBUF_NATIVE | 4670.9 / 4294.3 | 16666.4 / 17050.4 | 498.1 / 498.1 |
| collections | PROTOBUF_CHILL | 861.9 / 770.3 | 880.5 / 669.3 | 498.1 / 498.1 |
| collections | THRIFT_CHILL | 1164.8 / 1163.2 | 1064.2 / 1064.2 | 488.8 / 488.8 |
| collections | KRYO | 944.9 / 932.7 | 0.0 / 0.0 | 433.0 / 433.0 |
| large | POJO | 1445.8 / 1441.2 | 23.6 / 23.6 | 65582.9 / 65582.9 |
| large | TUPLE | 1349.5 / 1344.1 | 0.0 / 0.0 | 65571.2 / 65571.2 |
| large | AVRO_SPECIFIC | 1397.8 / 1459.1 | 132.8 / 132.8 | 65575.9 / 65575.9 |
| large | AVRO_GENERIC | 1457.6 / 1480.8 | 109.1 / 109.1 | 65575.9 / 65575.9 |
| large | PROTOBUF_NATIVE | 2163.1 / 2214.5 | 6208.8 / 6144.8 | 65563.9 / 65563.9 |
| large | PROTOBUF_CHILL | 6043.7 / 6047.2 | 65716.3 / 65716.3 | 65564.9 / 65564.9 |
| large | THRIFT_CHILL | 7328.5 / 7364.7 | 65696.2 / 65693.0 | 65619.8 / 65619.8 |
| large | KRYO | 3162.7 / 3092.8 | 0.0 / 0.0 | 65565.2 / 65565.2 |

The full 640-cell summaries include deserialize, reuse, object/stream copy, same-instance serialization, and fresh construction, with minimum and maximum fork scores.
Derived operations per second remain available in `measurements.csv`.

### Allocation and optimization candidates

On Protobuf 4, native scalar serialization allocates about 6,089 B/op versus 175 B/op for Chill, and scalar deserialization allocates about 4,496 versus 414 B/op.
Native large-payload serialization allocates about 6,145 B/op versus 65,716 B/op for Chill and takes about 0.366 times the paired Chill time.
Large-payload deserialization still allocates about 132,056 versus 131,532 B/op and has an inconclusive time-tolerance interval.
Avoiding an intermediate output payload array therefore does not make the whole serializer allocation-free or uniformly faster.

Separate short JFR diagnostics on the same Protobuf 4 source/configuration corroborate allocation sites in the stream encoder/decoder buffers and native sizing traversal.
Worker-thread allocation samples include byte arrays allocated by `CodedOutputStream.AbstractBufferedEncoder` and `CodedInputStream.StreamDecoder`, and sizing `Frame`, `Child`, and collection objects.
Execution samples also reach sizing traversal, identity maps, generated field access, and decoder refill paths.
The retained allocation/execution event exports cover native/Chill scalar serialize/deserialize and native collection serialization; diagnostic timings are not included in the baseline.

The source explains these sites: [serialization and deserialization](../../src/main/java/io/github/flink/gcp/protobuf/ProtobufTypeSerializer.java) create stream-based coded wrappers for each record, while [safe sizing](../../src/main/java/io/github/flink/gcp/protobuf/ProtobufMessageSize.java) builds per-call traversal metadata and reads populated fields.
Protobuf 4.33.6's `CodedOutputStream.newInstance(OutputStream)` and `CodedInputStream.newInstance(InputStream)` use 4,096-byte default buffers; Chill's adapter uses `toByteArray` for writing and parses a payload array when reading.
The pinned Protobuf source locations are `CodedOutputStream.DEFAULT_BUFFER_SIZE`, `CodedInputStream.StreamDecoder`, and `AbstractMessageLite.toByteArray`; dependency identities are in the classpath manifest.
JFR supports identifying exercised allocation sites, but sampling weights do not quantify each site's contribution to the baseline time gap.
No controlled ablation was performed.

The first optimization candidates are reducing fixed per-record buffers for small frames and reducing sizing traversal allocations while preserving overflow, depth, size, framing, classloading, and concurrency guarantees.
Construction benchmarks and collection-heavy messages must remain separate acceptance cases when evaluating such changes.
This issue establishes the baseline and investigation; it makes no production optimization or fastest-serializer claim.

## Benchmark remeasurement for 1.0.0

Before releasing 1.0.0, repeat the long serializer benchmarks from a fixed release-candidate commit for both supported Protobuf runtime/gencode profiles.
Record the candidate commit, packaged library SHA-256, exact dependency and toolchain versions, and host configuration with the raw results.
Use the same logical corpus, operation definitions, warmup and measurement settings, independent paired blocks, and predeclared practical tolerance as the development baseline.
Use temporary Google Compute Engine VMs for the next benchmark run and the 1.0.0 candidate measurements.
Use matching machine types, CPU platforms, regions, operating-system images, and JDK settings, and record their exact identities in the evidence.
Run one Protobuf profile per VM so that the profiles can execute concurrently without competing within a VM.
Keep the native/Chill pair ordering and independent fork contract within each profile unchanged.
Record VM interruptions and exclude interrupted runs before interpreting their results.
Export raw results and provenance, verify that the retained evidence is complete, and then delete the temporary VMs and their disks.
Recheck current machine availability and pricing when provisioning.

Compare the candidate with the retained 0.1.0 development baseline by workload and operation, including time, allocation, and encoded size.
If the supported Flink, JDK, Protobuf, adapter, or host versions changed, list those differences explicitly and do not attribute the entire measured difference to library changes.
Preserve the earlier baseline unchanged and publish a separate 1.0.0 report with raw evidence and uncertainty.
Benchmark results do not replace release API, state-compatibility, or published-artifact validation.
