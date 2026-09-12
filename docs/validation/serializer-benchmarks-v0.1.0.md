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

# Serializer benchmarks for v0.1.0

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

Long measurements have not yet been collected for this change.
The final report will link the retained raw results and describe workload-specific regressions, uncertainty, allocation, and encoded size before this issue is complete.
