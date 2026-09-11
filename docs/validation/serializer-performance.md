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

# Serializer implementation performance check

This local comparison measures the getter-name removal, metadata cache, and sizing refactor against commit `f6a6e33d272f74da80f82e9d77718399cda9702e`.
The candidate is the implementation accompanying this report.
It is a small implementation comparison, not a ranking against other formats or an end-to-end Flink throughput result.
The maintained cross-format benchmark and release baseline are tracked in [#30](https://github.com/flink-gcp/flink-datastream-protobuf/issues/30).

## Conditions

Measurements ran on 2026-09-11 using an Apple M1 Pro with 32 GiB RAM, macOS 26.6.2, and Temurin 17.0.20+8.
Both variants used Flink 2.3.0, JMH 1.37, G1 GC, a fixed 256 MiB heap, and one benchmark thread.
Protobuf runtime and generated fixtures were paired at 3.25.8 and 4.33.6 in separate classpaths.
Each case used two JVM forks, three 500 ms warmup iterations, five 500 ms measurement iterations, average time mode, and the JMH GC profiler.
Final comparisons ran sequentially without concurrent builds or another benchmark.
The table reports the JMH mean and its 99.9% confidence interval half-width; allocated bytes are rounded per operation.
These short runs on a development machine cannot establish small performance differences reliably.

The temporary harness compiles the baseline or candidate production sources with the corresponding generated test fixtures and JMH annotation processor.
Its timing command is equivalent to:

```sh
java -cp "$BENCHMARK_CLASSPATH" org.openjdk.jmh.Main SerializerBenchmark \
  -f 2 -wi 3 -i 5 -w 500ms -r 500ms -bm avgt -tu ns -t 1 \
  -jvmArgs '-Xms256m -Xmx256m -XX:+UseG1GC' -prof gc -rf json -rff results.json
```

The harness and raw JSON are local investigation artifacts; a maintained, independently runnable harness is part of #30.
No benchmark dependencies enter this library's production artifact.

## Workloads

All serialization cases reuse a single immutable generated value and a serializer with default settings.
The timed operation clears a preallocated 128 KiB `DataOutputSerializer`, calls `serialize(value, output)`, and returns the resulting framed length to JMH.
Setup checks that the frame length equals `value.toByteArray().length + 4` before measurement.
Inputs, generated accessors, memoized runtime sizes, and buffers are therefore warm; fresh-record construction and cold class initialization are not measured.

| Case | Input and timed operation |
| --- | --- |
| construct | Return a new `ProtobufTypeSerializer<>(ScalarMessage.class)`; the candidate's class metadata cache is already populated during warmup |
| scalar | `ScalarMessage` with `int32_value = 42` |
| utf8 | The scalar plus `string_value = "ascii é 中 😀".repeat(16)` |
| raw | `LegacyMessage.parseFrom(new byte[] {10, 2, (byte) 255, (byte) 254})` |
| nested_raw | Start with raw; eight times wrap the previous value in `groupvalue.child` of a new raw message, yielding nine ambiguous strings and parser depth 16 |
| large_bytes | The scalar plus a 65,536-byte zero-filled `bytes_value` |

## Results

| Protobuf major | Case | Before ns/op | After ns/op | Before B/op | After B/op |
| --- | --- | --- | --- | --- | --- |
| 3 | construct | 5718.2 ± 226.5 | 6.1 ± 0.6 | 19180 | 64 |
| 3 | scalar | 1065.1 ± 52.5 | 1016.1 ± 22.1 | 5808 | 5824 |
| 3 | utf8 | 1463.7 ± 205.5 | 1360.8 ± 17.0 | 5848 | 5848 |
| 3 | raw | 584.7 ± 122.0 | 528.4 ± 10.7 | 5384 | 5624 |
| 3 | nested_raw | 3098.4 ± 231.9 | 3160.4 ± 69.5 | 16288 | 15968 |
| 3 | large_bytes | 2251.4 ± 32.3 | 2320.8 ± 71.1 | 5864 | 5880 |
| 4 | construct | 6798.6 ± 916.7 | 5.9 ± 0.1 | 18972 | 64 |
| 4 | scalar | 1198.8 ± 143.4 | 1090.2 ± 25.9 | 5744 | 5760 |
| 4 | utf8 | 1867.6 ± 706.9 | 1477.0 ± 18.5 | 5800 | 5816 |
| 4 | raw | 670.9 ± 15.3 | 612.5 ± 51.8 | 5312 | 5520 |
| 4 | nested_raw | 3607.9 ± 57.3 | 3867.4 ± 418.0 | 16192 | 16312 |
| 4 | large_bytes | 2473.1 ± 307.5 | 2477.4 ± 60.9 | 5800 | 5800 |

Caching validated metadata removes repeated reflective default lookup, parser validation, and descriptor normalization from warm serializer construction.
That construction result does not imply a comparable per-record or whole-job speedup.
Ordinary serialization still traverses fields and allocates sizing metadata; it does not perform this initialization work on each record.

The raw-string path replaces generated getter-name guesses with one bounded counting pass through public APIs.
It can allocate more for a small raw message, while removing per-occurrence reflection/cache-key work in nested messages.
The candidate counts the root once after validating the entire graph, so nested ambiguous strings do not cause one counting pass per message node.
The implementation does not retain records or their size summaries between calls.

This check does not cover deserialization, copying, cold classloading, fresh-record allocation, other Flink/JDK versions, or all schema shapes.
Those results must not be inferred from this table; compatibility remains covered separately by the project's verification matrix.
No timing threshold is added to shared-runner CI.
