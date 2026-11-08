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

# Compilation modes of Protobuf 3 text writes

This record explains why the Protobuf 3 text write cells of the long benchmark runs take one of two times per JVM fork, for [#178](https://github.com/flink-gcp/flink-datastream-protobuf/issues/178).
In the [1.0.0 release-candidate run](serializer-benchmarks-v1.0.0.md), native text `serialize` forks took 639 to 642 or 1,467 to 1,469 ns/op, and the standard deviation of the five log native/Chill ratios was 0.39 to 0.76 for text `serialize`, `serializeSameInstance`, and `buildAndSerialize`, against at most 0.16 for every other cell.
The measurements below back the rule that [ADR-0004](../adr/0004-serializer-benchmarks.md) adopts for these cells; they are diagnostics, not a parity result.

## Method

The diagnosis ran on a local workstation rather than on Compute Engine: an AMD Ryzen AI 9 HX 370 under WSL2 (kernel 6.18.54.1), Temurin 17.0.21, Flink 2.3.0, and protobuf-java 3.25.9, with the benchmark tree built by `just benchmark-build 2.3.0 3` from `aa32fb6`, or for the DynamicMessage lanes from `2c4fd58`, whose benchmark code differs only in the flags the suite passes to its forks.
Each JMH fork used the ADR-0004 long-run settings for one cell: five one-second warmups, five one-second measurements, one thread, and `-Xms1g -Xmx1g -XX:+UseG1GC`.
Forks ran one at a time outside the block order of `just benchmark-run`, so every fork's diagnostic output could be matched to its time.
HotSpot compilation logs (`-XX:+LogCompilation`), inlining output (`-XX:+PrintInlining`), and JFR recordings stayed outside the repository.

## Modes of the unmodified JVM

The local host reproduces the spread.
Of 48 native text `serialize` forks, 23 took 284 to 350 ns/op, 16 took 361 to 443 ns/op, and 9 took 861 to 1,087 ns/op; the ratio between the fast and slow clusters, about 2.9, is close to the 2.3 of the release-candidate VM.
Chill forks of the three cells landed in the slow mode in 11 of 12 forks, while native `serializeSameInstance` and `buildAndSerialize` forks stayed fast in all 8, and DynamicMessage `serialize` and `buildAndSerialize` forks in all 16, so the modes do not occur equally often for both lanes of a pair.

JFR was not usable for comparing the modes: none of 16 native `serialize` forks recorded with the `profile` settings reached the slow mode (305 to 410 ns/op), presumably because the recording shifts the compilation timing that selects it; that timing was not measured.

## Cause

The compilation logs of a fast and a slow fork differ in one inlining decision.
protobuf-java 3.x writes a string through `CodedOutputStream.writeStringNoTag` into `Utf8.UnsafeProcessor.encodeUtf8`, a 612-byte loop that reads one `String.charAt` per character; C2 compiles it on its own, because it is too large to inline into its callers.
`charAt` on a UTF-16 string calls `StringUTF16.charAt` and then the 48-byte `String.checkIndex`.
In the fast fork, the profile of that call site recorded 1,546 calls when C2 compiled `encodeUtf8`, so C2 inlined `checkIndex` as a hot call.
In the slow fork, the profile was still empty (`count='-1'`), so C2 applied the size limit for calls without a profile and left `checkIndex` out of line (`inline_fail reason='too big'`); the resulting code was 3,368 bytes instead of 3,688.
The compiled loop then calls `checkIndex` once per UTF-16 character, and the text record's name holds about 390 of them.
In each of 16 logged forks, C2 compiled `encodeUtf8` exactly twice, an on-stack-replacement version and then the standard one, and neither was invalidated afterwards, so a fork never recompiles and keeps its mode for all warmup and measurement iterations; a longer warmup cannot remove it.

The empty profile is a race between the C1 profiling of the UTF-16 path and the invocation count at which `encodeUtf8` is queued for C2.
The `String.charAt` profile is shared by every caller in the JVM, and at that point only about 1% of its recorded calls had taken the UTF-16 branch, so the call into `checkIndex` had been reached about 1,500 times or not yet profiled at all.
Both lanes of a pair write through the same runtime path, so each fork draws its mode independently of the serializer under test.

## Forcing each compilation

A `CompileCommand` that fixes the inlining decision confirms the cause.

| Lane and operation | Unmodified | `dontinline` `checkIndex` | `inline` `checkIndex` |
|---|---|---|---|
| Native `serialize` | 284 to 1,087 (48 forks) | 852 to 963 (6) | 290 to 347 (16) |
| Native `serializeSameInstance` | 255 to 282 (4) | 820 to 849 (2) | 255 to 259 (3) |
| Native `buildAndSerialize` | 523 to 542 (4) | 1,601 to 1,695 (2) | 534 to 543 (3) |
| Chill `serialize` | 975 to 1,040 (4) | 962 to 964 (2) | 420 to 442 (3) |
| Chill `serializeSameInstance` | 935 to 1,015 (4) | 940 to 980 (2) | 698 to 751 (3) |
| Chill `buildAndSerialize` | 676 to 1,693 (4) | 1,674 to 1,810 (2) | 671 to 741 (3) |
| Own-descriptor DynamicMessage `serialize` | 327 to 331 (4) | 885 to 896 (2) | 331 to 333 (3) |
| Own-descriptor DynamicMessage `buildAndSerialize` | 861 to 927 (4) | 1,859 to 1,889 (2) | 863 to 896 (3) |
| Foreign-descriptor DynamicMessage `serialize` | 329 to 346 (4) | 862 to 867 (2) | 327 to 332 (3) |
| Foreign-descriptor DynamicMessage `buildAndSerialize` | 857 to 914 (4) | 1,809 to 1,810 (2) | 851 to 872 (3) |

Times are ns/op per fork.
Forbidding the inlining puts every fork in the slow mode, and forcing it puts every fork in the fast mode, in every measured lane and operation; DynamicMessage `serializeSameInstance` was not measured.
Forcing it also removed the intermediate 361 to 443 ns/op cluster of native `serialize`: all 16 forks took at most 347 ns/op.

## Scope of the diagnosis

The diagnosis covers JDK 17 only, the JDK of the long runs.
On Temurin 21.0.13, `String.checkIndex(int, int)` is a 10-byte method that delegates to `Preconditions.checkIndex`, below the size limit for calls without a profile, so the race described above would not leave it out of line; no JDK 21 fork was measured.

## Why the Protobuf 4 profile does not show the modes

protobuf-java 4.36.2 selects a `Utf8.ServerProcessor` outside Android, whose `encodeUtf8` calls `String.getBytes(StandardCharsets.UTF_8)` and copies the result, instead of reading one `charAt` per character.
The per-character call that the race leaves out of line does not exist on that path.

## Consequence for the benchmark

Without a fixed compilation, the native/Chill interval of these cells measures which mode each lane's fork drew, and the lanes draw the slow mode at different rates.
[ADR-0004](../adr/0004-serializer-benchmarks.md) states the rule that follows from this record.
