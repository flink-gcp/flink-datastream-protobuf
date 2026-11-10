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
The JDK 21 forks and the forks with the whole UTF-16 path forced ran on Temurin 21.0.13 and 17.0.21 with the tree built from `d619724`; each fork ran through the JMH command line outside `BenchmarkSuite`, so it received only the `CompileCommand` flags named below and not the suite's own.
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
Forbidding the inlining puts every fork in the slow mode in every measured lane and operation; DynamicMessage `serializeSameInstance` was not measured.
Forcing it removed the slow mode in every lane and operation except Chill `serializeSameInstance`, whose 698 to 751 ns/op is the [second Chill mode](#a-second-mode-in-the-chill-lane) described below, not the fast mode.
Forcing it also removed the intermediate 361 to 443 ns/op cluster of native `serialize`: all 16 forks took at most 347 ns/op.

## The same race on JDK 21

The first version of the rule forced only `String.checkIndex`, and the modes remained on Temurin 21.0.13.
There, `String.checkIndex` is a 10-byte method that delegates to the `Preconditions.checkIndex` intrinsic and is inlined without a profile.
The compilation logs of a slow fork instead leave `StringUTF16.charAt` out of line with `inline_fail reason='low call site frequency'`, its call site having recorded 773 calls when C2 compiled `encodeUtf8`, against 1,546 in a fast fork, where the whole path was inlined as hot; with `charAt` alone forced, `StringUTF16.checkIndex` stays out of line instead.
The method and the logged reason differ from JDK 17, but the mode again depends on how much of the UTF-16 path's profile exists when C2 compiles `encodeUtf8`.
Without forced inlining, all 16 native `serialize` forks took 276 to 343 ns/op; forbidding the inlining of `String.checkIndex` put all 3 forks in the slow mode (936 to 957 ns/op).

| JDK 21 Chill `serialize`, forced inlining | Forks | ns/op |
|---|---|---|
| None | 8 | 7 forks 887 to 959, 1 fork 472 |
| `String::checkIndex` | 4 | 3 forks 882 to 914, 1 fork 443 |
| `StringUTF16::charAt` | 6 | 4 forks 1,062 to 1,121, 2 forks 454 to 490 |
| `StringUTF16::charAt` and `String::checkIndex` | 6 | 3 forks 1,016 to 1,063, 3 forks 434 to 479 |
| The whole path (rule below) | 8 | 459 to 563 |

The rule therefore forces the inlining of the four methods that the UTF-16 branch of `String.charAt` calls below `charAt` itself and that are not intrinsics: `StringUTF16.charAt`, `StringUTF16.checkIndex`, `StringUTF16.length`, and `String.checkIndex`.
`String.charAt` and its `isLatin1` check stay subject to ordinary inlining, and the `getChar` and, on JDK 21, `Preconditions.checkIndex` intrinsics need no flag.
No logged fork left the 5-byte `StringUTF16.length` out of line; the rule names it so that no method of the path depends on its profile.

## Forcing the whole UTF-16 path

With all four methods forced, three forks per cell took the following times.

| Lane and operation | JDK 17 | JDK 21 |
|---|---|---|
| Native `serialize` | 286 to 289 | 302 to 307 |
| Native `serializeSameInstance` | 250 to 271 | 274 to 276 |
| Native `buildAndSerialize` | 537 to 541 | 501 to 504 |
| Chill `serialize` | 409 to 425 | 459 to 563 (8 forks) |
| Chill `serializeSameInstance` | 691, 696, and 334 | 336 to 338 |
| Chill `buildAndSerialize` | 649 to 668 | 635 to 638 |
| Own-descriptor DynamicMessage `serialize` | 330 to 331 | 337 to 365 |
| Own-descriptor DynamicMessage `serializeSameInstance` | 268 to 290 | 276 to 302 |
| Own-descriptor DynamicMessage `buildAndSerialize` | 869 to 931 | 708 to 713 |

Times are ns/op per fork; the foreign-descriptor DynamicMessage lane was not measured with the whole path forced.
Every cell has one mode except JDK 17 Chill `serializeSameInstance`.

## A second mode in the Chill lane

JDK 17 Chill `serializeSameInstance` keeps two modes with the whole path forced.
A further 8 forks with compilation logs took 684 to 725 ns/op in 7 forks and 354 ns/op in one, so the run above was not an outlier.
In these forks the UTF-16 path is inlined in the same way, so this mode has a different cause.
In the logs of a slow fork, C2 compiled `encodeUtf8` with a profile in which a branch leaving its ASCII-prefix loop had never been taken, and replaced it with an uncommon trap; the logged fast fork has no such trap.
No trap was hit and no method was recompiled afterwards, and whether that branch accounts for the difference was not tested; the cause of this mode is not identified.
No other lane, no other operation, and no JDK 21 fork showed it, but only 3 forks were measured per JDK 21 cell other than Chill `serialize`, which had 8.

## Modes occur unequally in the two lanes

The lanes of a pair do not draw the slow modes at the same rate.
Without forced inlining, the Chill lane landed in the slow mode in 11 of 12 JDK 17 forks and in 7 of 8 JDK 21 `serialize` forks, while native `serialize` did so in 9 of 48 JDK 17 forks and in none of 16 JDK 21 forks.
The second mode above occurs only in the Chill lane.

These rates come from benchmark JVMs that run nothing but one cell.
Whether a Flink TaskManager, where other code also reads UTF-16 strings and may profile the `charAt` path earlier, reaches the slow modes as often was not measured.

## Scope of the diagnosis

The diagnosis covers Temurin 17.0.21 and 21.0.13 on one local host and protobuf-java 3.25.9.

## Why the Protobuf 4 profile does not show the modes

protobuf-java 4.36.2 selects a `Utf8.ServerProcessor` outside Android, whose `encodeUtf8` calls `String.getBytes(StandardCharsets.UTF_8)` and copies the result, instead of reading one `charAt` per character.
The per-character call that the race leaves out of line does not exist on that path.

## Consequence for the benchmark

Without a fixed compilation, the native/Chill interval of these cells measures which mode each lane's fork drew, and the lanes draw the slow mode at different rates.
[ADR-0004](../adr/0004-serializer-benchmarks.md) states the rule that follows from this record.
