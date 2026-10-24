---
title: "ADR-0004: Opt-in serializer benchmarks with explicit comparison contracts"
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

# ADR-0004: Opt-in serializer benchmarks with explicit comparison contracts

- Status: Accepted
- Date: 2026-09-12
- Evidence content gate added: 2026-09-18, [issue #41](https://github.com/flink-gcp/flink-datastream-protobuf/issues/41)
- Temporary GCE execution added: 2026-10-11, [issue #44](https://github.com/flink-gcp/flink-datastream-protobuf/issues/44)
- Paired native/Chill mode added: 2026-10-11, [issue #42](https://github.com/flink-gcp/flink-datastream-protobuf/issues/42)
- Variant placement calibrated: 2026-10-21, [issue #109](https://github.com/flink-gcp/flink-datastream-protobuf/issues/109)
- Pairwise comparison placement added: 2026-10-22, [issue #115](https://github.com/flink-gcp/flink-datastream-protobuf/issues/115)
- Issue: [#30](https://github.com/flink-gcp/flink-datastream-protobuf/issues/30)

## Decision

Keep the single production Maven module and add a `benchmarks` profile with test-scoped JMH, Avro, Thrift, Chill, and Jackson result-reader dependencies.
Compile handwritten benchmark sources and generated records into test outputs only.
Keep the compiler toolchain optional and generated Java under `target`.
Do not publish a benchmark classifier, add production dependencies, or change the native serializer API for measurement.

Measure Flink `TypeSerializer` operations directly with JMH 1.37.
Inspect serializer selection and equivalent decoded values before starting any timed region.
The separate processes for POJO, Tuple, Avro specific, Avro generic, and native Protobuf disable generic types.
Only the explicit Kryo comparison processes enable them.
Use Flink's resolved Kryo and exclude Chill's competing `kryo-shaded` dependency.
Record actual adapter classes, loaded artifacts, registration policy, and support failures.
A failed adapter is unsupported for that configuration; a missing build prerequisite or failed value assertion is a failed benchmark.

Check the unmodified JVM configuration first.
On Flink 1.20, a comparison process may additionally open `java.base/java.util` to unnamed modules, with that flag recorded in the inventory and JMH output.
Never add this flag to production verification or native benchmark processes.
If the configured adapter still fails, retain the unsupported result without substituting another serializer.

Use one deterministic logical corpus and document optional, default, null, collection, and framing semantics.
Separate serialize, deserialize, object copy, and serialized stream copy, including mutable reuse overloads.
Report identity copy as identity copy and do not describe deserialize-and-reserialize stream copy as byte transfer.
Report cached input and fresh record construction as different operations.

Before measuring, fix the practical tolerance at a native/Chill time ratio of 1.10.
Use five independent adjacent native/Chill blocks and a Student-t interval on their log time ratios, with four degrees of freedom.
A per-cell 95% interval entirely at or below 1.10 establishes the tolerance; one entirely above it misses; an interval crossing it is inconclusive.
This is a one-sided practical non-regression target, not symmetric statistical equivalence.
Do not infer parity from overlapping individual score intervals or average away workload-specific regressions.
The five-block model assumes approximately normal independent log ratios; the report must retain raw data and discuss drift, scheduling, and limited replication.
There is no simultaneous confidence claim across all workload/operation cells.

Run correctness and short JMH smoke checks over the supported Flink/JDK/Protobuf matrix in CI: on every push to main, the weekly schedule, and manual runs, and on pull requests that change sources, the evidence gate, or build inputs.
Do not impose timing thresholds on shared runners.
Record the representative long-run baseline on the maintainer's Mac with Flink 2.3.0, Temurin 17, and both supported Protobuf profiles.
A controlled comparison of native serializer changes may use a paired mode that runs only the native and Chill Protobuf lanes.
It keeps the correctness gates, fork, warmup, measurement, and alternating block order of the long run, so each variant has its own native/Chill intervals.
The paired mode does not produce cross-format results and does not replace a complete baseline.
This baseline describes that machine and does not establish whole-job throughput or saved-state compatibility.
Later development comparisons and release-candidate measurements run on temporary Compute Engine VMs, each running one Protobuf profile, with every compared variant measured in the same plan.
Both variants of every comparison run sequentially on one VM.
By default all variants of a profile share that VM; a plan that declares a profile's comparisons as pairs of variants runs each comparison on its own VM instead.
A variant in several declared comparisons, such as a shared baseline, is measured again on each of their VMs; those runs are never compared with each other, combined, or used to derive an undeclared comparison, and the collection records which result directories form each VM's comparisons.
A calibration plan may instead run one tree on several VMs of a profile, without declared comparisons; the [placement calibration](../validation/gce-placement-calibration.md) did so and, under a rule fixed before measuring, rejected comparing variants across VMs.
Between identical trees on different VMs, the per-block Chill control left host offsets of 1% to 4% that the narrow intervals of large-record cells resolve as differences.
Placing compared variants on separate VMs requires a new calibration that passes the same rule, such as one for another machine series; it is not a choice made per plan.
The operator process alone holds cloud credentials; benchmark VMs have no service account, and results pass the evidence gate before retention.
A VM result is not compared causally with the Mac baseline or with another plan's machine.

## Evidence retention

Use one Python standard-library content gate for local retention, staged benchmark archives, and CI artifact uploads.
The gate is development tooling outside the production jar and requires neither Maven nor the opt-in benchmark compiler.
Support the existing normalized `.tar.gz` archives and benchmark output directories with an explicit file inventory and reviewed JSON/property structures.
Reject unknown inputs, unsafe archive metadata and paths, nested archives, raw diagnostics, binary members, and known credential patterns.
JFR-derived events are restricted to allocation and execution samples, including an allowlist for nested fields.
Keep human content review and credential-free measurement environments: pattern scanning cannot prove the absence of arbitrary secrets.

Retain a checked copy with hashes of the exact archive and member bytes, and upload only that copy after the complete gate succeeds.
A failed benchmark may retain partial diagnostic evidence only when the whole partial directory passes the gate; content validation does not change the measurement failure.
Repository CI inspects committed benchmark archives without uploading those archives again.
The staged check reads Git blobs instead of working-tree paths, and the retention procedure requires it immediately before an evidence commit.
No Git hook or publication authorization is implied.
The [development procedure](../development.md#automated-content-gate) specifies formats, resource limits, scanning boundaries, and the shared local/CI commands.

## Source basis and alternatives

Reviewed Apache Flink's [serialization comparison](https://github.com/apache/flink-benchmarks/blob/bbccee66dd2ba5e08c22cf4c0c5b6e7d08712790/src/main/java/org/apache/flink/benchmark/SerializationFrameworkMiniBenchmarks.java), [additional representations](https://github.com/apache/flink-benchmarks/blob/bbccee66dd2ba5e08c22cf4c0c5b6e7d08712790/src/main/java/org/apache/flink/benchmark/full/SerializationFrameworkAllBenchmarks.java), and [build](https://github.com/apache/flink-benchmarks/blob/bbccee66dd2ba5e08c22cf4c0c5b6e7d08712790/pom.xml).
Those benchmarks run Flink jobs and combine several costs; their representation conversions and dependency pins are not this project's measurement contract.
Adopt JMH and explicit Flink type configuration, without copying the job harness or its generated Thrift files.

The [Flink 2.3 third-party serializer documentation](https://github.com/apache/flink/blob/release-2.3.0/docs/content/docs/dev/datastream/fault-tolerance/serialization/third_party_serializers.md) describes adapter registration but includes historical dependency examples.
Use Chill 0.10.0 and independently verify its [Protobuf](https://github.com/twitter/chill/blob/v0.10.0/chill-protobuf/src/main/java/com/twitter/chill/protobuf/ProtobufSerializer.java) and [Thrift](https://github.com/twitter/chill/blob/v0.10.0/chill-thrift/src/main/java/com/twitter/chill/thrift/TBaseSerializer.java) implementations against the pinned runtime graph.
The [2020 Flink article](https://flink.apache.org/2020/04/15/flink-serialization-tuning-vol.-1-choosing-your-serializer-if-you-can/) is historical motivation, not a current performance baseline.

A separate Maven module or a shaded executable would add publication and classpath complexity without improving the isolation already provided by test scope and separate JVMs.
An ad-hoc timing loop would not provide JMH's fork, warmup, or allocation instrumentation.

Running each variant on its own VM would make a plan's wall-clock time independent of its variant count, which the sequential placement multiplies by about 70 minutes of measurement per paired variant.
The calibration rejected it, and a baseline repeated on every VM was not measured as a control for the host offsets it found.
Pairwise placement keeps both sides of each comparison on one VM, as the sequential placement does, and limits each VM to two variants.
It costs the repeated variants: three adjacent comparisons of four variants take six variant runs on three VMs instead of four on one, but the longest VM measures two variants instead of four.
Packing several comparisons onto fewer VMs would trade that wall-clock time back for VM count and is left to a later change that needs it.
