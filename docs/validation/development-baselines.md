---
title: "Development artifact bundles"
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

# Development artifact bundles

Issue [#40](https://github.com/flink-gcp/flink-datastream-protobuf/issues/40) prepares fixed development inputs for the state and API checks in #16 and #17.
The capture and reader tooling is implemented separately from the final v0.1.0 milestone capture.
Complete #12, #19, and #41 before selecting the final integrated source revision.
The tooling always labels its output `development-tooling-baseline` with `milestone.final=false`; a successful rehearsal does not close #40 or #6.
No 0.x coordinates are published to Maven Central.

## Capture

Run from the repository root of a clean, committed dedicated worktree on JDK 17.
Ignored files under `src` are rejected because Maven could otherwise package inputs absent from the retained source revision.
The destination's parent must exist, and the destination must be new and outside the repository.
Each invocation builds one library and its compiled consumers once, then captures state with those bytes on its supported Flink runtimes.

```sh
mise x -- just baseline-capture flink2 3 /tmp/baseline-flink2-pb3
mise x -- just baseline-capture flink2 4 /tmp/baseline-flink2-pb4
mise x -- just baseline-capture flink1 3 /tmp/baseline-flink1-pb3
mise x -- just baseline-capture flink1 4 /tmp/baseline-flink1-pb4
```

The two artifact lines and two paired Protobuf profiles produce four independent bundles.
The 2.x jar is compiled at 2.2.1 and also used unchanged at 2.3.0; the LTS jar is compiled at 1.20.4.
Both retain the actual development POM version, currently `0.1.0-SNAPSHOT`, with the adapter line identified separately in the manifest.
They are not renamed to imply published release coordinates.
Capture keeps the source archive and revision, effective Maven model, exact command arguments, resolved dependency tree and jars, JDK identity, and protoc version and executable checksum.
The application jars contain the original and changed schemas; neither application is on the parent classpath of the isolated recovery jobs.

The source archive includes the schemas/imports and the reader and consumer sources.
The companion `tools.jar` contains compiled test instruments, test messages, and test resources including the existing fixed fixtures; it is never attached as a Maven publication artifact.
It retains the compiled explicit type construction, builder, factory, registered transport, and LTS legacy-entry-point checks for subsequent API investigations.
The explicit consumer, registration transport check, and LTS legacy check run in separate JVMs from the state writer and reader.
LTS results include an additional `legacy-entry-point` case, giving 13 cases on LTS and 12 on 2.x.
This is a concrete consumer inventory, not an exhaustive public-API compatibility checker; that gate belongs to #17.

## Inspect and restore

```sh
mise x -- just baseline-check /tmp/baseline-flink2-pb3
mise x -- just baseline-read /tmp/baseline-flink2-pb3 2.2.1 /tmp/read-pb3-floor-17
mise x -- just baseline-read /tmp/baseline-flink2-pb3 2.3.0 /tmp/read-pb3-ceiling-17
mise x java@temurin-21 just -- just baseline-read /tmp/baseline-flink2-pb3 2.3.0 /tmp/read-pb3-ceiling-21
```

`baseline-check` reads the full file inventory and SHA-256 checksums without Maven, network access, or compilation.
`baseline-read` also starts the retained compiled code with the bundle's dependency jars, validates the loaded library and runtime, and writes results to a new directory outside the bundle.
The bundle is checked again after execution, including after a failed reader.
The reader does not use `target/classes`, regenerate messages, or replace a missing fixture.
To run without a checkout, use `java -cp <bundle>/tools.jar io.github.flink.gcp.protobuf.BaselineTool check <bundle>` or the corresponding `read <bundle> <flink-version> <new-results>` arguments.
Use trusted bundles: the reader executes their Java code, and the internal checksum inventory establishes integrity against that inventory, not publisher authenticity.

Snapshot fixtures contain the full descriptor closure and a framed representative value, with default and raised serializer settings.
Runtime fixtures contain canonical savepoints and full, nonincremental checkpoints for HashMap and RocksDB, with the existing baseline and raised test budgets.
State manifests record settings, scalar-keyed ValueState/MapState and operator ListState identities, UIDs, parallelism, the input cursor, expected counts, and unknown fields.
The retained application sources define the expected nested runtime values.
Capture archives state before closing the writer, deletes the writer's original state storage, and restores the relocated archives before checking continued processing.
It also checks checkpoint failure/replay and savepoint rejection for a schema change and a decreased size limit.
Incomplete archives cannot pass by finding files in the original writer directories.

CI captures each of the four bundles on JDK 17, moves the build output away, and reads the retained inputs on every claimed combination.
Flink 2.2.1 and 2.3.0 run on JDK 17 and 21; Flink 1.20.4 runs on JDK 17.
Each saved-state read uses its exact Flink version and Protobuf profile.
These fixtures use aligned checkpoints and local filesystem storage without entropy injection.
Cross-Flink state upgrades, cross-Protobuf restore, backend migration, rescaling, unaligned or incremental checkpoint portability, and universal development-version compatibility are outside this evidence.
The current companion jars include platform-specific runtime dependencies; recreate a rehearsal on another operating system or architecture rather than assuming portability of native dependencies.

## Retention and final capture

A failed capture leaves an incomplete destination without a successful checksum inventory; retry in a new directory.
Never repair historical provenance or replace files inside a completed bundle.
Ordinary verification continues to read the existing checked-in snapshot/savepoint fixtures and does not regenerate them.
The new bundles are separate inputs and do not supersede those earlier format baselines.

Keep bundles in an access-controlled external directory, backed up outside temporary build and worktree cleanup locations when retaining them for future work.
Do not use an expiring CI artifact or `/tmp` as the sole long-term copy.
CI rehearsals stay in runner temporary storage and are not uploaded by this workflow.
Long-term storage placement and the final milestone inventory remain part of #40 after its prerequisites are integrated.

Command records and build/runtime logs preserve exact local paths and may contain machine-identifying information.
Before copying any bundle to shared storage, inspect the exact inventory, archive members, source content, commands, and logs; keep credentials and raw JVM diagnostics out of capture processes.
The checksum checker is not the benchmark content gate in #41 and does not prove the absence of arbitrary secrets.
Retain original bytes privately if a separately sanitized derivative is needed, and identify that derivative with its own inventory.

For #16, consume the retained schema descriptors, snapshot bytes, state archives, expected-value manifests, and application identities with a separately built new-schema application.
For #17, retain both library jars and already-compiled consumer inputs per profile, and select a candidate library on an explicitly reviewed classpath without recompiling the old consumer.
Neither follow-up should rebuild a newer working tree and label it the old writer.
