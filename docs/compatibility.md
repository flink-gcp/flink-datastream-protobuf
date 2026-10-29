---
title: State compatibility
weight: 30
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

# Protobuf updates and state compatibility

Adding a field can preserve Protobuf binary compatibility, and the current development library restores state across that change.
It accepts unchanged schemas and the [supported schema changes](#schema-changes), and it rejects other changes to the declarations that saved values reach when the job restores its state.
No library release has been published yet.
The 0.x versions denote development milestones; 1.0.0 is the planned first Maven Central release.
Milestone upgrade checks use fixed development artifacts and fixtures with exact source/checksum provenance.

## Development builds before 1.0.0

The current source version is `0.2.0-SNAPSHOT`, a development build that restores supported schema changes.
The 0.1.0 milestone accepted unchanged schemas only.
It also accepted payload types whose graphs reach proto2 messages, which current builds reject.
Restore compares the messages reached through fields that both schemas keep, starting at the root, together with their enclosing messages and declaring files.
State that 0.1.0 wrote for such a type is therefore restored only when the new schema has a proto3 root and no such retained path still reaches a message that the saved schema declares in a proto2 file; removing a field and reserving its number cuts the paths through it, and the removed values remain unknown fields.
Moving a message on a retained path, or a proto2 root, to proto3 changes its declaring file's syntax, which restore rejects, so an application that keeps such a path must start with fresh state or replay its input.
The [retained 0.1.0 baseline](validation/development-baselines.md#retained-v010-baseline) uses proto3 types only.
Compatibility between arbitrary development builds is not implied.
Compatibility with the [retained 0.1.0 baseline](validation/development-baselines.md#retained-v010-baseline) is established only by its state-restore tests and API checks; the [upgrade guide from 0.1.0](upgrade.md) lists their results together with the API changes and intentional breaks of 0.2.0, and [verified coverage](#verified-coverage-and-upgrade-procedure) describes the state that the current tests restore.
The 0.x policy permits changes to APIs, defaults, runtime requirements, and saved-state compatibility.
Each development milestone must document its actual breaks, supported upgrade paths, and migration or fresh-state/replay requirements before completion.
Retain the exact source revision and artifact checksums alongside schemas and state; a SNAPSHOT version alone does not identify their writer.
The [development bundle tooling](validation/development-baselines.md) retains attributable inputs for these checks.
Historical serializer-format fixtures do not establish arbitrary full-job upgrade compatibility.

[Version 1.0.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13) is the planned stable API and forward-restore boundary, without implying compatibility with every earlier development artifact.
Its preparation must disclose unsupported development-to-release upgrade paths and follow the [quickstart's Maven Central transition](quickstart.md#switch-to-maven-central-at-100).

## Three compatibility checks

An application update needs three separate checks:

| Check | Question |
|---|---|
| Protobuf binary format | Can the new message definition interpret bytes written with the old definition? |
| Generated Java code and runtime | Can the generated classes execute with the selected `protobuf-java` dependency? |
| Flink managed state | Does this library accept the saved serializer metadata, and can the updated job restore and continue processing correctly? |

A successful binary parse answers only the first question.
Application logic must also handle the restored values, including absent fields and defaults.

## Schema changes

For a proto3 message, adding a field with a fresh number leaves old data readable by the new generated parser.
For example, consider adding `string label = 2;` while retaining the existing field:

```protobuf
syntax = "proto3";
message Event {
  int64 id = 1;
  string label = 2;
}
```

Old data has no `label`, so its getter returns the default empty string.
An old parser treats the new field as unknown; binary message parsing and serialization preserve unknown fields, while JSON conversion or copying only known fields can lose them.
These are Protobuf binary-format properties, not this library's state-restore policy.
See the [Protobuf schema-update rules](https://protobuf.dev/programming-guides/proto3/#updating).

The state-restore results below assume supported snapshot formats, unchanged Java/message names, and nondecreasing size/depth limits.
Deterministic-writing mode and large-payload aliasing may change in either direction.
They describe the forward direction: the saved state was written with the old schema and is restored by an application containing only the new generated classes.

| Schema change | Protobuf binary-format rule | Current library state restore |
|---|---|---|
| Complete normalized schema unchanged | Same schema | Accepted |
| Add a proto3 field with a fresh number | Wire-safe; new code must handle the field being absent in old data | Accepted when neither the number nor the name is a saved field or reservation |
| Remove a field and reserve its number | Wire-safe when the number is not reused | Accepted; a removal without reserving the number is rejected |
| Add an enum value | Wire-safe; application source code may still need changes | Accepted when the name and number are new and unreserved, the number is not an alias of a saved value, the first value stays first, and saved values keep their order |
| Remove an enum value | Saved values may carry the removed number | Rejected, even when the number is reserved |
| Renumber an existing field | Wire-unsafe | Rejected |
| Change `int32` to `int64` | Conditionally safe; values written later can exceed an old reader's range | Rejected |

The binary classifications follow the [proto3 guide](https://protobuf.dev/programming-guides/proto3/#updating); ProtoJSON has different rules.
The library applies the [directional evaluator](adr/0001-native-protobuf-type-integration.md#supported-schema-evolution-in-020) to the complete saved descriptors.
It compares the messages and enums that saved bytes can reach from the root message through retained fields, together with their enclosing messages and declaring files.
Within those declarations, every difference other than the supported changes above and added reservations or oneofs is rejected, including options, defaults, presence, oneof membership, and the order of retained fields or enum values.
Declarations that saved bytes cannot reach, such as unrelated messages in the same file, do not affect the decision.
Descriptor normalization removes only source-location/comment metadata (`SourceCodeInfo`), so runtime or protoc updates can still change a reached declaration without an edit to the application's own `.proto` file.
The Protobuf version number itself is not a field in the serializer compatibility decision.

Restoring with the older schema is a different direction, and it is often rejected: the old schema removes the added field without reserving its number, or removes the added enum value.
A rejected restore fails the job with a message that names the first incompatibility, for example:

```text
Cannot restore Protobuf serializer for io.example.Events$Event (example.Event): supply a supported generated class that can read the saved schema through Flink's user-code classloader. Cause: Generated class cannot read the saved Protobuf schema: Field 2 (count) of example.Event changes type from INT32 to SINT32
```

A decreased size or depth limit fails with Flink's `StateMigrationException`, which does not name the limit.
The [schema-evolution test](../src/test/java/io/github/flink/gcp/protobuf/ProtobufSchemaEvolutionITCase.java) verifies the accepted and rejected changes in separate jobs for ValueState, MapState values, and operator ListState on HashMap and RocksDB.

## Java runtime version updates

For existing generated Java code, upstream defines the following runtime compatibility window.
Version numbers here refer to Java Protobuf releases, independently of the `.proto` syntax, which must be `proto3` for this library.

| Runtime update with existing generated code | Upstream guarantee |
|---|---|
| Newer patch or minor within the same major | Supported |
| Major V generated code running on a V+1 runtime, such as Java 3 to 4 | Supported by the sliding compatibility window |
| Major V generated code running on V+2 or later | Unsupported |
| Generated code newer than its runtime | Unsupported, including patch-level mismatches |

Security fixes can require updating generated code and runtime together, overriding the usual compatibility window.
See the [upstream runtime guarantee](https://protobuf.dev/support/cross-version-runtime-guarantee/).
This project's regular CI pairs 4.36.2 generated code with runtime 4.36.2, and 3.25.9 generated code with runtime 3.25.9.
Each artifact line's library jar is built with 4.36.2, and CI runs that unchanged jar with both pairs, so one jar serves applications on either major.
Mixed generated-code/runtime pairs, including 3.x generated code with a 4.x runtime, are outside that CI matrix even when upstream supports them.
This table does not establish Flink savepoint compatibility.
Updating only `protobuf-java` and updating protoc plus regenerating application classes are different changes; check the resulting descriptors and the selected generated-code/runtime pair in either case.

## Verified coverage and upgrade procedure

The [serializer-format fixtures](../src/test/resources/snapshots/v1/README.md) written with Protobuf 3.25.8 and 4.33.6 are read in both profiles for the same test schema.
The [runtime suite](validation/runtime-recovery.md) restores complete savepoints within each pinned Flink/Protobuf combination.
It does not yet verify a complete savepoint written by one Protobuf version and restored by another, including patch and minor updates.
Separate fixture directories record each tested environment; they do not indicate that cross-version restore was attempted and failed.

The schema-evolution tests write and restore state with the same development build.
They check the restored values, continued processing, and a second restore from a savepoint that the new application wrote.
A field removed with a reservation remains in restored values as an unknown field, and the new application's savepoints keep it; the test reads the value back with the original schema.
The [retained-baseline upgrade check](validation/development-baselines.md#restore-retained-state-with-the-current-library) restores savepoints and checkpoints written by the [retained 0.1.0 artifact](validation/development-baselines.md#retained-v010-baseline) with the current library, unchanged and with the field addition and the reserved field removal, and requires the type change to be rejected.
It runs for each retained bundle at the Flink and Protobuf versions that wrote and read it, on HashMap and RocksDB.
The retained 0.1.0 runtime schema declares no enum, so enum-value addition is verified only with state written by 0.2.0 development builds.

Before deploying a dependency update, validate the actual old-writer/new-reader combination with the application's schemas, state, and runtime settings:

1. Write a savepoint with the old application and retain its exact application/library jars, schemas, settings, operator UIDs, state names, and runtime versions.
2. Compare the documented API, default, runtime, and state changes before replacing a development build or dependency.
3. Restore the savepoint with the updated application and check saved values, input positions, and continued processing.
4. Write another savepoint after processing with the updated application and verify restoration from that state too.

For a validated compatible update, stop the old job with a savepoint and resume the updated job from it.
See [Flink's savepoint operations](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/savepoints/#stopping-a-job-with-savepoint).
An intermediate run without restoring state does not convert old state or make an incompatible schema compatible.
If restoration is rejected, retain the old deployment or explicitly choose a documented migration or fresh-state/replay procedure.
Starting fresh requires an explicit decision about rebuilding state and replaying input; the library has no state-migration step and provides no migration path for rejected schema changes.
The [development guide](development.md#test-resources-and-version-updates) contains the capture commands and dependency-update checks.

## Preparing schema changes

The evaluator stays conservative about conditional numeric conversions and other unsupported changes; parsing success alone does not prove that values retain their meaning.

Reserve the number of every removed field, for example `reserved 2;`.
The evaluator rejects a removal without a reservation, and it rejects a later change that removes a reservation to reuse the number.
Protobuf parsing keeps old values of a removed field as unknown fields, so an unreserved number could later be parsed as an unrelated declaration.
No descriptor comparison can detect a field that keeps its number, name, and type but changes its meaning, so keep a removed number reserved instead of reusing it.
Reserving does not make removals of enum values acceptable; the evaluator rejects them.
Before adding a field, also check that saved values cannot carry an unknown field with its number, for example from input written with a newer schema; the new parser would read that retained value as the added field, and descriptors cannot reveal it.
The evaluator checks only messages that saved bytes reach through retained fields, so it does not check message types packed in an `Any` payload; reserve removed numbers in those types as well.
