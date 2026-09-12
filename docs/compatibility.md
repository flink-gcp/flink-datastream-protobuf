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

Adding a field can preserve Protobuf binary compatibility, but the current library rejects restoring state with that changed schema.
The implemented 0.1.0 contract accepts unchanged schemas; supported schema evolution is planned for 0.2.0.
No library release has been published yet.

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

The current state-restore results below assume supported snapshot formats, unchanged Java/message names, and nondecreasing size/depth limits.

| Schema change | Protobuf binary-format rule | Current library state restore |
|---|---|---|
| Complete normalized schema unchanged | Same schema | Accepted by the serializer compatibility check |
| Add a proto3 field with a fresh number | Wire-safe; new code must handle the field being absent in old data | Rejected because the saved and new descriptors differ |
| Remove a field and reserve its number | Wire-safe when the number is not reused | Rejected |
| Add an enum value | Wire-safe; application source code may still need changes | Rejected |
| Renumber an existing field | Wire-unsafe | Rejected |
| Change `int32` to `int64` | Conditionally safe; values written later can exceed an old reader's range | Rejected |

The binary classifications follow the [proto3 guide](https://protobuf.dev/programming-guides/proto3/#updating); ProtoJSON has different rules.
The current library compares the complete normalized descriptor content, including transitive imports, options, and declarations elsewhere in those files.
Only source-location/comment metadata (`SourceCodeInfo`) is removed; declaration order and other metadata changes can therefore cause rejection even when payload bytes remain readable.
Runtime or protoc updates can affect these descriptors without an edit to the application's own `.proto` file.
The Protobuf version number itself is not a field in the serializer compatibility decision.

This restriction is deliberate in the [0.1.0 contract](adr/0001-native-protobuf-type-integration.md#unchanged-schema-restore-in-010).
The [runtime recovery test](../src/test/java/io/github/flink/gcp/protobuf/ProtobufRecoveryITCase.java) verifies rejection of a field addition for ValueState, MapState values, and operator ListState on HashMap and RocksDB.

## Java runtime version updates

For existing generated Java code, upstream defines the following runtime compatibility window.
Version numbers here refer to Java Protobuf releases, independently of `proto2`/`proto3` syntax.

| Runtime update with existing generated code | Upstream guarantee |
|---|---|
| Newer patch or minor within the same major | Supported |
| Major V generated code running on a V+1 runtime, such as Java 3 to 4 | Supported by the sliding compatibility window |
| Major V generated code running on V+2 or later | Unsupported |
| Generated code newer than its runtime | Unsupported, including patch-level mismatches |

Security fixes can require updating generated code and runtime together, overriding the usual compatibility window.
See the [upstream runtime guarantee](https://protobuf.dev/support/cross-version-runtime-guarantee/).
This project's regular CI pairs 3.25.9 generated code with runtime 3.25.9, and 4.33.6 generated code with runtime 4.33.6.
Mixed pairs, including 3.x generated code with a 4.x runtime, are outside that CI matrix even when upstream supports them.
This table does not establish Flink savepoint compatibility.
Updating only `protobuf-java` and updating protoc plus regenerating application classes are different changes; check the resulting descriptors and the selected generated-code/runtime pair in either case.

## Verified coverage and upgrade procedure

The [serializer-format fixtures](../src/test/resources/snapshots/v1/README.md) written with Protobuf 3.25.8 and 4.33.6 are read in both profiles for the same test schema.
The [runtime suite](validation/runtime-recovery.md) restores complete savepoints within each pinned Flink/Protobuf combination.
It does not yet verify a complete savepoint written by one Protobuf version and restored by another, including patch and minor updates.
Separate fixture directories record each tested environment; they do not indicate that cross-version restore was attempted and failed.

Before deploying a dependency update, validate the actual old-writer/new-reader combination with the application's schemas, state, and runtime settings:

1. Write a savepoint with the old application, retaining its operator UIDs and state names.
2. Restore it with the updated application and check saved values, input positions, and continued processing.
3. Write another savepoint after processing with the updated application and verify restoration from that state too.

For a validated compatible update, stop the old job with a savepoint and resume the updated job from it.
See [Flink's savepoint operations](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/savepoints/#stopping-a-job-with-savepoint).
An intermediate run without restoring state does not convert old state or make an incompatible schema compatible.
Starting fresh requires an explicit decision about rebuilding state and replaying input; the current library provides no changed-schema migration path.
The [development guide](development.md#test-resources-and-version-updates) contains the capture commands and dependency-update checks.

## Planned schema evolution

The [0.2.0 design](adr/0001-native-protobuf-type-integration.md#supported-schema-evolution-in-020) adds a directional evaluator for supported field additions, removals/reservations, and enum additions, including referenced types.
Its evaluator is tracked in [#15](https://github.com/flink-gcp/flink-datastream-protobuf/issues/15), and restoration from published 0.1.0 state in [#16](https://github.com/flink-gcp/flink-datastream-protobuf/issues/16).
Those capabilities are planned and are not implemented by the current unchanged-schema tests.
The design remains conservative about conditional numeric conversions and other unsupported changes; parsing success alone does not prove that values retain their meaning.
