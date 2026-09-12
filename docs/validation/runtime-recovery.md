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

# Runtime state recovery

For resource locations, build/test invocation, capture, and dependency updates, see [Test resources and version updates](../development.md#test-resources-and-version-updates).
This page records the recovery scenarios and their measured scope.

`ProtobufRecoveryITCase` executes the production serializer in a two-TaskManager MiniCluster with parallelism 2 and max parallelism 16.
It uses stable integer stream keys, string MapState user keys, and generated messages as values in ValueState, MapState, and operator ListState.
HashMap and RocksDB backends run the same cases.
The ValueState implementation is checked at runtime to distinguish the selected backends.

The source stores its input cursor in Flink checkpoint state.
Filesystem controls release input and request one failure; they do not supply restored values or a replacement input cursor.
The checkpoint test saves eight records, processes four unsaved updates, and fails the source.
After restart, the test checks restored operator values, replay from the saved input cursor, and exact per-key, per-map-key, and per-subtask counts through subsequent input.
The checkpoint trigger completes before the unsaved updates and failure are permitted.

Separate jobs restore canonical savepoints with fixed operator UIDs and state names.
The tests accept unchanged settings, increases in either or both reader limits, and both deterministic-mode transitions.
Increased-limit jobs process values exceeding the old size or nesting budget, emit another savepoint, and restore that newly written state.
Independent ValueState, MapState, and operator-state cases reject each limit decrease, mixed increase/decrease transitions, and decreases after an earlier increase and new snapshot emission.
They also reject a wire-compatible field addition while retaining the Java class name and Protobuf full name.
The [compatibility guide](../compatibility.md#schema-changes) explains why Protobuf binary compatibility does not make that addition restorable under the current library contract.
Rejection assertions inspect the failure cause for incompatible state serializers or a changed Protobuf schema.

For `ProtobufRecoveryITCase` and `ProtobufSavepointFixtureITCase`, the application and generated classes live in separate test jars that are absent from the test runner's parent classpath.
Each job is constructed with a fresh application loader and submitted with its application jar attached.
The application checks that recovered messages belong to its task-side application loader and retain nested values and unknown fields.
The test runner checks that neither the application entry point nor its generated message can load through the parent.
Generic types are disabled, no module-opening JVM flags are added, and the existing fresh-JVM missing-registration controls remain required.
The five production transport shapes additionally retain the native snapshot implementation inside their composite serializers.

## Fixtures and compatibility boundaries

`ProtobufSavepointFixtureITCase` restores the [fixed development savepoints](../../src/test/resources/savepoints/v1/README.md) and continues processing.
The capture mode removes the original savepoint and the writer's checkpoint storage before testing the relocated archive, so missing external state files cannot be hidden by those directories.
Fixture manifests record the exact writer jar hash, base revision and source hashes, application jar hash, schema and descriptor hashes, settings, runtime versions, state identity, and expected values.
They are development evidence, not state written by a published release.

The suites run in the existing Flink 2.2.1/2.3.0 JDK 17/21 and Flink 1.20.4 JDK 17 matrix, with paired Protobuf 3.25.9 and 4.33.6 generated code and runtime.
The Protobuf 3.25.9 update replaces the 12 superseded 3.25.8 development savepoints with fixtures for the current matrix.
Savepoint selection uses the exact runtime version, so this update does not establish restoration of 3.25.8 savepoints on 3.25.9.
The snapshot-format fixtures written with 3.25.8 remain unchanged and are read by the current profiles.
The unchanged-jar lane retains the floor's application jars, descriptors, and compiled application classes and verifies their hashes after execution at the ceiling.
Application jars are built before tests and never recompiled by the tests or attached as publication artifacts.

Each saved-state scenario keeps its Flink version, Protobuf profile, backend, parallelism, and state identity fixed.
The suite does not establish backend migration, rescaling, cross-Flink state upgrades, cross-Protobuf restore, or cross-process message-key hashing.
`CommonTypesRecoveryITCase` adds [Well-Known Type and OpenTelemetry acceptance](../common-types.md#verification-boundaries), using the same MiniCluster controls with a separate job and values on the ordinary test classpath.
It verifies direct Struct ValueState, direct AnyValue MapState values, and an application envelope in operator ListState on both backends.
Published-artifact capture, validation of both artifact lines, and retention for future direct/sequential library upgrades remain in #13/#6.
