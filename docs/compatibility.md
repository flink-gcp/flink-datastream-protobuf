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

## Development builds before 1.0.0

The current source version is `0.3.0-SNAPSHOT`, a development build that restores supported schema changes.
The 0.x versions are development milestones, not releases, and their APIs, defaults, runtime requirements, and state formats are not compatibility contracts.
Retain the exact source revision and artifact checksums alongside schemas and state; a SNAPSHOT version alone does not identify their writer.

[Version 1.0.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13) is the planned first release and the start of the [stable API and forward-restore contract](#compatibility-from-100).
Its preparation follows the [quickstart's Maven Central transition](quickstart.md#switch-to-maven-central-at-100).
Releases after 1.0.0 are to be checked against the state, compiled consumers, and API retained from published releases with the [development bundle tooling](validation/development-baselines.md).

Version 1.0.0 restarts the persisted formats at version 1 under snapshot classes that the 0.1.0, 0.2.0, and 0.3.0 milestones never wrote, and it removes their snapshot classes.
It therefore does not read state written by those milestones: Flink fails the restore because the saved snapshot class is absent.
There is no migration or upgrade guide from a 0.x milestone or a development SNAPSHOT build to 1.0.0.
An application that ran one starts 1.0.0 with fresh state or replays its input, and decides how the rebuilt state and any repeated output affect its data.
Development APIs carry no guarantee either; compile the application against 1.0.0 and follow its [usage guide](usage.md).

## Compatibility from 1.0.0

From 1.0.0, each release keeps these contracts within its major version:

- The application-facing API annotated `@Public` or `@PublicEvolving` remains source and binary compatible; the [API compatibility gate](development.md#api-compatibility-gate) compares each candidate with the earlier release.
- Configuration defaults keep their meaning.
- A later release restores state that an earlier release of the same major version wrote, for unchanged schemas and the [supported schema changes](#schema-changes), with the same artifact line, Flink minor, and Protobuf runtime and generated code; each release checks this against state retained from the earlier published release.

Downgrades, restores across Flink minors or artifact lines, and restores across changed built-in descriptors are not part of the contract.
A change that breaks one of these contracts requires a new major version with migration documentation.

## Artifact lines and version order

Each release publishes two versions of `io.github.flink-gcp:flink-datastream-protobuf` from one source revision:

| Version | Flink runtime |
|---|---|
| `X.Y.Z` | The current and previous Flink 2.x minors |
| `X.Y.Z-1.20` | Flink 1.20 LTS |

Choose the line that matches the application's Flink runtime and stay on it; each line's jar is compiled for its own runtime.
Both lines share one artifactId, and Maven orders their versions alternately, `1.0.0 < 1.0.0-1.20 < 1.0.1 < 1.0.1-1.20`, so Maven Central and update tools report each release's `-1.20` build as the latest version.

Dependabot treats `-1.20` as an ordinary release version and does not keep either line.
Its version updates move to the highest version, which is each release's `-1.20` build: correct on the LTS line, wrong on the 2.x line.
Its security updates move to the lowest version that fixes the advisory, which can be the release's `X.Y.Z` on the LTS line.
Check that every proposal keeps the line, and list the other line's versions in an ignore condition, which accepts exact versions and Maven ranges but no pattern and also applies to security updates.
On the 2.x line, list each `-1.20` version, and Dependabot then proposes the same release's `X.Y.Z`; on the LTS line, list the bare versions, such as `[1.0.1]`:

```yaml
version: 2
updates:
  - package-ecosystem: maven
    directory: /
    schedule:
      interval: weekly
    ignore:
      - dependency-name: io.github.flink-gcp:flink-datastream-protobuf
        # Stay on the Flink 2.x line: skip each release's Flink 1.20 LTS build.
        versions: ["[1.0.0-1.20]", "[1.0.1-1.20]"]
```

Renovate's [`allowedVersions`](https://docs.renovatebot.com/configuration-options/#packagerulesallowedversions) accepts a regular expression, which keeps either line without an entry per release:

```json
{
  "packageRules": [
    {
      "matchPackageNames": ["io.github.flink-gcp:flink-datastream-protobuf"],
      "allowedVersions": "/^[0-9]+\\.[0-9]+\\.[0-9]+$/"
    }
  ]
}
```

On the LTS line, use `"/^[0-9]+\\.[0-9]+\\.[0-9]+-1\\.20$/"` instead.

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

The [runtime suite](validation/runtime-recovery.md) restores complete savepoints within each pinned Flink/Protobuf combination.
It does not yet verify a complete savepoint written by one Protobuf version and restored by another, including patch and minor updates.

The schema-evolution tests write and restore state with the same development build.
They check the restored values, continued processing, and a second restore from a savepoint that the new application wrote.
A field removed with a reservation remains in restored values as an unknown field, and the new application's savepoints keep it; the test reads the value back with the original schema.
DynamicMessage state follows the same schema-change rules; the [DynamicMessage recovery tests](validation/runtime-recovery.md#dynamicmessage) verify them with state that the same development build writes.

Before deploying a dependency update, validate the actual old-writer/new-reader combination with the application's schemas, state, and runtime settings:

1. Write a savepoint with the old application and retain its exact application/library jars, schemas, settings, operator UIDs, state names, and runtime versions.
2. Compare the documented API, default, runtime, and state changes before replacing the library or a dependency.
3. Restore the savepoint with the updated application and check saved values, input positions, and continued processing.
4. Write another savepoint after processing with the updated application and verify restoration from that state too.

For a validated compatible update, stop the old job with a savepoint and resume the updated job from it.
See [Flink's savepoint operations](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/savepoints/#stopping-a-job-with-savepoint).
An intermediate run without restoring state does not convert old state or make an incompatible schema compatible.
If restoration is rejected, retain the old deployment or explicitly choose a documented migration or fresh-state/replay procedure.
Starting fresh requires an explicit decision about rebuilding state and replaying input; the library has no state-migration step and provides no migration path for rejected schema changes.
The [development guide](development.md#test-resources-and-version-updates) contains the dependency-update checks.

## Preparing schema changes

The evaluator stays conservative about conditional numeric conversions and other unsupported changes; parsing success alone does not prove that values retain their meaning.

Reserve the number of every removed field, for example `reserved 2;`.
The evaluator rejects a removal without a reservation, and it rejects a later change that removes a reservation to reuse the number.
Protobuf parsing keeps old values of a removed field as unknown fields, so an unreserved number could later be parsed as an unrelated declaration.
No descriptor comparison can detect a field that keeps its number, name, and type but changes its meaning, so keep a removed number reserved instead of reusing it.
Reserving does not make removals of enum values acceptable; the evaluator rejects them.
Before adding a field, also check that saved values cannot carry an unknown field with its number, for example from input written with a newer schema; the new parser would read that retained value as the added field, and descriptors cannot reveal it.
The evaluator checks only messages that saved bytes reach through retained fields, so it does not check message types packed in an `Any` payload; reserve removed numbers in those types as well.
