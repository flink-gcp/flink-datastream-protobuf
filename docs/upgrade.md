---
title: Upgrade guide
weight: 35
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

# Upgrade from 0.1.0 to 0.2.0

This guide covers replacing the 0.1.0 development library with 0.2.0 in an application that keeps its Flink version, artifact line, and Protobuf generated-code/runtime pair.
Both versions are development milestones; neither is published to Maven Central, and the 0.x policy permits breaks between them.
The guide describes only the forward direction, from state that 0.1.0 wrote to an application built with 0.2.0, because that is the direction the tests restore.

## Four separate contracts

An upgrade can keep one contract and break another, so the following are checked separately:

| Contract | What it covers | From 0.1.0 to 0.2.0 |
|---|---|---|
| Application-facing API | Public and protected members of the `@Public` and `@PublicEvolving` types: `ProtobufTypeInformation`, its `Builder`, and `ProtobufTypeInfoFactory` | No member is removed or changed; 0.2.0 adds `Builder.largePayloadAliasing(boolean)` |
| Saved state | Whether 0.2.0 restores the serializer snapshots and values that 0.1.0 wrote | Restored with an unchanged schema or a [supported schema change](compatibility.md#schema-changes) |
| Key bytes | The hash codes and serialized bytes that Flink uses for DataStream keys and MapState user keys | Unaffected: neither version supports Protobuf messages as keys, and [scalar keys](usage.md#scalar-keys) use Flink's own serializers |
| Generated code and runtime | Whether the application's generated classes run with its `protobuf-java` | Unaffected: the upgrade keeps the application's pair, and both versions declare `protobuf-java` as a provided dependency |

The Protobuf binary-format and Flink managed-state questions of the [three compatibility checks](compatibility.md#three-compatibility-checks) together decide the saved-state contract here.
Changing the generated code or the runtime is a separate update with its own rules; see [Java runtime version updates](compatibility.md#java-runtime-version-updates).

## Changes from 0.1.0

The API change is an addition.
`ProtobufTypeInformation.Builder.largePayloadAliasing(boolean)` selects whether values decoded from a payload above 4 KiB may share the array that holds it.
Code compiled against 0.1.0 runs against 0.2.0 without recompilation, and recompiling it requires no source change; the [API compatibility gate](development.md#api-compatibility-gate) checks this against the retained 0.1.0 libraries and their compiled consumers.

Three intentional breaks can make an application that worked with 0.1.0 fail with 0.2.0:

- **Proto3-only payload graphs**: constructing type information fails with `IllegalArgumentException` when the message, or any message type that it reaches, is declared in a proto2 or editions file; 0.1.0 accepted proto2 messages.
  The message names the first such type and its file.
  State that 0.1.0 wrote for such a type restores only under the conditions in [development builds before 1.0.0](compatibility.md#development-builds-before-100).
- **Colliding unknown fields**: serialization fails with `IOException` for a value whose unknown field reuses the number of a field that the containing message declares, with a wire type that field accepts, because the reader would parse it into the declared field.
  Parsing never produces such a value; only unknown fields that the application sets or merges directly can, for example unknown fields copied from a message of another type.
  Reading state that 0.1.0 wrote with such values behaves as it did in 0.1.0: the reader parses those bytes into the declared field, or fails for malformed content; see [explicit type information](usage.md#explicit-type-information).
- **Unknown-field numbers above 536,870,911**: serialization fails with `IOException`, because the wire tag of such a number would wrap to another field's tag or to an invalid tag, so the reader would misparse it or fail.

Large-payload aliasing is enabled by default, so values decoded from a large payload, such as `bytes` fields, can keep the whole payload array reachable.
An application that keeps such values for a long time, for example in heap state or a cache, can disable it with `largePayloadAliasing(false)`; see [memory of large decoded messages](usage.md#memory-of-large-decoded-messages).

0.2.0 writes version 2 of the serializer snapshot, which records the aliasing setting, and also reads the version 1 that 0.1.0 writes.
The record frame format of serialized values keeps its version.
Other changes in 0.2.0 reduce serializer time and allocation without changing that format; their measurements are in the [buffer allocation](validation/serializer-buffer-allocation.md), [sizing allocation](validation/serializer-sizing-allocation.md), [generated size](validation/serializer-generated-size.md), and [large read](validation/serializer-large-read.md) reports.

## Upgrade procedure

Keep the Flink version, the artifact line, the generated-code/runtime pair, the generated Java class and Protobuf message names, the operator UIDs, and the state names unchanged across the library upgrade.
Keep the size and depth limits equal or raise them; a decreased limit fails the restore.
Change the application schema in the same deployment only as listed in [supported schema changes](compatibility.md#schema-changes).

Before stopping the old job, check the two things that 0.2.0 newly rejects:

1. Build the application with 0.2.0 and construct its type information, for example in its tests; a proto2 or editions type fails here.
2. Find the code that sets or merges unknown fields directly, if any, and exercise it with 0.2.0.

Then follow the [general upgrade procedure](compatibility.md#verified-coverage-and-upgrade-procedure), and keep the 0.1.0 savepoint with the 0.1.0 application jar.
That savepoint is the only state that a rollback to 0.1.0 can restore: 0.1.0 reads only version 1 of the serializer snapshot, so it cannot restore a checkpoint or savepoint that 0.2.0 writes.

## Tested restore results

The [retained-baseline upgrade check](validation/development-baselines.md#restore-retained-state-with-the-current-library) restores state that the [retained 0.1.0 artifact](validation/development-baselines.md#retained-v010-baseline) wrote with the current library, on each Flink version and JDK that read the retained bundles.
The application contains only the new generated classes, and each run keeps the Flink version and the generated-code/runtime pair that wrote the state.

The schemas below are the compiled application schemas that these tests use, rendered from their sources.
The 0.1.0 jobs wrote state with the [`original` schema](../src/test/runtime-app/proto/original/runtime.proto):

{{< example "original/runtime.proto#entry" >}}

The [`changed` schema](../src/test/runtime-app/proto/changed/runtime.proto) adds a field with a fresh number, and the restore is accepted; retained values have `added` empty, and new values carry it:

{{< example "changed/runtime.proto#entry" >}}

The [`reserved` schema](../src/test/runtime-app/proto/reserved/runtime.proto) removes a field and reserves its number and name, and the restore is accepted.
Retained values keep the removed `payload` as an unknown field, and the test decodes it with the `original` descriptor from the savepoint that the new application writes:

{{< example "reserved/runtime.proto#entry" >}}

The [`incompatible` schema](../src/test/runtime-app/proto/incompatible/runtime.proto) changes `count` from `int32` to `sint32`, and the restore is rejected, because saved varints would decode to different values:

{{< example "incompatible/runtime.proto#entry" >}}

The failure names the generated class and the first incompatibility, `Field 2 (count) of flink.protobuf.runtime.Entry changes type from INT32 to SINT32`; the [schema changes](compatibility.md#schema-changes) section shows a complete message.

The retained 0.1.0 schema declares no enum, so adding an enum value is verified only with state that 0.2.0 builds wrote, in the [schema-evolution test](validation/runtime-recovery.md#schema-changes).
That test restores state written with the [`enum-original` schema](../src/test/runtime-app/proto/enum-original/runtime.proto) into the [`enum-added` schema](../src/test/runtime-app/proto/enum-added/runtime.proto), which adds `KIND_SECOND`:

{{< example "enum-added/runtime.proto#entry" >}}

The tests do not cover a Flink minor or major change, a change of the generated-code/runtime pair, or state of proto2 types, and none of these paths is claimed.
