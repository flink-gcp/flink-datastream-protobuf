---
title: "ADR-0005: Descriptor-bound DynamicMessage type and snapshot contracts"
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

# ADR-0005: Descriptor-bound DynamicMessage type and snapshot contracts

- Status: Accepted
- Date: 2026-10-29
- Issue: [#21](https://github.com/flink-gcp/flink-datastream-protobuf/issues/21)
- Extends: The generated-message contracts in [ADR-0001](0001-native-protobuf-type-integration.md), which apply here except where this record states otherwise
- Implementation: Implemented by [issue #22](https://github.com/flink-gcp/flink-datastream-protobuf/issues/22); [issue #23](https://github.com/flink-gcp/flink-datastream-protobuf/issues/23) validates state recovery and documents usage

## Context

The 0.1.0 and 0.2.0 development milestones support generated full-runtime messages only.
Type information, serializers, and snapshots identify a payload type by its generated class, and the class supplies the descriptor, parser, and default instance.
The 0.3.0 milestone adds `com.google.protobuf.DynamicMessage`, which an application creates from a descriptor without a generated class.

One Java class represents every DynamicMessage schema, so the class cannot select a descriptor.
Flink's type extraction passes a registered `TypeInfoFactory` only the extracted type and its inferred generic parameters, so a factory registered against `AbstractMessage` receives `DynamicMessage.class` and nothing that names a schema.
A schema therefore has to come from the application through explicit type information.

Three properties of the pinned dependencies constrain the design.
First, Flink's type extraction turns only `InvalidTypesException` into a `MissingTypeInfo` that `.returns(...)` can replace; any other exception from a factory escapes graph construction before `.returns(...)` applies.
Second, DynamicMessage equality, hash codes, field access, and merging compare descriptor instances, not descriptor content.
A descriptor rebuilt on a TaskManager is a different instance from the one the application holds, even when the content is equal.
Third, heap state backends and operator state read restored values with the serializer that the saved snapshot restores, before the application registers its new serializer, as ADR-0001 records for generated messages.
A dynamic snapshot can restore only the saved descriptor, so after a supported schema change those values are expected to keep the saved descriptor while the new serializer writes them at the next checkpoint; issue #23 verifies this for DynamicMessage.

## Decision

### Scope

Support DynamicMessage values bound to one explicit root descriptor per type information and serializer, in DataStream transport and managed state, with the settings, framing, size and depth limits, and unknown-field rules of generated messages.
The 0.1.0 and 0.2.0 milestones did not support DynamicMessage; their artifacts reject it, and this record does not change what they accepted.

A stream or state whose values use several root schemas is not supported; use one type per schema.
Automatic descriptor discovery, schema registries, Any unpacking, conversion of saved state between generated and dynamic types, lite messages, and extension registries remain outside the 0.3.0 scope, as the [milestone tracker](https://github.com/flink-gcp/flink-datastream-protobuf/issues/20) states.
DynamicMessage is a separate concern from Struct, Any, and OpenTelemetry AnyValue, which are generated types with fixed schemas.

### Construction API

Add two static methods to `ProtobufTypeInformation`:

```java
public static ProtobufTypeInformation<DynamicMessage> of(Descriptor rootDescriptor);
public static Builder<DynamicMessage> newBuilder(Descriptor rootDescriptor);
```

`newBuilder` returns the existing `ProtobufTypeInformation.Builder` with its four settings and defaults, and `of(rootDescriptor)` is equivalent to `newBuilder(rootDescriptor).build()`.
The result serves `.returns(...)`, state descriptors, and explicit composite type information alike.
`getTypeClass()` returns `DynamicMessage.class`.
These methods are `@PublicEvolving` like the existing construction API.

The root descriptor is the only schema input.
An application needs a `Descriptor` to create or parse DynamicMessage values in any case, and a `FileDescriptorSet` with a message name can be turned into one with the Protobuf runtime.
A second overload taking that pair would duplicate descriptor building and its validation surface; it can be added later without changing this one.

### Class-only entry points

`ProtobufTypeInfoFactory.createTypeInfo` throws `InvalidTypesException` for `DynamicMessage.class`, with a message that names `ProtobufTypeInformation.of(Descriptor)`.
An `IllegalArgumentException`, which the 0.2.0 factory throws, would fail graph construction for every operator returning DynamicMessage while the factory is registered, even with `.returns(...)`.
With `InvalidTypesException`, the operator output becomes a `MissingTypeInfo` that `.returns(...)` replaces; without `.returns(...)`, Flink's missing-type error fails the job and carries the library message in its cause chain.
Other classes that the factory rejects keep failing with `IllegalArgumentException`, as in 0.2.0.

Flink handles `InvalidTypesException` for a POJO field by giving that field generic type information.
With `pipeline.generic-types: false`, as the library requires, serializer creation then fails; with generic types enabled, Flink's own generic serializer would be selected, not a library fallback.
A composite type that contains a DynamicMessage therefore needs explicit composite type information that includes the descriptor-bound type, such as Flink's `Types.TUPLE` or `Types.POJO` with field types.

`ProtobufTypeInformation.of(DynamicMessage.class)` and `newBuilder(DynamicMessage.class).build()` fail with `IllegalArgumentException` whose message names `of(Descriptor)`.

### Validation and descriptor limits

At `of` or `build`, reject a null descriptor, and apply the payload rules to the root: every message type that it reaches is declared in a proto3 file, declares no extensions or extension ranges, declares no group or required field, and declares no field number above 536,870,911.
The proto3 and extension rules are the generated-message rules of ADR-0001.
The others are new, because protoc enforces them but `FileDescriptor.buildFrom` does not.
It checks only that a field number is positive, so a hand-built descriptor can declare a number whose tag wraps when written, as ADR-0001 describes for unknown fields; the reader then takes the value as another field or rejects the frame.
It also accepts a group field, a required field, and an extension range in a proto3 file: sizing does not measure group nesting, a value without its required field is written but cannot be read, and an extension range lets an extension field be set and written outside the sized fields.
Normalize the root's file closure with complete descriptor normalization version 1.
Require the full name to fit `writeUTF`, and the deterministic encoding of the normalized set to be nonempty and at most the snapshot's fixed 64 MiB descriptor limit.
Then decode that encoding and rebuild the descriptors exactly as Java deserialization and snapshot restoration do, requiring the root to resolve once.
A descriptor that cannot be rebuilt from its own content, such as one built with unresolved placeholder dependencies, therefore fails at construction rather than on a TaskManager.
Failures throw `IllegalArgumentException` naming the root's full name, when there is a root, and the failed rule.

Decoding uses the extension registry and recursion limits of the generated snapshot reader, and Protobuf's descriptor validation through `FileDescriptor.buildFrom`.
The byte limit bounds the number and size of files; no separate file-count limit applies.
Normalization, rebuilding, and evaluation cost grows with the descriptor closure, which the application controls.

### Identity and equality

A dynamic type is identified by the Protobuf full name of its root and the complete normalized descriptor content, not by descriptor instances.
TypeInformation and serializer equality compare the type class, the full name, the normalized content, the framing and normalization versions, and all four settings, as for generated messages.
Comparing the type class distinguishes a dynamic type from a generated type with the same descriptor, because the generated class is never `DynamicMessage`.
Two type information objects built from distinct descriptor instances with equal content are equal.

The two kinds apply the shared evaluator to different identities.
A generated identity includes the Java binary class name, so the evaluator rejects a class rename.
A dynamic identity has no application class and its snapshot records no class name; for every dynamic comparison below, the evaluator's class rule is vacuous and its full-name, schema, and setting rules decide.

### Job-graph serialization

The type information and serializer serialize the type class, the settings, the full name, and the deterministic encoding of the normalized descriptor set.
They never serialize a `Descriptor`, which is not Java-serializable, a parser, or a classloader.
Java deserialization repeats the decoding, rebuilding, and validation above and fails with `InvalidObjectException` if they fail.
The rebuilt descriptors, default instance, parser, and sizing layouts are transient.
Each deserialized instance builds its own descriptors, and `duplicate()` shares them.
The instance returned by construction keeps the application's descriptor.

The job graph is built from the application at each submission, and savepoints persist serializer snapshots rather than Java-serialized serializers, so this form is not a restore input.

### Writing values

Writing rejects with `IOException`, before emitting any frame byte, a null value or one whose class is not `DynamicMessage`.

The serializer's reader parses each nested value with the declaration that its own schema gives for the field, so writing pairs every message value with that declaration.
The root pairs with the serializer's root, and a value under a field pairs with the message type that the serializer's paired declaration of the containing value gives for the same field number.
A value under a field number that the paired declaration does not declare has no pair; its reader keeps it as an unknown field.

A message value with a pair is accepted when one of these holds:

- Its descriptor is the paired declaration's instance.
- Its descriptor is the instance that its containing value's descriptor declares for the field, and the containing value was accepted; the evaluation that accepted the containing value compared that type.
- The 0.2.0 directional evaluator accepts its descriptor as saved metadata against the paired declaration as current metadata, with the serializer's settings on both sides.

Otherwise writing fails with `IOException` naming the field path and the evaluator's first incompatibility, which includes a full-name difference; an invalid descriptor closure, for which the evaluator throws `IllegalArgumentException`, also fails with `IOException`.
An evaluated descriptor must also satisfy the payload rules for every type that it reaches, because the evaluator does not compare declarations reached only through fields that the paired declaration does not keep.
A message value without a pair whose descriptor is not the type that its containing value declares must satisfy the payload rules too, because sizing its subtree assumes them.

Evaluation is the ordinary write path, not only a schema-change accommodation.
Values decoded by one serializer instance carry its descriptors, and in the same task another instance, such as the state serializer, writes them, so equal content behind distinct instances is common.
After a supported schema change, values restored on a heap backend carry the saved descriptor, which the restore already accepted against the new schema.

Each serializer instance caches up to 16 accepted descriptor pairs by instance identity, with the sizing layouts that each needs, and evicts the least recently used pair.
A pair outside the cache is evaluated again, so an application that creates a descriptor for each record pays for an evaluation on each record.

Unknown-field collision and field-number checks use the paired declaration, which is what the reader parses with, and a value without a pair has no collisions, because the reader keeps all of it as one unknown field; the size and depth checks and framing version 1 are those of generated messages.
The evaluator's field-addition assumption from ADR-0001, that no saved value carries an unknown field with an added number, does not reach writing: a value whose unknown field collides with a field that only the serializer's schema declares is rejected like any other collision.
Generated messages keep their current behavior.
Deterministic serialization uses the runtime's deterministic mode, which orders generated map fields but writes DynamicMessage map entries in insertion order in both pinned runtimes.

### Reading values

Reading parses with the parser of the serializer's descriptor and an empty extension registry, under the frame, limit, and aliasing rules of ADR-0001.
Decoded values carry the reading serializer's descriptor instance.
Because DynamicMessage compares descriptor instances, application code must resolve field descriptors through the value, for example with `value.getDescriptorForType().findFieldByName(name)`, and build a nested value from that field's `getMessageType()`; a field descriptor from another instance fails in `getField`, and values from different instances are never equal.

Heap state backends and operator state are expected to yield restored values with the saved descriptor, and RocksDB values with the new serializer's descriptor.
After a supported schema change, values with the saved descriptor have no added fields, so `findFieldByName` returns null for them.
They take the new descriptor from the next restore after a snapshot that the new serializer wrote, as the large-payload aliasing setting does in ADR-0001.

DynamicMessage values are treated as immutable: `copy` returns the same instance, `isImmutableType()` returns true, and `createInstance()` returns the default instance of the serializer's descriptor.
Decoded values hold `ByteString` for `bytes` fields, but `DynamicMessage.Builder` also accepts a `byte[]` and keeps that array without copying it.
An application that sets a `byte[]` must not modify it afterwards; the runtime's memoized size and hash code already assume this, and a heap backend's copy-on-write snapshot would otherwise share the modified array, because `copy` does not copy the value.
Writing and sizing accept such a value as its bytes.

### Keys

The value-only key policy of ADR-0001 applies unchanged to DataStream partitioning keys and MapState user keys: `isKeyType()` returns false, no message-key opt-in exists, and applications extract supported scalar keys.
DynamicMessage hash codes include the descriptor instance's identity hash, which differs between processes, so key-group assignment of a message key would differ between the sending and receiving tasks.

### Implementation sharing

Three structures were compared.

| Structure | Assessment |
|---|---|
| Separate public type information, serializer, and snapshot classes | Distinct types for each kind, but two construction APIs and builders with identical settings, and little extra type safety, because the common API already returns `ProtobufTypeInformation<DynamicMessage>` |
| Shared classes with a snapshot version 3 carrying a generated/dynamic discriminator | One snapshot class, but 0.3.0 generated state would change format without any generated behavior change, adding a generated version-3 reader and fixtures next to versions 1 and 2 |
| Shared type information and serializer with a separate dynamic snapshot class | One construction API, shared framing, sizing, normalization, and evaluation code, an unchanged 0.3.0 generated snapshot, and a discriminator that Flink's snapshot envelope already records |

Select the third structure.
`ProtobufTypeInformation` and the `@Internal` `ProtobufTypeSerializer` bind either a generated class or a root descriptor.
The serializer class is not persisted, so sharing it carries no state-format obligation.
Restoration is what differs between the kinds: a generated snapshot resolves an application class through the user-code classloader, while a dynamic snapshot rebuilds the saved descriptors without loading application classes.
A separate snapshot class gives each kind its own version line and reader, and that difference persists after the 1.0.0 format restart, which keeps one application-facing type for both kinds.

### Dynamic snapshot format version 1

Add `io.github.flink.gcp.protobuf.ProtobufDynamicMessageSerializerSnapshot`, `@Internal`, with a public no-argument constructor.
`getCurrentVersion()` returns `1`, and the payload after Flink's envelope has this fixed order:

| Field | Encoding |
|---|---|
| Message framing version | Big-endian `int32`, `1` |
| Descriptor normalization version | Big-endian `int32`, `1` |
| Protobuf message full name | `writeUTF` / `readUTF` |
| Deterministic serialization | One byte, `0` or `1` |
| Maximum payload size | Big-endian positive `int32` |
| Recursion limit | Big-endian positive `int32` |
| Large-payload aliasing | One byte, `0` or `1` |
| Descriptor byte length | Big-endian positive `int32` |
| Normalized FileDescriptorSet | Exactly the declared number of bytes |
| Descriptor fingerprint | Exactly 32 SHA-256 bytes, without a length prefix |

This is generated snapshot version 2 without the class name, with the same limits and fingerprint semantics.
The generated snapshot class keeps writing version 2 and reading versions 1 and 2 in 0.3.0.
Format identifiers, breaking changes, and the 1.0.0 format restart follow the [compatibility policy before 1.0.0](0001-native-protobuf-type-integration.md#compatibility-policy-before-100) in ADR-0001.

`readSnapshot` decodes the metadata and normalizes the saved set against its root, as the generated reader does, without loading any application class; malformed, truncated, or unsupported data fails there with `IOException`.
`restoreSerializer` rebuilds the saved descriptors, applies the construction validation, and returns a reader with the saved settings; a failure there throws `IllegalStateException` naming the full name and the failed rule.
No application input takes part, so the saved schema restores without an application class or descriptor.
`resolveSchemaCompatibility` evaluates a saved dynamic snapshot against the new one with the 0.2.0 directional evaluator and returns `compatibleAsIs` or `incompatible`, under the same supported schema changes and setting rules as generated messages.

Conversion between generated and dynamic state is not implemented even when both describe the same schema, so switching kinds requires fresh state or replayed input.
Resolution against a saved snapshot of the other kind returns `incompatible`; the existing generated snapshot already does so for any other snapshot class.
On a heap backend, which restores values before resolving compatibility, the visible failure depends on the saved kind.
Restoring saved generated state into a dynamic application fails in the generated `restoreSerializer` when the application no longer contains the generated class, and otherwise at resolution.
Restoring saved dynamic state into a generated application always fails at resolution, because the dynamic `restoreSerializer` needs no application input.

Flink's `incompatible` result carries no reason, and a dynamic `restoreSerializer` never compares against the new schema, so no restore exception names a dynamic incompatibility; the user documentation must state the evaluator rules that decide it.

### Impact on generated-message applications

The added overloads leave every existing signature unchanged.
The factory's exception for `DynamicMessage.class` changes from `IllegalArgumentException` to `InvalidTypesException`, an intentional behavior change for a class that 0.2.0 rejected as well; generated and other classes keep their behavior.
The generated snapshot class, format, and evaluator are unchanged in 0.3.0, so 0.1.0 and 0.2.0 generated state is expected to restore through the same reader.
No restore path is dropped in 0.3.0, so no breaking-change procedure is required for state.

The library still implements no Kryo or Chill serializer and no internal generic fallback, for either kind.

### Verification

Issue #22 verifies, on both Protobuf profiles and both Flink lines:

- Construction, Java serialization, and restoration failures, each with its exception type and a message naming the failed rule, including a declared field number above the maximum.
- Writing and sizing a value whose `bytes` field holds a `byte[]`.
- Write acceptance of an identical descriptor, of an equal-content distinct instance, of an evaluator-accepted earlier schema, and of nested values built from the containing value's field type or another instance; rejection of an incompatible root or nested descriptor, a non-proto3 type reached through a field that the serializer reserves, and an unknown field that collides with a field only the serializer declares; and the cache's evaluation count and eviction through a package-private seam.
- The factory with and without `.returns(...)`, with the library message in the cause chain, and a POJO field of type DynamicMessage failing serializer creation with generic types disabled.
- The evaluator reason for each rejected schema pair that issue #23 uses at runtime.

Issue #23 verifies on HashMap and RocksDB backends:

- Restore after a supported schema change for value state, MapState values, and operator list state, including the descriptor that restored values carry before and after a second restore, and restored values left untouched until the next checkpoint.
- Rejection of each unsupported schema change against an accepted control that differs only in that change, and of both cross-kind directions with and without the generated class.
- The direct and sequential generated upgrade paths from the retained 0.1.0 and 0.2.0 baselines with `api-compat` and `baseline-consume`, and the upgrade-guide entry for the factory change.
- The user documentation of descriptor handling, field access through the value, explicit composite type information, and the evaluator rules for dynamic restore.

### Source basis

The design was verified against these pinned sources:

- Protobuf [3.25.9](https://github.com/protocolbuffers/protobuf/blob/v25.9/java/core/src/main/java/com/google/protobuf/DynamicMessage.java#L56-L117) and [4.33.6](https://github.com/protocolbuffers/protobuf/blob/v33.6/java/core/src/main/java/com/google/protobuf/DynamicMessage.java#L56-L117) `DynamicMessage` creates and parses values from a descriptor; its [field access checks the containing descriptor instance](https://github.com/protocolbuffers/protobuf/blob/v33.6/java/core/src/main/java/com/google/protobuf/DynamicMessage.java#L281-L286) ([3.25.9](https://github.com/protocolbuffers/protobuf/blob/v25.9/java/core/src/main/java/com/google/protobuf/DynamicMessage.java#L279-L284)), its builder [does not check nested message descriptors](https://github.com/protocolbuffers/protobuf/blob/v33.6/java/core/src/main/java/com/google/protobuf/DynamicMessage.java#L609-L648), and its field set [accepts](https://github.com/protocolbuffers/protobuf/blob/v25.9/java/core/src/main/java/com/google/protobuf/FieldSet.java#L403-L404) and [retains](https://github.com/protocolbuffers/protobuf/blob/v25.9/java/core/src/main/java/com/google/protobuf/FieldSet.java#L1082-L1107) a `byte[]` for a `bytes` field.
- Protobuf 3.25.9 descriptor building [checks only that a field number is positive](https://github.com/protocolbuffers/protobuf/blob/v25.9/java/core/src/main/java/com/google/protobuf/Descriptors.java#L1521-L1523), as 4.33.6 does at [line 1934](https://github.com/protocolbuffers/protobuf/blob/v33.6/java/core/src/main/java/com/google/protobuf/Descriptors.java#L1933-L1935), and a tag is the number [shifted into an `int`](https://github.com/protocolbuffers/protobuf/blob/v25.9/java/core/src/main/java/com/google/protobuf/WireFormat.java#L52-L55).
- `AbstractMessage` [equality](https://github.com/protocolbuffers/protobuf/blob/v33.6/java/core/src/main/java/com/google/protobuf/AbstractMessage.java#L146) and [hash code](https://github.com/protocolbuffers/protobuf/blob/v33.6/java/core/src/main/java/com/google/protobuf/AbstractMessage.java#L158) use the descriptor instance in 4.33.6, as at [lines 127 and 139](https://github.com/protocolbuffers/protobuf/blob/v25.9/java/core/src/main/java/com/google/protobuf/AbstractMessage.java#L127-L139) in 3.25.9.
- Flink's `TypeExtractor` turns only `InvalidTypesException` into `MissingTypeInfo` for operator outputs ([2.3.0](https://github.com/apache/flink/blob/release-2.3.0/flink-core/src/main/java/org/apache/flink/api/java/typeutils/TypeExtractor.java#L619-L626), [1.20.4](https://github.com/apache/flink/blob/release-1.20.4/flink-core/src/main/java/org/apache/flink/api/java/typeutils/TypeExtractor.java#L617-L624)) and gives a POJO field generic type information on that exception ([2.3.0](https://github.com/apache/flink/blob/release-2.3.0/flink-core/src/main/java/org/apache/flink/api/java/typeutils/TypeExtractor.java#L2199-L2206), [1.20.4](https://github.com/apache/flink/blob/release-1.20.4/flink-core/src/main/java/org/apache/flink/api/java/typeutils/TypeExtractor.java#L2151-L2158)).
- Flink 2.3.0's heap restore reads state with the table's [current serializer](https://github.com/apache/flink/blob/release-2.3.0/flink-runtime/src/main/java/org/apache/flink/runtime/state/heap/StateTableByKeyGroupReaders.java#L72), which is the [restore serializer until the application registers its own](https://github.com/apache/flink/blob/release-2.3.0/flink-runtime/src/main/java/org/apache/flink/runtime/state/StateSerializerProvider.java#L151-L163).

These links establish the design premises, not the library's behavior, which the verification above establishes.

## Alternatives

Accepting only values whose descriptor content equals the serializer's would avoid evaluating schema pairs on the write path.
After a supported schema change, a heap backend's next checkpoint would then reject every restored value, which still carries the saved descriptor.

Checking nested values by descriptor instance alone would be cheaper, but values decoded by one serializer instance and rebuilt by application code with another instance's nested types would fail to checkpoint without any schema change.

A serializable descriptor supplier, or a process-wide registry of canonical descriptors, could give decoded values the same descriptor instance as application code.
A restored snapshot cannot call an application supplier, so heap-restored values would still carry rebuilt descriptors, and a process-wide registry would retain descriptors and their classloaders beyond a job.
Resolving fields through the value works without either.

Recording a Java class name in the dynamic snapshot would keep the generated layout, but the name would always be `DynamicMessage` and would carry no identity.
