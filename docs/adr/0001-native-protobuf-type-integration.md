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

# ADR-0001: Native Protobuf type integration for DataStream

- Status: Accepted
- Date: 2026-09-06
- Implementation: Pending; only feasibility probes exist
- Evidence: [Spike 0](../validation/spike-0.md)

## Context

Flink uses TypeInformation to select serializers for operator transport and managed state.
Generated Protobuf messages otherwise fall back to generic serialization in the tested configuration.
The goal is an explicit native integration that works with `pipeline.generic-types: false` and owns a documented checkpoint/savepoint compatibility policy.
Connector boundary SerializationSchema and DeserializationSchema implementations, and Table/SQL formats, are separate concerns.

Spike 0 established superclass factory registration and transport of generated messages at top level, inside POJOs, and inside tuples on Flink 2.3.0.
The tested Chill 0.7.6 configuration also transported a message successfully using Kryo 5.6.2.
Consequently, failure of that configuration is not a premise of this design.

## Decision

### Type integration

Implement `ProtobufTypeInformation`, `ProtobufTypeInfoFactory`, `ProtobufTypeSerializer`, and `ProtobufTypeSerializerSnapshot` under `io.github.flink.gcp.protobuf`.
Offer explicit TypeInformation construction for `.returns(...)` and configuration registration against `com.google.protobuf.AbstractMessage`.
The latter reaches both tested generations of full-runtime generated messages.
Registration against the Message interface is not the automatic registration path because the factory lookup follows superclasses.
Document the process-global registry and avoid claiming that registrations are isolated per job.

The initial target is Flink 2.3 with full-runtime generated messages, protobuf-java 3.25.x and 4.x, and JDK 17 and 21.
Flink core and protobuf-java remain provided and unrelocated so the application owns its runtime.
DynamicMessage, lite runtime, extension-registry support, Table formats, and DataStream API V2 are outside the initial implementation.
Validate unsupported input classes explicitly rather than letting reflection fail later.

### Serialization and keys

The production serializer will write a length followed by Protobuf wire bytes, without an intermediate payload byte array on serialization.
Generated messages are immutable: object copy returns the same instance and `isImmutableType()` returns true.
Resolve the parser once per serializer instance and retain it in a transient field; the serializer must survive Java serialization to a TaskManager.
The current probe uses a simple byte-array implementation solely to exercise Flink's routing and transport.
It is not the production serializer or a stable wire format.

Message values are not valid keys by default.
Provide an explicit opt-in on TypeInformation for applications that accept the key-byte constraints, and recommend extracting stable scalar keys.
Deterministic serialization can be an explicit serializer setting, but it is not canonical serialization and does not justify schema evolution of keyed-state keys.

### State compatibility

Snapshots will store their format version, generated Java class name, fully qualified Protobuf message name, transitive FileDescriptorSet, a descriptor fingerprint for fast comparison, and serializer settings.
The descriptor comparison is a separate pure evaluator, tested with old/new descriptor pairs.
A matching fingerprint is an optimization, not the sole acceptance criterion.
The first implementation returns either `compatibleAsIs` or `incompatible`; it does not promise migration via an old generated class that may no longer exist in the application jar.

Evaluate compatibility in the direction of restoring saved bytes with the new serializer.
Accept supported wire-safe field additions, removals/reservations, and enum additions, with recursive checks of referenced types.
Reject message or Java class renames, detected field-number reuse, required-field additions, incompatible type/cardinality changes, and moves into oneof for the initial implementation.
Treat numeric type changes classed as conditionally safe by Protobuf conservatively as incompatible; parsing alone does not establish preservation of values.
Descriptor pairs cannot prove that a reused number with an indistinguishable declaration still has the same business meaning, so reserving removed numbers remains an application schema obligation.
The implementation must define and test descriptor normalization, cycles, unknown fields, and setting changes before claiming restore compatibility.

The Protobuf guide distinguishes wire-safe changes from conditionally safe changes that may lose values.
See [Updating a message type](https://protobuf.dev/programming-guides/proto3/#updating), [runtime compatibility](https://protobuf.dev/support/cross-version-runtime-guarantee/), and [non-canonical serialization](https://protobuf.dev/programming-guides/serialization-not-canonical/).

## Alternatives

Keeping byte arrays or an application envelope is suitable when operators mostly forward payloads; this library targets applications that operate on generated messages.
A Kryo adapter leaves the message in Flink's generic-type path and does not meet the requirement to disable generic types.
It can be considered separately after the native integration.
Exact descriptor fingerprint equality alone would unnecessarily reject compatible field additions and removals.

Flink 2.2 and 1.20 support, cross-minor savepoint fixtures, lite messages, extensions, renamed types, and a Kryo adapter are later design work, not current compatibility claims.
