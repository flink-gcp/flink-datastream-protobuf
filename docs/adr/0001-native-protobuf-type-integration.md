---
title: "ADR-0001: Native Protobuf type integration for DataStream"
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

# ADR-0001: Native Protobuf type integration for DataStream

- Status: Accepted
- Date: 2026-09-06
- Publication policy amended: 2026-09-13; 0.x versions are unpublished development milestones and 1.0.0 is the first Maven Central release
- Release contract amended: 2026-09-08, [issue #7](https://github.com/flink-gcp/flink-datastream-protobuf/issues/7)
- Frame buffering amended: 2026-10-11, [issue #42](https://github.com/flink-gcp/flink-datastream-protobuf/issues/42)
- Sizing traversal amended: 2026-10-19, [issue #43](https://github.com/flink-gcp/flink-datastream-protobuf/issues/43)
- Generated sizes for bounded types amended: 2026-10-22, [issue #112](https://github.com/flink-gcp/flink-datastream-protobuf/issues/112)
- Large-frame reads amended: 2026-10-25, [issue #113](https://github.com/flink-gcp/flink-datastream-protobuf/issues/113)
- Implementation: Serializer, type integration, and descriptor snapshots implemented; the 0.2.0 descriptor evaluator is implemented, tested with descriptor pairs, and used by snapshot restore; MiniCluster tests verify runtime recovery with unchanged schemas and supported schema changes, both from state written by the same development build and from state retained by the 0.1.0 artifact ([issue #16](https://github.com/flink-gcp/flink-datastream-protobuf/issues/16))
- Evidence: [Spike 0](../validation/spike-0.md)
- Runtime scope superseded: 2026-09-10, [ADR-0003](0003-flink-version-compatibility.md)

## Context

Flink uses TypeInformation to select serializers for operator transport and managed state.
Generated Protobuf messages otherwise fall back to generic serialization in the tested configuration.
The goal is an explicit native integration that works with `pipeline.generic-types: false` and owns a documented checkpoint/savepoint compatibility policy.
Connector boundary SerializationSchema and DeserializationSchema implementations, and Table/SQL formats, are separate concerns.

Spike 0 established superclass factory registration and transport of generated messages at top level, inside POJOs, and inside tuples on Flink 2.3.0.
The tested Chill 0.7.6 configuration also transported a message successfully using Kryo 5.6.2.
Consequently, failure of that configuration is not a premise of this design.

## Decision

### Development stages and first publication

The maintainer amended the incremental release plan on 2026-09-13: 0.1.0, 0.2.0, and 0.3.0 are development milestones, with no Maven Central publication.
The descriptor-based design remains the target, with its compatibility evaluator delivered after basic generated-message integration.
Version 1.0.0 is the first Maven Central release, after design stabilization and release validation.
These are development acceptance requirements; the serializer, application-facing type integration, and descriptor snapshots with the directional evaluator are implemented, and MiniCluster tests cover runtime checkpoint/savepoint recovery with unchanged schemas and supported schema changes.

| Milestone | Required outcome |
|---|---|
| [0.1.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/6) | Generated full-runtime messages, native type integration and transport, and unchanged-schema checkpoint/savepoint restore; common Google Well-Known Types, OpenTelemetry generated composite messages, GitHub Pages documentation, and retained development artifact/benchmark evidence |
| [0.2.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/14) | Directional descriptor compatibility evaluation and restoration of supported schema changes from state written by a fixed 0.1.0 development artifact |
| [0.3.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/20) | Explicit-descriptor DynamicMessage integration, with its API/state upgrade behavior documented and tested under the 0.x policy below |

### Type integration and public entry points

Implement `ProtobufTypeInformation`, `ProtobufTypeInfoFactory`, `ProtobufTypeSerializer`, and `ProtobufTypeSerializerSnapshot` under `io.github.flink.gcp.protobuf`.
The application-facing construction API for 0.1.0 is:

```java
public static <T> ProtobufTypeInformation<T> of(Class<T> messageClass);
public static <T extends Message> Builder<T> newBuilder(Class<T> messageClass);
```

The public static nested `Builder<T extends Message>` provides `deterministicSerialization(boolean)`, `maxMessageSize(int)`, and `recursionLimit(int)`, each returning the same builder, and `build()` returning a new immutable `ProtobufTypeInformation<T>`.
For supported message classes, `of(messageClass)` is equivalent to `newBuilder(messageClass).build()` with the defaults below.
The construction API, descriptor snapshots, and managed-state acceptance tests for unchanged and supported changed schemas are implemented.

During issue #9 implementation, compilation showed that the originally declared `<T extends Message> of(Class<T>)` clashes by erasure with Flink's inherited `<T> TypeInformation<T> of(Class<T>)`.
The implemented `ProtobufTypeInformation<T>` and `of` therefore use an unbounded type parameter, preserving the documented method name and concrete return type.
`of(String.class)` compiles but fails with `IllegalArgumentException`; successful construction still requires the supported generated Message class.
The builder retains its `T extends Message` bound.
Renaming the factory would preserve a compile-time bound at that entry point but change the documented construction syntax; the unbounded signature keeps `of` consistent with Flink.

Use explicit TypeInformation for `.returns(...)`, state descriptors, and per-type settings.
Annotate the application-facing type information, builder, and factory APIs `@PublicEvolving`; serializer and snapshot implementations are `@Internal` integration machinery, not additional application construction APIs.
Their persisted identities still carry the state compatibility obligations below.

`ProtobufTypeInfoFactory` has a public no-argument constructor and creates type information with the same defaults as `of`.
Register it against `com.google.protobuf.AbstractMessage` through `pipeline.serialization-config` before type extraction.
This superclass path reaches both tested generations of full-runtime generated messages; registration against the Message interface is not the automatic path because factory lookup follows superclasses.
The registry is process-global, not isolated per job, and the factory has no global mutable settings or custom configuration syntax.
Custom settings require explicit TypeInformation.

The library implements no Kryo or Chill serializer and has no internal generic-serializer fallback.
Flink can still choose its own generic serializer when this integration is not selected.
Set `pipeline.generic-types: false` to reject that path, including in native examples and acceptance tests.
The optional test-only Chill comparison remains separate from production dependencies and behavior.

The target is full-runtime generated messages with protobuf-java 3.25.x and 4.x.
[ADR-0003](0003-flink-version-compatibility.md) replaces the initial Flink 2.3-only scope with current/previous Flink 2.x minor support and a separate Flink 1.20 LTS build.
It defines the JDK matrix, artifact version lines, and runtime-specific release acceptance requirements.
Flink core and protobuf-java remain provided and unrelocated so the application owns its runtime.
DynamicMessage is deferred to 0.3.0; lite runtime, extension-registry support, Table formats, and DataStream API V2 remain outside these release contracts.

At `of` or `build`, require a public, concrete full-runtime message class using the supported `GeneratedMessageV3` or `GeneratedMessage` hierarchy.
Require its public static `getDefaultInstance()` to return that concrete class, with a full descriptor and a parser for that message.
The factory accepts only a concrete `Class<?>` satisfying the same checks, not interfaces, abstract bases, builders, parameterized types, DynamicMessage, or lite classes.
Require every message type reachable from the payload, including types declared in imported files, to be declared in a file with `syntax = "proto3"`, visiting referenced message types with cycle detection.
Reject a reached message declared in a proto2 file, including `google/protobuf/descriptor.proto`, whose syntax protoc leaves unset, or in an editions file.
Proto3 cannot declare extension ranges or MessageSet wire format, so this also excludes payloads that depend on them; also reject a payload message that declares extensions, which proto3 permits for custom options.
Imports used only for descriptor options do not by themselves make the payload extension-dependent; retain their descriptors and option content in the snapshot.
Invalid classes or settings fail with `IllegalArgumentException` identifying the type or setting and the unsupported condition, before returning type information.

Proto3 values have no required fields, declared groups, closed enums, explicit defaults, or strings without UTF-8 validation, so the serializer, its sizing, and the schema evaluator do not handle them.
Unknown groups still reach proto3 values from the wire, and the size and depth checks below keep covering them.
Development builds through 0.1.0 accepted proto2 payloads.
Constructing type information for a payload type whose graph reaches a proto2 message now fails, so state written for it is restored only by a new schema with a proto3 root in which no path of fields that both schemas keep reaches a message that the saved schema declares in a proto2 file.
Removing a field with a reserved number cuts the paths through it; moving a message on a retained path, or a proto2 root, to proto3 changes its declaring file's syntax, which the evaluator rejects, so an application that keeps such a path must start with fresh state or replay its input.
The retained 0.1.0 development baselines use proto3 types only.

The 0.1.0 acceptance surface includes Struct, Value, ListValue, Any, Timestamp, Duration, Empty, FieldMask, scalar wrappers, and generated OpenTelemetry AnyValue, ArrayValue, KeyValueList, and KeyValue, as tracked in [issue #18](https://github.com/flink-gcp/flink-datastream-protobuf/issues/18).
Changing Struct keys or selecting a different AnyValue variant changes data within the declared schema.
Any preserves its type URL and opaque payload bytes, without lookup, automatic unpacking, or validation of the embedded message's schema.
Supporting these generated types does not implement DynamicMessage, ProtoJSON, OTLP export, or semantic application validation.

### Settings and keys

| Builder setting | Default | Validation |
|---|---|---|
| `deterministicSerialization` | `false` | Explicit opt-in for deterministic payload writing |
| `maxMessageSize` | `67108864` bytes (64 MiB) | Payload byte limit, from 1 through `Integer.MAX_VALUE`, inclusive |
| `recursionLimit` | `100` | Positive integer; the Protobuf parser's recursion budget |

Validate settings at `build()`.
Builder reuse must not mutate previously built type information or serializers.
The factory uses the documented default settings; changes in a later 0.x milestone require the compatibility review and upgrade documentation below.

This integration supports Protobuf messages as values, not as keys, throughout 0.1.0, 0.2.0, and 0.3.0.
This restriction covers both DataStream partitioning keys and MapState user keys; Protobuf messages may be MapState values with supported scalar user keys.
Extract stable scalar keys using Flink's supported scalar types; no message-key opt-in is part of the public API or snapshot format.
`isKeyType()` returns `false` as the type-information declaration.
It is not a universal rejection hook: both `keyBy(selector)` with inferred native key type information and `keyBy(selector, typeInformation)` can accept message keys without consulting that declaration, even with generic types disabled.
Using either path with a message key is unsupported; this library cannot intercept every native Flink key-selection path.

Flink assigns key groups using the key object's `hashCode()` on both sending and receiving tasks.
Generated Protobuf message hash codes incorporate descriptor identity, which is not stable across independent application classloaders or JVMs.
Deterministic serialization cannot repair that hash instability and does not provide canonical bytes across builds.
The earlier proposed message-key opt-in is withdrawn before any production API or snapshot is published; there is no released opt-in API or state format to remove.
Future message-key support requires a separate design proving stable hashing, equality, serialized identity, and recovery across processes; it is not implied by the DynamicMessage release.

Both TypeInformation and serializer equality/hashCode include the concrete Java class identity, Protobuf full name, complete normalized descriptor content, framing/normalization versions, and all three settings.
Class identity here includes the defining classloader; snapshot restore compares the saved Java binary name against the class resolved with the supplied user-code classloader.
`canEqual` follows the same TypeInformation implementation boundary.
Parser/cache object identity and the stored fingerprint are not substitutes for schema content or settings.
Serializer equality is stricter than restore compatibility: unequal configurations may still read the same saved bytes under the directional rules below.

### Message framing version 1

Write a four-byte, big-endian signed integer containing a nonnegative payload length, followed by exactly that many Protobuf wire bytes.
The length excludes its own four bytes; zero is valid for a message whose valid wire representation is empty.
There is no null sentinel, per-record class name, per-record version, or Protobuf varint-delimited outer envelope.
Null messages are invalid inputs.
Reject them with `IOException` before writing the length prefix.
Do not allocate a payload byte array for each record written.
A payload of at most 4 KiB may be encoded into one 4 KiB array that the serializer instance retains and reuses, then written after its exact size is confirmed; larger payloads are written incrementally to the target.
Enable `CodedOutputStream.useDeterministicSerialization()` only when configured.
Framing version 1 identifies this representation in the snapshot, independently of the snapshot format version.

Check serialized size so that integer overflow cannot accept a value, and enforce the configured size and recursion limits before emitting a frame.
The write-side depth check must match the configured parser budget, including nested message/map-entry payloads and unknown groups, so an accepted value is not written beyond its own reader's limits.
The parser reads an unknown field into a declared field of the containing message when the unknown field reuses that field's number with a wire type the field accepts.
A length-delimited value of a message or map field becomes a nested message, including any unknown groups nested in its bytes, and a map entry is such a message whatever the map's value type; a value of any other declared field becomes that field's value, which replaces a singular value already set or selects another oneof member, and invalid UTF-8 or a malformed packed value fails the read.
A field accepts the wire type of its own encoding, and a repeated numeric or enum field also accepts the packed length-delimited encoding, whichever encoding it declares.
Writing rejects a value with such a field with `IOException` before emitting a frame, naming the wire type, the field number, and the declared field, at the root and at every nested or map value; the check uses the containing message's type, so an unknown-field set shared with a type that leaves the number undeclared is still rejected where it collides.
Parsing a proto3 value never produces this state, because the parser merges those bytes into the declared field; only a value whose unknown fields an application set or merged directly can carry one.
Writing also rejects an unknown-field number above 536,870,911, the [largest field number](https://protobuf.dev/programming-guides/proto3/#assigning), at any depth including unknown groups.
A tag is the number shifted left by three bits and combined with the wire type in 32 bits; `UnknownFieldSet` accepts any positive number, and the runtime's `int` shift wraps a larger one either to another number's tag, which the parser then reads as that field, or to a tag for field zero, which the parser rejects as invalid.
The parser keeps unknown fields with undeclared numbers, other wire types, and the content of unknown groups unknown, so they are still sized and written as opaque bytes.
On reading, reject negative or excessive lengths before allocating storage from the length, use a stream bounded to that frame, and require complete valid parsing within it.
A payload of at most 4 KiB may be read completely into the same retained array and parsed without aliasing, so decoded values never refer to reused storage.
A larger payload may be read completely into a new array of exactly its length and parsed with aliasing enabled, through a decoder created from `UnsafeByteOperations.unsafeWrap`, because the public array factory treats its input as mutable and never aliases `ByteString` reads from it; that class is `@ExperimentalApi` in Protobuf 3.25.9 and not in 4.33.6.
The payload is copied into that array through the retained 4 KiB array, so the source never receives an array that decoded values may share; like Protobuf's stream decoder, this keeps decoded values intact even when a source keeps or later writes the arrays it filled.
The array starts at no more than 1 MiB and doubles as bytes arrive, so the arrays allocated for a corrupt prefix within the size limit total at most 1 MiB or less than four times the bytes present, and the read fails with `IOException` when the input ends; only bytes that arrive drive growth, and a growth step that the heap or the JVM's array-size limit cannot satisfy still fails with `OutOfMemoryError`.
The serializer never writes to, reuses, or exposes that array, so values that the runtime decodes as `ByteString`, including `bytes` fields and unknown length-delimited fields, may share it instead of being copied.
Any such value that an application keeps, including one taken from the message, keeps the whole array reachable.
A parser buffer must not consume a subsequent frame; `CodedInputStream.pushLimit` alone does not bound reads from the underlying stream.
Negative lengths, oversized/deep messages, malformed payloads, and truncated frames fail with `IOException` identifying the condition.
No resynchronization after malformed input is promised.
The byte and recursion limits are input constraints, not guarantees about total heap usage or stack capacity.

Generated messages are immutable: object copy returns the same instance and `isImmutableType()` returns true.
Resolve validated defaults, parsers, normalized descriptors, and sizing field layouts through a `ClassValue` cache keyed by the exact generated class; the serializer must survive Java serialization to a TaskManager.
This avoids repeated reflective lookup, schema normalization, and layout construction without a global map that retains application classloaders.
Concurrent first lookups can compute redundant metadata before the cache publishes one result; validation must not depend on running exactly once.
Reconstruct transient runtime objects under the application classloading context without serializing a parser or classloader in the job graph.
Stream copy preserves the complete frame bytes without parsing and reserializing the payload; it checks length and truncation but does not validate payload semantics or nesting.
The current probe uses a simple byte-array implementation solely to exercise Flink's routing and transport.
It is not the production serializer or a stable wire format.

Unless the generated-size path below applies, the serializer implementation checks message size with a dedicated field-wise calculation using `long` arithmetic before emitting the prefix.
It walks nested messages, map entries, and unknown groups with an explicit stack of frames over field layouts that each payload class computes once for every message type it reaches and caches with its other metadata.
Each serializer instance reuses its frames across calls and clears their record references after every call.
A shared child is counted at every occurrence and its depth is checked at each path.
Within one call, a subtree whose sizing examined more than a small constant amount of content is summarized by identity, so a large shared subgraph is not traversed again.
A smaller shared subtree is sized again at each occurrence, which bounds that repeated work by the constant.
The traversal does not trust `MessageLite.getSerializedSize()`, whose documented result can overflow for messages larger than `Integer.MAX_VALUE`, including wrapping to a small positive value; the generated-size path below trusts it only where the scratch encoding confirms it.
Scalar sizing follows the wire encoding, including packed fields, map entries, and UTF-8 strings.
The calculation reads fields through public runtime reflection, so it does not reproduce protoc's Java getter naming rules or access private fields.
Generated field accessors already cache their reflective methods in Protobuf's accessor table; the library does not maintain a second method cache.
The dedicated calculation requires differential tests against both supported runtimes.

A payload type whose reachable type graph is acyclic has a static height: the greatest parser depth that a value can reach without unknown groups or colliding unknown fields.
When that height is within the recursion limit, the serializer reads the value's message fields to find unknown groups, colliding unknown fields, and values reporting another descriptor, and its repeated strings and string-valued maps to bound their wire size from below; without either and within that bound, it uses the runtime's memoized `getSerializedSize()` if the result is at most 4 KiB and the size limit.
Until one of the runtime's `int` sums first exceeds `Integer.MAX_VALUE`, each addend is the exact length that the generated writer emits, so a reported size that wrapped implies at least 2 GiB of output.
Encoding such a value into the 4 KiB array fails before any frame byte reaches the target, and the serializer then runs the traversal, which reports the exceeded limit.
The traversal decides instead for a recursive type or one whose static height exceeds the recursion limit, and for a value with unknown groups, an unknown field that collides with a declared field, or an unknown-field number above the maximum, a value reporting another descriptor, more than 64 counted nested values, repeated strings or string-valued maps whose bound exceeds 4 KiB or the size limit, a generated size above 4 KiB or the size limit, or a runtime failure to compute the size.
The runtime sizes every distinct value in full, including strings, lists, and unknown fields that values share, and measures a String again at each repeated or map occurrence; the count and the string bound limit that repeated work before the traversal rejects an oversized value.
Field reads on this path use the same reflective accessors as the traversal, so it relies on no protoc getter names; the library still keeps no method cache of its own.

`snapshotConfiguration()` captures the versioned descriptors and settings defined below.
Unit tests verify metadata round trips, compatibility decisions, and serializer restoration with isolated user-code classloaders.
The runtime acceptance suite verifies checkpoint recovery and separate-job canonical savepoint restoration with HashMap and RocksDB backends.
See the [runtime evidence](../validation/runtime-recovery.md) for scenarios and limits.

### Complete descriptor normalization version 1

Collect the root message's defining file and the complete transitive import closure as `FileDescriptorProto` content, including declarations unrelated to the root within those files.
Deduplicate by file name; conflicting content for one name or an unresolved import is invalid.
Order files topologically with dependencies first, choosing the lexicographically smallest file name using Java String ordering whenever several files are eligible.
A cyclic file-import graph is invalid; recursive message references within a valid file graph are supported and traversed with a visited set.

Remove only `SourceCodeInfo` from each file.
Preserve file names, syntax/edition, packages, declarations and their order, dependency lists and their index-bearing public/weak import entries, options, defaults, reservations, oneofs, enums, and unknown fields.
Do not reorder dependencies within a file or field/enum declarations, or erase metadata considered irrelevant to wire parsing.
Serialize and parse descriptor content with an empty extension registry before comparing it, so a custom option has the same representation whether the application's descriptor originally knew that option extension or retained it as unknown data.
This does not discard the option bytes.

Compare the entire normalized descriptor message content after decoding both sides with the same descriptor reader.
Do not substitute equality of serialized byte ordering or a stored digest for this content comparison.
Use deterministic serialization to write the normalized FileDescriptorSet and SHA-256 over those exact stored bytes as its fingerprint.
The fingerprint permits integrity checks and comparison optimizations; a digest match alone never establishes compatibility, and a difference between stored fingerprints alone never establishes incompatibility across descriptor readers.
In particular, file iteration order and SourceCodeInfo changes normalize away, while reordered declarations, option changes, or changes to an imported built-in descriptor do not.

### Snapshot format version 1

The 0.1.0 snapshot class is `io.github.flink.gcp.protobuf.ProtobufTypeSerializerSnapshot`, with a public no-argument constructor.
Later releases that support reading this snapshot must retain a loadable reader under that class name.
Published format identifiers must never be reused for a different layout: changing the payload requires a new format version or snapshot class, even during 0.x development.
Flink writes its snapshot-class envelope and the integer returned by `getCurrentVersion()`; return `1`, and do not duplicate that envelope inside `writeSnapshot`.
The version-1 payload has this fixed order:

| Field | Encoding |
|---|---|
| Message framing version | Big-endian `int32`, `1` |
| Descriptor normalization version | Big-endian `int32`, `1` |
| Generated Java binary class name | `DataOutput.writeUTF` / `DataInput.readUTF` |
| Protobuf message full name | `writeUTF` / `readUTF` |
| Deterministic serialization | One byte, `0` or `1` |
| Maximum payload size | Big-endian positive `int32` |
| Recursion limit | Big-endian positive `int32` |
| Descriptor byte length | Big-endian positive `int32` |
| Normalized FileDescriptorSet | Exactly the declared number of bytes |
| Descriptor fingerprint | Exactly 32 SHA-256 bytes, without a length prefix |

The names must be nonempty and fit `writeUTF`'s 65535-byte modified-UTF encoding limit.
The descriptor block has a fixed 64 MiB maximum independent of `maxMessageSize`; validate its length before allocation on read and its size before writing a snapshot.
Reject unsupported snapshot, framing, or normalization versions, invalid flag bytes/settings, invalid or incomplete descriptor graphs, missing named root messages, digest mismatches, and truncated data explicitly.
Snapshot read/write failures use `IOException` with a useful condition; no fallback reader guesses at an unknown format.

`readSnapshot` decodes and validates metadata while retaining Flink's supplied user-code classloader; it does not require the old generated class to load or perform schema compatibility resolution.
Compatibility is resolved by the new snapshot against the old snapshot.
`restoreSerializer` resolves the stored Java binary name using that supplied classloader and validates the resulting class, descriptor identity, and settings before constructing a reader.
Missing, unsupported, or mismatched classes fail with an actionable restore error.
In 0.1.0 the restored class must satisfy the unchanged-schema contract; 0.2.0 uses its supported directional evaluator for value serializers.
Neither release relies on retaining the old generated class in the application jar.

### Unchanged-schema restore in 0.1.0

Return `compatibleAsIs` only for the same snapshot implementation with supported format versions, equal Java binary class and Protobuf message names, equal framing/normalization versions, equal complete normalized descriptors, and reader limits satisfying the directional rules below.
Return `incompatible` for other valid snapshots, differences in those schema/format identities, or a decrease in either reader limit.
Corrupt or unsupported snapshot data fails during reading rather than becoming a successful compatibility result.

| Setting transition from saved to new serializer | Restore result when the schema/format identities match |
|---|---|
| `maxMessageSize` equal or increased, and `recursionLimit` equal or increased | `compatibleAsIs` |
| Either limit decreased, even if the other increased | `incompatible` |
| `deterministicSerialization` changed in either direction | Does not change compatibility; apply the limit rules above |

These rules apply to value serializers, including MapState values; they do not grant compatibility for Protobuf MapState user keys, which are unsupported.
Both resource limits must be nondecreasing because the snapshot records permitted input bounds, not the actual maximum size/depth of every saved value.
For example, increasing 64 MiB to 128 MiB permits restoration of state written under the old limit; reducing it to 32 MiB cannot be accepted without knowing that every saved value fits.
Deterministic mode affects future write ordering, not the readability of previously serialized values.
On `compatibleAsIs`, the new serializer keeps its explicitly requested settings; `restoreSerializer()` reconstructs a reader with the settings recorded by its own snapshot.
Snapshots subsequently emitted by the new serializer record its new settings, so lowering them on a later restart is not implicitly allowed by an older restore decision.
These directional setting rules also apply in 0.2.0, together with its schema evaluator.

All schema changes remaining after normalization are rejected in 0.1.0, including wire-safe additions/removals and enum additions.
No `compatibleAfterMigration` or `compatibleWithReconfiguredSerializer` outcome is provided.
Unchanged-schema acceptance is about reading values; direct Protobuf keys remain unsupported.

### Supported schema evolution in 0.2.0

The separate pure evaluator is added in 0.2.0 and tested with old/new descriptor pairs.
Evaluate compatibility in the direction of restoring saved bytes with the new serializer.
Accept supported wire-safe field additions, removals/reservations, and enum additions, with recursive checks of referenced types.
Reject message or Java class renames, detected field-number reuse, incompatible type/cardinality changes, and moves into oneof.
Treat numeric type changes classed as conditionally safe by Protobuf conservatively as incompatible; parsing alone does not establish preservation of values.
Descriptor pairs cannot prove that a reused number with an indistinguishable declaration still has the same business meaning, so reserving removed numbers remains an application schema obligation.
Reject changes outside the evaluator's explicitly supported rules and apply the directional setting rules above; message keys remain unsupported.
The 0.2.0 path retains the 0.1.0 snapshot reader, normalization definition, framing, and defaults, adding the evaluator without rewriting saved state or requiring an old class.
An intentional departure from that path requires the documented 0.x breaking-change process below; it must not be reported as compatible restore.
The evaluator must consume the complete retained descriptors, including unknown fields and metadata, rather than a reduced 0.1.0 schema summary.
Any wrapper compatibility does not validate the opaque schema of its payload.

The implemented evaluator ([issue #15](https://github.com/flink-gcp/flink-datastream-protobuf/issues/15)) is a package-private function over the saved and new class name, message full name, settings, and normalized descriptor closure.
It reads no snapshot bytes and loads no generated class, and it returns either `compatibleAsIs` or `incompatible` with a diagnostic naming the first incompatible message, field, enum value, file, or setting.
An invalid descriptor closure, or one without the named root, throws `IllegalArgumentException` instead of producing a result.
It checks names and the directional setting rules first, then normalizes both descriptor sets; equal normalized content is compatible, and no fingerprint takes part in the decision.

Snapshot integration ([issue #16](https://github.com/flink-gcp/flink-datastream-protobuf/issues/16)) calls the evaluator from both decision points.
`resolveSchemaCompatibility` evaluates the saved snapshot against the new snapshot and maps the result to `compatibleAsIs` or `incompatible`.
`restoreSerializer` evaluates the saved metadata against the class resolved through the user-code classloader, keeping the saved settings, so only the message name and schema can differ there.
Flink's `incompatible` result carries no reason, so the evaluator's diagnostic reaches the user through the restore exception; the [runtime evidence](../validation/runtime-recovery.md#schema-changes) records where it appears.

Otherwise, the evaluator pairs types by their resolved full names, starting at the root and following each field number present in both schemas.
Only these reached messages and enums are compared, together with their enclosing messages and declaring files, because saved bytes cannot select any other declaration.
A declaration that only a removed or newly added field references is therefore not compared, and neither is an unrelated declaration, service, or import change.
The declared type of an Any payload is selected by data, so the evaluator never reaches it.
Within the reached declarations, everything must match except the following supported differences:

| Supported difference | Condition |
|---|---|
| Field addition | The number is not a saved field or reservation, and the name is not a saved field's name or reservation |
| Field removal | The new message reserves the removed number; reserving the name is optional |
| Added reservation | Saved number and name reservations remain covered, even if ranges are split or merged |
| Enum value addition | The name and number are new and unreserved, the number is not an alias of a saved value, the first declared value stays first, and saved values keep their relative order |
| Oneof addition or extension | Every member of a new oneof, and every new member of an existing oneof, is an added field |

Every other difference in reached declarations is incompatible, including renames, renumbering, type or cardinality changes, presence changes, moves into, out of, or between oneofs, JSON names, options, reordering of retained fields, oneofs, or enum values, and unknown descriptor content.
Declaring-file syntax, edition, package, options, and unknown content must also match, because they determine inherited features such as presence, packing, enum openness, and UTF-8 validation.
A type that moves to another file or enclosing scope is rejected even when its full name is unchanged.
The descriptor set's own unknown content must be equal.

Requiring a reservation on removal makes later reuse of that number detectable: a new field with that number must remove the reservation, which the evaluator rejects.
A number removed without reservation could later return with a different declaration, and values preserved as unknown fields would then be parsed as that declaration.
The evaluator still cannot detect a field whose number, name, type, and options are unchanged but whose meaning has changed; that remains the application's schema obligation.

A descriptor pair also cannot show which unknown fields the saved values carry.
Binary parsing preserves unknown fields, so saved values can contain a number that the saved descriptor does not declare: from an earlier removal without reservation, or from input that was parsed from bytes written with a different schema.
When an added field uses such a number, the new parser reads the retained value as that field.
On 2026-10-11, a probe with Protobuf 3.25.9 generated classes confirmed the three outcomes: a value with the same wire type becomes the field's value, a different wire type stays unknown, and invalid UTF-8 retained under a new proto3 `string` field makes parsing fail.
Accepting a field addition therefore assumes that no saved value carries an unknown field with that number; the evaluator cannot verify that assumption.

### Compatibility policy before 1.0.0

The unpublished 0.x milestones are an initial development period; public APIs, defaults, supported dependency combinations, message framing, snapshot formats, and supported restore paths may change incompatibly.
The maintainer selected this policy to allow the generated-message and DynamicMessage designs to mature before stabilizing them at 1.0.0.
The current API and format definitions guide 0.1.0 implementation; they do not freeze all subsequent 0.x designs.
Prefer compatible extensions when practical, and record the design benefit and migration cost when a breaking change is chosen.
Neither `@PublicEvolving` nor a version number alone communicates the impact to an application author.

Every intentional 0.x break must be documented before milestone completion in the affected ADR, development change notes, upgrade guide, and compatibility matrix.
Identify affected development artifacts, APIs/defaults or state formats, the supported upgrade direction, and the exact application or operational steps required.
If saved state cannot be restored or migrated, say so explicitly and document that starting with fresh state or replaying input is required, including the application's responsibility for resulting state/data effects.
Do not imply that a migration tool or compatible reader exists unless it has been implemented and tested.
A blanket 0.x warning is not sufficient to waive an unexplained regression.

API source/binary compatibility, state-byte compatibility, and application gencode/runtime compatibility remain separate checks.
For supported unchanged-API paths, run already-compiled consumers against the new artifact without recompilation.
For an intentional API break, retain evidence that identifies the affected usage and verify the documented replacement with compiled examples.
A known API difference may be accepted only through an explicit, narrowly scoped record linked to the breaking-change documentation; unrelated failures still block milestone completion.
For each claimed state restore or migration path, assert restored values and continued processing using attributable fixtures from the fixed earlier development artifacts.
For a declared unsupported path, test explicit rejection without falling back to generic serialization, guessing at unknown formats, or reporting successful restore while dropping state.

Track direct and sequential upgrade outcomes rather than assuming transitive compatibility:

| Candidate development upgrade path | Required decision and evidence |
|---|---|
| 0.1.0 to 0.2.0 | The 0.2.0 restore path from [#16](https://github.com/flink-gcp/flink-datastream-protobuf/issues/16), using the evaluator from [#15](https://github.com/flink-gcp/flink-datastream-protobuf/issues/15), restores supported 0.1.0 state; verify that path, or document and test an intentional breaking change with its replacement procedure |
| 0.1.0 directly to 0.3.0 | State whether direct upgrade works, requires an intermediate version or migration, or is unsupported; test the claimed result |
| 0.2.0 to 0.3.0 | Assess generated-message applications as well as dynamic additions, including state written after supported 0.2.0 schema evolution |
| 0.1.0 through 0.2.0 to 0.3.0 | If offered, test each step with newly emitted snapshots and the resulting schema/settings; direct-path success does not prove this path |

Keep the application schema, compatible gencode/runtime pair, and Flink version fixed in library-only upgrade controls; test changes to those inputs separately.
The tested Protobuf profile matrix does not promise restore across changed built-in descriptors, cross-Flink-minor restore, or downgrade support.

DynamicMessage requires an explicit root descriptor because its Java class alone does not identify a schema.
Issue #21 must compare shared type-information/serializer/snapshot implementations with separate implementations before selecting the API and persisted representation for 0.3.0.
A common construction API or a versioned snapshot with an explicit generated/dynamic discriminator is allowed; separate classes are also allowed when their restoration responsibilities justify them.
Share framing, descriptor normalization, and compatibility evaluation where their semantics agree.
Assess type safety, configuration/validation, classloading, restoration, test coverage, and the impact on existing generated-message development applications; compatibility is one design constraint rather than a requirement to choose separation.
Any change to the generated snapshot layout uses a new version or class and follows the breaking-change process if an old restore path is dropped.
Automatic generated-to-dynamic saved-state conversion remains outside the release scope unless separately designed and tested; unifying implementation classes does not establish that conversion.

The current capability roadmap ends at 0.3.0; the 1.0.0 milestone tracks stabilization and first publication, without a calendar deadline or automatic completion after 0.3.0.
After 0.3.0, assess remaining design issues, missing implementation and validation gaps.
If further development is needed, define 0.4.0 or later 0.x milestones from that assessment; otherwise, refine the compatibility contract and prepare 1.0.0.
Version 1.0.0 is the stabilization point for the application-facing API, configuration defaults, persisted formats and supported forward-restore contract.
Before publishing it, resolve the generated/dynamic construction and restoration design, define the supported runtime and upgrade matrix, and verify those guarantees against retained development and candidate fixtures.
Consumer verification and fixture capture from the actual published 1.0.0 artifacts complete the publication gate; no earlier published artifact exists.
Within 1.x, preserve the declared public API and supported state-reading paths; any future incompatible contract change requires a new major version and migration documentation.
The 0.x-to-1.0.0 transition must have its own explicit upgrade guidance and evidence; 1.0.0 does not retroactively guarantee all development APIs or state formats.

### Development fixtures and publication evidence

For milestone-to-milestone checks, retain attributable snapshot and message-byte fixtures, complete savepoints, and expected restored values from fixed development artifacts, including the 0.1.0 baseline.
Record the exact source revision and packaged artifact checksum; a newer working-tree build cannot stand in for that writer.
For first-publication acceptance, capture and consumer-verify fixtures written by both published 1.0.0 artifact lines.
Development fixtures do not satisfy this published-artifact gate.
Record the Maven artifact coordinates/checksum, source tag/commit, snapshot class/version, settings, Flink/JDK/protoc/runtime versions, `.proto` sources/imports and descriptors, generation commands, and job/state identity needed to reproduce each fixture.
Preserve both deterministic modes, scalar-keyed and operator value state, representative resource settings, and representative nested/unknown-field and supported generated-type cases.
Test the current setting contract: unchanged settings, each monotonic limit increase, both deterministic-mode transitions, and rejected decreases, including decreases after an earlier increase and new snapshot emission.
Do not regenerate or overwrite released fixtures using a newer writer and call them evidence from the original release.
Issues [#10](https://github.com/flink-gcp/flink-datastream-protobuf/issues/10), [#11](https://github.com/flink-gcp/flink-datastream-protobuf/issues/11), and [#13](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13) own format fixtures, development runtime savepoints and capture/restore tooling, and published-artifact provenance respectively.
Actual capture and consumer verification using both published 1.0.0 artifact lines are completion gates of #13 and the 1.0.0 milestone; they are not required to close the development runtime work in #11.
Retain the original 0.1.0 fixtures and add independently attributable 0.2.0 and later fixture sets, including fixtures for formats that a later release explicitly stops supporting.
Supported generated-message restore tests use the new application with old generated classes absent.
Current aggregate CI, successful tests of every claimed supported path, explicit rejection of unsupported paths, and complete breaking-change documentation are release gates.
The first release is not complete until publication, consumer verification, and preservation of that release's fixtures.

### Source basis

The Protobuf guide distinguishes wire-safe changes from conditionally safe changes that may lose values.
See [Updating a message type](https://protobuf.dev/programming-guides/proto3/#updating), [runtime compatibility](https://protobuf.dev/support/cross-version-runtime-guarantee/), and [non-canonical serialization](https://protobuf.dev/programming-guides/serialization-not-canonical/).
Flink 2.3's [TypeSerializerSnapshot contract](https://github.com/apache/flink/blob/release-2.3.0/flink-core/src/main/java/org/apache/flink/api/common/typeutils/TypeSerializerSnapshot.java) specifies the version envelope, supplied classloader, and new-to-old snapshot comparison.
Protobuf's [CodedInputStream](https://github.com/protocolbuffers/protobuf/blob/v33.6/java/core/src/main/java/com/google/protobuf/CodedInputStream.java) describes recursion/size limits and why `pushLimit` alone does not bound the underlying stream.
Its [CodedOutputStream](https://github.com/protocolbuffers/protobuf/blob/v33.6/java/core/src/main/java/com/google/protobuf/CodedOutputStream.java) defines the limited deterministic-serialization guarantee.
Flink 2.3's [KeyedStream](https://github.com/apache/flink/blob/release-2.3.0/flink-runtime/src/main/java/org/apache/flink/streaming/api/datastream/KeyedStream.java) validates hashability without a general `isKeyType()` check; [KeyGroupRangeAssignment](https://github.com/apache/flink/blob/release-2.3.0/flink-runtime/src/main/java/org/apache/flink/runtime/state/KeyGroupRangeAssignment.java) assigns groups from the key object's hash code.
Generated [StringValue](https://github.com/protocolbuffers/protobuf/blob/v33.6/java/core/src/main/java/com/google/protobuf/StringValue.java) mixes in its [descriptor](https://github.com/protocolbuffers/protobuf/blob/v33.6/java/core/src/main/java/com/google/protobuf/Descriptors.java) hash code.
On 2026-09-07, disposable checks with the pinned dependencies confirmed graph construction despite `isKeyType() == false` on Flink 2.3.0, and different hashes/key groups for identical StringValue bytes in isolated classloaders with both Protobuf 3.25.8 and 4.33.6.
On 2026-09-08, further disposable checks confirmed both inferred and explicit keyBy graph construction with native non-key type information on JDK 17/21 and both pinned Protobuf profiles.
Those checks establish the unsupported-key counterexamples, not a production cluster recovery result.
These APIs exist in both pinned runtime profiles; their existence does not establish production acceptance coverage.

## Alternatives

Keeping byte arrays or an application envelope is suitable when operators mostly forward payloads; this library targets applications that operate on generated messages.
A Kryo adapter leaves the message in Flink's generic-type path and does not meet the requirement to disable generic types.
It is outside this library's staged release scope.
Exact full-descriptor equality deliberately rejects all schema changes in 0.1.0; using it as the permanent value-state policy would unnecessarily reject the compatible changes planned for 0.2.0.
Fingerprint equality alone is insufficient in either development milestone.

Caching a method handle for each generated getter would make the remaining traversal cheaper, but it requires reproducing protoc's Java accessor names or reaching private accessor tables.
The generated-size path removes reflective reads of scalar fields for bounded types without either, and recursive types keep the traversal.

Validating a colliding unknown field instead of rejecting it, by parsing its bytes as the declared field's message within the remaining recursion budget, would accept every such value that its reader accepts.
It would allocate a message for each collision, and a parse reports only whether the bytes fit the budget, not their depth, so per-call summaries could not reuse the result at a deeper path.
For other declared fields, rejecting only the encodings that fail the read would still let a well-formed value replace a set field or switch a oneof member when it reads back.
Rejecting costs one lookup of each unknown-field number among the declared fields, inside the pass over a value's unknown fields that sizing already makes; a set that a per-call summary reuses under another containing type is instead probed with that type's declared numbers, without copying or scanning the set, and the check never affects a value that parsing produced; documenting the gap instead would leave the write-side depth guarantee open for message and map fields and let other values read back changed or not at all.

Streaming a payload above 4 KiB through Protobuf's coded-input buffer, as before issue #113, needs no payload-sized array, but the pinned runtimes' stream decoder copies a `bytes` value that does not fit in that buffer twice: into one array and then into its `ByteString`, or, for an unbuffered remainder of at least 4 KiB that the stream does not report as available, into chunks and then into one array.
Reading into a new array and parsing it without aliasing still copies each value once after the frame copy.
Reading directly into the final array would avoid one copy of the payload, but it would hand the source an array that decoded values share.
Allocating the declared length before reading would avoid the growth copies above 1 MiB, but a corrupt prefix could then allocate up to the size limit and fail with `OutOfMemoryError` instead of `IOException`.
Aliasing removes the value copies at the cost of retaining the frame array through decoded values; the [large-read report](../validation/serializer-large-read.md) measures aliasing against streaming.

Supporting proto2 payloads, as development builds through 0.1.0 did, keeps required fields, declared groups, closed enums, explicit defaults, and strings without UTF-8 validation in the serializer, its sizing, and the schema evaluator.
Proto2 also adds unknown fields that the parser reinterprets under a declared field, which the write-side depth check of [issue #120](https://github.com/flink-gcp/flink-datastream-protobuf/issues/120) would otherwise have to cover: parsing keeps a closed-enum map entry with an unknown value as an unknown field under the map's number, and an unknown group can match a declared group field.
The maintainer chose to drop proto2 because no new application is expected to define its payload in it.
Protobuf documents [editions](https://protobuf.dev/editions/overview/) as the successor to both proto2 and proto3, and editions files can express the same proto2 features.
Accepting only editions files whose resolved features match proto3 would keep a migrated proto3 file usable, but it adds a feature-resolution rule that the maintainer declined; an application that converts a proto3 file to editions must keep it in proto3 to use this library.

Cross-Flink-version savepoint fixtures, lite messages, extensions, renamed types, and a Kryo adapter are later design work, not current compatibility claims.
