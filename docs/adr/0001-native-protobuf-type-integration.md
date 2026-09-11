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
- Release contract amended: 2026-09-08, [issue #7](https://github.com/flink-gcp/flink-datastream-protobuf/issues/7)
- Implementation: Serializer implemented; type integration and snapshots pending
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

### Release stages

The maintainer selected an incremental first release on 2026-09-06.
The descriptor-based design remains the target, with its compatibility evaluator delivered after the first usable library.
These are release requirements; the serializer is implemented, while application-facing integration and state compatibility remain unimplemented.

| Release | Required outcome |
|---|---|
| [0.1.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/6) | Generated full-runtime messages, native type integration and transport, and unchanged-schema checkpoint/savepoint restore; common Google Well-Known Types, OpenTelemetry generated composite messages, GitHub Pages documentation, and a verified Maven Central release |
| [0.2.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/14) | Directional descriptor compatibility evaluation and restoration of supported schema changes from state written by the published 0.1.0 artifact |
| [0.3.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/20) | Explicit-descriptor DynamicMessage integration, with its API/state upgrade behavior documented and tested under the 0.x policy below |

### Type integration and public entry points

Implement `ProtobufTypeInformation`, `ProtobufTypeInfoFactory`, `ProtobufTypeSerializer`, and `ProtobufTypeSerializerSnapshot` under `io.github.flink.gcp.protobuf`.
The application-facing construction API for 0.1.0 is:

```java
public static <T extends Message> ProtobufTypeInformation<T> of(Class<T> messageClass);
public static <T extends Message> Builder<T> newBuilder(Class<T> messageClass);
```

The public static nested `Builder<T extends Message>` provides `deterministicSerialization(boolean)`, `maxMessageSize(int)`, and `recursionLimit(int)`, each returning the same builder, and `build()` returning a new immutable `ProtobufTypeInformation<T>`.
`of(messageClass)` is equivalent to `newBuilder(messageClass).build()` with the defaults below.
The snippets define the planned API; they are not currently available usage examples.
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
Reject a payload message graph containing extension declarations, extension ranges, or MessageSet wire format, visiting referenced message types with cycle detection.
Imports used only for descriptor options do not by themselves make the payload extension-dependent; retain their descriptors and option content in the snapshot.
Invalid classes or settings fail with `IllegalArgumentException` identifying the type or setting and the unsupported condition, before returning type information.

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
The factory uses the documented release defaults; changes in a later 0.x release require the compatibility review and upgrade documentation below.

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
Null and uninitialized messages are invalid inputs.
Reject them with `IOException` before writing the length prefix.
Write the payload without an intermediate payload byte array, enabling `CodedOutputStream.useDeterministicSerialization()` only when configured.
Framing version 1 identifies this representation in the snapshot, independently of the snapshot format version.

Check serialized size without integer-overflow arithmetic and enforce the configured size and recursion limits before emitting a frame.
The write-side depth check must match the configured parser budget, including nested message/map-entry payloads and unknown groups, so an accepted value is not written beyond its own reader's limits.
On reading, reject negative or excessive lengths before allocating storage from the length, use a stream bounded to that frame, and require complete valid parsing within it.
A parser buffer must not consume a subsequent frame; `CodedInputStream.pushLimit` alone does not bound reads from the underlying stream.
Negative lengths, oversized/deep messages, malformed payloads, and truncated frames fail with `IOException` identifying the condition.
No resynchronization after malformed input is promised.
The byte and recursion limits are input constraints, not guarantees about total heap usage or stack capacity.

Generated messages are immutable: object copy returns the same instance and `isImmutableType()` returns true.
Resolve validated defaults, parsers, and normalized descriptors through a `ClassValue` cache keyed by the exact generated class; the serializer must survive Java serialization to a TaskManager.
This avoids repeated reflective lookup and schema normalization without a global map that retains application classloaders.
Concurrent first lookups can compute redundant metadata before the cache publishes one result; validation must not depend on running exactly once.
Reconstruct transient runtime objects under the application classloading context without serializing a parser or classloader in the job graph.
Stream copy preserves the complete frame bytes without parsing and reserializing the payload; it checks length and truncation but does not validate payload semantics or nesting.
The current probe uses a simple byte-array implementation solely to exercise Flink's routing and transport.
It is not the production serializer or a stable wire format.

The serializer implementation checks message size with a dedicated field-wise calculation using `long` arithmetic before emitting the prefix.
It uses an explicit traversal stack for nested messages and unknown groups, retaining per-call size and depth summaries for shared child instances.
This avoids trusting `MessageLite.getSerializedSize()`, whose documented result can overflow for messages larger than `Integer.MAX_VALUE`, including wrapping to a small positive value.
Scalar sizing follows the wire encoding, including packed fields, map entries, UTF-8 strings, and retained proto2 string bytes.
Ordinary values need one output encoding pass.
For a proto2 string without UTF-8 validation, a decoded replacement character can hide retained invalid bytes, so the reflected String does not establish the wire length.
The implementation computes a lower size bound and validates the entire graph, then counts the root's generated wire output through a bounded sink before writing the actual frame.
This uses public runtime APIs and avoids reproducing protoc's Java getter naming rules or accessing private raw-field methods.
The counting sink retains no payload bytes and uses one additional fixed-size encoder buffer; it counts the root once even when multiple nested strings are ambiguous.
Generated field accessors already cache their reflective methods in Protobuf's accessor table; the library does not maintain a second method cache.
The dedicated calculation requires differential tests against both supported runtimes; performance measurements must distinguish ordinary values from the counting path.

`snapshotConfiguration()` currently fails explicitly with an `UnsupportedOperationException` identifying issue #10.
This is an intermediate implementation boundary, not a release contract: 0.1.0 cannot be published until the versioned snapshot implementation replaces that failure and the state acceptance tests pass.

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
In 0.1.0 the restored class must satisfy the unchanged-schema contract; 0.2.0 may use its supported directional evaluator for value serializers.
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
Reject message or Java class renames, detected field-number reuse, required-field additions, incompatible type/cardinality changes, and moves into oneof.
Treat numeric type changes classed as conditionally safe by Protobuf conservatively as incompatible; parsing alone does not establish preservation of values.
Descriptor pairs cannot prove that a reused number with an indistinguishable declaration still has the same business meaning, so reserving removed numbers remains an application schema obligation.
Reject changes outside the evaluator's explicitly supported rules and apply the directional setting rules above; message keys remain unsupported.
The planned 0.2.0 path retains the 0.1.0 snapshot reader, normalization definition, framing, and defaults, adding the evaluator without rewriting saved state or requiring an old class.
An intentional departure from that path requires the documented 0.x breaking-change process below; it must not be reported as compatible restore.
The evaluator must consume the complete retained descriptors, including unknown fields and metadata, rather than a reduced 0.1.0 schema summary.
Any wrapper compatibility does not validate the opaque schema of its payload.

### Compatibility policy before 1.0.0

The 0.x releases are an initial development period; public APIs, defaults, supported dependency combinations, message framing, snapshot formats, and supported restore paths may change incompatibly.
The maintainer selected this policy to allow the generated-message and DynamicMessage designs to mature before stabilizing them at 1.0.0.
The current API and format definitions guide 0.1.0 implementation; they do not freeze all subsequent 0.x designs.
Prefer compatible extensions when practical, and record the design benefit and migration cost when a breaking change is chosen.
Neither `@PublicEvolving` nor a version number alone communicates the impact to an application author.

Every intentional 0.x break must be documented before publication in the affected ADR, release notes, upgrade guide, and release compatibility matrix.
Identify affected releases, APIs/defaults or state formats, the supported upgrade direction, and the exact application or operational steps required.
If saved state cannot be restored or migrated, say so explicitly and document that starting with fresh state or replaying input is required, including the application's responsibility for resulting state/data effects.
Do not imply that a migration tool or compatible reader exists unless it has been implemented and tested.
A blanket 0.x warning is not sufficient to waive an unexplained regression.

API source/binary compatibility, state-byte compatibility, and application gencode/runtime compatibility remain separate checks.
For supported unchanged-API paths, run already-compiled consumers against the new artifact without recompilation.
For an intentional API break, retain evidence that identifies the affected usage and verify the documented replacement with compiled examples.
A known API difference may be accepted only through an explicit, narrowly scoped record linked to the breaking-change documentation; unrelated failures still block publication.
For each claimed state restore or migration path, assert restored values and continued processing using published-artifact fixtures.
For a declared unsupported path, test explicit rejection without falling back to generic serialization, guessing at unknown formats, or reporting successful restore while dropping state.

Track direct and sequential upgrade outcomes rather than assuming transitive compatibility:

| Candidate upgrade path | Required release decision and evidence |
|---|---|
| 0.1.0 to 0.2.0 | The planned evaluator restores supported 0.1.0 state; verify that path, or document and test an intentional breaking change with its replacement procedure |
| 0.1.0 directly to 0.3.0 | State whether direct upgrade works, requires an intermediate version or migration, or is unsupported; test the claimed result |
| 0.2.0 to 0.3.0 | Assess generated-message applications as well as dynamic additions, including state written after supported 0.2.0 schema evolution |
| 0.1.0 through 0.2.0 to 0.3.0 | If offered, test each step with newly emitted snapshots and the resulting schema/settings; direct-path success does not prove this path |

Keep the application schema, compatible gencode/runtime pair, and Flink version fixed in library-only upgrade controls; test changes to those inputs separately.
The tested Protobuf profile matrix does not promise restore across changed built-in descriptors, cross-Flink-minor restore, or downgrade support.

DynamicMessage requires an explicit root descriptor because its Java class alone does not identify a schema.
Issue #21 must compare shared type-information/serializer/snapshot implementations with separate implementations before selecting the API and persisted representation for 0.3.0.
A common construction API or a versioned snapshot with an explicit generated/dynamic discriminator is allowed; separate classes are also allowed when their restoration responsibilities justify them.
Share framing, descriptor normalization, and compatibility evaluation where their semantics agree.
Assess type safety, configuration/validation, classloading, restoration, test coverage, and the impact on published generated-message applications; compatibility is one design constraint rather than a requirement to choose separation.
Any change to the generated snapshot layout uses a new version or class and follows the breaking-change process if an old restore path is dropped.
Automatic generated-to-dynamic saved-state conversion remains outside the release scope unless separately designed and tested; unifying implementation classes does not establish that conversion.

The current roadmap ends at 0.3.0; 1.0.0 is a stabilization goal rather than a scheduled release or an automatic next step.
After 0.3.0, assess remaining design issues, missing implementation and validation gaps.
If further development is needed, define 0.4.0 or later 0.x milestones from that assessment; otherwise, refine the compatibility contract and prepare 1.0.0.
Version 1.0.0 is the stabilization point for the application-facing API, configuration defaults, persisted formats and supported forward-restore contract.
Before publishing it, resolve the generated/dynamic construction and restoration design, define the supported runtime and upgrade matrix, and verify those guarantees against released fixtures.
Within 1.x, preserve the declared public API and supported state-reading paths; any future incompatible contract change requires a new major version and migration documentation.
The 0.x-to-1.0.0 transition must have its own explicit upgrade guidance and evidence; 1.0.0 does not retroactively guarantee all 0.x APIs or state formats.

### Released fixtures and publication evidence

Retain attributable snapshot and message-byte fixtures, complete savepoints and expected restored values written by the published 0.1.0 artifact.
Record the Maven artifact coordinates/checksum, source tag/commit, snapshot class/version, settings, Flink/JDK/protoc/runtime versions, `.proto` sources/imports and descriptors, generation commands, and job/state identity needed to reproduce each fixture.
Preserve both deterministic modes, scalar-keyed and operator value state, representative resource settings, and representative nested/unknown-field and supported generated-type cases.
Test the current setting contract: unchanged settings, each monotonic limit increase, both deterministic-mode transitions, and rejected decreases, including decreases after an earlier increase and new snapshot emission.
Do not regenerate or overwrite released fixtures using a newer writer and call them evidence from the original release.
Issues [#10](https://github.com/flink-gcp/flink-datastream-protobuf/issues/10), [#11](https://github.com/flink-gcp/flink-datastream-protobuf/issues/11), and [#13](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13) own format fixtures, runtime savepoints, and published-artifact provenance respectively.
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
Fingerprint equality alone is insufficient in either release.

Cross-Flink-version savepoint fixtures, lite messages, extensions, renamed types, and a Kryo adapter are later design work, not current compatibility claims.
