---
title: Usage and configuration
weight: 20
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

# Configure native Protobuf values

The [quickstart](quickstart.md) builds and runs the application examples.
The public application entrypoints are `ProtobufTypeInformation` and `ProtobufTypeInfoFactory`.
The serializer and snapshot classes are internal machinery; construct types through the public factory or builder.

## Explicit type information

Use explicit type information for `.returns(...)` and per-type settings.
The following construction example uses the generated full-runtime `Event` class from the quickstart.
The [explicit example](../examples/datastream/src/main/java/io/github/flink/gcp/protobuf/examples/ExplicitExample.java) shows the imports for these types.

`ProtobufTypeInformation` comes from this library; `Event` comes from the application's generated code.
Replace the `Event` import and class reference with the application's message type.
The example's [`event.proto`](../examples/datastream/src/main/proto/event.proto) sets `java_package = "io.github.flink.gcp.protobuf.examples.generated"` and `java_multiple_files = true`, so `Event` is a top-level class in that Java package.

Construct the default type or configure it through the builder:

```java
var defaults = ProtobufTypeInformation.of(Event.class);
var configured = ProtobufTypeInformation.newBuilder(Event.class)
        .deterministicSerialization(true)
        .maxMessageSize(1024 * 1024)
        .recursionLimit(64)
        .build();
```

The defaults disable deterministic writing, allow a 64 MiB payload and parser recursion depth of 100, and enable [large-payload aliasing](#memory-of-large-decoded-messages).
Numeric limits must be positive; `build()` validates settings and the generated class.
`of(Class<T>)` matches Flink's unbounded static factory signature, so unsupported classes can compile but fail with `IllegalArgumentException` at construction.
The builder also enforces `T extends Message` at compile time.
Use `of(Event.class)` or the builder for explicit native selection.
The inherited `of(TypeHint)` overload follows Flink's ordinary type extraction and can select a generic type when the factory is not registered.
Builder reuse does not change previously built type information or serializers.
These limits constrain accepted inputs, not total heap usage or stack capacity.

Serialization rejects a value that exceeds either limit with `IOException` before writing its frame.
It also rejects a value with an unknown field that reuses the number of a field that the containing message declares, with a wire type that field accepts, naming the wire type, the field number, and the declared field.
The reader would parse such a field into the declared one.
A length-delimited value of a message or map field becomes that field's message, whose nesting could exceed the recursion limit that the writer checked; any other value becomes that field's value, which replaces a singular value already set, adds an element to a repeated field, selects another oneof member, or fails the read for invalid UTF-8 or a malformed packed value.
A repeated numeric or enum field accepts both its packed and its unpacked encoding.
Parsing never produces such a field, because the parser merges those bytes into the declared field; only a value whose unknown fields the application set or merged directly can carry one, for example unknown fields copied from a message of another type.
Earlier development builds, including 0.1.0, wrote such values, so an application that builds them now fails when serializing them.
State that those builds wrote is not affected: reading either merges the bytes into the declared field, so restored values no longer carry the collision, or fails for malformed content, as it already did with those builds.
Serialization likewise rejects an unknown-field number above 536,870,911, the [largest field number](https://protobuf.dev/programming-guides/proto3/#assigning), because its wire tag would wrap to another field's tag, which the reader then parses as that field, or to an invalid tag for field zero, which fails the read.
Other unknown fields are written unchanged, and the reader keeps them as unknown fields: those with undeclared numbers, those with a wire type that the declared field does not accept, and everything inside unknown groups.

## Memory of large decoded messages

Aliasing of large payloads is enabled by default.
A payload above 4 KiB is read into its own array, and decoded `bytes` fields and unknown length-delimited fields may share that array instead of copying it.
Disable it with `largePayloadAliasing(false)` on the builder:

```java
var copying = ProtobufTypeInformation.newBuilder(Event.class)
        .largePayloadAliasing(false)
        .build();
```

Payloads of at most 4 KiB never share storage, and the setting changes neither the wire bytes, state compatibility, nor decoded values.

| | Aliasing enabled (default) | Aliasing disabled |
|---|---|---|
| Large-payload read | In the [large-read report](validation/serializer-large-read.md), reading a 64 KiB payload takes 0.43 to 0.44 times the previous serializer's time and 0.54 to 0.55 times Chill's | Each shared value is copied again after the payload is read; not measured on the benchmark protocol |
| Allocation per large read | About the payload size for a 64 KiB `bytes` payload in the same report | The payload array plus a copy of each shared value |
| Memory of kept values | Keeping any value that shares the array, including a `ByteString` taken from the message, keeps the whole payload array reachable | Each value owns its own copy |

Disable aliasing when an application keeps decoded values for long:

- A small `ByteString` that shares a large message's array, such as a `bytes` field value, keeps that whole array while it is kept in a long-lived map, cache, or heap state.
  With aliasing enabled, copying that one value instead also works, for example with `ByteString.copyFrom(value.asReadOnlyByteBuffer())`.
- Large messages kept in heap state, such as `HashMapStateBackend` values updated from records or restored from a checkpoint, keep their arrays when they hold a value that shares it.
  When a message mixes such a value with large other content, such as proto3 strings or repeated numbers, the array also holds that content's wire form in addition to its decoded form; how much more memory this takes depends on the record's shape.

Records that are processed and then dropped, and values read from RocksDB state for one access, keep their arrays only as long as the application keeps the values, so aliasing costs no retained memory there.

HashMap state and operator state restored from a checkpoint or savepoint are read with the setting recorded when that state was saved, and RocksDB keyed state with the new setting.
A changed setting reaches those restored heap values from the next restore after a new checkpoint or savepoint, for state that the restored job registers again.

## Factory registration

The example's [config.yaml](../examples/datastream/config/config.yaml) contains ordinary [Flink pipeline configuration](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/config/#pipeline-serialization-config).
It holds the two options needed for this registration example; the library does not define a separate configuration-file format.
Add these options to the Flink configuration used when constructing the job, or load a configuration directory explicitly as shown below.
If `pipeline.serialization-config` already has entries for other types, add this entry to the existing list.

Apply the configuration before any type extraction, including `TypeInformation.of`, `TypeHint`, and source/operator construction:

{{< example "config.yaml#configuration" >}}

### What each entry means

| Entry | Meaning and value to use |
|---|---|
| `pipeline.generic-types: false` | Disable Flink's generic serialization path so missing native type information fails instead of silently selecting Kryo. This option alone does not register the Protobuf factory. |
| `pipeline.serialization-config` | Flink's list of type/serialization registrations. Keep the YAML list and mapping structure in the linked `config.yaml`. |
| `com.google.protobuf.AbstractMessage` | The fully qualified Protobuf superclass to register, supplied by `protobuf-java`. Flink's superclass lookup reaches supported generated subclasses through this type. |
| `type: typeinfo` | Select Flink's TypeInfoFactory registration mechanism. Use this value for native type information. |
| `class: io.github.flink.gcp.protobuf.ProtobufTypeInfoFactory` | The fully qualified factory class supplied by this library. This is the class Flink instantiates to construct native type information. |

Use both fully qualified class names exactly as listed in the table; they are class names, not package prefixes or wildcard patterns.
The `class` value is the factory, not `ProtobufTypeInformation`, `ProtobufTypeSerializer`, or an application's generated message class.
For example, the application's message is `io.github.flink.gcp.protobuf.examples.generated.Event`, but that name does not replace either class name in this superclass registration.
The same registration covers supported generated classes in other Java packages, so a separate entry for each message is unnecessary.
The library jar, generated application classes, and matching `protobuf-java` runtime must be available on the application's classpath; YAML does not install these dependencies.
See Flink's [type information factory documentation](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/serialization/types_serialization/#defining-type-information-using-a-factory) for the underlying mechanism.

### Load the configuration before constructing the job

The factory class is named in YAML, so this Java code needs no `ProtobufTypeInfoFactory` import.
The [registration application](../examples/datastream/src/main/java/io/github/flink/gcp/protobuf/examples/RegisteredExample.java) reads the actual [config.yaml](../examples/datastream/config/config.yaml) before constructing its job:

{{< example "RegisteredExample#registration" >}}

The argument `args[0]` names the directory containing `config.yaml`, not the YAML file itself.
In the [quickstart](quickstart.md#run-registration-and-state-examples), `exec:exec@registered` passes `examples/datastream/config` as an absolute path to the application.
For another application, supply a configuration directory that exists where its entrypoint runs and pass the loaded `Configuration` to the environment before creating sources or operators.
Loading YAML produces a `Configuration`; configuring the environment applies the factory registration.
Placing a file beside a job jar without loading it does not apply these options.

### Registration scope and per-type settings

On Flink 1.20, put the YAML list in `config.yaml`.
The legacy `flink-conf.yaml` parser does not read the list, and that file takes precedence when both files exist.
Register against `AbstractMessage`: superclass lookup reaches generated classes, while registering the `Message` interface does not.
The registry is process-global and offers no per-job isolation.
Explicit construction neither registers the factory nor changes its defaults.
The factory uses the default settings; per-type settings require [explicit TypeInformation](#explicit-type-information).
Do not add `maxMessageSize`, `recursionLimit`, `deterministicSerialization`, or `largePayloadAliasing` to this YAML entry; configure those library settings through `ProtobufTypeInformation.newBuilder(...)`.

## Nested values and supported types

| Shape | Native selection |
|---|---|
| Generated message at top level | Explicit type information or registered factory |
| POJO message field or tuple message element | Registered factory |
| Row | Explicit `RowTypeInfo`, for example `Types.ROW(messageType)` |
| List | Explicit `ListTypeInfo<>(messageType)` on every supported runtime |
| Inferred `TypeHint<List<Event>>` | Native registered element on Flink 2.x; generic fallback on 1.20 |

Inference from an erased runtime List instance is not promised.
Keep generic types disabled, including when explicit type information is used elsewhere in the job.
The [common-types guide](common-types.md) covers generated Google Struct/Value/ListValue, Any, scalar/time wrappers, and OpenTelemetry AnyValue/ArrayValue/KeyValueList.
Struct keys and AnyValue variants are data within a fixed schema; they do not require DynamicMessage.
Any payload bytes remain opaque, without automatic unpacking or schema validation.

Every message type that a payload reaches, including types in imported files, must be declared in a file with `syntax = "proto3"`.
Construction rejects a payload graph that reaches a message declared in a proto2 or editions file, naming the first such message and its file; this includes `google/protobuf/descriptor.proto` types, while the well-known types such as Struct, Any, and Timestamp are proto3.
Construction also rejects lite messages and payload messages that declare extensions.
The library concerns internal DataStream transport and managed values, not connector boundary encoders, ProtoJSON, OTLP export, or the separate Flink Table/SQL Protobuf format.

## DynamicMessage values

The DynamicMessage class does not identify a schema, so DynamicMessage values need type information bound to a root descriptor:

```java
ProtobufTypeInformation<DynamicMessage> info = ProtobufTypeInformation.of(descriptor);
```

`ProtobufTypeInformation.newBuilder(descriptor)` accepts the same four settings as the generated-class builder.
The descriptor's payload graph must satisfy the generated-message rules above, and no reached message may declare an extension range, a group or required field, or a field number above 536,870,911; protoc rejects all of these in proto3, but a hand-built descriptor can declare them.
`ProtobufTypeInformation.of(DynamicMessage.class)` fails and names the descriptor overload.
An operator that returns DynamicMessage needs `.returns(info)`: the registered factory reports DynamicMessage as a missing type, and without the factory Flink selects a generic type, which fails with generic types disabled.
A POJO or tuple that contains a DynamicMessage needs explicit composite type information, such as `Types.POJO(MyPojo.class, Map.of("message", info))` or `Types.TUPLE(Types.STRING, info)`; otherwise Flink selects a generic type for the field, which fails with generic types disabled.

Type information and serializers are identified by the root's full name and its descriptor content, not by descriptor instances, and Java deserialization on a TaskManager rebuilds the descriptors.
Decoded values carry the reading serializer's descriptor instances.
DynamicMessage field access and equality compare descriptor instances, so resolve field descriptors through the value, for example with `value.getDescriptorForType().findFieldByName("payload")`, and build a nested value from that field's `getMessageType()`.
Writing accepts a value whose descriptors are other instances when the schema has the same content or is a [supported earlier schema](compatibility.md#schema-changes) of the serializer's schema, and rejects other values with the field path and the first incompatibility.
Treat a `byte[]` set on a `bytes` field as immutable afterwards, because the runtime keeps the array without copying it.
Deterministic serialization does not order DynamicMessage map entries: the runtime writes them in insertion order.

DynamicMessage state uses its own snapshot format, which restores the saved descriptors without an application class or descriptor.
The same supported schema changes and reader-limit rules apply, and a switch between generated and DynamicMessage types of the same schema is incompatible.
Runtime restore evidence for DynamicMessage state and the complete usage guide are tracked in [#23](https://github.com/flink-gcp/flink-datastream-protobuf/issues/23); [ADR-0005](adr/0005-descriptor-bound-dynamic-messages.md) records the design.

## Scalar keys

Messages are values, not supported DataStream partitioning keys or MapState user keys.
Extract a stable scalar such as an account string.
There is no message-key opt-in, including with deterministic serialization enabled.
Both inferred and explicitly typed `keyBy` can bypass `isKeyType()`, so lack of an exception does not establish support.
Deterministic bytes neither stabilize generated-message hashing across classloaders/JVMs nor define canonical key bytes across builds.

## State and recovery

The [state example](../examples/datastream/src/main/java/io/github/flink/gcp/protobuf/examples/StatefulExample.java) partitions by a scalar and assigns a stable operator UID.
`RunningTotal` is a nested operator class defined in that example; its `total` and `recent` fields have types `ValueState<Event>` and `MapState<String, Event>` respectively.
The linked source includes the complete operator and its Flink imports.

{{< example "StatefulExample#scalar-state" >}}

It creates native value descriptors in the operator's `open` method:

{{< example "StatefulExample#state-descriptors" >}}

`MapState<String, Event>` is supported: the map key is a string and its value is Protobuf.
`MapState<Event, ...>` remains unsupported even if a particular runtime accepts its descriptor.
The bounded example checks state updates during execution and may finish before a checkpoint could complete.

For a continuously running application using these types, enable periodic checkpointing with `env.enableCheckpointing(...)` and configure persistent checkpoint storage accessible to the cluster.
Flink's configured restart strategy can then recover the job from a completed checkpoint.
For an intentional stop and restart, including a restart with a supported schema change, use Flink's savepoint operations on that running application:

```sh
"$FLINK_HOME/bin/flink" stop --savepointPath "$SAVEPOINT_DIRECTORY" "$JOB_ID"
"$FLINK_HOME/bin/flink" run -s "$SAVEPOINT_PATH" -c com.example.Application application.jar
```

Here `Application` is the application's entrypoint, not the bounded demonstration job.
Keep the generated Java class and Protobuf message names, operator UIDs, state names, and matching runtime/artifact line unchanged.
The schema may stay the same or change as listed in [supported schema changes](compatibility.md#schema-changes); the restored job rejects other changes to the declarations that saved values reach.
Reader size/depth limits may stay the same or increase; deterministic-writing mode and large-payload aliasing may change in either direction.
A successful Protobuf parse alone does not establish state compatibility.

The [MiniCluster recovery suite](validation/runtime-recovery.md) verifies completed-checkpoint recovery and separate-job savepoint restoration for HashMap and RocksDB, including continued processing.
It also verifies restoration across supported schema changes and rejection of unsupported changes and reduced reader limits.
It is separate from these application examples and the historical [Spike 0](validation/spike-0.md).
Cross-Flink, cross-Protobuf, backend migration, and rescaling claims require their own evidence and are not established by these examples.

## Development upgrades and API reference

The 0.1.0 contract rejected schema changes, including otherwise wire-safe field additions.
Current `0.3.0-SNAPSHOT` development builds restore the supported schema changes; the [retained-baseline upgrade check](validation/development-baselines.md#restore-retained-state-with-the-current-library) restores state written by the retained 0.1.0 artifact across a field addition and a reserved field removal.
The [upgrade guide from 0.1.0](upgrade.md) lists the API changes and intentional breaks of 0.2.0 and the upgrade procedure.
[DynamicMessage values](#dynamicmessage-values) are new in the [0.3.0 milestone](https://github.com/flink-gcp/flink-datastream-protobuf/issues/20).
Read [Protobuf/state compatibility and development-build upgrade guidance](compatibility.md) before changing an application or dependency.
The unpublished 0.x milestones can break APIs, defaults, runtime requirements, and saved-state compatibility.
Version 1.0.0 is the planned first Maven Central release and stabilization point, without a calendar deadline.

Generate the API reference with `mise x -- just docs-javadoc`, then open `target/apidocs/index.html`.
The documentation site includes this API reference for each [retained version](versions.md).
The public documentation site is live and linked from README; final live verification after the search initialization repair remains tracked in [#19](https://github.com/flink-gcp/flink-datastream-protobuf/issues/19).
