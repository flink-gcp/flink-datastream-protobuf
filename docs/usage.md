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

The defaults disable deterministic writing and allow a 64 MiB payload and parser recursion depth of 100.
Numeric limits must be positive; `build()` validates settings and the generated class.
`of(Class<T>)` matches Flink's unbounded static factory signature, so unsupported classes can compile but fail with `IllegalArgumentException` at construction.
The builder also enforces `T extends Message` at compile time.
Use `of(Event.class)` or the builder for explicit native selection.
The inherited `of(TypeHint)` overload follows Flink's ordinary type extraction and can select a generic type when the factory is not registered.
Builder reuse does not change previously built type information or serializers.
These limits constrain accepted inputs, not total heap usage or stack capacity.

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
Do not add `maxMessageSize`, `recursionLimit`, or `deterministicSerialization` to this YAML entry; configure those library settings through `ProtobufTypeInformation.newBuilder(...)`.

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

Current construction rejects DynamicMessage, lite messages, and payload graphs requiring extensions or MessageSet.
DynamicMessage support is planned for the 0.3.0 milestone.
The library concerns internal DataStream transport and managed values, not connector boundary encoders, ProtoJSON, OTLP export, or the separate Flink Table/SQL Protobuf format.

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
For an intentional stop and unchanged-schema restart, use Flink's savepoint operations on that running application:

```sh
"$FLINK_HOME/bin/flink" stop --savepointPath "$SAVEPOINT_DIRECTORY" "$JOB_ID"
"$FLINK_HOME/bin/flink" run -s "$SAVEPOINT_PATH" -c com.example.Application application.jar
```

Here `Application` is the application's entrypoint, not the bounded demonstration job.
Keep the generated Java class and Protobuf message names, complete normalized descriptors, operator UIDs, state names, and matching runtime/artifact line unchanged.
Reader size/depth limits may stay the same or increase; deterministic-writing mode may change in either direction.
A successful Protobuf parse alone does not establish state compatibility.

The [MiniCluster recovery suite](validation/runtime-recovery.md) verifies completed-checkpoint recovery and separate-job savepoint restoration for HashMap and RocksDB, including continued processing.
It also verifies rejection of changed schemas and reduced reader limits.
It is separate from these application examples and the historical [Spike 0](validation/spike-0.md).
Cross-Flink, cross-Protobuf, backend migration, and rescaling claims require their own evidence and are not established by these examples.

## Development upgrades and API reference

The 0.1.0 contract rejects schema changes, including otherwise wire-safe field additions.
Supported schema evolution is planned for 0.2.0, and descriptor-driven DynamicMessage support for 0.3.0.
Read [Protobuf/state compatibility and development-build upgrade guidance](compatibility.md) before changing an application or dependency.
The unpublished 0.x milestones can break APIs, defaults, runtime requirements, and saved-state compatibility.
Version 1.0.0 is the planned first Maven Central release and stabilization point, without a calendar deadline.

Generate the API reference with `mise x -- just docs-javadoc`, then open `target/apidocs/index.html`.
The documentation site includes this API reference for each [retained version](versions.md).
The public documentation site is live and linked from README; final live verification after the search initialization repair remains tracked in [#19](https://github.com/flink-gcp/flink-datastream-protobuf/issues/19).
