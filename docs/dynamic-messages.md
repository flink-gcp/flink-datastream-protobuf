---
title: DynamicMessage values
weight: 25
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

# DynamicMessage values

`com.google.protobuf.DynamicMessage` holds a Protobuf message whose schema comes from a descriptor at runtime rather than from a generated class.
The library transports these values and keeps them in managed state when the application supplies type information bound to a root descriptor.

The [DynamicMessage example](../examples/datastream/src/main/java/io/github/flink/gcp/protobuf/examples/DynamicExample.java) is compiled and run with the other [quickstart](quickstart.md) examples; the snippets below come from it.
[ADR-0005](adr/0005-descriptor-bound-dynamic-messages.md) records the design.

## DynamicMessage and generated common types

Several generated message types also carry data whose shape varies at runtime.
They are a different tool: their schema is fixed and known to the library through their generated class.

| Type | Where its schema comes from | What varies at runtime |
|---|---|---|
| `DynamicMessage` | The application's root descriptor | The values of fields that the descriptor declares; another descriptor is another schema |
| Google `Struct`, `Value`, `ListValue` | Generated classes in protobuf-java | Map keys, list elements, and the JSON-like value kinds, all as data of one fixed schema |
| Google `Any` | A generated class in protobuf-java | A type URL and the bytes of another message, which stay opaque; the library does not unpack them |
| OpenTelemetry `AnyValue` | Classes generated from the pinned OTel schema | One of eight fixed value kinds, as data of one fixed schema |

The generated types use generated-class type information and the registered factory, as [Supported types](common-types.md) shows.
Changing a Struct key, an Any payload, or the AnyValue variant changes data, so unchanged-schema state restoration covers it.
For DynamicMessage, the descriptor is the schema, and restoring with another descriptor is a [schema change](#state-and-restore).

## Supply a descriptor

An application needs a root `Descriptor` to create or parse DynamicMessage values in any case.
The example builds one from a descriptor set in the job jar.
Its build has protoc write that set, with every imported file, into the classes directory, which the jar packages as the resource `/event.pb`:

```xml
<plugin><groupId>io.github.ascopes</groupId><artifactId>protobuf-maven-plugin</artifactId>
    <configuration><protoc>${protobuf.version}</protoc>
        <outputDescriptorFile>${project.build.outputDirectory}/event.pb</outputDescriptorFile>
        <outputDescriptorIncludeImports>true</outputDescriptorIncludeImports>
    </configuration>
</plugin>
```

Outside Maven, `protoc --include_imports --descriptor_set_out=event.pb event.proto` writes such a set.
The application then builds each file with the files that it imports, which the set lists before it:

{{< example "DynamicExample#dynamic-descriptor" >}}

A file's name in the set is its path relative to the proto source root, such as `event.proto`, and `findMessageTypeByName` takes the message name without its package.
A descriptor that a generated class supplies also works, but then the generated class itself is the simpler type.

## Explicit type information

`ProtobufTypeInformation.of(descriptor)` creates the type information, and `ProtobufTypeInformation.newBuilder(descriptor)` accepts the four [settings](usage.md#explicit-type-information) of the generated-class builder.
Give it to every operator that returns DynamicMessage values:

{{< example "DynamicExample#dynamic-returns" >}}

Creating the type information checks the descriptor.
Every message type that the root reaches must be declared in a proto3 file, as for [generated messages](usage.md#nested-values-and-supported-types), with no extensions or extension ranges, no group or required field, and no field number above 536,870,911.
protoc rejects groups, required fields, extension ranges, and such field numbers in proto3 files, but descriptor building accepts them in a descriptor built by hand.
It also rebuilds the descriptors from their serialized form, as a TaskManager does, so a descriptor that cannot be rebuilt from its own content, such as one built against missing imports, fails here.
A failure throws `IllegalArgumentException` that names the root and the failed rule.

Flink cannot infer this type.
With the registered factory, an operator that returns DynamicMessage without `.returns(...)` fails with Flink's missing-type error, whose cause names `ProtobufTypeInformation.of(Descriptor)`; without the factory, Flink selects a generic type, which fails with generic types disabled.
`ProtobufTypeInformation.of(DynamicMessage.class)` fails as well, because the class does not name a schema.
A POJO or tuple that contains a DynamicMessage needs explicit composite type information, such as `Types.POJO(MyPojo.class, Map.of("message", eventType))` or `Types.TUPLE(Types.STRING, eventType)`.

Type information is Java-serializable and identified by the root's full name, the descriptor content, and the settings, not by descriptor instances; a DynamicMessage type never equals the generated type of the same schema.

## Values and field access

A `Descriptor` is not serializable, so a function that creates values obtains its own descriptor, here when it opens:

{{< example "DynamicExample#dynamic-values" >}}

DynamicMessage field access, equality, and merging compare descriptor instances.
Decoded values carry the descriptor instances of the serializer that read them, and another task, serializer, or restore has other instances.
Resolve field descriptors through the value that you read or write, and build a nested value from that field's `getMessageType()`:

{{< example "DynamicExample#dynamic-fields" >}}

A field descriptor from another instance fails in `getField`.

When writing, the serializer accepts a value whose descriptors are other instances if their content is the same as its own schema or a supported earlier schema of it, as restored values have after a schema change.
It rejects other values with `IOException`, before writing any byte, naming the field path and the first incompatibility, such as a value of another message type in the stream.
A `bytes` field set from a `byte[]` keeps that array without copying it, so treat the array as immutable afterwards.
Deterministic serialization does not order DynamicMessage map entries; both supported runtimes write them in insertion order.

## Keys and state

Key streams and MapState by stable scalar values extracted from the message, as for generated messages:

{{< example "DynamicExample#dynamic-keys" >}}

DynamicMessage keys are not supported: their hash codes include the descriptor instance's identity, which differs between the tasks that send and receive a record.
See [scalar keys](usage.md#scalar-keys).
State descriptors take the same type information as the stream:

{{< example "DynamicExample#dynamic-state" >}}

## State and restore

DynamicMessage state uses its own serializer snapshot, which records the root's full name, the complete normalized descriptor set, and the settings.
A restore rebuilds the saved descriptors without the application's descriptor or any application class.
The new application's type information then decides whether it can read the saved state, by the [schema-change rules](compatibility.md#schema-changes) of generated messages and the same rules for the size and depth limits and the other settings, except that no Java class name is involved: the root's full name must stay the same.

A restore that these rules reject fails with Flink's `StateMigrationException` for an incompatible new serializer, which names no reason.
Check a schema change against the rules above before deploying it, and test the restore with a savepoint as the [upgrade procedure](compatibility.md#verified-coverage-and-upgrade-procedure) describes.

After a supported schema change, restored values do not all carry the new descriptor:

- Values from operator state, and ValueState and MapState values on the HashMap backend, carry the saved descriptor.
  A field that only the new schema declares has no field descriptor in them, so `findFieldByName` returns `null`.
- RocksDB reads keyed state with the new application's serializer, so its values carry the new descriptor.
- Values that the new serializer writes and the next restore reads carry the new descriptor.

Saved generated-message state does not restore into a DynamicMessage type, and saved DynamicMessage state does not restore into a generated type, even for the same schema; switching between them requires fresh state or replayed input.
Restoring DynamicMessage state into a generated type, or generated state into a DynamicMessage type whose application still contains the generated class, fails with `StateMigrationException`.
When the application no longer contains the generated class, restoring generated state fails earlier, because the saved class cannot load.

The [DynamicMessage recovery tests](validation/runtime-recovery.md#dynamicmessage) verify checkpoint recovery, savepoint restores with unchanged and supported schemas, the descriptors that restored values carry, and the rejections above on HashMap and RocksDB, with a DynamicMessage application whose jar contains no generated class.

## Scope and exclusions

A stream or state whose values use several root schemas is not supported; use one type information object per schema.
The library also does not discover descriptors automatically, integrate a schema registry, support lite messages, or use an extension registry.
