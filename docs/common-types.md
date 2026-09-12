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

# Google Well-Known Types and OpenTelemetry values

Google Well-Known Types (WKT) and OpenTelemetry composite values use generated message classes with declared descriptors.
They use the same `ProtobufTypeInformation` and serializer as application messages.
Changing a Struct key or choosing another AnyValue variant changes the data within that descriptor, so unchanged-schema restoration supports those changes.

## Tested types

| Types | Tested values |
|---|---|
| Google `Struct`, `Value`, `ListValue` | Nested maps and lists, null, booleans, strings, numbers, empty containers, and unset versus present-default oneof cases |
| Google `Any` | Default values, a type URL and opaque bytes, and nesting inside an application message |
| Google `Timestamp`, `Duration`, `Empty`, `FieldMask` | Defaults, representative populated messages, and nested use |
| Google `DoubleValue`, `FloatValue`, `Int64Value`, `UInt64Value`, `Int32Value`, `UInt32Value`, `BoolValue`, `StringValue`, `BytesValue` | Default and populated wrappers, including a present wrapper containing a default scalar |
| OTel `AnyValue`, `ArrayValue`, `KeyValueList`, `KeyValue` | Nested heterogeneous values, bytes, empty containers, unset and all eight present oneof cases, and profiling string/key dictionary indices |

Wrappers remain supported for existing schemas.
For new schemas, prefer explicit field presence, such as proto3 `optional`, as described in the [WKT guidance](https://protobuf.dev/reference/protobuf/google.protobuf/).

Tests use protobuf-java/protoc 3.25.8 together and protobuf-java/protoc 4.33.6 together.
Google classes come from the selected protobuf-java runtime; the pinned OTel schema and application envelope are generated with that profile's protoc.
The [Flink matrix](../README.md#supported-flink-versions) runs both profiles: Flink 2.2.1/2.3.0 on JDK 17/21, and Flink 1.20.4 on JDK 17.
The unchanged-jar lane also reruns the floor's compiled tests and application jars on Flink 2.3.0.
These checks do not claim compatibility with every published OTel Java artifact or restore across Protobuf profiles or Flink versions.

## Constructing values

Use `Struct` for a map whose values follow Google's `Value` schema.
Its number variant is a double; it has no separate int64 or bytes variant.
With imports for `com.google.protobuf.Struct`, `com.google.protobuf.Value`, and `io.github.flink.gcp.protobuf.ProtobufTypeInformation`:

```java
var attributes = Struct.newBuilder()
        .putFields("service", Value.newBuilder().setStringValue("checkout").build())
        .putFields("healthy", Value.newBuilder().setBoolValue(true).build())
        .build();
var structType = ProtobufTypeInformation.of(Struct.class);
```

Google `Any` wraps serialized bytes with a type URL.
An application can explicitly pack a known message, importing `com.google.protobuf.Any`:

```java
var packed = Any.pack(attributes);
var anyType = ProtobufTypeInformation.of(Any.class);
```

The library preserves `type_url` and payload bytes without fetching the URL, automatically unpacking the message, or checking its embedded schema.
The saved Any descriptor describes the wrapper; it provides no compatibility promise for the payload's schema.

OTel `AnyValue` has its own schema with string, bool, int64, double, array, key-value-list, bytes, and the pinned profiling string-index variant.
It is not a Google Any wrapper or a Struct value.
Using generated `io.opentelemetry.proto.common.v1.AnyValue` and `ArrayValue`:

```java
var values = AnyValue.newBuilder()
        .setArrayValue(ArrayValue.newBuilder()
                .addValues(AnyValue.newBuilder().setIntValue(42))
                .addValues(AnyValue.newBuilder().setStringValue("checkout")))
        .build();
var otelType = ProtobufTypeInformation.of(AnyValue.class);
```

Supply these type information objects to sources or `.returns(...)`, and keep `pipeline.generic-types: false`.
The [superclass factory configuration](../README.md#native-type-information) also selects these generated types.
Applications supply their own OTel generated classes and compatible Protobuf runtime; the library does not bundle OTel classes.
`CommonTypesTest` compiles and exercises these construction examples.

The library implements binary message transport, not ProtoJSON, OTLP export, a schema registry, or application validation.
For example, it transports Timestamp fields outside the semantic timestamp range and duplicate OTel list keys without validating them.
Applications remain responsible for those rules and for resolving profiling dictionary references.
`DynamicMessage` is a separate, planned path for working from descriptors without generated classes; it is not needed for these examples and is not currently accepted by the public factory.

## Verification boundaries

`CommonTypesTest` verifies explicit production selection, payload parsing, immutable object copy, frame copy, unknown fields, and serializer snapshot restoration for every listed type.
It checks both deterministic settings, complete descriptor dependency content, and rejection of a missing import.
`CommonTypesExplicitITCase` and `CommonTypesRegistrationITCase` run in separate JVMs with generic types disabled.
They verify native selection for every tested type and transport representative Struct, Any, AnyValue, and application envelopes across a rebalance boundary.
Envelope transport covers top-level, tuple, Row, and explicit List shapes; registration also covers POJO fields.

`CommonTypesRecoveryITCase` stores Struct directly in ValueState, AnyValue directly in string-keyed MapState, and envelopes containing all listed types in operator ListState.
Both HashMap and RocksDB runs save 12 inputs, process eight unsaved inputs, fail, restore the completed checkpoint, and replay through input 27.
Separate jobs restore a canonical savepoint after 12 inputs and continue through input 27.
The job checks exact previous keyed values and every recovered operator value, including unknown fields and Any payload bytes.
Subsequent inputs change Struct keys and AnyValue variants without changing descriptors.
These cases use default serializer settings, fixed parallelism and state identities, and temporary state generated during the test.

The common-types job shares the existing MiniCluster controls and attached application jar.
Its WKT classes come from the runtime, and its OTel/envelope classes come from the ordinary test classpath.
The separate [isolated application recovery suite](validation/runtime-recovery.md) retains the generated-class isolation and settings-transition coverage.
The existing 24 development savepoint archives are unchanged; published-artifact fixtures and consumer validation remain in issues #13/#6.

## OTel schema provenance and regeneration

The test source `src/test/proto/opentelemetry/proto/common/v1/common.proto` is an unmodified copy of the [official schema at `bb8796bff67cf6e1c7f218e21de6eaec0841871e`](https://github.com/open-telemetry/opentelemetry-proto/blob/bb8796bff67cf6e1c7f218e21de6eaec0841871e/opentelemetry/proto/common/v1/common.proto).
Its SHA-256 is `0429795169e29089cc53490b541a0ae89fa7c80492ac58ed2b3c4171cce4ecee`, checked by `CommonTypesTest`.
The original OpenTelemetry copyright and Apache-2.0 license header are retained.
Other messages in that file are generated but are outside the tested-type list above.

To compare the vendored source with upstream without overwriting it:

```sh
gh api 'repos/open-telemetry/opentelemetry-proto/contents/opentelemetry/proto/common/v1/common.proto?ref=bb8796bff67cf6e1c7f218e21de6eaec0841871e' \
  -H 'Accept: application/vnd.github.raw+json' > /tmp/otel-common.proto
cmp /tmp/otel-common.proto src/test/proto/opentelemetry/proto/common/v1/common.proto
shasum -a 256 /tmp/otel-common.proto
```

Regenerate and verify each paired profile with `mise x -- just verify-protobuf 3` and `mise x -- just verify-protobuf 4`.
The existing Maven test generation places Java under `target/generated-test-sources/protobuf`; ordinary builds do not fetch the OTel repository.
The schemas, generated OTel classes, and test application classes are test inputs and are absent from the library jar.
