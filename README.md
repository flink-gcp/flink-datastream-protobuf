# Flink DataStream Protobuf

Native Protocol Buffers type integration for the Apache Flink DataStream API.

The repository implements native serialization, explicit TypeInformation, and superclass factory registration for generated messages, and descriptor-bound TypeInformation for DynamicMessage values.
Versioned descriptor snapshots support serializer restoration for unchanged schemas and supported schema changes.
MiniCluster tests verify checkpoint recovery and savepoint restoration with HashMap and RocksDB state backends, including restoration across supported schema changes.
Maven Central publishes each release in two versions of `io.github.flink-gcp:flink-datastream-protobuf`: `1.1.0` for the current and previous Flink 2.x minors, and `1.1.0-1.20` for Flink 1.20 LTS.

```xml
<dependency>
    <groupId>io.github.flink-gcp</groupId>
    <artifactId>flink-datastream-protobuf</artifactId>
    <version>1.1.0</version>
</dependency>
```

A build of this source produces `io.github.flink-gcp:flink-datastream-protobuf:1.1.0-SNAPSHOT`.
Start with the [quickstart](https://flink-gcp.github.io/flink-datastream-protobuf/docs/quickstart/) to run the compiled application examples against the published library or a local build.
It targets full-runtime generated Protobuf messages and DynamicMessage values with protobuf-java 4.x and 3.25.x.
Each artifact line is built with Protobuf 4.36.2, and CI runs that jar unchanged with protobuf-java/protoc 4.36.2 and 3.25.9.

## Supported Flink versions

The [compatibility policy](docs/adr/0003-flink-version-compatibility.md) follows flink-connector-gcp: the current and previous Flink 2.x minors share one artifact, and Flink 1.20 LTS uses a separate build from the same source tree.
The matrix covers the production serializer, type information, registration, native transport, and checkpoint/savepoint recovery with unchanged and supported changed schemas alongside separate feasibility probes.

| Flink runtime | Tested patch | Java | Release version |
|---|---|---|---|
| 2.2 | 2.2.1 (compile floor) | 17, 21 | `X.Y.Z` |
| 2.3 | 2.3.0 (runtime ceiling) | 17, 21 | The same `X.Y.Z` jar |
| 1.20 LTS | 1.20.5 | 17 | `X.Y.Z-1.20`, compiled for 1.20 |

Both version lines use `io.github.flink-gcp:flink-datastream-protobuf`, and Maven orders each release's `X.Y.Z-1.20` after its `X.Y.Z`; stay on the line that matches your Flink runtime, as the [compatibility guide](https://flink-gcp.github.io/flink-datastream-protobuf/docs/compatibility/#artifact-lines-and-version-order) describes for Dependabot and Renovate.
There is no binary compatibility promise between Flink 1.x and 2.x or support for Java 11.
Source and binary compatibility do not establish savepoint compatibility across Flink versions.
On Flink 1.20, supply explicit `ListTypeInfo` for lists of messages; `TypeHint<List<MessageType>>` falls back to a generic type in the production integration and probe tests.

## Native type information

Use `ProtobufTypeInformation.of(MyMessage.class)` or its builder for explicit sources, `.returns(...)`, and state descriptors.
The message and every message type it reaches must be declared with `syntax = "proto3"`; a reached message in a proto2 or editions file is rejected.
Alternatively, register `ProtobufTypeInfoFactory` against `com.google.protobuf.AbstractMessage` before type extraction.
Protobuf messages are supported as values; use stable scalar keys for DataStream partitioning and MapState user keys.
There is no message-key opt-in: deterministic bytes do not make generated message hash codes stable across application classloaders or JVMs.
Both inferred and explicitly typed `keyBy` can accept message keys despite `isKeyType() == false`; that use remains unsupported.
The library has no Kryo/Chill implementation or internal fallback; Flink can still select its own generic serializer when native integration is not selected, so native usage must set `pipeline.generic-types: false`.
For DynamicMessage values, use `ProtobufTypeInformation.of(descriptor)` with `.returns(...)` and state descriptors; see [DynamicMessage values](https://flink-gcp.github.io/flink-datastream-protobuf/docs/dynamic-messages/).

The [usage and configuration guide](https://flink-gcp.github.io/flink-datastream-protobuf/docs/usage/) covers settings, registration timing, nested values, state, and unsupported paths.
The [quickstart](https://flink-gcp.github.io/flink-datastream-protobuf/docs/quickstart/) runs the explicit, registered, scalar-keyed state, and DynamicMessage examples against a locally installed development jar.
Browse the [API reference](https://flink-gcp.github.io/flink-datastream-protobuf/api/java/), or generate it locally with `mise x -- just docs-javadoc` and open `docs/static/api/java/index.html`.
The [documentation site](https://flink-gcp.github.io/flink-datastream-protobuf/) includes version selection and search.
See [documentation versions](https://flink-gcp.github.io/flink-datastream-protobuf/docs/versions/) for the hosted versions and their retention policy.

## Common generated types

The tested value types include Google Struct, Value, ListValue, Any, Timestamp, Duration, Empty, FieldMask, scalar wrappers, and OpenTelemetry AnyValue, ArrayValue, KeyValueList, and KeyValue.
They use the native integration above, including unchanged-schema state restoration.
See [common types and examples](https://flink-gcp.github.io/flink-datastream-protobuf/docs/common-types/) for the exact schema/runtime pins, tested configurations, and the distinction between Struct, Any, and OTel AnyValue.

## State compatibility

Value state restores across supported field additions, reserved field removals, and enum value additions, with equal or increased reader limits and either deterministic mode.
Version 1.0.0 establishes the stable API and forward-restore contract; see [Protobuf updates and state compatibility](https://flink-gcp.github.io/flink-datastream-protobuf/docs/compatibility/) for that contract, schema examples, the exact supported changes, and Java runtime version guarantees.

## Development

Install [mise](https://mise.jdx.dev/), then run:

```sh
mise trust
mise install
mise x -- just verify
mise x -- just lint
```

`just verify` generates the test messages using protoc from Maven Central and runs formatting checks, Checkstyle, unit tests, MiniCluster integration tests, and Apache RAT.
It needs neither Docker nor Google Cloud credentials.
See [Development](docs/development.md) for the version matrix and the separate Chill comparison.

## Design and evidence

- [Spike 0 results](docs/validation/spike-0.md) record the feasibility checks and their limits.
- [Architecture decisions](docs/adr/README.md) describe the chosen design and development process.
- [Contributing](CONTRIBUTING.md) describes the pull request workflow.

This library concerns Flink's internal DataStream serialization, including managed value state with stable scalar keys.
For external Table/SQL Protobuf bytes, Apache Flink provides the separate [`flink-sql-protobuf` format](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/protobuf/).

## License and provenance

Licensed under the [Apache License, Version 2.0](LICENSE).
Build configuration and development conventions are adapted from [flink-connector-gcp](https://github.com/flink-gcp/flink-connector-gcp), which is developed in the same organization; this library does not depend on that project.
The Apache-derived Checkstyle configuration retains its original headers and attribution in [NOTICE](NOTICE).

This is an independent project, not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google.
Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.
