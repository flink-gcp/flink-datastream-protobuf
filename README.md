# Flink DataStream Protobuf

Native Protocol Buffers type integration for the Apache Flink DataStream API.

This project is under development.
The repository implements native serialization, explicit TypeInformation, and superclass factory registration for generated messages.
Versioned descriptor snapshots support serializer restoration for unchanged schemas.
MiniCluster tests verify unchanged-schema checkpoint recovery and savepoint restoration with HashMap and RocksDB state backends.
No release is available.
Versions 0.1.0, 0.2.0, and 0.3.0 identify development milestones only; the first Maven Central release is planned for 1.0.0.

The development artifact is `io.github.flink-gcp:flink-datastream-protobuf:0.1.0-SNAPSHOT`.
Start with the [quickstart](https://flink-gcp.github.io/flink-datastream-protobuf/docs/quickstart/) to build/install it locally and run the compiled application examples.
It targets full-runtime generated Protobuf messages with protobuf-java 3.25.9 and 4.33.6.

## Supported Flink versions

The [compatibility policy](docs/adr/0003-flink-version-compatibility.md) follows flink-connector-gcp: the current and previous Flink 2.x minors share one artifact, and Flink 1.20 LTS uses a separate build from the same source tree.
The matrix covers the production serializer, type information, registration, native transport, and unchanged-schema checkpoint/savepoint recovery alongside separate feasibility probes.
Published-artifact capture and consumer verification remain release requirements.

| Flink runtime | Tested patch | Java | Planned release version |
|---|---|---|---|
| 2.2 | 2.2.1 (compile floor) | 17, 21 | `X.Y.Z` |
| 2.3 | 2.3.0 (runtime ceiling) | 17, 21 | The same `X.Y.Z` jar |
| 1.20 LTS | 1.20.4 | 17 | `X.Y.Z-1.20`, compiled for 1.20 |

Both version lines use `io.github.flink-gcp:flink-datastream-protobuf`.
There is no binary compatibility promise between Flink 1.x and 2.x or support for Java 11.
Source and binary compatibility do not establish savepoint compatibility across Flink versions.
On Flink 1.20, supply explicit `ListTypeInfo` for lists of messages; `TypeHint<List<MessageType>>` falls back to a generic type in the production integration and probe tests.

## Native type information

Use `ProtobufTypeInformation.of(MyMessage.class)` or its builder for explicit sources, `.returns(...)`, and state descriptors.
Alternatively, register `ProtobufTypeInfoFactory` against `com.google.protobuf.AbstractMessage` before type extraction.
Always disable generic types, and use stable scalar keys.

The [usage and configuration guide](https://flink-gcp.github.io/flink-datastream-protobuf/docs/usage/) covers settings, registration timing, nested values, state, and unsupported paths.
The [quickstart](https://flink-gcp.github.io/flink-datastream-protobuf/docs/quickstart/) runs the explicit, registered, and scalar-keyed state examples against a locally installed development jar.
Browse the [API reference](https://flink-gcp.github.io/flink-datastream-protobuf/api/java/), or generate it locally with `mise x -- just docs-javadoc` and open `target/apidocs/index.html`.
The [documentation site](https://flink-gcp.github.io/flink-datastream-protobuf/) includes version selection and search.
See [documentation versions](https://flink-gcp.github.io/flink-datastream-protobuf/docs/versions/) for the Development-only period and the release retention policy from 1.0.0.

## Common generated types

The tested value types include Google Struct, Value, ListValue, Any, Timestamp, Duration, Empty, FieldMask, scalar wrappers, and OpenTelemetry AnyValue, ArrayValue, KeyValueList, and KeyValue.
They use the native integration above, including unchanged-schema state restoration.
See [common types and examples](docs/common-types.md) for the exact schema/runtime pins, tested configurations, and the distinction between Struct, Any, and OTel AnyValue.

## Development milestones and first release

The [release contract](docs/adr/0001-native-protobuf-type-integration.md) separates generated-message integration, schema evolution, and DynamicMessage development before the first public artifact.
The serializer implements generated-class validation, bounded framing, immutable copying, configurable size, depth, and deterministic-writing behavior, and versioned descriptor snapshots.
The milestone table includes implemented transport, common-type acceptance, and serializer restoration, plus the remaining documentation and publication requirements.

| Milestone | Planned capability |
|---|---|
| [0.1.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/6) | Native generated-message integration and unchanged-schema state restore, including common Google Well-Known Types and OpenTelemetry composite messages; GitHub Pages development documentation and benchmark evidence |
| [0.2.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/14) | Supported descriptor-based schema evolution and restore from retained 0.1.0 development state |
| [0.3.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/20) | Explicit-descriptor DynamicMessage support, with documented API/state upgrade behavior |

The 0.1.0 contract rejects changed schemas while allowing unchanged-schema value-state restore with increased reader limits or a deterministic-mode change.
This includes otherwise wire-safe field additions; see [Protobuf updates and state compatibility](docs/compatibility.md) for schema examples, Java runtime version guarantees, and the current versus planned restore behavior.
Protobuf messages are supported as values; use stable scalar keys for DataStream partitioning and MapState user keys.
There is no message-key opt-in: deterministic bytes do not make generated message hash codes stable across application classloaders or JVMs.
Both inferred and explicitly typed `keyBy` can accept message keys despite `isKeyType() == false`; that use remains unsupported.
The unpublished 0.x milestones may contain breaking changes to APIs, defaults, runtime requirements, or saved-state compatibility while the design matures toward 1.0.0.
Each milestone must document its specific breaks, supported upgrade paths, and required migration or fresh-state/replay procedure; read the [development-build compatibility guidance](docs/compatibility.md#development-builds-before-100) before upgrading.
Separate API checks and attributable development-state fixtures must verify supported paths and explicit rejection of unsupported state.
Version 1.0.0 will establish the stable API and forward-restore contract; compatibility with every earlier development artifact is not implied.
The current roadmap ends at 0.3.0; the remaining work will determine whether further development milestones or preparation for 1.0.0 comes next.
The [1.0.0 publication work](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13) must validate and publish both `1.0.0` and `1.0.0-1.20`, verify consumers, and retain fixtures from each published artifact.
No calendar deadline is set.
The library has no Kryo/Chill implementation or internal fallback; Flink can still select its own generic serializer when native integration is not selected, so native usage must set `pipeline.generic-types: false`.
The [development sequence](docs/development.md#next-implementation-steps) tracks implementation and release validation separately from the existing probes.

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
