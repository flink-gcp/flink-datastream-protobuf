# Flink DataStream Protobuf

Native Protocol Buffers type integration for the Apache Flink DataStream API.

This project is under development.
The repository implements native serialization, explicit TypeInformation, and superclass factory registration for generated messages.
Versioned descriptor snapshots support serializer restoration for unchanged schemas.
MiniCluster tests verify unchanged-schema checkpoint recovery and savepoint restoration with HashMap and RocksDB state backends.
No release is available.

The planned artifact is `io.github.flink-gcp:flink-datastream-protobuf`.
It targets full-runtime generated Protobuf messages with protobuf-java 3.25.8 and 4.33.6.

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

Use explicit type information for `.returns(...)` and per-type settings.
For a generated full-runtime class named `MyMessage`, the construction API is:

```java
var defaults = ProtobufTypeInformation.of(MyMessage.class);
var configured = ProtobufTypeInformation.newBuilder(MyMessage.class)
        .deterministicSerialization(true)
        .maxMessageSize(1024 * 1024)
        .recursionLimit(64)
        .build();
```

Import `io.github.flink.gcp.protobuf.ProtobufTypeInformation` and supply the application's generated class.
The defaults disable deterministic writing and allow a 64 MiB payload and parser recursion depth of 100.
Numeric limits must be positive; `build()` validates settings and the generated class.
`of(Class<T>)` matches Flink's unbounded static factory signature, so unsupported classes can compile but fail with `IllegalArgumentException` at construction.
The builder also enforces `T extends Message` at compile time.
Use `of(MyMessage.class)` or the builder for explicit native selection.
The inherited `of(TypeHint)` overload follows Flink's ordinary type extraction and can select a generic type when the factory is not registered.
Builder reuse does not change previously built type information or serializers.
These limits constrain accepted inputs, not total heap usage or stack capacity.

For automatic selection, apply this configuration before any type extraction, including calls to `TypeInformation.of`, `TypeHint`, and source/operator construction:

```yaml
pipeline.generic-types: false
pipeline.serialization-config:
  - com.google.protobuf.AbstractMessage: {type: typeinfo, class: io.github.flink.gcp.protobuf.ProtobufTypeInfoFactory}
```

On Flink 1.20, put this YAML list in `config.yaml`.
The legacy `flink-conf.yaml` parser does not read this list, and that file takes precedence over `config.yaml` when both exist.

Flink follows superclasses when finding a factory, so register `AbstractMessage`, not the `Message` interface.
The registry is process-global; it does not provide per-job registration isolation.
The factory always uses the documented defaults, and explicit construction does not register it or change those defaults.
Unsupported types fail explicitly without an internal generic/Kryo fallback.
Keep generic types disabled for explicit construction too, to reject accidental generic serialization elsewhere in the job.

Production tests verify registered top-level, POJO-field, and tuple-element selection and transport.
Row transport uses explicit `RowTypeInfo`; lists use explicit `ListTypeInfo` on every supported runtime.
Flink 2.x also infers the native element type from `TypeHint<List<MyMessage>>`; Flink 1.20 needs explicit list element information.
This does not promise inference from an erased runtime List instance.

## Planned releases

The [release contract](docs/adr/0001-native-protobuf-type-integration.md) separates the first usable release from schema evolution.
The serializer implements generated-class validation, bounded framing, immutable copying, configurable size, depth, and deterministic-writing behavior, and versioned descriptor snapshots.
The release table includes implemented transport and serializer restoration, plus the remaining value-type acceptance, documentation, and publication requirements.

| Release | Planned capability |
|---|---|
| [0.1.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/6) | Native generated-message integration and unchanged-schema state restore, including common Google Well-Known Types and OpenTelemetry composite messages; GitHub Pages documentation and Maven Central publication |
| [0.2.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/14) | Supported descriptor-based schema evolution and restore from published 0.1.0 state |
| [0.3.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/20) | Explicit-descriptor DynamicMessage support, with documented API/state upgrade behavior |

The 0.1.0 contract rejects changed schemas while allowing unchanged-schema value-state restore with increased reader limits or a deterministic-mode change.
This includes otherwise wire-safe field additions; see [Protobuf updates and state compatibility](docs/compatibility.md) for schema examples, Java runtime version guarantees, and the current versus planned restore behavior.
Protobuf messages are supported as values; use stable scalar keys for DataStream partitioning and MapState user keys.
There is no message-key opt-in: deterministic bytes do not make generated message hash codes stable across application classloaders or JVMs.
Both inferred and explicitly typed `keyBy` can accept message keys despite `isKeyType() == false`; that use remains unsupported.
The 0.x releases may contain breaking changes to APIs, defaults, runtime requirements, or saved-state compatibility while the design matures toward 1.0.0.
Each release must document its specific breaks, supported upgrade paths, and required migration or fresh-state/replay procedure; check that guidance before upgrading.
Separate API checks and released-state fixtures must verify supported paths and explicit rejection of unsupported state.
Version 1.0.0 will establish the stable API and forward-restore contract; compatibility with every earlier 0.x release is not implied.
The current roadmap ends at 0.3.0; the remaining work will determine whether further 0.x releases or preparation for 1.0.0 comes next.
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
