# Flink DataStream Protobuf

Native Protocol Buffers type integration for the Apache Flink DataStream API.

This project is under development.
The repository currently contains the build, development workflow, design records, and executable feasibility probes.
It does not yet provide a production TypeInformation, serializer, or snapshot implementation, and no release is available.

The planned artifact is `io.github.flink-gcp:flink-datastream-protobuf`.
The initial target is Flink 2.3, full-runtime generated Protobuf messages, and JDK 17 and 21.
The probes run with protobuf-java 3.25.8 and 4.33.6.

## Planned releases

The [release contract](docs/adr/0001-native-protobuf-type-integration.md) separates the first usable release from schema evolution.
None of these production capabilities is implemented yet.

| Release | Planned capability |
|---|---|
| [0.1.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/6) | Native generated-message integration and unchanged-schema state restore, including common Google Well-Known Types and OpenTelemetry composite messages; GitHub Pages documentation and Maven Central publication |
| [0.2.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/14) | Supported descriptor-based schema evolution and restore from published 0.1.0 state |
| [0.3.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/20) | Explicit-descriptor DynamicMessage support, with documented API/state upgrade behavior |

The 0.1.0 contract rejects changed schemas while allowing unchanged-schema value-state restore with increased reader limits or a deterministic-mode change.
Protobuf messages are supported as values; use stable scalar keys for DataStream partitioning and MapState user keys.
There is no message-key opt-in: deterministic bytes do not make generated message hash codes stable across application classloaders or JVMs.
Both inferred and explicitly typed `keyBy` can accept message keys despite `isKeyType() == false`; that use remains unsupported.
The 0.x releases may contain breaking changes to APIs, defaults, runtime requirements, or saved-state compatibility while the design matures toward 1.0.0.
Each release must document its specific breaks, supported upgrade paths, and required migration or fresh-state/replay procedure; check that guidance before upgrading.
Separate API checks and released-state fixtures must verify supported paths and explicit rejection of unsupported state.
Version 1.0.0 will establish the stable API and forward-restore contract; compatibility with every earlier 0.x release is not implied.
The current roadmap ends at 0.3.0; the remaining work will determine whether further 0.x releases or preparation for 1.0.0 comes next.
The planned library has no Kryo/Chill implementation or internal fallback; Flink can still select its own generic serializer when native integration is not selected, so native usage will set `pipeline.generic-types: false`.
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

This library concerns Flink's internal DataStream serialization, including the planned integration with managed state.
For external Table/SQL Protobuf bytes, Apache Flink provides the separate [`flink-sql-protobuf` format](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/protobuf/).

## License and provenance

Licensed under the [Apache License, Version 2.0](LICENSE).
Build configuration and development conventions are adapted from [flink-connector-gcp](https://github.com/flink-gcp/flink-connector-gcp), which is developed in the same organization; this library does not depend on that project.
The Apache-derived Checkstyle configuration retains its original headers and attribution in [NOTICE](NOTICE).

This is an independent project, not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google.
Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.
