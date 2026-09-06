# Flink DataStream Protobuf

Native Protocol Buffers type integration for the Apache Flink DataStream API.

This project is under development.
The repository currently contains the build, development workflow, design records, and executable feasibility probes.
It does not yet provide a production TypeInformation, serializer, or snapshot implementation, and no release is available.

The planned artifact is `io.github.flink-gcp:flink-datastream-protobuf`.
The initial target is Flink 2.3, full-runtime generated Protobuf messages, and JDK 17 and 21.
The probes run with protobuf-java 3.25.8 and 4.33.6.

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
