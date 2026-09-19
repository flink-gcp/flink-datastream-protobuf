---
title: Quickstart
weight: 10
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

# Run a native Protobuf DataStream job

Build the development library locally, install it into an isolated Maven repository, and run the standalone [application example](../examples/datastream/pom.xml).
No library version is available from Maven Central yet.
The 0.x versions are development milestones; 1.0.0 is the planned first public release.

## Build the development artifact

Install [mise](https://mise.jdx.dev/) and Python 3.9 or newer, then clone this repository.
The recipe uses Python's standard library; `mise install` does not install Python.
Run these commands from its root with JDK 17:

```sh
mise trust
mise install
example_repo="$(mktemp -d /tmp/protobuf-example-maven.XXXXXX)"
mise x -- just examples-verify 2.2.1 3 "$example_repo"
```

Keep `example_repo` set for the following commands in the same shell.
The recipe builds the Flink 2.x library at its 2.2.1 compile floor, installs it locally, and compiles and tests the independent application against that jar.
It also packages the application, launches all three entrypoints using the packaged classes, and checks the Maven launch commands below.
It needs neither a running Flink cluster nor Docker.
Initial dependency downloads require network access.

The application uses the normal Maven dependency:

```xml
<dependency>
    <groupId>io.github.flink-gcp</groupId>
    <artifactId>flink-datastream-protobuf</artifactId>
    <version>${protobuf.integration.version}</version>
</dependency>
```

The example POM currently sets `protobuf.integration.version` to `0.1.0-SNAPSHOT`.
The library declares Flink core and `protobuf-java` as provided dependencies, so the application explicitly supplies its Flink dependencies and Protobuf runtime.
The example also generates its own [Event message](../examples/datastream/src/main/proto/event.proto) using the matching protoc version.
It does not import this repository's test messages or source directories.

Both development Flink adapter builds currently use the same SNAPSHOT coordinates.
Use a separate Maven repository for each adapter/Protobuf-major combination; the recipe rejects reuse across those combinations.
A SNAPSHOT version alone does not identify the writer of saved state: retain the exact source revision, jar checksum, schema, and runtime versions when keeping development state.
See [development baselines](validation/development-baselines.md).

## Run explicit type selection

```sh
mise x -- ./mvnw -ntp -f examples/datastream/pom.xml \
  "-Dmaven.repo.local=$example_repo" exec:exec@explicit
```

The result includes `alice:3` and `bob:8`; task prefixes and output order can vary.
The linked [explicit example](../examples/datastream/src/main/java/io/github/flink/gcp/protobuf/examples/ExplicitExample.java) contains the imports for the snippets below.

`Event` is generated from the example's `event.proto`; replace its import with the generated message class from the application.
The example disables generic types before creating the environment:

{{< example "ExplicitExample#explicit-configuration" >}}

It supplies native type information to the source:

{{< example "ExplicitExample#explicit-source" >}}

The transformation declares the same native output type:

{{< example "ExplicitExample#explicit-returns" >}}

See [ExplicitExample.java](../examples/datastream/src/main/java/io/github/flink/gcp/protobuf/examples/ExplicitExample.java) for the complete executable entrypoint, including environment creation.
The library has no internal Kryo fallback.
Flink can select its own generic serializer when native integration is absent; disabling generic types makes that accidental selection fail.

## Run registration and state examples

Run the factory example; its Maven execution passes the directory containing the supplied `config.yaml`:

```sh
mise x -- ./mvnw -ntp -f examples/datastream/pom.xml \
  "-Dmaven.repo.local=$example_repo" \
  exec:exec@registered
```

The result includes `alice:2` and `bob:7`.
The Maven executions use `exec:exec` to launch a separate JVM with the application classpath, avoiding the in-process `exec:java` classloader.
Each command launches a fresh JVM; factory registration is process-global.
The [usage guide](usage.md#factory-registration) explains registration timing and nested types.

Run the state example:

```sh
mise x -- ./mvnw -ntp -f examples/datastream/pom.xml \
  "-Dmaven.repo.local=$example_repo" \
  exec:exec@stateful
```

The result includes `alice:2:0`, `alice:5:2`, `alice:9:3`, and `bob:7:0`.
Each record contains the account, running total, and previous event amount for that account.
This bounded job demonstrates ValueState and MapState values; it is not a checkpoint/savepoint recovery test.
See [state and recovery](usage.md#state-and-recovery) for persistent-state usage and the separate recovery evidence.

## Submit the packaged application

The verified application jar is `examples/datastream/target/example-job.jar`.
It contains the example classes, generated Event, library, and unrelocated Protobuf runtime.
It excludes Flink classes, which the matching Flink distribution supplies.
Submit the explicit example to a running Flink 2.2.1 cluster with:

```sh
"$FLINK_HOME/bin/flink" run examples/datastream/target/example-job.jar
```

Use `-c io.github.flink.gcp.protobuf.examples.StatefulExample` for the state entrypoint.
For the registration entrypoint, use `-c io.github.flink.gcp.protobuf.examples.RegisteredExample` and pass `examples/datastream/config` after the jar path; that directory must be available where the entrypoint executes.
Avoid supplying a second, conflicting Protobuf runtime through the cluster classpath.
The tests launch the packaged entrypoints locally with Flink dependencies; they do not deploy a remote cluster.

## Select another tested runtime

| Flink | JDK | Matching protoc and protobuf-java |
|---|---|---|
| 2.2.1 and 2.3.0 | 17, 21 | 3.25.9 or 4.33.6 |
| 1.20.4 | 17 | 3.25.9 or 4.33.6 |

For example, use a new repository to verify Flink 1.20 with Protobuf 4:

```sh
lts_repo="$(mktemp -d /tmp/protobuf-example-lts.XXXXXX)"
mise x -- just examples-verify 1.20.4 4 "$lts_repo"
mise x -- ./mvnw -ntp -f examples/datastream/pom.xml \
  "-Dmaven.repo.local=$lts_repo" -Dflink.version=1.20.4 \
  -Dprotobuf.version=4.33.6 exec:exec@explicit
```

Keep the same version overrides when running the other entrypoints.
Clean and rebuild when switching runtime/gencode combinations; the recipe does this for both projects.
The recipe compiles a 1.20 library for LTS, and uses a floor-built 2.x library for either supported 2.x consumer runtime.
These profile-specific builds do not establish that one library jar works across Protobuf majors.
Mixed generated-code/runtime pairs and cross-version state restoration are outside this verification.

## Switch to Maven Central at 1.0.0

The application already resolves the library by Maven coordinates; it needs no source or classpath rewrite for publication.
During [1.0.0 preparation](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13), change the dependency version to `1.0.0` for Flink 2.x or `1.0.0-1.20` for LTS, and replace the local-build introduction with published-artifact instructions.
Keep the development recipe for contributors.
After both artifacts are published, verify these same examples using a fresh Maven repository without first installing the library, and with the release's supported runtime/gencode settings.
Only then describe Central resolution as verified.
This switch does not promise source or state compatibility with arbitrary development snapshots; consult the [compatibility and upgrade guidance](compatibility.md) first.
