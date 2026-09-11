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

# Spike 0 validation

## Outcome

On 2026-09-06, the required superclass-registration and three transport paths passed on Flink 2.3.0 with both tested Protobuf runtimes and both JDKs.
The stop condition did not occur, so the project can proceed with configuration registration as a supported design path alongside explicit TypeInformation.

The initial Chill transport succeeded: Chill 0.7.6 transported a StringValue while Kryo 5.6.2 was loaded.
H1 is not supported by this observation.
The experiment did find both Kryo major versions in the dependency graph; their coexistence did not make this particular job fail.

## Environment and method

The initial experiment ran in a disposable Maven project on macOS arm64 before the first repository commit.
It used Maven 3.9.16, Flink 2.3.0, JUnit 5.14.4, and protobuf-maven-plugin 5.1.8.
Temurin 17.0.20+8 and Temurin 21.0.5+11 ran without `--add-opens`.
Each native run regenerated the same message schema with protoc matching the runtime, and each test class ran in its own JVM.

| Java | protobuf-java / protoc | Initial native result |
|---|---|---|
| 17 | 3.25.8 | 2 tests passed, no failures or skips |
| 17 | 4.33.6 | 2 tests passed, no failures or skips |
| 21 | 3.25.8 | 2 tests passed, no failures or skips |
| 21 | 4.33.6 | 2 tests passed, no failures or skips |

One initial test checked registration, child TypeInformation, and MiniCluster transport together; the other was an unregistered negative control.
The retained suite also includes a separate Message-interface registration control.
The matrix is source compilation and execution for each combination, not a single precompiled library artifact tested across runtimes.

### Native registration

Configuration registers the test factory against `com.google.protobuf.AbstractMessage` before type extraction and sets `pipeline.generic-types: false`.
The test asserts that the factory produces the concrete generated message type and its dedicated probe serializer.
Top-level source inference, a POJO's message field, and a Tuple2 element all resolve that TypeInformation without `.returns(...)` overrides.
Without registration, serializer creation fails because generic types are disabled.
The Message interface is a negative control for the superclass-only lookup.

Five bounded sources feed a local MiniCluster at parallelism two with operator chaining disabled and rebalance boundaries before reading message fields.
The results distinguish top-level, POJO, tuple, Row, and List paths.
Row uses an explicitly supplied RowTypeInfo; this does not establish automatic Row field inference.
List element information is inferred from `TypeHint<List<ProbeMessage>>` and then supplied to the source; this does not establish inference from an erased runtime List instance.
That observation describes the original Flink 2.3 probe.
The [ADR-0003 compatibility matrix](../adr/0003-flink-version-compatibility.md) extends verification to Flink 2.2 and 1.20: the current probe transports an explicit `ListTypeInfo` on every runtime and separately asserts the inference result, which falls back to a generic type on 1.20.

An initial exploratory assertion expected List TypeHint extraction to fall back to generic serialization.
It did not: Flink 2.3.0 produced ListTypeInfo with the registered element type.
The probe was corrected to assert and exercise that observed behavior before recording the successful matrix above.

### Chill comparison

The comparison registers `com.twitter.chill.protobuf.ProtobufSerializer` as a default Kryo serializer for Message through `pipeline.serialization-config`.
It leaves generic types enabled, as required by that path, and uses protobuf-java's built-in StringValue so no application gencode/runtime mismatch can explain its outcome.
The tested runtime is protobuf-java 3.25.8; this is not an exact reproduction of the older protobuf-java 3.7.0 documentation example.

```text
com.esotericsoftware:kryo:5.6.2                  (Flink core)
com.twitter:chill-protobuf:0.7.6
  com.twitter:chill-java:0.7.6
  com.esotericsoftware.kryo:kryo:2.21
com.google.protobuf:protobuf-java:3.25.8

Loaded Kryo:             kryo-5.6.2.jar
Loaded ProtobufSerializer: chill-protobuf-0.7.6.jar
Loaded StringValue:      protobuf-java-3.25.8.jar
StringValue type:        GenericType<com.google.protobuf.StringValue>
Observed output:        chill-probe
Tests:                  1 passed, no failures or skips
```

This establishes one successful bounded transport on JDK 17.
It does not establish all Chill registrations, classpath orders, Protobuf versions, or savepoint compatibility.
The library's rationale is native type integration and an explicit state compatibility policy, rather than a claim that this Chill configuration fails.

## Reproduction

From a trusted checkout with the development toolchain installed, select the original Flink 2.3.0 baseline explicitly:

```sh
mise x java@temurin-17 just -- just verify-flink 2.3.0 3
mise x java@temurin-17 just -- just verify-flink 2.3.0 4
mise x java@temurin-21 just -- just verify-flink 2.3.0 3
mise x java@temurin-21 just -- just verify-flink 2.3.0 4
mise x java@temurin-17 -- ./mvnw -ntp clean -Pchill -Dflink.version=2.3.0 -Dtest.excluded.groups= verify
./mvnw -ntp -Pchill -Dflink.version=2.3.0 dependency:tree
```

Exact JDK patches may differ from the dated initial observation because the development commands select a maintained major version.
The retained tests and schema are the reproducible evidence; generated classes and local logs stay outside version control.
The optional comparison test prints the loaded artifact locations and checks the observed payload.

## Limits

The test-only serializer deliberately rejects snapshot creation.
These probes establish factory selection and transport, not checkpoint/savepoint restore, a stable serialization format, schema evolution, isolated user-code classloaders, or production performance.
Those are acceptance tests for the implementation steps in [Development](../development.md).
The original observation did not measure Flink 2.2 or 1.20; the current compatibility checks and their limits are described in [Development](../development.md#flink-compatibility-checks).
