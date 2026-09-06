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

# ADR-0002: A single Maven module with a shared local and CI workflow

- Status: Accepted
- Date: 2026-09-06
- Current instructions: [Development](../development.md) and [Contributing](../../CONTRIBUTING.md)

## Decision

Use one jar module with Maven coordinates `io.github.flink-gcp:flink-datastream-protobuf`, starting at `0.1.0-SNAPSHOT`.
Develop it in the flink-gcp organization without depending on flink-connector-gcp.
Reuse that project's Apache-2.0 licensing, collective copyright identity, Maven Wrapper, connector parent 2.0.0, mise/just entry points, Java formatting, Checkstyle, Apache RAT, JUnit 5, and AssertJ conventions.
The project metadata and packaged notices identify The flink-gcp authors rather than inheriting ASF ownership.
Keep third-party attribution on the Apache-derived Checkstyle files.

Compile for Java 17 and verify on JDK 17 and 21.
Resolve protobuf-java and protoc together from a Maven profile for each tested major.
Generate test messages under target and keep test instruments out of the jar.
Run each test class in a fresh JVM to isolate Flink's static factory registry.
The current probes require no Docker, cloud credentials, or module-opening flags.

CI calls the same just recipes used locally.
The PR orchestrator has no path filter and requires both reusable workflows to succeed through one `CI passed` gate.
An unexpected skipped, failed, or cancelled dependency cannot satisfy that gate.
Required-check policy can name this aggregate check without depending on individual matrix job names.
Actions are pinned to commit SHAs; linter versions live in mise.toml, while Java build-tool versions live in Maven.

All changes after the initial empty main commit use a dedicated worktree and a Draft PR with the WHAT/WHY template.
Require two distinct self-review rounds, an independent reviewer that did not author the change, and current aggregate CI before Ready.
Freeze the base and head for review, use range-diff for bounded repairs after a completed pass, and keep feedback inline.
Agent guidance and skills make this procedure discoverable without copying private memory, credentials, or connector-specific instructions.

## Consequences

A parent reactor and separate adapter/examples modules are unnecessary before there is code to separate.
Hugo, deployment infrastructure, release automation, and a published-API binary-compatibility gate can be added when those artifacts exist.
This bootstrap creates neither a Maven Central release nor a stable public API.
