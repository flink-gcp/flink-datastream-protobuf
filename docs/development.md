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

# Development

## Toolchain

The Maven Wrapper pins Maven 3.9.16; all Maven recipes in just invoke the wrapper.
The default local JDK is Temurin 17, and CI runs Temurin 17 and 21.
Java output targets release 17.
`mise.toml` supplies just, the workflow and Markdown linters, pinact, and uv for optional Serena integration.
Maven supplies Java build plugins and protoc; protoc does not need to be on PATH.

```sh
mise trust
mise install
mise x -- just --list
```

## Build and test commands

| Command | What it runs |
|---|---|
| `just verify` | Formatting checks, Checkstyle, compilation, unit tests, integration tests, jar packaging, and Apache RAT |
| `just format` | Spotless with Flink's AOSP Java formatting and import order |
| `just verify-protobuf 3` | Clean verification with protobuf-java and protoc 3.25.8 |
| `just verify-protobuf 4` | Clean verification with protobuf-java and protoc 4.33.6 |
| `just probe-chill` | Clean verification including the optional Chill comparison |
| `just lint` | actionlint with the pinned shellcheck, and markdownlint-cli2 |
| `just pin-actions` | Pin GitHub Actions references to commit SHAs |

Run a targeted class while iterating, for example `./mvnw -ntp -Dtest=UnregisteredTest test`.
Run the full `just verify` for this single-module library before pushing build or Java changes.
CI runs `just verify-protobuf 3` and `just verify-protobuf 4` on each supported JDK; it uses the versions in the Maven profiles.
To reproduce the JDK 21 lane locally, use `mise x java@temurin-21 just -- just verify-protobuf 4`.
The default tool set includes JDK 17; mise may install JDK 21 for that command.

Tests named `*Test` run in Maven's test phase.
Tests named `*ITCase` run through the connector parent's Surefire integration-test execution.
Each class gets a fresh JVM because Flink's TypeInfoFactory registry is process-global.
Native probes disable generic types and run without `--add-opens`.
The optional `chill` profile adds Chill only in test scope, and its tagged comparison runs only when the recipe clears the default tag exclusion.

The committed `.proto` file is generated into `target/generated-test-sources/protobuf`.
Clean when switching profiles so generated code and runtime stay paired.
Neither generated messages nor any test instrumentation belongs in the library jar.
The native transport probe deliberately has no serializer snapshot implementation; its results do not establish state compatibility.

## Dependencies and packaging

Flink core and protobuf-java are `provided` dependencies.
Applications must supply a runtime compatible with their own generated messages.
This library has no Google Cloud BOM, connector dependency, or shaded dependencies.
The optional Chill dependency is absent from ordinary builds.
The root LICENSE and NOTICE are copied to the jar's META-INF directory; the inherited ASF resource bundle is disabled.

## Agent tooling

AGENTS.md is shared guidance; CLAUDE.md imports it.
Repository skills live under `.agents/skills`, with `.claude/skills` pointing there for Claude Code.
The optional Context7 and Serena connections are declared in `.mcp.json` and `.codex/config.toml`.
Activate the current worktree when using Serena, and keep credentials and personal configuration outside tracked files.
Neither MCP server is required to build or test the project.

## Next implementation steps

1. Add the production TypeInformation and TypeInfoFactory with explicit and superclass-registration entry points, generated-class validation, and the key policy.
2. Add the immutable-message serializer with a transient parser and tested framing, copy, Java-serialization, and failure behavior.
3. Add descriptor snapshots and a separate compatibility evaluator with descriptor-pair tests for the accepted and rejected changes in ADR-0001.
4. Exercise operator transfer, checkpoint/savepoint restore, and isolated user-code classloaders. Establish versioned savepoint fixtures once the snapshot format exists.
5. Add compiled usage examples, complete API documentation, and release preparation after the implementation is usable.

The current matrix proves that these feasibility probes compile and run in each combination.
It does not yet prove binary compatibility of a production library jar across Protobuf majors or compatibility of saved state.
