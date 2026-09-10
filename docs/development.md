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
| `just skills-sync` | Refresh the four shared workflow skills at the recorded dev-tools commit |

Run a targeted class while iterating, for example `./mvnw -ntp -Dtest=UnregisteredTest test`.
Run the full `just verify` for this single-module library before pushing build or Java changes.
CI runs `just verify-protobuf 3` and `just verify-protobuf 4` on each supported JDK; it uses the versions in the Maven profiles.
The required checks above do not include a mutation-testing batch.
Apply the shared review safeguards to any batch that is performed.
To reproduce the JDK 21 lane locally, use `mise x java@temurin-21 just -- just verify-protobuf 4`.
The default tool set includes JDK 17; mise may install JDK 21 for that command.

Tests named `*Test` run in Maven's test phase.
Tests named `*ITCase` run through the connector parent's Surefire integration-test execution.
Each class gets a fresh JVM because Flink's TypeInfoFactory registry is process-global.
Native probes disable generic types and run without `--add-opens`.
The optional `chill` profile adds Chill only in test scope, and its tagged comparison runs only when the recipe clears the default tag exclusion.

The committed `.proto` files are generated into `target/generated-test-sources/protobuf`.
Clean when switching profiles so generated code and runtime stay paired.
Neither generated messages nor any test instrumentation belongs in the library jar.
The native transport probe deliberately has no serializer snapshot implementation; its results do not establish state compatibility.

### Native serializer implementation

The production `ProtobufTypeSerializer` is internal machinery with package-private construction.
The serializer tests construct it directly; application-facing TypeInformation and factory construction belong to issue #9.
The native transport probes still exercise their separate test serializer.

The serializer writes a four-byte big-endian payload length followed by Protobuf wire bytes, without materializing the serialized message in a payload array.
Before writing the prefix, a dedicated field-wise calculation checks size with `long` arithmetic and verifies nested message, map-entry, and group depth against the configured parser budget.
It uses a traversal stack and per-call identity-based summaries for shared child values.
Ordinary values are encoded once; ambiguous proto2 strings additionally count generated root output to preserve retained invalid UTF-8 bytes without guessing Java getter names.
The whole graph is validated before this single counting pass, including when several nested strings are ambiguous; its cost is measured separately.
Sizing allocates traversal metadata for each distinct message or unknown-field set visited.
The output wrapper rejects writes beyond the calculated length and checks the actual byte count after flushing.
Tests compare the calculated lengths with actual Protobuf output for scalar and packed encodings, Unicode and retained proto2 string bytes, and unknown fields.
A separate shared-data test verifies rejection before encoding when the logical size exceeds 4 GiB and the runtime's integer size calculation overflows.
The limits constrain accepted inputs; they do not promise a bound on total heap usage or sufficient stack capacity for arbitrarily high configured recursion limits.

Reading validates the prefix before allocating from its value and bounds underlying reads to the current frame.
Object copy returns the immutable input instance; stream copy preserves frame bytes and checks length and truncation without parsing payload semantics or nesting.
Stream copy lazily allocates and reuses a private 4 KiB buffer so Flink memory output views can grow as needed.
Malformed input and I/O failures may leave partial input consumption or output; no resynchronization or transactional output is promised.
The tests also cover parser reconstruction after Java serialization with an isolated generated-message classloader.
Validated defaults, parsers, and normalized schemas are reused through `ClassValue`, keyed by the exact application class rather than its name.
Protobuf's generated accessor tables already retain reflective field methods; the library adds no per-record method lookup or duplicate field-method cache.
Serializer-local copy buffers remain independent, and record size/depth summaries live for one sizing call only.
The [local performance comparison](validation/serializer-performance.md) measures warm construction and representative serialization paths before and after these changes, including the raw-string counting path.
These tests do not establish checkpoint/savepoint recovery or packaged-artifact compatibility across runtime majors.

`snapshotConfiguration()` explicitly rejects managed-state use until issue #10 supplies the versioned snapshot implementation.
The library cannot publish 0.1.0 with this intermediate failure in place.

## Dependencies and packaging

Flink core and protobuf-java are `provided` dependencies.
Applications must supply a runtime compatible with their own generated messages.
This library has no Google Cloud BOM, connector dependency, or shaded dependencies.
The optional Chill dependency is absent from ordinary builds.
The root LICENSE and NOTICE are copied to the jar's META-INF directory; the inherited ASF resource bundle is disabled.

## Agent tooling

AGENTS.md is shared guidance; CLAUDE.md imports it.
Repository skills live under `.agents/skills`, with `.claude/skills` pointing there for Claude Code.
The four workflow skills come from [flink-gcp-dev-tools](https://github.com/flink-gcp/flink-gcp-dev-tools), at the full commit SHA recorded as `dev_tools_revision` in `justfile`.
Their tracked copies work offline and are present in new Git worktrees; ordinary builds do not download them.

To update them, choose a reviewed commit from dev-tools main, update the pin, and run `just skills-sync` from this repository's root.
The recipe requires Bash, Git, `gh`, tar, and just, and preserves unrelated skills.
It refuses to overwrite differing uncommitted changes in the managed directories.
Review and commit the pin and resulting skill changes together.
Keep `dev-tools.just` aligned with `examples/skills.just` at the selected upstream commit when that recipe changes.
Use the same procedure with an earlier pin to roll back.
The shared skills preserve the original connector workflow, including its detailed checks, examples and recorded unavailable-reviewer exception.
Their historical connector examples do not require connector-specific tools here: use the single-module verification commands and Protobuf/JDK compatibility requirements above.
The original one-commit push procedure and matching WHAT/WHY commit/PR description apply.
Apply the closing-reference check to an issue actually assigned to the PR.
When no closing issue has been assigned, verify that the PR closes none; creating or routing an issue requires the maintainer's decision.
Project-specific commands and compatibility bindings belong in AGENTS.md or this guide, outside those four directories.
The WHAT/WHY PR template is a deliberate copy of the shared template and is reviewed separately when it changes.

The optional Context7 and Serena connections are declared in `.mcp.json` and `.codex/config.toml`.
Activate the current worktree when using Serena, and keep credentials and personal configuration outside tracked files.
Neither MCP server is required to build or test the project.

## Next implementation steps

The [ADR-0001 release contract](adr/0001-native-protobuf-type-integration.md) defines the initial public entry points, settings/defaults, framing, snapshot format, and descriptor normalization.
The 0.x policy permits documented breaking changes while the design matures; 1.0.0 is the stabilization point.
The release sequence and current implementation status are:

1. The immutable serializer in [#8](https://github.com/flink-gcp/flink-datastream-protobuf/issues/8) implements generated-class validation, transient parser reconstruction, limits, framing, copy, failure behavior, and the descriptor normalization needed for serializer identity.
2. Add TypeInformation and superclass TypeInfoFactory registration in [#9](https://github.com/flink-gcp/flink-datastream-protobuf/issues/9), and complete versioned descriptor snapshots with unchanged-schema restore in [#10](https://github.com/flink-gcp/flink-datastream-protobuf/issues/10).
3. Verify production transport, checkpoint/savepoint recovery, and isolated user-code classloading in [#11](https://github.com/flink-gcp/flink-datastream-protobuf/issues/11); cover Google Well-Known Types and OpenTelemetry generated composite messages in [#18](https://github.com/flink-gcp/flink-datastream-protobuf/issues/18).
4. Add compiled usage examples and the complete guide in [#12](https://github.com/flink-gcp/flink-datastream-protobuf/issues/12), then implement the shared-design GitHub Pages site in [#19](https://github.com/flink-gcp/flink-datastream-protobuf/issues/19), amending ADR-0002 with the actual publishing workflow.
5. Prepare publication and packaged-artifact validation in [#13](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13). Complete [release #6](https://github.com/flink-gcp/flink-datastream-protobuf/issues/6) only after 0.1.0 is published, a consumer verifies it, and snapshot/savepoint fixtures from that published artifact are preserved with provenance.
6. For [0.2.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/14), add the pure directional evaluator in [#15](https://github.com/flink-gcp/flink-datastream-protobuf/issues/15), integrate supported schema evolution and restore from published 0.1.0 state in [#16](https://github.com/flink-gcp/flink-datastream-protobuf/issues/16), and add release-to-release API checks and upgrade documentation in [#17](https://github.com/flink-gcp/flink-datastream-protobuf/issues/17).
7. For [0.3.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/20), compare shared and separate explicit-descriptor DynamicMessage APIs, serializers and snapshots in [#21](https://github.com/flink-gcp/flink-datastream-protobuf/issues/21) before implementing integration and state recovery in [#22](https://github.com/flink-gcp/flink-datastream-protobuf/issues/22) and [#23](https://github.com/flink-gcp/flink-datastream-protobuf/issues/23). Document API/state compatibility decisions and migration requirements for generated-message users, and test the declared direct/sequential upgrade outcomes before release.

After 0.3.0, assess remaining issues, missing implementation and validation gaps before adding another milestone.
Define further 0.x work when needed; prepare 1.0.0 only when the design and compatibility contract are ready to stabilize.
There is no scheduled 1.0.0 milestone in the current roadmap.

Implementation PRs update documentation for behavior they actually deliver.
Until they land, API examples in the ADR are contract declarations, not runnable library usage, and the README retains the unimplemented status.
The optional Chill probe is not a production fallback or a release acceptance substitute.

### Contract acceptance scenarios

The following scenarios are obligations for the linked implementation issues, not tests already passed by this documentation change.

| Area | Required scenarios | Owner |
|---|---|---|
| Construction and identity | Supported generated hierarchies; invalid classes and extension-dependent graphs; isKeyType remains false with all settings; no message-key opt-in; immutable builder results; default and nondefault settings in equality/hashCode/canEqual | #8, #9 |
| Framing and limits | Empty and adjacent frames; negative, excessive, overflow-prone, and truncated lengths; malformed/uninitialized payloads; size/depth boundaries on write and read; nested/map/unknown-group recursion; no read-ahead into the next frame; exact stream copy | #8 |
| Native selection | Generic types disabled; explicit and AbstractMessage registration; fresh-JVM missing-registration and Message-interface controls; top-level/POJO/tuple transport; explicitly typed Row/List paths | #9, #11 |
| Snapshot and normalization | Version-1 round trips; unsupported/corrupt data and flags; missing/conflicting imports; recursive message graphs; deterministic dependency ordering; SourceCodeInfo ignored but options/declaration order/unknown content retained; same fingerprint with differing descriptor content cannot establish compatibility | #10 |
| Unchanged-schema restore | Changed fields, names, imported descriptors and framing rejected; unchanged/increased limits and either deterministic-mode transition accepted; any decreased limit rejected, including mixed transitions and decreases after new snapshot emission; isolated classloaders; missing/unsupported classes; old metadata readable without old generated classes | #10, #11 |
| Runtime values and types | Continued processing and restored values in scalar-keyed and operator state, including MapState values with scalar user keys; unknown fields; nested WKT/OTel values; Any payload bytes kept opaque; negative controls show both inferred and explicitly typed keyBy bypass the non-key declaration and equal message bytes can hash into different groups across isolated classloaders | #9, #11, #18 |
| Published release baseline | Attributable message/snapshot bytes and complete savepoints from the published artifact, expected values, settings, schemas/imports, job/state identity, generation commands, and exact source/artifact/toolchain provenance; retain fixtures without overwriting them with newer writers | #10, #11, #13 |
| 0.2.0 upgrade | Directional accepted/rejected descriptor pairs; real restoration of claimed supported released 0.1.0 fixtures with old generated classes absent; explicit rejection and migration guidance for any intentional break; API checks separate from state and gencode/runtime checks | #15, #16, #17 |
| 0.3.0 upgrade decisions | Source/API checks; already-compiled consumers for unchanged APIs and compiled replacements for intentional breaks; direct and sequential upgrade outcomes, including newly emitted state after 0.2.0 evolution; retain published fixtures and test supported restore/migration or explicit rejection; document each break and required procedure | #20, #21, #22, #23 |

Run production acceptance on JDK 17/21 and the pinned Protobuf 3.25.x/4.x profiles with matching application gencode/runtime pairs, including a packaged-library consumer.
Compatibility across changed built-in descriptors is not implied by supporting both runtime profiles.
Single-JVM MiniCluster success does not prove cross-process key hashing; the supported keyed scenarios extract scalar keys and do not use generated messages as keys.

The current matrix proves that these feasibility probes compile and run in each combination.
It does not yet prove binary compatibility of a production library jar across Protobuf majors or compatibility of saved state.
