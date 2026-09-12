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
The binary compatibility recipe also needs Python 3.9 or newer, using only its standard library.

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
| `just verify-flink 1.20.4 3` | Clean Flink 1.20 verification with the `flink1` adapters and Protobuf 3 |
| `just verify-flink 2.3.0 4` | Clean Flink 2.3 verification with Protobuf 4 |
| `just binary-compat 2.3.0 3` | Build at the POM's 2.x floor, then run the unchanged jar and tests at the ceiling |
| `just probe-chill` | Clean verification including the optional Chill comparison |
| `just lint` | actionlint, shellcheck for project scripts, and markdownlint-cli2 |
| `just pin-actions` | Pin GitHub Actions references to commit SHAs |
| `just skills-sync` | Refresh the four shared workflow skills at the recorded dev-tools commit |

Run a targeted class while iterating, for example `./mvnw -ntp -Dtest=UnregisteredTest test`.
Run the full `just verify` for this single-module library before pushing build or Java changes.
CI verifies both Protobuf profiles on Flink 2.2.1 and 2.3.0 with JDK 17/21, and Flink 1.20.4 with JDK 17.
It also runs the binary compatibility recipe for each Protobuf profile on JDK 17.
These lanes run for each PR, main push, manual verification, and weekly schedule.
The required checks above do not include a mutation-testing batch.
Apply the shared review safeguards to any batch that is performed.
To reproduce the JDK 21 lane locally, use `mise x java@temurin-21 just -- just verify-protobuf 4`.
The default tool set includes JDK 17; mise may install JDK 21 for that command.

Tests named `*Test` run in Maven's test phase.
Tests named `*ITCase` run through the connector parent's Surefire integration-test execution.
Each class gets a fresh JVM because Flink's TypeInfoFactory registry is process-global.
Native integration tests and probes disable generic types and run without `--add-opens`.
The optional `chill` profile adds Chill only in test scope, and its tagged comparison runs only when the recipe clears the default tag exclusion.

The ordinary test schemas in `src/test/proto` generate Java into `target/generated-test-sources/protobuf`.
This includes the [pinned official OTel common schema and WKT application envelope](common-types.md#otel-schema-provenance-and-regeneration).
The OTel source retains its upstream license header and is generated with each profile's protoc; no OTel Java artifact is added as a dependency.
The isolated runtime application has separate schemas and outputs, described in [Test resources and version updates](#test-resources-and-version-updates).
Clean when switching profiles so generated code and runtime stay paired.
Neither generated messages nor any test instrumentation belongs in the library jar.
The native transport probe deliberately has no serializer snapshot implementation; its results do not establish state compatibility.

### Flink compatibility checks

The [compatibility decision](adr/0003-flink-version-compatibility.md) defines the supported range and publication requirements.
The default `flink.version` in `pom.xml` is the 2.x compile floor; `FLINK_CEILING` in `verify.yaml` selects the other supported minor.
The `flink1` Maven profile activates with `-Dflink.compat=flink1` and supplies the pinned LTS version.
`just verify-flink` selects the matching adapter and cleans before compilation; use it when changing Flink versions.
For example, `just verify-protobuf 4 -Dflink.compat=flink1` tests the POM's LTS default with Protobuf 4.
Maven rejects mismatched Flink majors and adapter selections during validation.
The alternate `src/main/java-flink1` and `src/main/java-flink2` roots bridge the production type information's legacy `createSerializer(ExecutionConfig)` method.
The matching test roots retain separate probe adapters and test the legacy production entry point on 1.20.
Both runtime variants are formatted and linted, but only the selected production and test roots are compiled.

`just verify-flink` and `just binary-compat` delegate to `scripts/verify-flink.sh` and `scripts/binary-compat.py` respectively.
Run them through just so the scripts execute from the repository root with the recipe arguments.
The binary recipe first runs a clean full verification at the floor.
It then invokes only the two Surefire goals at the ceiling, loading production code from the packaged jar and retaining the floor's compiled tests.
Runtime assertions verify the actual Flink core jar version and production code location.
The recipe compares every test's class/name, rejects missing or unsuccessful inventories, and checks hashes of the jar and all compiled classes before and after the rerun.
It clears the generated reports before rerunning so stale reports cannot hide omitted tests.
Each Protobuf profile has its own floor build; this is not a claim that a jar built against one Protobuf major has been tested on the other.

The production integration and transport probe use explicit list element information on all runtimes and additionally check inferred lists: Flink 2.x infers the registered element type, while 1.20 falls back to a generic type that cannot create a serializer with generic types disabled.
Top-level, POJO, tuple, Row, and explicit List transport remain required on every runtime.
Production integration tests independently verify the native type information and serializer on those transport paths.
The separate [runtime acceptance suite](validation/runtime-recovery.md) verifies managed-state recovery for both version lines.
Issue #13 retains verification of the published consumer classpath.

### Native serializer implementation

The production `ProtobufTypeSerializer` is internal machinery with package-private construction.
Serializer unit tests construct it directly; application-facing construction uses `ProtobufTypeInformation.of` or its builder.
`ProtobufTypeInfoFactory` selects the same implementation with the default settings.
The native transport probes still exercise their separate test serializer; `ProtobufRegistrationITCase` and `ProtobufExplicitITCase` exercise production classes.
The production registration suite covers superclass lookup under both generated hierarchies through the paired Protobuf profiles, with fresh-JVM unregistered and Message-interface controls.
The key restriction tests retain both inferred and explicit keyBy bypass controls and show that identical message bytes can select different key groups across isolated generated-class loaders.
Those controls document unsupported message-key use; they are not a promise that the library rejects every such use.
See the [construction and configuration examples](../README.md#native-type-information) for registration timing, defaults, and explicit type settings.

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

`snapshotConfiguration()` returns an internal version-1 `ProtobufTypeSerializerSnapshot`.
It stores both names, framing/normalization versions, all three settings, the complete normalized import closure, and a SHA-256 integrity fingerprint.
The snapshot class/version envelope is written by Flink; the payload follows ADR-0001 exactly, including the independent 64 MiB descriptor limit.
Reading validates metadata without resolving the saved generated class.
Restoration resolves the class through the supplied user-code classloader and checks the complete schema before constructing a serializer with the recorded settings.

Compatibility compares decoded normalized descriptor content, never just bytes or fingerprints.
An unchanged schema permits equal or increased size/depth limits and either deterministic-mode transition; any decreased limit is incompatible.
New snapshots record the new settings, so a later decrease remains incompatible after an earlier successful increase.
No migration or reconfigured serializer result is provided.
Unsupported formats, corrupt data, and invalid descriptors fail during reading; missing, unsupported, or changed generated classes fail explicitly during restoration.

The [format fixtures](../src/test/resources/snapshots/v1/README.md) retain fixed development snapshot/message bytes and writer provenance.
Tests verify the Flink envelope, normalized metadata, actual restored values, isolated classloaders, and rejected corrupt or incompatible inputs.
The separate runtime suite verifies job recovery and retains [development savepoints](../src/test/resources/savepoints/v1/README.md).
Issues #13/#6 own published-artifact fixture capture, consumer verification, and release completion.

## Test resources and version updates

For application upgrade decisions, read [Protobuf updates and state compatibility](compatibility.md).
It distinguishes binary-format rules, Java runtime version guarantees, and this library's current rejection of changed schemas, including field additions.
The procedures below describe how to maintain and verify the test resources.

### Resource inventory

The repository keeps editable test inputs separate from generated build outputs and fixed compatibility fixtures.
Paths below are relative to the repository root.

| Resource | Purpose | How it is produced or consumed |
|---|---|---|
| `src/test/proto/` | Schemas for serializer, snapshot, and transport tests | Maven runs the selected protoc; generated Java goes under `target/generated-test-sources/protobuf` and compiles with the ordinary tests |
| `src/test/runtime-app/java/` and `src/test/runtime-app/proto/{original,changed}/` | Controllable Flink jobs and two runtime schemas with the same generated class/message names; the changed schema adds a field for rejection tests | Maven builds each variant separately under `target/runtime-app/{original,changed}/`; `RuntimeJob` uses isolated runtime gencode, while `CommonTypesRuntimeJob` uses WKT and ordinary test messages from the parent classpath |
| `target/runtime-app/application-{original,changed}.jar` | Application jars submitted to MiniCluster jobs by `RuntimeRecoveryHarness` | Built before tests; disposable build outputs, never published or committed |
| `src/test/resources/snapshots/v1/*.properties` | Four fixed serializer snapshot/message fixtures, including checksums and writer provenance | `ProtobufSnapshotFixtureTest` reads all four in each Protobuf profile and checks restoration and current writer bytes; generation is a separate [format-fixture procedure](../src/test/resources/snapshots/v1/README.md) |
| `src/test/resources/savepoints/v1/` | Complete canonical savepoints, original schema, descriptor set, and manifest | `ProtobufSavepointFixtureITCase` selects the exact runtime version directory and restores it; generation is an explicit [savepoint capture procedure](../src/test/resources/savepoints/v1/README.md#capture) |

`runtime-app` is test application code, not another Maven module or a collection of JUnit tests.
Its `ControlledSource`, `RuntimeJob`, and `CommonTypesRuntimeJob` run inside the MiniCluster jobs started by the integration tests.
The fixture copies of `runtime.proto` and `schema.pb` record writer inputs; Maven generates the current application from `src/test/runtime-app/proto`, not from those copies.

### What verification executes

`just verify` invokes `./mvnw -ntp verify`.
The relevant build and test sequence is:

1. The Protobuf plugin generates ordinary test messages and both runtime application variants with the selected `protoc.version`.
   Each runtime variant also gets a `schema.pb` descriptor set including imports.
2. During `test-compile`, Maven compiles the ordinary tests and separately compiles the shared runtime application Java with each generated variant.
   The runtime outputs are `target/runtime-app/original/classes` and `target/runtime-app/changed/classes`.
3. During `process-test-classes`, the Ant jar tasks package the two application jars.
4. During `test`, Surefire runs `*Test`, including `ProtobufSnapshotFixtureTest`.
5. During `integration-test`, the parent POM's Surefire execution runs `*ITCase`, including the runtime recovery suites below.
   The `verify` lifecycle includes this phase; no separate runtime-test command is needed.

| Test | State source | What it verifies |
|---|---|---|
| `ProtobufRecoveryITCase` | Checkpoints and savepoints created during the current run | Checkpoint restart and replay; separate-job savepoint restore; settings transitions and schema rejection for HashMap/RocksDB |
| `ProtobufSavepointFixtureITCase` | Committed savepoint archives during ordinary verification | Archive hashes, recorded settings, restoration through a fresh application loader, expected state values, and continued processing for HashMap/RocksDB |
| `CommonTypesRecoveryITCase` | Checkpoints and savepoints created during the current run | WKT/OTel values, changing Struct keys and AnyValue variants, and continued processing for HashMap/RocksDB |

These suites use `RuntimeRecoveryHarness` to start jobs from the attached application jars.
The original variant serves successful restores; the changed variant exercises schema incompatibility in `ProtobufRecoveryITCase`.
The common-types job uses the original jar's entry point and source, with WKT classes from protobuf-java and OTel/envelope classes from the ordinary test classpath.
Its [coverage](common-types.md#verification-boundaries) complements the isolated generated-class tests without modifying the existing savepoint fixtures.
The [runtime evidence](validation/runtime-recovery.md) describes the state assertions and compatibility boundaries.
Ordinary verification creates temporary state for its jobs but does not rewrite either committed fixture family.
`just verify-protobuf` and `just verify-flink` clean first, then run this same lifecycle with the selected versions.

### Selecting and capturing savepoint fixtures

Savepoint lookup uses this exact path, including patch versions:

```text
src/test/resources/savepoints/v1/
  flink-<flink.version>/protobuf-<protobuf.version>/<hashmap|rocksdb>/<baseline|raised>/
    savepoint.zip
    manifest.properties
    runtime.proto
    schema.pb
```

Changing a version in `pom.xml` does not modify an existing ZIP.
Verification selects the new version's directory and fails with a capture instruction if its manifest is missing.
It does not fall back to a previous version or automatically capture a replacement.
The current matrix has 24 fixtures: three Flink versions × two Protobuf profiles × two backends × two settings.
Updating one Protobuf profile across that matrix requires 12 fixtures for the new version; adding one Flink version requires eight across both Protobuf profiles.
JDK 17 captures also serve the matching JDK 21 verification lanes.

Use the [capture commands](../src/test/resources/savepoints/v1/README.md#capture) from the repository root for each affected Flink/Protobuf pair.
First run a clean package build with `-DskipTests`, which compiles the test applications without attempting to restore the not-yet-existing fixtures.
Then invoke `surefire:test@integration-tests` with `-Dtest=ProtobufSavepointFixtureITCase`, the same profile/version selections, and these properties:

| Property | Value |
|---|---|
| `test.production.classes` | Absolute path to the packaged library jar from that build; adjust the README example if the project version or final jar name changes |
| `protobuf.fixture.capture` | Output root outside the repository; every destination fixture directory must be absent |
| `protobuf.fixture.revision` | Writer checkout's base revision, as supplied by the README command |

One invocation captures both backends and both settings.
After closing each writer, it archives the complete savepoint, removes the original savepoint and writer checkpoint storage, and restores the relocated archive before checking continued processing.
Review all four files in each resulting fixture, including the manifest's jar/source hashes, runtime and protoc versions, settings, state identity, and expected values.
Copy the reviewed version directories into `src/test/resources/savepoints/v1/` explicitly, then run normal verification with capture disabled.
An interrupted capture can leave a partial destination; retry with a fresh output root.

Add new version directories without relabeling the old writer's provenance.
Any replacement or removal of development fixtures must be an explicit reviewed change.
Regenerated ZIP bytes are not a reproducibility or compatibility verdict; recovery assertions establish the behavior being tested.
Published-release baselines must be retained without rewriting them with later writers; their capture and consumer validation remain in #13/#6.

### Updating Protobuf, schemas, or Flink

Use this sequence for a dependency update, including patch updates proposed by Dependabot.

1. Review the intended compatibility change against [ADR-0001](adr/0001-native-protobuf-type-integration.md) and [ADR-0003](adr/0003-flink-version-compatibility.md).
   Record whether the change affects a runtime version, generated code, test schema, or supported Flink window.
2. Update the matching configuration entries below.
   Keep protobuf-java and application gencode paired, and clean whenever changing profiles.
3. Capture new development savepoints for every newly selected Flink/Protobuf pair using the preceding procedure.
   Capture uses JDK 17 and the packaged library jar, before normal verification can select those new fixtures.
4. Run the existing snapshot-format fixtures unchanged and investigate any incompatibility or byte mismatch.
   Do not regenerate them merely to make an update pass: unlike savepoints, all four are loaded on every run and are not selected by exact runtime version.
   Use their separate generation procedure only for an intentional reviewed baseline change.
5. Run the full matrix below, including the unchanged floor-jar checks for dependency/build changes.
   Update version references and measured scope in this guide, the root README, fixture documentation, and runtime evidence.
   Review configuration, fixture additions, and documentation together before pushing; wait for the current PR CI result.

| Change | Configuration and fixture impact |
|---|---|
| Protobuf 3 runtime | Change the default `protobuf.version` in `pom.xml`; the `protobuf3` profile uses that default |
| Protobuf 4 runtime | Change `protobuf.version` in the `protobuf4` profile |
| protoc only | `protoc.version` normally follows `protobuf.version`; an intentional override changes generated code but not the savepoint lookup path. Preserve the distinct recorded protoc version and review any fixture replacement explicitly |
| Runtime application `.proto` | Edit `src/test/runtime-app/proto/{original,changed}/runtime.proto`; preserve the intended positive/rejection relationship. An incompatible schema change requires an explicit baseline/test decision rather than silently overwriting saved schemas |
| Snapshot-test `.proto` | Review `src/test/proto/snapshot.proto`, its imports, and the fixed snapshot-format contract together; these schemas do not generate the runtime application |
| Flink 2.x floor | Change the default `flink.version` in `pom.xml` |
| Flink 2.x ceiling | Change `FLINK_CEILING` in `.github/workflows/verify.yaml` |
| Flink 1.20 LTS | Change `flink.version` in the `flink1` profile; keep `flink.compat=flink1` on both capture commands |
| Supported Flink minor window | Review and advance floor/ceiling together under ADR-0003; check adapters and CI lanes. Dependabot does not advance Flink major/minor versions automatically |

From the repository root, run these checks with the updated pins; substitute the new ceiling for `2.3.0` when it changes:

```sh
(
  set -e
  for protobuf_major in 3 4; do
    mise x -- just verify-protobuf "$protobuf_major"
    mise x -- just verify-flink 2.3.0 "$protobuf_major"
    mise x java@temurin-21 just -- just verify-protobuf "$protobuf_major"
    mise x java@temurin-21 just -- just verify-flink 2.3.0 "$protobuf_major"
    mise x -- just verify-protobuf "$protobuf_major" -Dflink.compat=flink1
    mise x -- just binary-compat 2.3.0 "$protobuf_major"
  done
  mise x -- just lint
)
```

The subshell stops on the first failure and preserves that command's exit status without changing the calling shell's options.
The binary check preserves the floor's library jar, compiled tests, application jars/classes, and descriptors when executing at the ceiling.
That check and the per-version savepoint fixtures do not establish saved-state migration between Flink versions or Protobuf profiles.
Adding such a guarantee requires an explicit old-writer/new-reader recovery scenario and a documented compatibility decision.

## Dependencies and packaging

Flink core and protobuf-java are `provided` dependencies.
Applications must supply a runtime compatible with their own generated messages.
This library has no Google Cloud BOM, connector dependency, or shaded dependencies.
The optional Chill dependency is absent from ordinary builds.
The root LICENSE and NOTICE are copied to the jar's META-INF directory; the inherited ASF resource bundle is disabled.

### Dependency updates

Dependabot checks Maven and GitHub Actions weekly and proposes ungrouped version updates, subject to its ignore rules and open-PR limit.
During dependency maintenance, also check the `protobuf4` and `flink1` profile pins and `FLINK_CEILING` through the version-update table above; the absence of a Dependabot PR does not establish that these pins are current.
Review Flink major/minor changes as a coordinated supported-window update under ADR-0003; patch updates remain eligible.
Follow the [version-update procedure](#updating-protobuf-schemas-or-flink) for Flink patches as well as Protobuf updates, including new exact-version fixtures and matrix checks.
Keep each Protobuf runtime and protoc pair together.
The existing `org.apache.flink:*` exclusion also covers `flink-connector-parent` major/minor upgrades; check its releases during dependency maintenance and review relevant build-tool changes separately from runtime-window changes.

JUnit major upgrades follow the supported Flink floor, using the same adoption condition as [connector issue #906](https://github.com/flink-gcp/flink-connector-gcp/issues/906).
Check the upstream Apache Flink release's root POM (`flink-parent`) for this condition; this library inherits `flink-connector-parent` separately.
Adopt JUnit 6 when the floor's upstream POM pins JUnit 6, then verify every supported runtime, including LTS.
The current [floor POM (2.2.1)](https://github.com/apache/flink/blob/450c63e961805a90aa18cb815d6b412f44303b9b/pom.xml#L158) pins JUnit 5.11.4 and the [LTS POM (1.20.4)](https://github.com/apache/flink/blob/f6265b2a32fd1571cd5dcea694c923b057239ac4/pom.xml#L155) pins 5.10.1; this project maintains its own tested JUnit 5 minor/patch pin.
Dependabot ignores major updates to `org.junit:junit-bom`, so recheck this condition whenever the supported Flink window changes.

Keep `slf4j-simple` on the SLF4J API major used by the supported Flink runtimes.
The current runtimes use SLF4J 1.7.36; upgrading only the binding to 2.x leaves the 1.7 API without a usable binding and disables test logging.
The upstream pins are recorded in the [2.2.1](https://github.com/apache/flink/blob/450c63e961805a90aa18cb815d6b412f44303b9b/pom.xml#L136), [2.3.0](https://github.com/apache/flink/blob/c0f8d1a1e09f209885a88f9c19ceb9d9e9870283/pom.xml#L136), and [1.20.4](https://github.com/apache/flink/blob/f6265b2a32fd1571cd5dcea694c923b057239ac4/pom.xml#L131) release POMs.
Dependabot ignores binding major updates; revisit the rule when reviewing a change to Flink's logging dependencies.

Chill remains an optional comparison dependency with individually reviewed updates.
Ordinary CI excludes its tagged tests, so run the comparison explicitly and inspect the resolved and loaded Kryo/Chill versions before accepting an update.
Keep historical measurements distinct from results for the new dependency version.

Dependabot alerts and security-update PRs are repository settings, separate from the weekly version-update schedule in `.github/dependabot.yml`.
Keep both enabled and review security-update PRs through the same verification and review flow.
GitHub also applies [ignore conditions to security updates](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/manage-your-dependency-security/controlling-dependencies-updated#ignoring-specific-dependencies), so enabling them does not guarantee a PR for every alert.
Review alerts even when no PR appears, and prepare an excluded upgrade manually when needed; an ignore rule is not a vulnerability assessment.

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
2. TypeInformation and superclass TypeInfoFactory registration in [#9](https://github.com/flink-gcp/flink-datastream-protobuf/issues/9) provide production transport integration.
   Versioned descriptor snapshots and serializer-level unchanged-schema restoration are implemented by [#10](https://github.com/flink-gcp/flink-datastream-protobuf/issues/10).
3. Production transport, checkpoint/savepoint recovery, and isolated user-code classloading are verified by [#11](https://github.com/flink-gcp/flink-datastream-protobuf/issues/11).
   Google Well-Known Types and OpenTelemetry generated composite message acceptance in [#18](https://github.com/flink-gcp/flink-datastream-protobuf/issues/18) is covered by the [common-types tests and examples](common-types.md).
4. Add compiled usage examples and the complete guide in [#12](https://github.com/flink-gcp/flink-datastream-protobuf/issues/12), then implement the shared-design GitHub Pages site in [#19](https://github.com/flink-gcp/flink-datastream-protobuf/issues/19), amending ADR-0002 with the actual publishing workflow.
5. Prepare publication and packaged-artifact validation for both `0.1.0` and `0.1.0-1.20` in [#13](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13), following ADR-0003's effective-model guards and two-artifact validation requirement. Complete [release #6](https://github.com/flink-gcp/flink-datastream-protobuf/issues/6) only after both versions are published, consumers verify them, and snapshot/savepoint fixtures from each published artifact are preserved with provenance.
6. For [0.2.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/14), add the pure directional evaluator in [#15](https://github.com/flink-gcp/flink-datastream-protobuf/issues/15), integrate supported schema evolution and restore from published 0.1.0 state in [#16](https://github.com/flink-gcp/flink-datastream-protobuf/issues/16), and add release-to-release API checks and upgrade documentation in [#17](https://github.com/flink-gcp/flink-datastream-protobuf/issues/17).
7. For [0.3.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/20), compare shared and separate explicit-descriptor DynamicMessage APIs, serializers and snapshots in [#21](https://github.com/flink-gcp/flink-datastream-protobuf/issues/21) before implementing integration and state recovery in [#22](https://github.com/flink-gcp/flink-datastream-protobuf/issues/22) and [#23](https://github.com/flink-gcp/flink-datastream-protobuf/issues/23). Document API/state compatibility decisions and migration requirements for generated-message users, and test the declared direct/sequential upgrade outcomes before release.

After 0.3.0, assess remaining issues, missing implementation and validation gaps before adding another milestone.
Define further 0.x work when needed; prepare 1.0.0 only when the design and compatibility contract are ready to stabilize.
There is no scheduled 1.0.0 milestone in the current roadmap.

Implementation PRs update documentation for behavior they actually deliver.
Construction and registration examples describe implemented APIs; the runtime acceptance suite verifies unchanged-schema checkpoint/savepoint recovery.
The README distinguishes current transport support from the remaining release requirements.
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
| Published release baseline | Attributable message/snapshot bytes and complete savepoints from the published artifact, expected values, settings, schemas/imports, job/state identity, generation commands, and exact source/artifact/toolchain provenance; retain fixtures without overwriting them with newer writers | #13, #6 |
| 0.2.0 upgrade | Directional accepted/rejected descriptor pairs; real restoration of claimed supported released 0.1.0 fixtures with old generated classes absent; explicit rejection and migration guidance for any intentional break; API checks separate from state and gencode/runtime checks | #15, #16, #17 |
| 0.3.0 upgrade decisions | Source/API checks; already-compiled consumers for unchanged APIs and compiled replacements for intentional breaks; direct and sequential upgrade outcomes, including newly emitted state after 0.2.0 evolution; retain published fixtures and test supported restore/migration or explicit rejection; document each break and required procedure | #20, #21, #22, #23 |

Run production acceptance across the ADR-0003 matrix: JDK 17/21 for both supported Flink 2.x minors and JDK 17 for Flink 1.20, with the pinned Protobuf 3.25.x/4.x profiles and matching application gencode/runtime pairs, including a packaged-library consumer for each artifact line.
Compatibility across changed built-in descriptors is not implied by supporting both runtime profiles.
Single-JVM MiniCluster success does not prove cross-process key hashing; the supported keyed scenarios extract scalar keys and do not use generated messages as keys.

The current matrix covers the production serializer and type integration as well as separate feasibility probes in each combination.
It also covers unchanged-schema saved-state recovery within each pinned Flink/Protobuf combination.
It does not prove compatibility of one production library jar across Protobuf majors or saved-state upgrades between Flink versions.
