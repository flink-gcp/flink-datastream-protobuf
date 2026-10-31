---
title: Development
weight: 60
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

# Development

## Toolchain

The Maven Wrapper pins Maven 3.10.0; all Maven recipes in just invoke the wrapper.
The default local JDK is Temurin 17, and CI runs Temurin 17 and 21.
Java output targets release 17.
`mise.toml` supplies just, the workflow and Markdown linters, pinact, Python 3.14, uv, Hugo Extended 0.164.0, and Go 1.26.5.
Maven supplies Java build plugins and protoc; protoc does not need to be on PATH.
The repository's Python tools and their tests use the non-packaged uv project at the root.
`mise.toml` provides Python 3.14, `requires-python = "==3.14.*"` in `pyproject.toml` limits uv to that minor, and `uv.lock` locks pytest and PyYAML as development dependencies.
`pyproject.toml` sets `python-preference = "only-system"` and `python-downloads = "never"`, so uv takes Python 3.14 from PATH, where `mise x` and mise activation put the `mise.toml` interpreter, and fails rather than download one.
An activated virtual environment or an existing `.venv` takes precedence over PATH; deactivate other environments and delete a `.venv` built from another interpreter.
Manage Python with mise, including the global default, rather than `uv python install` or `uv python pin --global`; the repository has no `.python-version`, so a global uv pin can override the mise interpreter or make the recipes fail.
Recipes run the tools with `uv run --locked --isolated --no-dev`: each run uses a temporary environment with only the standard library, and a stale lock fails instead of being rewritten.
These recipes take `uv` from PATH, which `mise x -- just` or a mise-activated shell provides.

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
| `just verify-protobuf 4` | Clean verification with protobuf-java and protoc 4.36.2, the default |
| `just verify-protobuf 3` | Clean verification with protobuf-java and protoc 3.25.9 |
| `just verify-flink 1.20.5 3` | Clean Flink 1.20 verification with the `flink1` adapters and Protobuf 3 |
| `just verify-flink 2.3.0 4` | Clean Flink 2.3 verification with Protobuf 4 |
| `just binary-compat flink2 2.3.0` | Build the 2.x jar at the POM's floor with Protobuf 4, then run it unchanged at the ceiling and with Protobuf 3 tests at the floor and ceiling |
| `just binary-compat flink1` | Build the LTS jar with Protobuf 4, then run it unchanged with Protobuf 3 tests |
| `just baseline-capture <line> <major> <output> [<published-version>]` | Capture a development bundle in a new external directory, or, from the release tag, a bundle that the published jar writes |
| `just baseline-check <bundle>` | Check retained inventory and checksums without rebuilding |
| `just baseline-read <bundle> <version> <output>` | Restore retained inputs into a new result directory |
| `just baseline-fetch <tag> <output> [bundle...] [--archives <dir>]` | Download, verify, extract, and check retained release bundles in a new external directory |
| `just baseline-fetch-test` | Run the retrieval checks against synthetic releases without network access |
| `just baseline-release read\|review\|assemble\|verify ...` | Read, review, assemble, and verify the retained bundles of a milestone pre-release or a published release; see [publishing a milestone baseline](validation/development-baselines.md#publish-a-milestone-baseline) and [retaining a published release](validation/development-baselines.md#retain-a-published-release) |
| `just baseline-release-test` | Run the release assembly checks against synthetic bundles, including retrieval of the assembled assets |
| `just baseline-upgrade <bundle> <version>` | Restore a verified bundle's retained state with the current library and new-schema applications; see [restoring retained state](validation/development-baselines.md#restore-retained-state-with-the-current-library) |
| `just api-compat <bundle>` | Build the candidate for the bundle's line with Protobuf 4, compare its annotated API with the retained library, and run the negative control |
| `just baseline-consume <bundle> <candidate-jar> <version> <output>` | Run the bundle's compiled consumers against a candidate jar without recompilation |
| `just examples-verify <version> <major> <external-maven-repository> [<published-version>]` | Build/install the library, or resolve a published version from Maven Central, and verify the standalone consumer and Maven launch goals |
| `just docs-javadoc <flink1\|flink2> <3\|4>` | Generate API documentation with full doclint and warnings as errors |
| `just release-rehearse <version> <output> [--unsigned]` | Stage `<version>` and `<version>-1.20` from the committed HEAD into a new external directory without uploading, check the release-line guard, then inspect both bundles; see [Release artifacts](#release-artifacts) |
| `just release-artifacts-test` | Run the bundle inspection, rehearsal, Central Portal helper, and `Release` workflow checks with synthetic inputs and a stub `just` |
| `just central-portal status\|publish\|drop <id>...` | Inspect, publish, or drop Central Portal deployments by ID when recovering a release; see [Publishing a release](#publishing-a-release) |
| `just release-stage <version>` | Re-version the current checkout and stage one line on the Central Portal; only for the release workflow and the rehearsal's exported tree |
| `just probe-chill` | Clean verification including the optional Chill comparison |
| `just benchmark-tools <cache>` | Build the pinned opt-in Thrift compiler in an external cache |
| `just benchmark-tools-check <compiler>` | Reject a compiler that is not the pinned version or cannot generate the benchmark corpus |
| `just benchmark-check <version> <major> <output>` | Validate every benchmark lane without timing; output must be new |
| `just benchmark-smoke <version> <major> <output>` | Validate the corpus and run short JMH harness checks |
| `just benchmark-run <version> <major> <output>` | Collect the five-block benchmark baseline and parity report |
| `just benchmark-pair <version> <major> <output>` | Collect the same five-block parity report for only the native/Chill Protobuf lanes |
| `just benchmark-evidence-check <input> <new-output>` | Validate a benchmark directory or normalized `.tar.gz`; retain checked bytes and their inventory |
| `just benchmark-evidence-staged <new-output>` | Validate retained benchmark archives from the Git index before committing |
| `just benchmark-evidence-repository <new-output>` | Validate retained benchmark archives from HEAD and retain inspection inventories |
| `just benchmark-evidence-test` | Run the gate's synthetic positive and negative controls |
| `just gce-benchmark <command> ...` | Plan, run, collect, and clean up temporary GCE benchmark VMs; see [Temporary GCE execution](#temporary-gce-execution) |
| `just gce-benchmark-test` | Run the GCE operator tooling tests against a recording gcloud double and the VM runner tests against local stubs, without cloud access |
| `just upstream-versions` | Compare the Flink and Protobuf pins with Maven Central and name each pin that a newer release overtakes; see [Upstream release check](#upstream-release-check) |
| `just upstream-versions-test` | Run the upstream version check against synthetic pins and metadata without network access |
| `just test-scripts [test-file...]` | Run the `scripts/tests` suite through the locked pytest, or only the given files; the per-tool test recipes delegate to it |
| `just lint` | actionlint, shellcheck for project scripts, and markdownlint-cli2 |
| `just pin-actions` | Pin GitHub Actions references to commit SHAs |
| `just skills-sync` | Refresh the four shared workflow skills at the recorded dev-tools commit |

Run a targeted class while iterating, for example `./mvnw -ntp -Dtest=UnregisteredTest test`.
Run the full `just verify` for this single-module library before pushing build or Java changes.
CI verifies both Protobuf profiles on Flink 2.2.1 and 2.3.0 with JDK 17/21, and Flink 1.20.5 with JDK 17.
It also runs the binary compatibility recipe for each artifact line on JDK 17.
Lint runs `just test-scripts` with the workflow and Markdown checks.
These lanes run for each PR, main push, manual verification, and weekly schedule.
A repository ruleset requires the GitHub Actions `CI passed` check for every update to main, so a PR merges only after that check succeeds for its head commit; the branch need not be up to date with main.
On a PR, that check verifies the merge of the head into main that GitHub computed for the triggering event, not the head commit alone.
The ruleset matches the check by the `name` of the `ci_passed` job in `.github/workflows/ci.yaml`, so changing that job's `needs` list or renaming a workflow needs no ruleset change.
Changing that `name` needs a coordinated ruleset change: the renaming PR cannot merge while the ruleset names the old check, and once it names the new one, PRs that report only the old check cannot merge until a new PR event triggers a run, whose merge with main includes the rename; rerunning an earlier run reuses its old merge commit.
The required checks above do not include a mutation-testing batch.
Apply the shared review safeguards to any batch that is performed.
To reproduce the JDK 21 lane locally, use `mise x java@temurin-21 just -- just verify-protobuf 4`.
The default tool set includes JDK 17; mise may install JDK 21 for that command.

Tests named `*Test` run in Maven's test phase.
Tests named `*ITCase` run through the connector parent's Surefire integration-test execution.
Each class gets a fresh JVM because Flink's TypeInfoFactory registry is process-global.
Native integration tests and probes disable generic types and run without `--add-opens`.
The optional `chill` profile adds Chill only in test scope, and its tagged comparison runs only when the recipe removes `chill` from the default tag exclusion; the `retained-baseline` group runs only through `baseline-upgrade`.

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
The binary recipe first runs a clean full verification with Protobuf 4, at the floor for `flink2`, and copies the packaged jar outside `target/`.
For `flink2`, it then invokes only the two Surefire goals at the ceiling, loading production code from that jar and retaining the floor's compiled tests.
Next it cleans and builds through `process-test-classes` with Protobuf 3, which compiles the production classes, tests, and runtime applications with protobuf-java and protoc 3.25.9, and invokes the two Surefire goals with the copied Protobuf 4 jar, for `flink2` at both the floor and the ceiling.
Runtime assertions verify the actual Flink core and protobuf-java jar versions and the production code location.
The recipe compares every test's class/name with the Protobuf 4 build, rejects missing or unsuccessful inventories and reports from another Protobuf major, and checks hashes of the jar and all compiled classes before and after each Surefire-only run.
It clears the generated reports before each such run so stale reports cannot hide omitted tests.
A jar built with Protobuf 3 has no binary check, because releases are built with Protobuf 4.

The production integration and transport probe use explicit list element information on all runtimes and additionally check inferred lists: Flink 2.x infers the registered element type, while 1.20 falls back to a generic type that cannot create a serializer with generic types disabled.
Top-level, POJO, tuple, Row, and explicit List transport remain required on every runtime.
Production integration tests independently verify the native type information and serializer on those transport paths.
The separate [runtime acceptance suite](validation/runtime-recovery.md) verifies managed-state recovery for both version lines.
Issue #13 retains verification of the published consumer classpath.

### Application examples and API documentation

`just examples-verify <flink-version> <protobuf-major> <external-maven-repository>` calls [verify-examples.sh](../scripts/verify-examples.sh) to build and install the development library, then run the standalone consumer's verification lifecycle.
The 2.x library is compiled at the floor even for a ceiling consumer; LTS uses its separate adapter.
The library package step uses the normal dependency cache, then `install-file` installs its jar and POM into the isolated consumer repository.
The package step skips its tests because the ordinary verification lane owns those; run `just verify` before pushing changes.
Use a distinct external Maven repository for each adapter/Protobuf-major pair to avoid overwriting the shared development SNAPSHOT coordinates.
With a fourth argument, a released `X.Y.Z` for Flink 2.x or `X.Y.Z-1.20` for Flink 1.20, the recipe builds and installs nothing: the consumer resolves that version into the given repository, which must be new or empty, and the recipe then compares the resolved jar and POM with the SHA-1 checksums on `repo1.maven.org`, so that neither a locally built jar nor a repository that Maven settings add can stand in for the published one.
The recipe rejects a version of the other artifact line.
The application generates its own messages, tests explicit and registered transport and scalar-keyed state updates, and launches each entrypoint from the packaged jar with Flink supplied separately.
The recipe also executes all three documented Maven launch goals with the selected versions, so their classpaths and entrypoint configuration are exercised in CI.
`DocumentationExamplesTest` checks that the example's default dependency versions and the quickstart's explicit version arguments match the root POM pins.
Registration tests use separate JVMs and every native path disables generic types without module-opening flags.
These checks run in every existing source-build CI matrix cell.

The library's formatting and Checkstyle checks also cover the handwritten application sources.
The quickstart and usage guide reference source excerpts through the Hugo `example` shortcode.
Its only argument identifies a file and region: `ExplicitExample#explicit-source` selects that named region in `ExplicitExample.java`, `config.yaml#configuration` selects the marked YAML, and `changed/runtime.proto#entry` selects the marked message in that [runtime application schema](#test-resources-and-version-updates).
Use the shortcode calls in `docs/quickstart.md` and `docs/usage.md` as authoring examples.
Hugo reads the selected version's Example and runtime schema sources between matching `docs:start` and `docs:end` markers.
Markdown stores no copy of these excerpts, so changing an Example requires no snippet synchronization.
Missing, repeated, reversed or empty marker regions fail the site build.
GitHub does not expand Hugo shortcodes; adjacent source links let readers open the executable examples and configuration directly.
`DocumentationExamplesTest` checks launch versions; the standalone consumer compiles and executes the Example sources, and Hugo validates their referenced regions.
Generated application code and jars remain under the example's ignored `target/` directory and are not library publication artifacts.

`just docs-javadoc <flink1|flink2> <3|4>` runs a clean `javadoc:javadoc` for the selected production adapter and Protobuf profile and writes `target/apidocs/`, which the site mounts at `api/java/`.
The POM replaces the connector parent's Javadoc options, which disable doclint and add an export that javadoc rejects with `--release 17`, with full doclint and `failOnWarnings`, and the plugin passes `--release 17` from `maven.compiler.release`.
The site's API reference and the release `javadoc:jar` goal therefore use one configuration and fail on the same warnings.
The recipe cleans first, because the plugin skips generation when its previous output is current.
Both adapters and Protobuf profiles are checked in CI.

### Documentation site and publication

The site imports `flink-gcp-dev-tools/hugo` at commit `632527214d3d1a41cc0c5099e64d3893358059f9`, recorded in `docs/go.mod` and `docs/go.sum`.
This module supplies Hugo Book v0.14.0 and the shared layout, theme toggle, typography and code palettes.
Search initialization and its regression test are maintained in the shared repository.
The site imports that same pinned Hugo Book version directly to restrict its static mount to the favicons and Fuse module.
Unused KaTeX, Mermaid and asciinema bundles are not distributed; enabling those features requires reviewing their assets and notices.
The shared assets retain Apache-2.0 attribution; the [site licenses](licenses.md) retain the MIT notices for Hugo Book and modern-normalize.
Update the module pin deliberately and review the resulting theme, navigation and mobile rendering.
The version assembly and controls are adapted from connector commit `83b05d26c736625549824cf54b021eeb9f70c18c` under Apache-2.0.

Existing Markdown remains the canonical prose for GitHub and Hugo; Example source files own the rendered code excerpts.
The site mounts the guides, ADRs and validation reports without duplicating their bodies or publishing raw evidence archives.
Some Javadoc distributions emit a DejaVu stylesheet import without the font resources.
Site preparation removes that import only when its target is absent, leaving the doclet's system-font fallbacks in effect.
The assembled-site check validates CSS resources as well as HTML links; other missing resources fail publication.
The link render hook resolves documentation links within the selected version and other repository links to the recorded source commit.
The source-excerpt partial follows the connector tagged-snippet implementation and uses this repository's existing `docs:start` / `docs:end` markers.
Real Hugo fixtures verify that source changes appear without editing Markdown, malformed markers fail, and each version reads its own checkout.

| Command | Purpose |
|---|---|
| `just docs` | Render prose with warnings fatal and check repository source links; include any existing `target/apidocs` |
| `just docs-serve` | Preview at localhost:1313; run `docs-validate` first for the API reference |
| `just docs-validate` | Check launch versions, build and run the standalone default-profile examples, generate strict Javadoc, and render the site |
| `just test-doc-versions` | Run the locked Python tests plus real Hugo and Node navigation fixtures |
| `just docs-site plan <manifest>` | Read all published GitHub Releases and resolve retained tags and checkout HEAD to immutable SHAs |
| `just docs-site build <manifest> <id> <source> <output>` | Validate and build one version from a separate clean checkout at its planned SHA |
| `just docs-site assemble <manifest> <inputs> <output>` | Combine every planned version into a fresh output directory |
| `just docs-site check <manifest> <output>` | Check local page, asset and anchor destinations in the combined site |

Version IDs are `development` or a minor such as `1.0` or the `0.1` development milestone; each manifest entry records its `kind` as `release`, `milestone`, or `development`.
Planning requires `gh` authentication and locally fetched tags; an API error or unresolved tag fails instead of silently omitting releases.
Run `plan` from the controller checkout whose HEAD should supply Development.
Create separate detached source worktrees at the manifest SHAs, run `build` for each ID with a shared new external output directory, then `assemble` and `check` into another new external directory.
Generated HTML, Python environments, Hugo caches and temporary application repositories are not committed.
The [version policy](versions.md) retains two released minor series and Development, and 0.x development milestones until the first stable release; zero eligible releases is a valid Development-only site.

The CI orchestrator freezes the source revision before calling Verify, Lint and Docs.
PRs build the same retained-version Pages artifact without deploying.
Main pushes and manual runs on main resolve current main after entering the serialized publication group and deploy only after `CI passed` succeeds.
A failed generation leaves the previously deployed site in place.
Only the deployment job receives `pages: write` and `id-token: write`, through the `github-pages` environment.
Standalone Verify retains its scheduled and manual runs; it does not deploy.

The [`Release` workflow](#publishing-a-release) publishes by tag push and creates the stable `vX.Y.Z` GitHub Release only after both artifact lines are published.
The CI `workflow_run` hook then rebuilds the selected release sources and current main; failed runs and manual dry runs do not deploy documentation.
A bare tag without a published GitHub Release is not a documentation version.
Development milestone pre-releases are created outside the `Release` workflow, so the hook does not see them; run `gh workflow run ci.yaml --ref main` afterwards, or wait for the next main push, to add the milestone to the site.

GitHub Pages uses GitHub Actions as the build source, and the initial main deployment is live.
README links point to the public site.
Verify the actual repository URL, redirects, search, assets, API reference, mobile menu and both themes after deploying the search initialization repair before treating [#19](https://github.com/flink-gcp/flink-datastream-protobuf/issues/19) as complete.
No 0.x milestone archive or Maven Central publication is required for this rollout.

### Native serializer implementation

The production `ProtobufTypeSerializer` is internal machinery with package-private construction.
Serializer unit tests construct it directly; application-facing construction uses `ProtobufTypeInformation.of` or its builder.
`ProtobufTypeInfoFactory` selects the same implementation with the default settings.
The native transport probes still exercise their separate test serializer; `ProtobufRegistrationITCase` and `ProtobufExplicitITCase` exercise production classes.
The production registration suite covers superclass lookup under both generated hierarchies through the paired Protobuf profiles, with fresh-JVM unregistered and Message-interface controls.
The key restriction tests retain both inferred and explicit keyBy bypass controls and show that identical message bytes can select different key groups across isolated generated-class loaders.
Those controls document unsupported message-key use; they are not a promise that the library rejects every such use.
See the [construction and configuration examples](usage.md) for registration timing, defaults, and explicit type settings.

The serializer writes a four-byte big-endian payload length followed by Protobuf wire bytes, without allocating a payload array for each record.
Payloads of at most 4 KiB are encoded into a lazily allocated 4 KiB array that the serializer instance retains; larger payloads stream through Protobuf's per-call coded-output buffer.
Unless the generated-size path described below applies, a dedicated field-wise calculation checks size with `long` arithmetic and verifies nested message, map-entry, and group depth against the configured parser budget before the prefix is written.
It also rejects an unknown field that reuses a declared field's number with a wire type that field accepts, which the parser would read into that field.
`ProtobufTypeSerializerTest` covers that rejection at the root, in nested, repeated, oneof, and map values, for a map with scalar values, and for a set shared with a type that does not declare the number, and, for every scalar, string, bytes, enum, oneof, packed, and unpacked kind, checks that the parser reads the rejected value into the field or fails, as well as the wire types that stay opaque; it also rejects unknown-field numbers above 536,870,911 at the root and inside an unknown group, and accepts that maximum on both sizing paths.
It iterates over field layouts that each payload class computes once for every message type it reaches, using traversal frames that each serializer instance reuses and clears after every call.
Per-call identity-based summaries cover only subtrees whose sizing examined more than a small constant amount of content; smaller shared children are sized again at each occurrence.
Sizing reads fields through Protobuf's public reflection accessors, which allocate on their own; once its frames exist, the traversal itself allocates only for summaries, unknown-field sets and groups, boxed elements of unpacked repeated scalars, frames beyond the 64 it retains, and layouts derived for a value whose descriptor differs from its field's type.
The streaming output wrapper rejects writes beyond the calculated length as they happen and checks the byte count after flushing.
The array encoder is bounded only by its 4 KiB array, so its remaining space is checked after encoding and before a small frame is written, reporting both overruns and short output.
Tests compare the calculated lengths with actual Protobuf output for scalar and packed encodings, Unicode strings, unknown fields, and random graphs with shared children.
`ProtobufMessageSizeTest` also checks multi-byte packed length prefixes, implicit-presence defaults, shared children above and below the summary bound and a summarized unknown-field set at deeper paths, a doubling shared graph that would be exponential without summaries, reuse after failed checks, and that a sizer keeps no checked record reachable, including siblings pending when a check fails.
A separate shared-data test verifies rejection before encoding when the logical size exceeds 4 GiB and the runtime's integer size calculation overflows.
For a payload type whose acyclic type graph is no deeper than the recursion limit, a value without unknown groups or colliding unknown fields instead uses the runtime's memoized generated size when that size is at most 4 KiB and the size limit; encoding into the scratch array confirms it, and a failed or short encoding reruns the traversal to report the exceeded limit ([ADR-0001](adr/0001-native-protobuf-type-integration.md#message-framing-version-1)).
`ProtobufGeneratedSizeTest` compares serialization through this path with the traversal's decisions for random graphs at size and depth boundaries, checks which values are eligible through a test value that reports a generated size one byte larger, including that a colliding unknown field leaves the decision to the traversal, rejects a String shared by 262,144 repeated or map occurrences without measuring each one, and rejects generated sizes that wrapped around `int` through shared data in the root, a nested value, and a map value at the static height.
The limits constrain accepted inputs; they do not promise a bound on total heap usage or sufficient stack capacity for arbitrarily high configured recursion limits.

Reading validates the prefix before allocating from its value and bounds underlying reads to the current frame.
A payload of at most 4 KiB is read completely into the same retained array and parsed by an array decoder without aliasing.
A larger payload is copied through that array into a new array that starts at no more than 1 MiB and doubles until it holds the payload, then parsed through `UnsafeByteOperations.unsafeWrap(...).newCodedInput()` with aliasing unless `largePayloadAliasing` is disabled; decoded `ByteString` values may share that array ([ADR-0001](adr/0001-native-protobuf-type-integration.md#message-framing-version-1)).
Tests check that the two `bytes` values of a large frame share one array and those of a small frame do not, that large-frame values share no array with `largePayloadAliasing` disabled, that payloads just below, at, and above 1 MiB grow to their exact length without reading an adjacent frame, that decoded values survive a source that overwrites every array it filled, and that truncated frames declaring `Integer.MAX_VALUE` bytes fail with `IOException`.
For both sizes, tests overwrite the caller's frame and then read small and large frames through the same serializer after decoding, and check that bytes, strings, unknown fields and groups, lazy fields (which the full runtime parses eagerly), and nested values are unchanged.
Object copy returns the immutable input instance; stream copy preserves frame bytes and checks length and truncation without parsing payload semantics or nesting.
Stream copy reuses the same 4 KiB array in chunks so Flink memory output views can grow as needed.
Malformed input and I/O failures may leave partial input consumption or output; no resynchronization or transactional output is promised.
The tests also cover parser reconstruction after Java serialization with an isolated generated-message classloader.
Validated defaults, parsers, normalized schemas, and sizing field layouts are reused through `ClassValue`, keyed by the exact application class rather than its name.
Protobuf's generated accessor tables already retain reflective field methods; the library adds no per-record method lookup or duplicate field-method cache.
Each duplicate owns its scratch array and traversal frames, so instances remain thread-confined, and record size/depth summaries live for one sizing call only.
The [local performance comparison](validation/serializer-performance.md) measures warm construction and representative serialization paths before and after these changes, including the raw-string counting path that the move to proto3-only payloads later removed.
The [buffer-allocation report](validation/serializer-buffer-allocation.md) separates the effect of the small-frame scratch array on writes and reads with controlled variants measured on temporary GCE VMs.
The [sizing-allocation report](validation/serializer-sizing-allocation.md) measures the final traversal change against the previous sizing and, for an earlier candidate, separates traversal bookkeeping, descriptor work, and the remaining safe-sizing cost from output work with controlled variants.
The [generated-size report](validation/serializer-generated-size.md) measures the generated-size path against the traversal alone.
The [large-read report](validation/serializer-large-read.md) measures the aliased large-frame read against the streamed one.
These tests do not establish checkpoint/savepoint recovery or packaged-artifact compatibility across runtime majors.

`snapshotConfiguration()` returns an internal version-1 `ProtobufGeneratedSerializerSnapshot` for generated messages and a version-1 `ProtobufDynamicSerializerSnapshot` for DynamicMessage values.
The generated snapshot is described here; [ADR-0005](adr/0005-descriptor-bound-dynamic-messages.md#dynamic-snapshot-format-version-1) defines the dynamic one.
Neither reads the snapshot classes that 0.x milestones wrote.
It stores both names, framing/normalization versions, all four settings, the complete normalized import closure, and a SHA-256 integrity fingerprint.
The snapshot class/version envelope is written by Flink; the payload follows ADR-0001 exactly, including the independent 64 MiB descriptor limit.
Reading validates metadata without resolving the saved generated class.
Restoration resolves the class through the supplied user-code classloader and checks that it can read the saved schema before constructing a serializer with the recorded settings.

Both compatibility resolution and restoration delegate to the package-private `ProtobufSchemaCompatibility` evaluator, which compares decoded normalized descriptor content, never just bytes or fingerprints.
It accepts unchanged schemas and the supported changes in ADR-0001, together with equal or increased size/depth limits and either deterministic-mode transition; any decreased limit is incompatible.
New snapshots record the new generated class's schema, and the new settings for state that the new job registers, so a later reverse schema change or limit decrease remains incompatible after an earlier successful restore.
No migration or reconfigured serializer result is provided.
Flink's compatibility result carries no reason, so restoration reports the evaluator's first incompatibility in its exception.
Unsupported formats, corrupt data, and invalid descriptors fail during reading; missing or unsupported generated classes, and classes that cannot read the saved schema, fail explicitly during restoration.

Tests verify the Flink envelope, the fixed payload layout, normalized metadata, actual restored values, isolated classloaders, and rejected corrupt or incompatible inputs.
The separate runtime suite verifies job recovery with state that the same build writes.
Issue #13 and the 1.0.0 milestone own published-artifact fixture capture, consumer verification, and first-release completion; #6 tracks development acceptance only.

## Test resources and version updates

For application upgrade decisions, read [Protobuf updates and state compatibility](compatibility.md).
It distinguishes binary-format rules, Java runtime version guarantees, and the schema changes this library restores.
The procedures below describe how to maintain and verify the test resources.

### Resource inventory

The repository keeps editable test inputs separate from generated build outputs and fixed evidence.
Paths below are relative to the repository root.

| Resource | Purpose | How it is produced or consumed |
|---|---|---|
| `src/test/proto/` | Schemas for serializer, snapshot, and transport tests | Maven runs the selected protoc; generated Java goes under `target/generated-test-sources/protobuf` and compiles with the ordinary tests |
| `src/test/runtime-app/java/` and `src/test/runtime-app/proto/<variant>/` | Controllable Flink jobs and six runtime schemas with the same generated class/message names: `original`, `changed`, `reserved`, `incompatible`, `enum-original`, and `enum-added` ([schema changes](validation/runtime-recovery.md#schema-changes)) | Maven builds each variant separately under `target/runtime-app/<variant>/`; `RuntimeJob` uses isolated runtime gencode and reaches fields that only some variants declare through descriptors, `DynamicRuntimeJob` builds DynamicMessage values from a variant's `schema.pb` and is packaged with only the controlled source in `application-dynamic.jar`, while `CommonTypesRuntimeJob` uses WKT and ordinary test messages from the parent classpath |
| `target/runtime-app/application-<variant>.jar`, `application-dynamic.jar` | Application jars submitted to MiniCluster jobs by `RuntimeRecoveryHarness` | Built before tests; disposable build outputs, never published or committed |
| `target/baseline/tools.jar` | Retained test instruments, messages, and resources | Built during `process-test-classes`; never attached as a Maven publication artifact |
| `src/benchmark/{java,test,proto,avro,thrift}/` | Opt-in harness, negative tests, and equivalent corpus schemas | The `benchmarks` profile compiles these as test inputs and generates sources under `target`; none enter the production jar |
| `docs/validation/data/serializer-benchmarks-v0.1.0/` | Fixed measurement and diagnostic evidence with source provenance | The [data inventory](validation/data/serializer-benchmarks-v0.1.0/README.md) records checksums and supported contents; replacements require the [evidence-retention review](#retaining-diagnostic-evidence) |
| `docs/validation/data/serializer-buffer-allocation-v0.2.0/` | Paired native/Chill measurements of the #42 buffer variants with GCE run records | The [data inventory](validation/data/serializer-buffer-allocation-v0.2.0/README.md) records checksums and how to reconstruct each measured tree; replacements require the [evidence-retention review](#retaining-diagnostic-evidence) |
| `docs/validation/data/serializer-sizing-allocation-v0.2.0/` | Paired native/Chill measurements of the #43 sizing variants with GCE run records | The [data inventory](validation/data/serializer-sizing-allocation-v0.2.0/README.md) records checksums and how to reconstruct each measured tree; replacements require the [evidence-retention review](#retaining-diagnostic-evidence) |
| `docs/validation/data/serializer-generated-size-v0.2.0/` | Paired native/Chill measurements of the #112 generated-size change with GCE run records | The [data inventory](validation/data/serializer-generated-size-v0.2.0/README.md) records checksums and how to reconstruct each measured tree; replacements require the [evidence-retention review](#retaining-diagnostic-evidence) |
| `docs/validation/data/serializer-large-read-v0.2.0/` | Paired native/Chill measurements of the #113 large-frame read change with GCE run records | The [data inventory](validation/data/serializer-large-read-v0.2.0/README.md) records checksums and how to reconstruct each measured tree; replacements require the [evidence-retention review](#retaining-diagnostic-evidence) |
| `docs/validation/data/gce-placement-calibration-v0.2.0/` | Paired native/Chill measurements of one unchanged tree on three VMs per profile, two runs each, with GCE run records | The [data inventory](validation/data/gce-placement-calibration-v0.2.0/README.md) records checksums, VM placement, and the measured commit; replacements require the [evidence-retention review](#retaining-diagnostic-evidence) |

`runtime-app` is test application code, not another Maven module or a collection of JUnit tests.
Its `ControlledSource`, `RuntimeJob`, and `CommonTypesRuntimeJob` run inside the MiniCluster jobs started by the integration tests.
The bundle copies of `runtime.proto` and `schema.pb` record writer inputs; Maven generates the current application from `src/test/runtime-app/proto`, not from those copies.

### What verification executes

`just verify` invokes `./mvnw -ntp verify`.
The relevant build and test sequence is:

1. The Protobuf plugin generates ordinary test messages and every runtime application variant with the selected `protoc.version`.
   Each runtime variant also gets a `schema.pb` descriptor set including imports.
2. During `test-compile`, Maven compiles the ordinary tests and separately compiles the shared runtime application Java with each generated variant.
   The runtime outputs are `target/runtime-app/<variant>/classes`.
3. During `process-test-classes`, the Ant jar tasks package the application jars and the development bundle companion jar.
4. During `test`, Surefire runs `*Test`, including the snapshot layout tests.
5. During `integration-test`, the parent POM's Surefire execution runs `*ITCase`, including the runtime recovery suites below.
   The `verify` lifecycle includes this phase; no separate runtime-test command is needed.

| Test | State source | What it verifies |
|---|---|---|
| `ProtobufRecoveryITCase` | Checkpoints and savepoints created during the current run | Checkpoint restart and replay; separate-job savepoint restore; settings transitions and rejection of decreased limits for HashMap/RocksDB |
| `ProtobufSchemaEvolutionITCase` | Savepoints created during the current run | Restored values, continued processing, and a second restore across supported schema changes; rejection with the first incompatibility for unsupported changes; HashMap/RocksDB |
| `CommonTypesRecoveryITCase` | Checkpoints and savepoints created during the current run | WKT/OTel values, changing Struct keys and AnyValue variants, and continued processing for HashMap/RocksDB |
| `ProtobufDynamicMessageRecoveryITCase` | Checkpoints and savepoints created during the current run | The checkpoint and schema-change cases with DynamicMessage values from `application-dynamic.jar`, the descriptors that restored values carry, and rejection of cross-kind restores for HashMap/RocksDB |

These suites use `RuntimeRecoveryHarness` to start jobs from the attached application jars.
The original variant serves the unchanged-schema restores; `ProtobufSchemaEvolutionITCase` pairs the other variants with it or with each other.
The common-types job uses the original jar's entry point and source, with WKT classes from protobuf-java and OTel/envelope classes from the ordinary test classpath.
Its [coverage](common-types.md#verification-boundaries) complements the isolated generated-class tests.
The [runtime evidence](validation/runtime-recovery.md) describes the state assertions and compatibility boundaries.
Ordinary verification creates temporary state for its jobs; the repository commits no state fixtures.
`just verify-protobuf` and `just verify-flink` clean first, then run this same lifecycle with the selected versions.

### Fixed development bundles

The [development bundle procedure](validation/development-baselines.md) provides `baseline-capture`, `baseline-check`, and `baseline-read` for #40.
It retains packaged library jars, compiled consumers, resolved runtime dependencies, snapshots, and checkpoint/savepoint archives outside the repository.
The reader uses retained bytes without rebuilding them; CI verifies the same jars on the applicable JDK/Flink combinations.
The final v0.1.0 bundles, captured from source revision `f5063f784bdf6799250c521c39a47701900133ea`, are retained with `FINAL-BASELINE.json`, `INVENTORY.json`, and `SHA256SUMS` as assets of the `v0.1.0` pre-release; see the [retained baseline](validation/development-baselines.md#retained-v010-baseline).
The final v0.2.0 bundles, captured from source revision `e697ee957000f3258ba4b6fd5940458514fada5c` and assembled with `baseline-release`, are the assets of the `v0.2.0` pre-release; see the [retained v0.2.0 baseline](validation/development-baselines.md#retained-v020-baseline).
The final v0.3.0 bundles, captured from source revision `142862b9d86f449e3e4894715602a991a018fd63`, are the assets of the `v0.3.0` pre-release; see the [retained v0.3.0 baseline](validation/development-baselines.md#retained-v030-baseline).
Retrieve them with `baseline-fetch` locally or the `.github/actions/baseline-fetch` action in CI, as described in [retrieving the retained baseline](validation/development-baselines.md#retrieve-the-retained-baseline).
`baseline-upgrade` restores a bundle's state with the current library and applications, resolving this build's dependencies instead of the retained ones.
The current library does not read the state of these 0.x bundles; the same commands apply to bundles retained from published releases, starting with 1.0.0, and CI meanwhile runs them on its own [development bundle](validation/development-baselines.md#inspect-and-restore).

### API compatibility gate

Issue [#17](https://github.com/flink-gcp/flink-datastream-protobuf/issues/17) added two complementary checks of a candidate's application-facing API against a retained library.
The first compares class files with japicmp; the second runs the retained compiled consumers against the candidate jar.
Both are separate from saved-state restoration and from application gencode/runtime compatibility.

```sh
mise x -- just baseline-fetch <tag> /tmp/baseline-<tag>
mise x -- just api-compat /tmp/baseline-<tag>/flink2-pb3
mise x -- just baseline-consume /tmp/baseline-<tag>/flink2-pb3 target/flink-datastream-protobuf-1.0.0-SNAPSHOT.jar 2.3.0 /tmp/consume-flink2-pb3-2.3.0
```

`api-compat` first checks the bundle with the current tree's `BaselineTool`, so it does not execute code from the bundle.
It reads the adapter line and compile Flink version from the bundle manifest.
It then runs a clean `verify` with tests skipped and the `api-compat` profile, selecting `-Pprotobuf4`, the build baseline that ADR-0001 sets for releases, and the same `-Dflink.compat` and `-Dflink.version` as the retained library.

The profile uses the japicmp-maven-plugin version managed by `flink-connector-parent` but replaces the parent's configuration, which compares `org.apache.flink` coordinates and covers `@Public` only.
The comparison covers public and protected members of types annotated `@Public` or `@PublicEvolving` and of their nested types, excluding elements annotated `@Internal`.
In the current tree these are `ProtobufTypeInformation`, its `Builder`, and `ProtobufTypeInfoFactory`.
japicmp lists the members that these class files declare; the LTS `createSerializer(ExecutionConfig)` override is declared in the `@Internal` adapter superclass, so the retained legacy-entry-point consumer, rather than the report, exercises it.

japicmp decides compatibility and fails the build on the binary- and source-incompatible changes that its build break covers, naming the member and the change type, for example `ProtobufTypeInformation$Builder.recursionLimit(int):METHOD_REMOVED`.
The break does not cover every change that the report marks source-incompatible: with japicmp 0.18.5, changing only the generic type of a parameter, such as `newBuilder(Class<T>)` to `newBuilder(Class<? extends T>)`, is reported as `METHOD_PARAMETER_GENERICS_CHANGED` and passes.
The erased signatures stay binary compatible, so review generic signature changes in public types by hand.
An intentional break is accepted only by adding that member to the plugin-level japicmp `<excludes>` in the profile, such as `io.github.flink.gcp.protobuf.ProtobufTypeInformation$Builder#recursionLimit(int)`, with a comment linking the release documentation that explains the break and its replacement.
Exclude the affected member, never its whole class or package; an excluded member is no longer compared at all, so remove the entry when the baseline moves past the break.
Do not add excludes inside an execution: Maven merges an execution's `<excludes>` with the plugin-level list element by element, so the first entry would replace the `@Internal` exclude for that comparison.
Review these entries, and the build-break settings beside them, as part of the gate; nothing else checks them.
The profile currently excludes no member.
Do not exclude a member to work around an unexplained difference; only documented intentional breaks are accepted.

The same Maven run checks a negative control built from the candidate, with japicmp's build break disabled for that comparison only.
Its old jar holds the candidate's main classes plus the compiled test classes and generated test messages; its new jar drops `ProtobufTypeInfoFactory` and every test class.
`ApiNegativeControl`, run in Java source-file mode, requires `ProtobufTypeInfoFactory` to be the only incompatible type in that report and to be reported as removed.
This shows that the configured scope detects an accidental removal and ignores unannotated test and generated classes; japicmp's configuration cannot express that expectation itself.
`ApiNegativeControlTest` covers the check with synthetic reports during ordinary verification.

`baseline-consume <bundle> <candidate-jar> <flink-version> <new-output>` runs the bundle's `tools.jar` instruments on the candidate without recompiling them.
These are the compiled explicit-construction, builder, and factory consumer, the registered transport check, and on Flink 1.20 the legacy entry-point check.
Only the first is limited to the annotated API: the transport check names `ProtobufTypeSerializer` and the generated snapshot class, and the legacy check calls the package-private serializer constructor and settings record.
A change to those `@Internal` members can therefore fail `consume` while the public API is unchanged; such a failure needs a reviewed decision, because a japicmp exclude does not apply to it.
The bundle's `dependencies/` tree was resolved on the capture host, so the command resolves the current tree's test-scope dependencies for the bundle's line, Protobuf profile, and the selected Flink version into the output directory.
It pins protobuf-java to the bundle's version, because the retained instruments require their own runtime.
It copies the candidate and the bundle manifest into `candidate/` in the output, where the retained instruments look for the library.
The retained consumer records the hash of the jar it loaded, and the transport and legacy instruments assert that the library classes come from that copy.
The results record the candidate and baseline hashes, the baseline source revision, and the revision and cleanliness of the checkout that ran the command; the candidate jar itself carries no source revision, so that checkout identifies the candidate only when it also built the jar, as in CI.
Build the candidate with the bundle's line and Protobuf 4, as `api-compat` does, so that the consumers of a Protobuf 3 bundle run a release-shaped jar on their Protobuf 3 runtime; the command cannot tell which adapter a jar was compiled for.
The bundle is checked again afterwards, as for `baseline-read`.

Until a published release is retained, CI runs both checks only on its own [development bundle](validation/development-baselines.md#inspect-and-restore).
Neither check publishes or uploads artifacts.
When `flink-connector-parent` changes the japicmp version, confirm that a deliberate break still fails the baseline comparison and that the XML report attributes the gate reads are unchanged.

### Updating Protobuf, schemas, or Flink

Use this sequence for a dependency update, including patch updates proposed by Dependabot.

1. Review the intended compatibility change against [ADR-0001](adr/0001-native-protobuf-type-integration.md) and [ADR-0003](adr/0003-flink-version-compatibility.md).
   Record whether the change affects a runtime version, generated code, test schema, or supported Flink window.
2. Update the matching configuration entries below.
   Keep protobuf-java and application gencode paired, and clean whenever changing profiles.
3. Run the full matrix below, including the unchanged-jar checks for dependency/build changes.
   Update version references and measured scope in this guide, the root README, and runtime evidence.
   Review configuration and documentation together before pushing; wait for the current PR CI result.

| Change | Configuration impact |
|---|---|
| Protobuf 4 runtime | Change the default `protobuf.version` in `pom.xml` and `examples/datastream/pom.xml`, the capture check in `BaselineTool`, and the versions in `docs/quickstart.md`; the `protobuf4` profile uses the root default |
| Protobuf 3 runtime | Change `protobuf.version` in the `protobuf3` profile, the capture check in `BaselineTool`, and the explicit runtime override in `docs/quickstart.md` |
| Protobuf Maven plugin | `BaselineTool` records the executed protoc from `target/protobuf-maven-plugin/`; review this staging-layout coupling and run a fresh capture when updating the plugin |
| protoc only | `protoc.version` normally follows `protobuf.version`; an intentional override changes generated code. `BaselineTool` records the distinct protoc version in captured bundles |
| Runtime application `.proto` | Edit `src/test/runtime-app/proto/<variant>/runtime.proto`; preserve each variant's accepted or rejected relationship to its predecessor. Keep the `docs:start entry` and `docs:end entry` markers, which the documentation can render. Changing `original` or `changed` also changes their relationship to the schemas in retained bundles and requires an explicit baseline/test decision rather than silently overwriting saved schemas |
| Snapshot-test `.proto` | Review `src/test/proto/snapshot.proto`, its imports, and the snapshot-format tests together; these schemas do not generate the runtime application |
| Flink 2.x floor | Change the default `flink.version` and `release.flink.floor` in `pom.xml`, the example and benchmark `floor` versions and the development-bundle `versions` in `.github/workflows/verify.yaml`, `examples/datastream/pom.xml`, `docs/quickstart.md`, the accepted version in `benchmarks.just`, the `flink2` runtimes in `BaselineTool` and its test, and the `flink2` readers in `scripts/baseline-release.py`; the benchmark lanes then check `benchmark.avro.version` and `benchmark.jackson.version` (see [Dependency updates](#dependency-updates)) |
| Flink 2.x ceiling | Change `FLINK_CEILING` in `.github/workflows/verify.yaml` (keep it as a two-space-indented `env` entry of the form `FLINK_CEILING: 'x.y.z'`, with single quotes and no trailing comment; the `Dependency submission` workflow parses it after merge, and the [upstream release check](#upstream-release-check) parses it weekly and in its Lint test on PRs), the development-bundle `versions` in the same workflow, the accepted version in `benchmarks.just`, the `flink2` runtimes in `BaselineTool` and its test, and the `flink2` readers in `scripts/baseline-release.py`; the benchmark lanes then check `benchmark.avro.version` and `benchmark.jackson.version` (see [Dependency updates](#dependency-updates)) |
| Flink 1.20 LTS | Change `flink.version` in the `flink1` profile and `release.flink.lts` in `pom.xml`, the example and benchmark `lts` versions and the development-bundle `versions` in `.github/workflows/verify.yaml`, `docs/quickstart.md`, the accepted version in `benchmarks.just`, the `flink1` runtimes in `BaselineTool` and its test, and the `flink1` readers and negative controls in `scripts/baseline-release.py`; keep `flink.compat=flink1` on LTS commands; the benchmark lanes then check `benchmark.avro.version` and `benchmark.jackson.version` (see [Dependency updates](#dependency-updates)) |
| Supported Flink minor window | Review and advance floor/ceiling together under ADR-0003; check adapters and CI lanes. Dependabot does not advance Flink major/minor versions automatically |

Dependabot scans the root Maven project.
Review the standalone example's dependency and plugin pins in `examples/datastream/pom.xml` alongside root updates; they are maintained explicitly.
When changing the development library version, update the example's `protobuf.integration.version` and the quickstart together.
The launch-version test rejects runtime or development-coordinate drift before the commands reach users.

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
  done
  mise x -- just binary-compat flink2 2.3.0
  mise x -- just binary-compat flink1
  mise x -- just lint
)
```

The subshell stops on the first failure and preserves that command's exit status without changing the calling shell's options.
The binary check preserves the floor's library jar, compiled tests, application jars/classes, and descriptors when executing at the ceiling, and the Protobuf 4 jar when executing with Protobuf 3.
That check does not establish saved-state migration between Flink versions or Protobuf profiles.
Adding such a guarantee requires an explicit old-writer/new-reader recovery scenario and a documented compatibility decision.

## Serializer benchmarks

The optional `benchmarks` profile adds comparison-only test sources, generated schemas, and JMH instrumentation.
Use `just benchmark-smoke <flink-version> <protobuf-major> <new-output-directory>` for correctness and short harness verification, or `just benchmark-run` with the same arguments for long measurements.
Generation requires the Thrift compiler pinned in `benchmarks.just`; `just benchmark-tools <cache>` builds it with CMake, Flex, Bison 3.8 or newer, and a C++ compiler.
On macOS, put a modern Bison ahead of `/usr/bin/bison`, which is too old (Homebrew's `bison` is keg-only, so prepend `$(brew --prefix bison)/bin` to `PATH`); after fixing `PATH`, rerun with the same cache without deleting `<cache>/build`.
Set `BENCHMARK_THRIFT_COMPILER` to `<cache>/build/compiler/cpp/bin/thrift`; otherwise generation uses `thrift` from `PATH` and rejects any other version.
The [0.1.0 benchmark report](validation/serializer-benchmarks-v0.1.0.md) documents the corpus, operation semantics, matrices, and evidence format.
Beyond the lanes of that report, the suite measures two DynamicMessage lanes over the generated lane's schema and corpus: `PROTOBUF_DYNAMIC` values carry the serializer's own descriptors, and `PROTOBUF_DYNAMIC_FOREIGN` values carry an equal-content descriptor from another instance.
`just benchmark-run` writes their paired time ratios to `dynamic.csv` without a verdict, as [ADR-0004](adr/0004-serializer-benchmarks.md) decides.
`just benchmark-pair` keeps the long-run fork, warmup, measurement, and block-order settings but measures only the native and Chill Protobuf lanes.
Use it for controlled comparisons of native serializer changes; it does not replace a complete cross-format baseline.
These recipes do not replace ordinary native verification or binary compatibility checks.
Benchmark source code follows the normal formatter, Checkstyle, and Apache RAT rules; generation stays under `target`.
The CI build matrix runs the smoke after ordinary native verification and accepts no timing thresholds.
`benchmark-build` and `benchmark-measure` split those recipes into the clean package and the measurement step, so a runner can check host load between them.

### Temporary GCE execution

`just gce-benchmark` runs [gce-benchmark.py](../scripts/gce-benchmark.py) on the operator's machine to plan, start, monitor, collect, and delete temporary Compute Engine VMs.
[gce-benchmark-vm.sh](../scripts/gce-benchmark-vm.sh) runs on each VM.
The operator tool uses Python's standard library and gcloud; the runner uses Bash; neither enters the library jar.
By default, one VM runs one Protobuf profile, measuring its variants sequentially, so profiles can run concurrently without competing within a VM.
Compare optimization variants only within one plan and only between runs on one VM; earlier local timings are not a causal baseline for a VM result.

Only the operator process uses cloud credentials.
It reads `CLOUDSDK_CONFIG`, `GOOGLE_CLOUD_PROJECT`, `GCE_BENCHMARK_ZONE`, and `GCE_BENCHMARK_MACHINE_TYPE` from the environment, falling back to the gitignored `.env` in the repository root, and ignores every other key in that file.
Set `GCE_BENCHMARK_ENV` to read another file.
VMs are created without a service account or access scopes, so benchmark processes cannot obtain cloud tokens from the metadata server.
The VM receives a Git bundle of the planned source commit, optional variant patches, the runner script, and non-secret run settings.
It installs the pinned mise release after checking its published SHA-256, uses mise only to install Java and just, and runs every build and measurement as an unprivileged user with a minimal environment under `/srv/benchmark`.
The operator account name does not appear in retained paths.

A specification is a JSON file:

```json
{
  "runId": "issue42-ablation-1",
  "mode": "run",
  "source": "<commit>",
  "profiles": [
    {"protobuf": 4, "variants": [{"name": "baseline", "patch": "baseline.patch"}, {"name": "candidate"}]},
    {"protobuf": 3}
  ]
}
```

`mode` is a `BenchmarkSuite` mode, and `flinkVersion` one of the benchmark recipe's versions; `plan` reads both lists from the source commit itself.
Each variant applies its patch, relative to the specification file, to a fresh checkout of `source`; a variant without a patch measures the commit unchanged, and a profile without variants measures one `candidate`.
For calibration, a variant may set `vm` to a group name of 1 to 16 lowercase letters and digits that starts with a letter but not with `pb` and a digit.
Variants of one profile with the same group run sequentially, in specification order, on VM `<runId>-pb<major>-<group>`; variants without a group run on `<runId>-pb<major>`.
The [placement calibration](validation/gce-placement-calibration.md) rejected comparing variants across VMs, so `plan` accepts groups in a profile only when all its variants name the same patch file or none.

A profile may instead declare `comparisons`, each a pair of its variant names, to shorten a plan with several comparisons:

```json
{"protobuf": 4,
 "variants": [{"name": "baseline", "patch": "baseline.patch"}, {"name": "descriptors", "patch": "descriptors.patch"},
              {"name": "candidate"}, {"name": "floor", "patch": "floor.patch"}],
 "comparisons": [["baseline", "descriptors"], ["descriptors", "candidate"], ["candidate", "floor"]]}
```

Each comparison runs on its own VM, `<runId>-pb<major>-c<n>` for the `n`th comparison, which measures its two variants sequentially in the order given.
A variant in several comparisons is measured once on each of their VMs, and those runs are never compared with each other or combined; report only the declared comparisons.
Every variant must belong to a comparison, a pair may be declared only once, and a profile with comparisons cannot use groups.
A profile without `comparisons` keeps the sequential or group placement, in which every pair of variants on one VM is comparable.

`benchmark-build` compiles and runs the benchmark contract tests, so a patch that reverts production code must also revert tests that depend on it.
Run `just benchmark-build` locally for every variant and profile before planning; `plan` checks neither patch application nor compilation.
Optional fields select `project`, `zone`, `machineType` (default `c3-standard-8`), `minCpuPlatform`, `image` family and project (default Ubuntu 24.04 LTS), `bootDiskGb`, `bootDiskType`, `flinkVersion` (default `2.3.0`), the `base` commit for retained source patches (default: merge base with `origin/main`), and `java`, `just`, and `mise` tool versions.

| Command | Effect |
|---|---|
| `just gce-benchmark plan <spec> <new-dir>` | Resolve the exact image, source, ancestor base, machine shape, and quota; estimate VM-hours and wall-clock hours; create no resources |
| `just gce-benchmark execute <dir> --approve` | Refuse if instances with the run label exist; create the planned VMs, upload inputs, and start each runner as a systemd unit |
| `just gce-benchmark status <dir>` | Report instance state, runner state, boot identity, and completed result count |
| `just gce-benchmark collect <dir> <new-dir>` | Download each VM's results, reject recorded maintenance events, run the evidence gate on every result directory into `checked/<vm>/<result>`, then delete the VMs |
| `just gce-benchmark cleanup <dir>` | Delete every instance labelled with the run and verify that no instance or boot disk remains |

Before `execute`, confirm that the plan targets the intended project and billing account, that its labels identify the run, and that regional quota covers the planned VMs.
`plan` counts every VM against the regional `CPUS`, machine-family (for example `C3_CPUS`), `INSTANCES`, and `IN_USE_ADDRESSES` quotas, because each VM uses an ephemeral external address for SSH, and against the project-wide `CPUS_ALL_REGIONS` quota.
It reads each quota from its own scope and skips a quota that GCE does not report.
Recheck current Compute Engine and persistent-disk pricing for the zone; the plan reports estimated VM-hours, not a price.
Each VM's estimate is 25 minutes of setup plus a per-variant time that includes both load gates: 20 minutes for `check`, 25 for `smoke`, 95 for `pair`, and 370 for `run`.
The VM-hour estimate sums all VMs, including repeated variants, and the wall-clock estimate is the longest VM's.
For the four variants of the comparison example in `pair` mode, the sequential placement takes one VM for 6.75 hours; the pairwise placement takes three VMs for 3.58 hours each, 10.75 VM-hours, so two such profiles need six VMs, 48 vCPUs on `c3-standard-8`, against the quotas above.
`--approve` records that review; no other command creates resources.
Keep plan and collection directories outside the repository: they contain the source bundle, raw results, and the resource ledger.
They are not deleted automatically; remove them after the checked evidence and the ledger are retained in the external review records.

`resources.jsonl` records each creation request, creation, start, collection, deletion, and the final verification, including after a failed start.
A failure or interruption while creating or starting any VM deletes every instance already requested for the plan.
`collect` deletes the VMs after downloading results, including when a download or check fails; `--keep` leaves them for inspection, after which `cleanup` is required.
Run `cleanup` after any interrupted session, and check `gcloud compute instances list --filter=labels.purpose=flink-protobuf-benchmark` before ending the work.

VMs use standard provisioning, `TERMINATE` host maintenance, and no automatic restart.
The runner watches the metadata server's maintenance event, logging failed requests as `UNAVAILABLE` and retrying them.
Any event other than `NONE` marks the run interrupted and keeps stopping benchmark-user processes until the runner exits, so work launched after the event cannot proceed; the run fails at the next step boundary, and later steps cannot replace that status.
Once the watcher has marked the run interrupted, the runner's exit completes that record from the marker, so a watcher stopped before recording it, or a status update that started before the marker, cannot leave a stale status or final event.
`status` also reports a stopped instance or changed boot ID as interrupted, and `collect` rejects any result whose `maintenance-events.log` records an event.
`collect` refuses incomplete runs; `--partial` downloads and gates failed or interrupted runs for diagnosis only and still reports failure.
Neither form collects while a runner is still preparing or measuring, or while a running instance's state cannot be read, because collection deletes the VMs; use `cleanup` to abandon such a run.
Retry an interrupted or failed run with a new run ID and new output directories; never combine results from different attempts.

Before each build and each measurement, the runner applies the local load gate: ten consecutive observations about 30 seconds apart, each measuring CPU idle over two seconds, must average at least 90% idle with none below 85%, one-minute load below 10, no swap-out, and at most 1 MiB swap-in per observation.
A gate that does not pass within 60 minutes fails the run; the VM keeps running and billing until `collect --partial` or `cleanup` deletes it.
Each result directory contains the JMH and support files plus `vm-manifest.txt`, `maintenance-events.log`, the gate logs, build and console logs, invocation, start/end times, `source-base.txt`, and `measurement-source.patch`.
The VM manifest records the zone, instance name, machine type, CPU platform and model, image, scheduling, kernel, OS release, memory, mise version and hash, resolved JDK, just, Thrift, source, base, mode, variant patch hash, runner hash, and boot ID.
C3 machine types run only on Intel Sapphire Rapids; for a series offered on several platforms, set `minCpuPlatform`.
`collect` fails when the results of one plan differ in machine type, CPU platform, image, kernel, or JDK, including results from different VMs of one profile, so a plan's variants stay comparable.
`collection.json` records which VM produced each result directory and, per VM, the pairs of result directories that form its comparisons: the declared pair, or every pair on a VM without declared comparisons.
A repeated variant's result directories keep the name `protobuf-<major>-<variant>` on every VM, so failure messages name the VM.
JMH JVM arguments, the corpus seed, and dependency hashes remain in each result's JMH JSON, `manifest.properties`, and `classpath-sha256.txt`.
The gate accepts these files; human review of the checked copies remains required before retaining them under `docs/validation/data`.

Validate the tooling with one small run before a long matrix:

```sh
just gce-benchmark plan smoke.json /tmp/gce-benchmark/smoke-plan
just gce-benchmark execute /tmp/gce-benchmark/smoke-plan --approve
just gce-benchmark status /tmp/gce-benchmark/smoke-plan
just gce-benchmark collect /tmp/gce-benchmark/smoke-plan /tmp/gce-benchmark/smoke-results
```

Here `smoke.json` contains `{"runId": "smoke-1", "mode": "smoke", "source": "<commit>", "profiles": [{"protobuf": 4}]}`.
For the representative long matrix, use `"mode": "run"` with both profiles; each profile's VM then measures for several hours.

The runner's offline tests in [test_gce_benchmark_vm.py](../scripts/tests/test_gce_benchmark_vm.py) run it on Linux against a loopback metadata server and the command stubs in [gce-benchmark-vm-stubs.sh](../scripts/tests/gce-benchmark-vm-stubs.sh), without network or cloud access; other systems skip them, and the Lint workflow runs them through `just test-scripts`.
They cover the load gate's pass, reset, window, and timeout, a maintenance event during a root-only step, the gate, or a benchmark process, unavailable metadata requests, a failure in package or tool installation, the mise checksum, checkout, the Thrift build, a manifest lookup, the build, or the measurement, a changed boot ID, and the retained result files and source patch.
Their stubs shorten every wait, so they check the order of the runner's steps but not the intervals of its gate, watcher, or retries.
Only a real VM checks those intervals, the image's packages, the apt, mise, Java, just, and Thrift installations, the metadata server's real responses and maintenance announcements, systemd, `runuser`, process ownership, and the measurement itself.

### CI Maven resolution

CI reads Maven artifacts from Google's mirror of Maven Central before Central, and restores one shared local repository.
Every CI job that runs Maven calls the [maven](../.github/actions/maven/action.yml) action after its JDK setup.
The action adds `--settings` with [.github/maven/settings.xml](../.github/maven/settings.xml) to `MAVEN_ARGS`, which Maven prepends to every later invocation, including those that recipes, Python tools, and `BaselineTool` start.
That file adds `https://maven-central.storage-download.googleapis.com/maven2/` as a repository and plugin repository ahead of Central.
It is not a `<mirror>`, so an artifact that the mirror has not synced yet still resolves from Central.
The action also sets `MVNW_REPOURL`, so the Maven Wrapper downloads Maven itself from the mirror, but only when the mirror holds the Maven version of every checkout's wrapper; otherwise it reports a warning and the wrapper downloads from Central, because `MVNW_REPOURL` replaces the wrapper's only URL.
The Docs build calls the controller checkout's action, because the milestone sources it builds predate it.
Local builds read neither variable and resolve from Central.
The `Release` job sets `MAVEN_ARGS` to pass the same file with `--global-settings`, because `--settings` would replace the user settings in which `actions/setup-java` names the `central` server; it restores no Maven cache, and its Maven Wrapper downloads Maven from Central.

The action then restores `~/.m2/repository` and `~/.m2/wrapper`, in place of `setup-java`'s `cache: maven`.
Its key hashes every `pom.xml`, the wrapper properties, and the CI settings, and the `maven-repository-` prefix restores the newest entry when the exact key is missing.
In Docs, whose workspace holds two checkouts, the key never matches exactly, so the prefix always selects the entry.
A job that restores nothing reports a warning.
Only `floor / JDK 17 / Protobuf 4` on `main` saves an entry, through the action's `save` input.
That job restores only the exact key, so a new key starts from an empty repository, and the entry is saved after the job succeeds.
The entry therefore holds what that job resolved when its key was new; a version that a script names outside a POM, such as the `install-file` plugin in `scripts/verify-examples.sh`, reaches it only with the next POM change.
Pull requests, Docs, and Dependency submission only restore, so pull requests create no entry that only their own ref can read.
The other lanes fetch their runtime-specific dependencies, which the entry does not hold, from the mirror on each run, and the standalone examples resolve into their own disposable repository.

Retained bundles are captured locally, as described in [development artifact bundles](validation/development-baselines.md#inspect-and-restore), and do not use these settings.
CI captures are disposable rehearsals whose command records omit `MAVEN_ARGS`.
A bundle records the resolved dependency tree and jars, not the repository that served them.

### CI compiler reuse and job budgets

Each Verify matrix job restores the compiler built by `just benchmark-tools` from an Actions cache entry.
The private `benchmark-tools-identity` recipe names the entry from the pinned Thrift version, a source archive checksum prefix, a digest of the `benchmark-tools` recipe text, the distribution release, the architecture, and a digest of the C++ compiler's `--version` output (`$CXX`, or `c++`).
The restore uses only that exact key.
A restored compiler is used only after `just benchmark-tools-check` confirms its version and generates Java from the benchmark corpus with the Maven build's generator options.
On a miss, the job installs the build prerequisites and builds the pinned source with `just benchmark-tools`, which ends with the same check, then saves the compiler before any benchmark process starts.
Concurrent misses build independently; the first save creates the entry, and the other jobs log a reservation conflict without replacing it.
A restored compiler that fails the check produces a warning and a rebuild in that job, but the existing entry is not replaced; delete it with `gh cache delete <key>`, using the key shown by the restore step.
Any edit to the `benchmark-tools` recipe text, including its comment, the pinned source, or the compiler version produces a new key.
CMake, Bison, and Flex versions are not part of the key, because the check verifies the resulting compiler rather than its build tools.

Cache and artifact credentials stay in their own action steps; benchmark processes run in later shell steps without them.
The smoke command runs under a five-minute `timeout`, so a hang fails the step with an error annotation and the existing content gate still checks any partial evidence.
The step then kills any process left in the timeout's process group, so the gate does not read output that a surviving JVM is still writing.
A job time limit cancels the job instead, and the gate does not run after cancellation.

The organization's GitHub Free plan runs at most 20 jobs at a time, and a pull-request run of Verify, Lint, and Docs starts more, so the longest matrix jobs can wait for a runner.
To shorten pull-request runs, CI's first job, `Freeze source revision`, uses [dorny/paths-filter](https://github.com/dorny/paths-filter) to compare a pull request with its base branch and passes the result to Verify as the `benchmarks` and `baseline` inputs.
An empty token makes the filter use git, so the job needs no `pull-requests` permission, and the selection adds no job that the matrix jobs would wait for.
The development bundle jobs run only when the pull request changes `src/`, `.gitignore`, or a build input: `pom.xml`, `justfile`, `benchmarks.just`, `mise.toml`, the Maven wrapper, `tools/maven/`, the CI Maven settings and action, the CI and Verify workflows, or `scripts/api-compat.sh`.
Capture builds and runs the current library and test sources from a clean tree, so all of those except the API gate script are its inputs; the same job runs that script on the captured bundle.
Capture also archives every tracked file and rejects names that are not valid UTF-8 or that contain a colon, a backslash, or a control character; `just lint`, which Lint runs on every pull request, checks those names so that a skipped capture cannot hide one.
Matrix jobs run the benchmark compiler and smoke steps for the same paths and for the evidence gate, `scripts/benchmark-evidence.py` and its `scripts/lib/credential_rules.py`; the benchmark build compiles the test sources as well.
Pushes to main, the weekly schedule, and manual `workflow_dispatch` runs always run both, so a change that a pull request skipped is still checked after it merges; start Verify manually on the branch to run them before merging.
When a change adds an input to either check, add its path to the filters in `ci.yaml`; `baseline` lists the inputs shared by both checks.

Verify job and smoke limits are at least twice the slowest measured successful run, rounded up to five minutes.
The [CI budget measurements](validation/ci-budget.md) record the GitHub runner durations behind the current limits.
Recheck them when a matrix job adds substantial work, when the runner image changes distribution release, or when the measured maximum moves past half of its limit.
Shared-runner durations set time limits only; they are not benchmark results or pass/fail thresholds.

### Retaining diagnostic evidence

Run profiling against the synthetic benchmark corpus in a process environment containing only the variables needed by the toolchain.
Keep credentials out of that environment, JVM arguments, and benchmark parameters.
Store raw JFR recordings, heap dumps, and other process dumps in a private directory outside the repository and CI artifact upload paths.
These files can capture environment variables, system properties, command arguments, and application data.
The ignore rules cover common raw-dump filenames; they do not inspect archives or make diagnostic output safe to publish.

For the allocation investigation, export only `jdk.ObjectAllocationSample` and `jdk.ExecutionSample` events into JSON.
Inspect every field in the export, including thread names and stack metadata, before retaining it.
Retain JMH result JSON, reviewed logs, comparison summaries, and the toolchain/source provenance needed to reproduce the measurements.
Never copy an entire profiling output directory into a publication archive.

Prepare a publication copy with personal home-directory prefixes replaced by `/benchmark-home`, preserving toolchain versions and relative path suffixes.
Treat the rewritten command paths as provenance, not portable invocation paths.
Set tar owner IDs and member timestamps to zero, clear owner names, and remove extended headers carrying the original metadata; omit the gzip filename and set its timestamp to zero.
Archive timestamps no longer preserve per-result completion times, so retain the run-window records and result ordering separately when the report depends on them.
Compare every member before and after preparation to confirm that only the documented path substitutions changed its contents, and record those transformations in the evidence README.

Before staging an evidence archive, enumerate all members and inspect their decompressed contents.
Reject raw recordings, heap dumps, nested archives, unexpected binary files, and members outside the intended evidence inventory.
Check the retained text, commands, and metadata for credentials without printing credential values in review output.
Credential-pattern scanning supplements this content review; a scan with no findings does not prove that arbitrary sensitive data is absent.
Recompute the final archive checksum and confirm that its measurement inventory and summaries still agree with the report.
Apply the same review to CI artifact uploads and replacement archives before publishing them.

#### Automated content gate

The [evidence validator](../scripts/benchmark-evidence.py) implements the content gate for the current benchmark evidence formats.
It uses Python's standard library and is not packaged in the production jar.
Use a new output directory outside the input tree:

```sh
just benchmark-evidence-check /private/benchmark-publication/protobuf-3.tar.gz /tmp/checked-benchmark-evidence
```

A successful check creates `evidence/` with the exact inspected bytes and `inventory.json` with one SHA-256 over the validator and the credential rules it imports, policy version, archive checksum when applicable, and every member's relative path, format, byte count, and SHA-256.
It does not extract archive paths onto the filesystem, rewrite contents, or remove offending members to make a bundle pass.
An error rejects the entire input, removes its temporary copy, and leaves no successful output directory.
Error messages contain fixed rule identifiers and, for content-rule failures, a member ordinal rather than matched values, input paths, parser excerpts, or exception details.
Use those identifiers to inspect the original input privately.

The initial container formats are a directory of benchmark files and a normalized, single-stream gzip containing POSIX ustar.
Archive owner IDs, owner names, and timestamps must match the normalization above; gzip optional headers, tar extended headers, links, special entries, duplicate paths, unsafe paths, conflicting file/directory paths, and uninspected trailing data are rejected.
Only regular files and the optional `protobuf-3`, `protobuf-4`, or `diagnostics` directory prefix are supported.
Directory inputs reject symbolic and hard links.
The limits are 10,000 members, 64 MiB per decompressed file, 256 MiB of member contents, and JSON nesting depth 64.
Compressed inputs also have a 256 MiB limit, with bounded decompression including tar headers and padding.
These are input limits, not a claim about a fixed peak process memory footprint.

The file inventory permits the harness's lane/workload/operation result names and the existing measurement, support, and provenance filenames.
It validates JMH JSON structure, diagnostic command string arrays, diagnostic summary structure, the three CSV headers and row widths, and the reviewed Java properties keys and syntax.
JMH numeric fields accept its string encodings `NaN`, `-INF`, and `+INF` where measurements are undefined.
Logs, source patches, and provenance text must be UTF-8 without binary control bytes and match the documented inventory in the validator.
Unknown filenames, unknown JSON fields, duplicate JSON keys, unreadable or malformed inputs, raw JFR, heap dumps, binary files, and nested archives fail closed.
A file extension does not override content checks.
This gate validates content retention; it does not establish measurement completeness or the statistical claims in the benchmark report.

JFR exports accept only `recording.events` entries for `jdk.ObjectAllocationSample` and `jdk.ExecutionSample`.
Allocation values contain `startTime`, `eventThread`, `stackTrace`, `objectClass`, and `weight`; execution values contain `startTime`, `sampledThread`, `stackTrace`, and `state`.
The validator's `JFR_SCHEMAS` defines the reviewed fields and types for nested threads, thread groups, classes, class loaders, packages, modules, methods, stack frames, and stack truncation.
JFR's null references and nullable strings are supported; additional fields or event types require a reviewed policy change.
Environment, system-property, and command-line metadata are not accepted as JFR events or additional fields.
Renaming a recognizable JFR export to a log does not bypass the JFR policy.
Recorded benchmark command arrays and JMH JVM arguments are separate supported provenance formats and remain subject to credential scanning and human review.

The credential rules cover private-key markers, GitHub tokens, AWS access-key IDs, Google API keys and OAuth tokens, Slack tokens, authentication headers, and common credential assignments.
The gate scans raw text and decoded JSON strings, keys, and Java property values without printing matches.
It does not recognize arbitrary secrets, every provider's credentials, encoded/encrypted secrets, or sensitive prose that lacks a known pattern.
A passing inventory explicitly records that human review is still required; it is not publication approval.
Keep the credential-free measurement process and member-by-member human review above.

After reviewing a publication copy, validate it before staging, then stage only the checked evidence and its reviewed documentation.
Keep the corresponding inventory with the retained evidence in the external review records.
Run the index check immediately before the commit, using another new output directory:

```sh
just benchmark-evidence-staged /tmp/staged-benchmark-evidence-inventories
```

The index check inspects the exact staged blobs under `docs/validation/data`, records their Git object IDs and archive checksums, and rejects unrecognized retained files except the evidence README files.
It scans all retained benchmark archives, including unchanged ones; unstaged working-tree replacements cannot hide unsafe staged bytes.
A failure stops the evidence commit procedure.
Rerun after changing the index; Git hooks are not installed by this tool.
The repository check applies the same rules to HEAD in CI and uploads only the inspection inventories, without uploading the existing archives again.

CI smoke uploads use `benchmark-evidence-check` after the measurement process has stopped, including when it failed and left partial output.
Only a successful gate enables the upload, and the upload reads the checked copy and its inventory rather than the original measurement directory.
Missing output produces no upload; malformed or unsafe partial output fails the gate and prevents the entire upload.
A successful content check does not clear the benchmark step's failure.
The synthetic gate tests and retained-evidence checks run in the lint workflow and therefore participate in the aggregate `CI passed` result.
Changes to the gate do not authorize publishing existing private evidence or changing a publication hold.

## Dependencies and packaging

Flink core and protobuf-java are `provided` dependencies.
Applications must supply a runtime compatible with their own generated messages.
This library has no Google Cloud BOM, connector dependency, or shaded dependencies.
The optional Chill dependency is absent from ordinary builds.
The root LICENSE and NOTICE are copied to the jar's META-INF directory; the inherited ASF resource bundle is disabled.

### Release artifacts

[ADR-0006](adr/0006-central-portal-releases.md) defines how both artifact lines are staged on the Central Portal and published.
`just release-stage <version>` runs `versions:set` on the current checkout and then `clean deploy -Drelease -Dmaven.test.skip=true -Dmaven.install.skip=true`, adding `-Dflink.compat=flink1` for a `-1.20` version.
Run it only in a disposable tree; without `-DskipUpload=true` it uploads the bundle and waits for Central validation, which requires Central credentials.
`-Drelease` activates the parent's `release` profile, which signs at `verify` and attaches the Javadoc jar, and this project's `central-release` profile.
That profile attaches the sources jar, signs with the Bouncy Castle signer from `MAVEN_GPG_KEY` and `MAVEN_GPG_PASSPHRASE`, and replaces `deploy` with `central-publishing-maven-plugin` without automatic publication; it fixes `ignorePublishedComponents=false`, because that check reads Central credentials even when the upload is skipped, and `waitForPublishCompletion=false`, because that deprecated option switches the upload to automatic publication despite `autoPublish=false`.
Its `enforce-release-line-toolchain` rule fails the build at `validate` unless the effective model matches the version line: a bare `X.Y.Z` with `flink2` at `release.flink.floor`, or `X.Y.Z-1.20` with `flink1` at `release.flink.lts`, and Protobuf 4 for both.
A SNAPSHOT or any other version shape also fails.
The execution sets `skip` and `fail` explicitly, so `-Denforcer.skip`, `-Denforcer.fail=false`, and the parent's `fast` profile do not disable it.
Arguments that replace or skip the configured rules, such as `-Drules`, or that deactivate the profile are outside its protection; the release workflow passes none, and the rehearsal's guard check below runs with the same `MAVEN_ARGS`, so an inherited argument that disables the guard fails the rehearsal.

`just release-rehearse <version> <output> [--unsigned]` exports the committed HEAD into `<output>/source`, stages `<version>` and `<version>-1.20` there with `-DskipUpload=true`, and keeps both bundles in `<output>/bundles`.
The exported tree excludes uncommitted changes, as a tag does.
It accepts no Maven arguments, passes `-DskipUpload=true` last, and never reads Central credentials; without `--unsigned`, it requires the signing variables.
It then stages `<version>-1.20` with `-Dflink.compat=flink2` and fails unless the release-line guard rejects that build, so a change that disables the guard fails the rehearsal.
Each bundle is inspected against the exported sources, and the command fails on any of these defects:

- An unreadable archive, a repeated entry name in the bundle or a jar, or a file other than the version's POM, jar, sources jar and Javadoc jar, their signatures, and their checksums, such as a local-repository file or the other line's version.
- A missing artifact, a missing MD5 or SHA-1 checksum of an artifact, a checksum that does not match, or a missing signature in a signed rehearsal.
- A main-jar entry that is neither the manifest, Maven metadata, `META-INF/LICENSE` or `META-INF/NOTICE`, nor a class whose outermost class is named by a source file in `src/main/java` or the line's `src/main/java-<adapter>` root; this rejects test classes, generated messages, probes, resources, and shaded runtimes, and requires every top-level class to have its own source file.
- A production source without its own class file, which a nested class does not replace, or a class whose class-file version is not Java 17.
- A class whose class names or descriptors name a Kryo, Chill, or Flink Kryo-serializer type or Flink's `GenericTypeInfo`, or that references a Flink member whose name contains `Kryo`, such as `SerializerConfig.isForceKryoEnabled` or, in Flink 1.20, `ExecutionConfig.registerKryoType`. Every UTF-8 constant is searched, including declared members' descriptors, so a string literal in internal slash form also fails; dotted names in messages and this library's own member names do not.
- LICENSE or NOTICE bytes in the jar or sources jar that differ from the tracked files.
- A sources jar whose `.java` entries differ from the line's production sources; this also distinguishes the two adapters.
- A Javadoc jar without its index and package summary.
- A POM with other coordinates, without the name, description, URL, license, developer, or SCM metadata that Central requires, or with a dependency whose scope is neither `provided` nor `test`, including profile dependencies.

The published POM is the project POM with the version set; its parent, `flink-connector-parent`, declares no dependencies, so consumers resolve no transitive dependency from either line.
The Verify workflow's `release-rehearsal` job runs an unsigned rehearsal of the POM version without `-SNAPSHOT` on every run; it uploads no bundle.
The release build skips test compilation and the runtime-application jars as well as tests; the test `protoc` executions still run.
Before changing the release profile, the guard, or the inspection, also run a signed rehearsal locally with a throwaway key, for example one created with `gpg --quick-gen-key` in a temporary `GNUPGHOME`, rather than the release key.

### Publishing a release

The `Release` job in `.github/workflows/release.yaml` runs in the `release` environment, whose secrets are `MAVEN_GPG_KEY` and `MAVEN_GPG_PASSPHRASE`, the project release key shared with flink-connector-gcp, and `CENTRAL_TOKEN_USERNAME` and `CENTRAL_TOKEN_PASSWORD`, a Central Portal user token for the `io.github.flink-gcp` namespace.
The environment admits only the `main` branch and `v*` tags and has no required reviewers, and the repository ruleset `release tags` lets only administrators create, move, or delete `v*` tags.
Pull requests and Verify never read the secrets.
Register a replacement value with `gh secret set <name> --env release`; the repository holds no release secret of its own.
The workflow's `actions/setup-java` step writes Maven settings whose `central` server reads the token pair from the environment, which only the staging steps set.

1. Merge a pull request that prepares the release.
   Set `project.version` in `pom.xml`, `protobuf.integration.version` in `examples/datastream/pom.xml`, and every page that names the development version (`git grep -n <current-version>`) to `X.Y.Z-SNAPSHOT` if they differ, so that Verify's rehearsal on the commit to tag stages exactly `X.Y.Z`; the workflow takes the version from the tag and does not compare it with the POM.
   Switch the README, the quickstart, and the compatibility guide to the `X.Y.Z` and `X.Y.Z-1.20` coordinates in the same pull request, because the release's documentation version is built from the tagged source, and revise `DocumentationExamplesTest`, which requires those pages and the usage guide to name the POM's development version.
   The release's acceptance evidence, such as the 1.0.0 benchmark rerun on Google Compute Engine ([#45](https://github.com/flink-gcp/flink-datastream-protobuf/issues/45)), names the commit it measured; a later performance-relevant change before the tag, such as to production code or a dependency, requires a rerun or an explicit invalidation of that evidence.
2. Optionally, on that commit, after its `CI passed` succeeds, run `gh workflow run release.yaml --ref main -f version=X.Y.Z`.
   The default `rehearse` mode builds, signs, and inspects both lines and checks the guard without uploading; the tag run repeats it before any upload, so this only gives earlier feedback.
3. Optionally run it again with `-f mode=validate` to upload both lines, wait for Central validation, and drop both deployments; like a tag run, it refuses a commit that `main` does not contain.
   Its final step drops every captured deployment that is `VALIDATED` or `FAILED`, including the first when the second staging step failed; a dropped deployment may still count toward the namespace's monthly Central usage.
4. Create a signed tag `vX.Y.Z` on that commit and push it.
   The workflow refuses a commit that `main` does not contain and any tag other than a bare `vX.Y.Z`; it repeats the signed rehearsal, stages `X.Y.Z` and `X.Y.Z-1.20`, publishes both after both validate, and then creates the GitHub Release with generated notes.
   The CI `workflow_run` hook then adds the release to the documentation site.

After the workflow succeeds:

1. Check both published versions under `https://repo1.maven.org/maven2/io/github/flink-gcp/flink-datastream-protobuf/<version>/`: each holds the POM, the jar, the sources jar, and the Javadoc jar, each with an `.asc` signature that `gpg --verify` accepts with the release key `679AB13942F51CAF09F06E73608AC6CD5F89628E`, which `keyserver.ubuntu.com` serves.
2. Verify consumers and capture fixtures from both published versions; ADR-0001 makes this the completion gate of the first release.
   Run `just examples-verify <flink-version> <protobuf-major> <new-repository> <published-version>` with `X.Y.Z` at the 2.x floor and ceiling and with `X.Y.Z-1.20` at the LTS version, each with Protobuf 3 and 4, and a new repository for every run.
   Capture and retain the four bundles that the published versions write as the [development baselines guide](validation/development-baselines.md#retain-a-published-release) describes, and attach them to the release.
3. Edit the GitHub Release's generated notes: summarize the API, default, runtime, and state-format changes, and link the compatibility guide.
   For 1.0.0, state that no 0.x milestone or development build upgrades to it, as the [compatibility guide](compatibility.md#development-builds-before-100) does.
4. Merge a pull request that sets the next development version, such as `X.Y+1.0-SNAPSHOT`, in `pom.xml`, `examples/datastream/pom.xml`, and every page that names the development version.

The workflow runs one release at a time; GitHub cancels a pending run when a newer one queues in the same group, so re-run a cancelled tag run from the Actions page, because it uploaded nothing and the tag still exists.

The run summary records each captured deployment ID and state change.
Recovery needs the Central token pair locally, from the maintainer's own copy because repository secrets cannot be read back, exported as `CENTRAL_TOKEN_USERNAME` and `CENTRAL_TOKEN_PASSWORD`; inspect both deployments with `just central-portal status <2x-id> <1x-id>`.
Before any Publish request, drop leftover `VALIDATED` or `FAILED` deployments with `just central-portal drop <id>...`, then rerun the workflow; leave a deployment that is still validating until its state can be checked again.
After a Publish request, do not rerun the workflow, which would stage another pair under immutable coordinates; resume with `just central-portal publish <2x-id> <1x-id>`, which publishes only `VALIDATED` deployments and waits for those already publishing.
If one deployment is `PUBLISHED` and the other `FAILED`, `publish` refuses the pair; read the Portal's error for the failed line, drop that deployment, and restage only that line from the tag in a fresh checkout with the signing variables, the token pair, and a `central` server entry in the Maven settings, using `just central-portal capture <new-id-file> just release-stage <line>`.
Then run `just central-portal publish <published-id> <new-id>`; the published line's coordinates stay as they are, and a defect that needs a source change instead needs a new version.
Once both deployments are `PUBLISHED`, whether after a resumed publication or when only the GitHub Release failed, create it with `gh release create vX.Y.Z --verify-tag --generate-notes --title vX.Y.Z`.
The documentation hook acts only on a successful tag-push run, so then run `gh workflow run ci.yaml --ref main` to add the release to the site.
A tag on the wrong commit can be deleted and recut only before the first Publish request; changed artifacts afterwards need a new version.

### Dependency updates

Dependabot checks Maven, GitHub Actions, and the root uv project weekly and proposes ungrouped version updates, subject to its ignore rules and open-PR limit.
Update the Python minor by hand: change `python` in `mise.toml` and `requires-python` together, relock, and run `just test-scripts` and `just test-doc-versions`.
The absence of a Dependabot PR does not establish that the `protobuf3` and `flink1` profile pins or `FLINK_CEILING` are current; the [upstream release check](#upstream-release-check) reports them.
Dependabot also proposes Maven Wrapper updates; update the Maven version under [Toolchain](#toolchain) in the same change.
Review Flink major/minor changes as a coordinated supported-window update under ADR-0003; patch updates remain eligible.
Follow the [version-update procedure](#updating-protobuf-schemas-or-flink) for Flink patches as well as Protobuf updates, including the matrix checks.
Keep each Protobuf runtime and protoc pair together.
The existing `org.apache.flink:*` exclusion also covers `flink-connector-parent` major/minor upgrades; check its releases during dependency maintenance and review relevant build-tool changes separately from runtime-window changes.
A parent update that changes the managed japicmp version also needs the [API compatibility gate](#api-compatibility-gate) review of its build break and report format.

JUnit major upgrades follow the supported Flink floor, using the same adoption condition as [connector issue #906](https://github.com/flink-gcp/flink-connector-gcp/issues/906).
Check the upstream Apache Flink release's root POM (`flink-parent`) for this condition; this library inherits `flink-connector-parent` separately.
Adopt JUnit 6 when the floor's upstream POM pins JUnit 6, then verify every supported runtime, including LTS.
The current [floor POM (2.2.1)](https://github.com/apache/flink/blob/450c63e961805a90aa18cb815d6b412f44303b9b/pom.xml#L158) pins JUnit 5.11.4 and the [LTS POM (1.20.5)](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/pom.xml#L155) pins 5.10.1; this project maintains its own tested JUnit 5 minor/patch pin.
Dependabot ignores major updates to `org.junit:junit-bom`, so recheck this condition whenever the supported Flink window changes.

Keep `slf4j-simple` on the SLF4J API major used by the supported Flink runtimes.
The current runtimes use SLF4J 1.7.36; upgrading only the binding to 2.x leaves the 1.7 API without a usable binding and disables test logging.
The upstream pins are recorded in the [2.2.1](https://github.com/apache/flink/blob/450c63e961805a90aa18cb815d6b412f44303b9b/pom.xml#L136), [2.3.0](https://github.com/apache/flink/blob/c0f8d1a1e09f209885a88f9c19ceb9d9e9870283/pom.xml#L136), and [1.20.5](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/pom.xml#L131) release POMs.
Dependabot ignores binding major updates; revisit the rule when reviewing a change to Flink's logging dependencies.

Chill remains an optional comparison dependency with individually reviewed updates.
Ordinary CI excludes its tagged tests, so run the comparison explicitly and inspect the resolved and loaded Kryo/Chill versions before accepting an update.
Keep historical measurements distinct from results for the new dependency version.

Update `benchmark.thrift.version` together with `benchmark_thrift_version` and `benchmark_thrift_sha256` in `benchmarks.just`; both values enter the CI compiler cache identity.
The same property pins `libthrift` and the required compiler version; updating only the Maven runtime pin makes benchmark initialization reject the older compiler.

Keep `benchmark.avro.version` on the Avro version of every supported Flink runtime, so the Avro comparison measures the dependency graph that applications run with.
Flink sets that version as `avro.version` in its root POM (`flink-parent`): the [2.2.1](https://github.com/apache/flink/blob/450c63e961805a90aa18cb815d6b412f44303b9b/pom.xml#L151), [2.3.0](https://github.com/apache/flink/blob/c0f8d1a1e09f209885a88f9c19ceb9d9e9870283/pom.xml#L151), and [1.20.5](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/pom.xml#L149) release POMs pin Avro 1.11.4, and a patch release can move it (1.20.0 pinned 1.11.3).
Keep `benchmark.jackson.version` on the exact Jackson version of the selected Avro release; Avro 1.11.4 imports [Jackson BOM 2.14.3](https://github.com/apache/avro/blob/6f119ef077e3a50382ba13164afecdf84408fc2f/lang/java/pom.xml#L41).
The benchmark's direct dependencies override the versions `flink-avro` brings, so the `benchmarks` profile runs a `dependencyConvergence` rule on `org.apache.avro:avro` and `com.fasterxml.jackson.core`.
A benchmark lane fails at `validate`, naming both dependency paths, when its Flink runtime brings another Avro or Jackson version, or when only `jackson-databind` moves; that pair would otherwise fail in Avro's schema parser with a `NoClassDefFoundError` for `StreamConstraintsException`.
A lane sees a Flink patch update only after the floor, ceiling, or LTS row above moves that lane's version; a Dependabot PR that changes only `pom.xml` leaves the benchmark lanes on their previous runtime.
The build has one `benchmark.avro.version`, so runtimes that pin different Avro versions need a per-runtime property before the rule can pass on every lane; Avro 1.12 also rejects the generated benchmark records until their package is trusted through `org.apache.avro.SERIALIZABLE_PACKAGES`.
If a security fix requires a newer Jackson than Avro imports, override `jackson-core` together with `jackson-databind`, remove `com.fasterxml.jackson.core` from the rule's `includes` with a comment naming the advisory, and validate the Avro lanes explicitly with `just benchmark-check <version> <protobuf-major> <new-output>`.
When updating `maven-enforcer-plugin` in that profile, confirm that a divergent `-Dbenchmark.avro.version` still fails `validate`, because a rule that stops matching passes on every lane.
Dependabot ignores every `org.apache.avro:*` and `com.fasterxml.jackson.core:*` update, including patches, because the rule accepts only the exact versions Flink and Avro pin; revisit both ignore rules when that pin moves.

For a Thrift or Avro update, regenerate the benchmark schemas, run both Protobuf profiles across the ADR-0003 smoke matrix, and retain new provenance separately from the fixed baseline.
Review Dependabot Thrift proposals as coordinated changes; a Maven-only version bump is not sufficient.

Dependabot alerts and security-update PRs are repository settings, separate from the weekly version-update schedule in `.github/dependabot.yml`.
Keep both enabled and review security-update PRs through the same verification and review flow.
GitHub also applies [ignore conditions to security updates](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/manage-your-dependency-security/controlling-dependencies-updated#ignoring-specific-dependencies), so enabling them does not guarantee a PR for every alert.
Review alerts even when no PR appears, and prepare an excluded upgrade manually when needed; an ignore rule is not a vulnerability assessment.

Alerts and security updates depend on the repository's dependency graph, which must stay enabled.
GitHub's static `pom.xml` parsing records only direct dependencies and plugins, leaves versions set by profile properties (such as the `benchmarks` pins) unresolved, and omits transitive dependencies, so alerts cannot match them.
The `Dependency submission` workflow therefore submits the resolved Maven graph from `main` on every push, weekly, and on demand, one snapshot each for the floor with Protobuf 3 and 4, the `FLINK_CEILING` runtime, the LTS runtime, and the `benchmarks` profile at the floor.
GitHub [prefers submitted data](https://docs.github.com/en/code-security/concepts/supply-chain-security/dependency-graph-data) over static parsing for the same manifest, so the root `pom.xml` graph no longer lists build plugins: an advisory against a plugin raises no alert, and plugins other than `avro-maven-plugin` (held with `benchmark.avro.version`) stay current only through Dependabot version updates.
Test-scoped dependencies, including the whole benchmark graph, are submitted with the development scope.
The standalone example is not submitted, because it resolves the unpublished development library, and keeps its static entry.
A failed job appears in the Actions tab and notifies the user who triggered it (for the weekly run, the user who last changed its cron line); an HTTP error from the snapshot API fails the job, but a response whose result is neither `SUCCESS` nor `ACCEPTED` is only logged as an error in a passing job, so check the run log when an expected alert is missing.

### Upstream release check

Dependabot proposes runtime updates only for the root `pom.xml` properties: patches of the Flink floor, and minors and patches of the default Protobuf pin.
Its ignore rules exclude new Flink minors and majors and new Protobuf majors, and it reads neither the `flink1` and `protobuf3` profile pins nor `FLINK_CEILING`.
The weekly `Upstream versions` workflow, also started through `workflow_dispatch`, therefore runs `just upstream-versions`, which compares the Flink 2.x floor, ceiling, and 1.20 LTS pins and both Protobuf pins with the `org.apache.flink:flink-core` and `com.google.protobuf:protobuf-java` metadata on Maven Central.
CI does not call this workflow, so a new upstream release never fails a pull request; Lint runs only the check's offline tests.

The check counts only final `x.y.z` releases, not previews or release candidates.
It fails, naming each overtaken pin and the file that holds it, when Maven Central lists one of the following releases:

| Pin | Overtaking release |
|---|---|
| Flink 2.x floor, ceiling, and 1.20 LTS | A newer patch of the pinned minor |
| Flink 2.x ceiling | A newer minor of the ceiling's major, or a release of a newer Flink major |
| Protobuf 4 default and `protobuf3` profile | A newer minor or patch of the pinned major |
| Protobuf 4 default | A release of a newer Protobuf major |

When it cannot read a pin or the metadata, it fails with an error that names the file, URL, or artifact instead of listing pins; rerun it after a transient network error.
A failed scheduled run notifies the user who last changed its cron line, and a finding repeats every week until its pin moves, including a Protobuf 4 update whose Dependabot PR is still open.

Respond to each finding through the [version-update procedure](#updating-protobuf-schemas-or-flink):

- **A new Flink 2.x minor**: follow the "Supported Flink minor window" row of the version-update table. The next library release containing the moved window is a new minor version.
- **A newer patch or Protobuf minor**: update the pin and its exact-version fixtures. This leads to a release only when the update fixes a defect that affects users.
- **A new Flink or Protobuf major**: decide its support under [ADR-0003](adr/0003-flink-version-compatibility.md) or [ADR-0001](adr/0001-native-protobuf-type-integration.md) before moving any pin. To defer support, raise that upstream's entry in `DECIDED_MAJORS` in `scripts/upstream-versions.py` in the change that records the decision; the check then reports only later majors.

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
Both run Serena on the Python 3.14 that `mise x python@3.14` puts on PATH, and stop `uvx` from downloading one.
uv would still prefer a uv-managed 3.14 if one were installed, so manage Python with mise rather than `uv python install`.
uv tool commands ignore the project's uv settings; without these options `uvx` uses uv's default interpreter, which can be a newer Python for which Serena 1.7.0's dependencies cannot be built.
Activate the current worktree when using Serena, and keep credentials and personal configuration outside tracked files.
Neither MCP server is required to build or test the project.

## Next implementation steps

The [ADR-0001 release contract](adr/0001-native-protobuf-type-integration.md) defines the initial public entry points, settings/defaults, framing, snapshot format, and descriptor normalization.
Versions 0.1.0, 0.2.0, and 0.3.0 are unpublished development milestones and permit documented breaking changes while the design matures.
Version 1.0.0 is the stabilization point and first Maven Central publication.
The development sequence and current implementation status are:

1. The immutable serializer in [#8](https://github.com/flink-gcp/flink-datastream-protobuf/issues/8) implements generated-class validation, transient parser reconstruction, limits, framing, copy, failure behavior, and the descriptor normalization needed for serializer identity.
2. TypeInformation and superclass TypeInfoFactory registration in [#9](https://github.com/flink-gcp/flink-datastream-protobuf/issues/9) provide production transport integration.
   Versioned descriptor snapshots and serializer-level unchanged-schema restoration are implemented by [#10](https://github.com/flink-gcp/flink-datastream-protobuf/issues/10).
3. Production transport, checkpoint/savepoint recovery, and isolated user-code classloading are verified by [#11](https://github.com/flink-gcp/flink-datastream-protobuf/issues/11).
   Google Well-Known Types and OpenTelemetry generated composite message acceptance in [#18](https://github.com/flink-gcp/flink-datastream-protobuf/issues/18) is covered by the [common-types tests and examples](common-types.md).
4. The [quickstart](quickstart.md), [usage guide](usage.md), and standalone compiled examples implement the documentation work in [#12](https://github.com/flink-gcp/flink-datastream-protobuf/issues/12). The shared-design site and its versioned API reference are implemented under [#19](https://github.com/flink-gcp/flink-datastream-protobuf/issues/19); the initial Pages deployment is live, with final live verification awaiting the search initialization repair.
5. Complete the [0.1.0 development tracker #6](https://github.com/flink-gcp/flink-datastream-protobuf/issues/6) after development acceptance, documentation, and [benchmark evidence #30](https://github.com/flink-gcp/flink-datastream-protobuf/issues/30). Retain fixed development artifacts and state fixtures with source/checksum provenance for later milestone checks; Maven Central publication does not block this milestone.
6. For [0.2.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/14), the pure directional evaluator from [#15](https://github.com/flink-gcp/flink-datastream-protobuf/issues/15) is implemented with descriptor-pair tests, and snapshot restore uses it for supported schema evolution under [#16](https://github.com/flink-gcp/flink-datastream-protobuf/issues/16), which also restores retained 0.1.0 development state with `baseline-upgrade`; [#17](https://github.com/flink-gcp/flink-datastream-protobuf/issues/17) adds the upgrade guide from 0.1.0 and the [API compatibility gate](#api-compatibility-gate), which checks each candidate against the retained 0.1.0 libraries; [#63](https://github.com/flink-gcp/flink-datastream-protobuf/issues/63) then retains the 0.2.0 baseline and publishes the `v0.2.0` pre-release, which the next documentation deployment lists as the 0.2 milestone.
7. For [0.3.0](https://github.com/flink-gcp/flink-datastream-protobuf/issues/20), [#21](https://github.com/flink-gcp/flink-datastream-protobuf/issues/21) designs explicit-descriptor DynamicMessage support in [ADR-0005](adr/0005-descriptor-bound-dynamic-messages.md), [#22](https://github.com/flink-gcp/flink-datastream-protobuf/issues/22) implements its type information, serializer, and snapshot, and [#23](https://github.com/flink-gcp/flink-datastream-protobuf/issues/23) verifies state recovery and completes the usage documentation. Document API/state compatibility decisions and migration requirements for generated-message users, and test the declared direct/sequential development upgrade outcomes before milestone completion.
8. [#140](https://github.com/flink-gcp/flink-datastream-protobuf/issues/140) restarts the persisted formats at version 1 for 1.0.0 under new snapshot class names; the library no longer reads 0.x state, and the 0.x fixtures, upgrade guide, and CI checks against the 0.x bundles are removed.
9. Prepare publication and packaged-artifact validation for `1.0.0` and `1.0.0-1.20` in [#13](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13), following ADR-0003's effective-model guards and two-artifact validation requirement. Rerun the [long benchmarks](validation/serializer-benchmarks-v0.1.0.md#benchmark-remeasurement-for-100) on Google Compute Engine from the fixed candidate. Complete the first-release milestone only after both versions are published, consumers verify them, and snapshot/savepoint fixtures from each published artifact are preserved with provenance.

No further 0.x milestone followed 0.3.0, and the remaining work prepares 1.0.0.
The 1.0.0 milestone tracks first publication, with no calendar deadline.

Implementation PRs update documentation for behavior they actually deliver.
Construction and registration examples describe implemented APIs; the runtime acceptance suite verifies checkpoint/savepoint recovery with unchanged and supported changed schemas.
The README distinguishes current transport support from the remaining release requirements.
The optional Chill probe is not a production fallback or a release acceptance substitute.

### Contract acceptance scenarios

The following scenarios are obligations for the linked implementation issues, not tests already passed by this documentation change.

| Area | Required scenarios | Owner |
|---|---|---|
| Construction and identity | Supported generated hierarchies; invalid classes, graphs reaching messages declared in proto2 or editions files, and extension-dependent graphs; isKeyType remains false with all settings; no message-key opt-in; immutable builder results; default and nondefault settings in equality/hashCode/canEqual | #8, #9 |
| Framing and limits | Empty and adjacent frames; negative, excessive, overflow-prone, and truncated lengths; malformed payloads; size/depth boundaries on write and read; nested/map/unknown-group recursion; no read-ahead into the next frame; exact stream copy | #8 |
| Native selection | Generic types disabled; explicit and AbstractMessage registration; fresh-JVM missing-registration and Message-interface controls; top-level/POJO/tuple transport; explicitly typed Row/List paths | #9, #11 |
| Snapshot and normalization | Version-1 round trips; unsupported/corrupt data and flags; missing/conflicting imports; recursive message graphs; deterministic dependency ordering; SourceCodeInfo ignored but options/declaration order/unknown content retained; same fingerprint with differing descriptor content cannot establish compatibility | #10 |
| 0.1.0 unchanged-schema restore | Changed fields, names, imported descriptors and framing rejected; unchanged/increased limits and either deterministic-mode transition accepted; any decreased limit rejected, including mixed transitions and decreases after new snapshot emission; isolated classloaders; missing/unsupported classes; old metadata readable without old generated classes | #10, #11 |
| Runtime values and types | Continued processing and restored values in scalar-keyed and operator state, including MapState values with scalar user keys; unknown fields; nested WKT/OTel values; Any payload bytes kept opaque; negative controls show both inferred and explicitly typed keyBy bypass the non-key declaration and equal message bytes can hash into different groups across isolated classloaders | #9, #11, #18 |
| Published release baseline | Attributable message/snapshot bytes and complete savepoints from the published artifact, expected values, settings, schemas/imports, job/state identity, generation commands, and exact source/artifact/toolchain provenance; retain fixtures without overwriting them with newer writers | #13 (1.0.0) |
| 0.2.0 upgrade | Directional accepted/rejected descriptor pairs; real restoration of claimed supported fixed 0.1.0 development fixtures with old generated classes absent; explicit rejection and migration guidance for any intentional break; API checks separate from state and gencode/runtime checks | #15, #16, #17 |
| 0.3.0 upgrade decisions | Source/API checks; already-compiled consumers for unchanged APIs and compiled replacements for intentional breaks; direct and sequential upgrade outcomes, including newly emitted state after 0.2.0 evolution; retain development fixtures and test supported restore/migration or explicit rejection; document each break and required procedure | #20, #21, #22, #23 |

Run production acceptance across the ADR-0003 matrix: JDK 17/21 for both supported Flink 2.x minors and JDK 17 for Flink 1.20, with the pinned Protobuf 3.25.x/4.x profiles and matching application gencode/runtime pairs, including a packaged-library consumer for each artifact line.
Compatibility across changed built-in descriptors is not implied by supporting both runtime profiles.
Single-JVM MiniCluster success does not prove cross-process key hashing; the supported keyed scenarios extract scalar keys and do not use generated messages as keys.

The current matrix covers the production serializer and type integration as well as separate feasibility probes in each combination.
It also covers saved-state recovery with unchanged and supported changed schemas within each pinned Flink/Protobuf combination.
The unchanged-jar checks run each line's Protobuf 4 jar with Protobuf 3 generated code and runtime; neither these checks nor the matrix prove saved-state upgrades between Flink versions or Protobuf runtimes.
