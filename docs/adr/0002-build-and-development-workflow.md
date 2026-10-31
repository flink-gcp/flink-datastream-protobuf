---
title: "ADR-0002: A single Maven module with a shared local and CI workflow"
bookHidden: true
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

# ADR-0002: A single Maven module with a shared local and CI workflow

- Status: Accepted
- Date: 2026-09-06
- Release contract amended: 2026-09-08, [issue #7](https://github.com/flink-gcp/flink-datastream-protobuf/issues/7)
- Dependency update policy amended: 2026-09-12
- Benchmark Avro and Jackson update exclusions amended: 2026-10-11
- Benchmark Avro and Jackson convergence check amended: 2026-10-12, [issue #84](https://github.com/flink-gcp/flink-datastream-protobuf/issues/84)
- Maven dependency graph submission amended: 2026-10-12, [issue #82](https://github.com/flink-gcp/flink-datastream-protobuf/issues/82)
- Development bundle tooling amended: 2026-09-16, [issue #40](https://github.com/flink-gcp/flink-datastream-protobuf/issues/40)
- CI compiler reuse and job budgets amended: 2026-10-11, [issue #46](https://github.com/flink-gcp/flink-datastream-protobuf/issues/46); evidence: [CI budget measurements](../validation/ci-budget.md)
- Retained baseline retrieval amended: 2026-10-11, [issue #61](https://github.com/flink-gcp/flink-datastream-protobuf/issues/61)
- Python tooling through uv amended: 2026-10-12, [issue #96](https://github.com/flink-gcp/flink-datastream-protobuf/issues/96)
- Development milestone documentation amended: 2026-10-19, [issue #62](https://github.com/flink-gcp/flink-datastream-protobuf/issues/62)
- Required `CI passed` ruleset amended: 2026-10-19
- CI Maven mirror and shared repository cache amended: 2026-10-30, [issue #135](https://github.com/flink-gcp/flink-datastream-protobuf/issues/135)
- Retained 0.x checks ended by the 1.0.0 format restart: 2026-10-31, [issue #140](https://github.com/flink-gcp/flink-datastream-protobuf/issues/140)
- Current instructions: [Development](../development.md) and [Contributing](../../CONTRIBUTING.md)

## Decision

Use one jar module with Maven coordinates `io.github.flink-gcp:flink-datastream-protobuf`, starting at `0.1.0-SNAPSHOT`.
Develop it in the flink-gcp organization without depending on flink-connector-gcp.
Reuse that project's Apache-2.0 licensing, collective copyright identity, Maven Wrapper, connector parent 2.0.0, mise/just entry points, Java formatting, Checkstyle, Apache RAT, JUnit 5, and AssertJ conventions.
The project metadata and packaged notices identify The flink-gcp authors rather than inheriting ASF ownership.
Keep third-party attribution on the Apache-derived Checkstyle files.

Compile for Java 17 and verify on JDK 17 and 21 for Flink 2.x, and JDK 17 for Flink 1.20 LTS, as amended by [ADR-0003](0003-flink-version-compatibility.md).
Keep the shared implementation in this module, with alternate source roots only where Flink APIs differ.
Resolve protobuf-java and protoc together from a Maven profile for each tested major.
Generate test messages under target and keep test instruments out of the jar.
Run each test class in a fresh JVM to isolate Flink's static factory registry.
The current probes require no Docker, cloud credentials, or module-opening flags.

CI calls the same just recipes used locally; the `Dependency submission` workflow runs only from `main`, outside the `CI passed` gate, and calls its GitHub action directly.
The CI orchestrator has no path filter and requires source selection, Verify, Lint and Docs to succeed through one `CI passed` gate.
It freezes the source SHA before calling each reusable workflow, on PRs and trusted publication events.
An unexpected skipped, failed, or cancelled dependency cannot satisfy that gate.
Required-check policy can name this aggregate check without depending on individual matrix job names.
A repository ruleset requires it on main without requiring an up-to-date branch, because requiring one would make each PR update and rerun the full matrix whenever main moves before it merges.
CI on main then verifies the merged result; a newer main run replaces a pending one but not a running one, so a burst of merges can leave intermediate commits unverified.
Actions are pinned to commit SHAs; linter versions live in mise.toml, while Java build-tool versions live in Maven, except the dependency-graph plugin that the submission action pins.

Repository Python tools and their tests run through one non-packaged uv project at the root, which limits uv to Python 3.14 and locks the test dependencies.
mise.toml supplies that Python minor like the other tools, and the project settings stop uv from preferring uv-managed interpreters or downloading one.
Recipes and workflows call `uv run --locked`, and no step depends on a system Python.
The tools use only the standard library and run in a temporary environment without the test dependencies, so the evidence gate and baseline retrieval load no third-party code; a test imports every tool without site-packages.
Recipes take uv from PATH rather than a nested `mise x`, which would replace a JDK selected for the command and move Java subprocesses off a JDK 21 lane.
One pytest recipe runs the `scripts/tests` suite in Lint; the documentation tests, some of which need Hugo and Node, run in Docs.
The documentation-site tool runs in the same project rather than a separate `docs` project or uv workspace member: neither is a package or has runtime dependencies, and one project gives one Python minor, one dependency lock, and one Dependabot entry.
Add a workspace member when a tool needs its own runtime dependencies, as in flink-connector-gcp.

Verify reuses the pinned benchmark compiler through an exact-key Actions cache rather than rebuilding it in every matrix job.
The key identifies the Thrift version, source checksum, build recipe, distribution release, architecture, and C++ compiler.
Each job checks a restored compiler before use and rebuilds the pinned source when the entry is missing or fails the check; only a job that missed the entry saves its newly built and checked compiler, before benchmark processes run.
A shared preparation job was rejected because every matrix job would wait for it, including runs with a usable cache entry.
Separate smoke jobs were rejected because they would repeat checkout, JDK, and Maven setup in ten more jobs; Verify already starts 16 jobs per run, and the organization's [GitHub Free plan](https://docs.github.com/en/actions/reference/limits) allows 20 concurrent standard-runner jobs.
Neither alternative changes the supported runtime matrix or the aggregate gate; the cache keeps the smoke in the job that verified the same runtime.
After Verify grew to 26 jobs and pull-request runs waited for runners at that limit, pull requests run the smoke and the capture rehearsal only when they change sources or build inputs; pushes to main, the weekly schedule, and manual runs always run both, so a skipped check is still exercised after merge.
Job limits come from measured GitHub runner durations with at least twice the slowest successful run, and the smoke has a command limit so that a hung measurement fails before the job limit cancels evidence retention.
Shared-runner durations never become benchmark pass/fail thresholds.

CI resolves Maven artifacts from Google's mirror of Maven Central before Central, through a CI-only settings file, because Central fails requests from shared runners often enough to fail jobs that changed nothing: HTTP 502 responses failed lanes on `main` in this repository, and HTTP 429 rate limiting did so in [flink-connector-gcp](https://github.com/flink-gcp/flink-connector-gcp/issues/1887).
The goal is reliable resolution, not shorter jobs.
The mirror is an additional repository ahead of Central rather than a `<mirror>`, because replacing Central would fail a build whenever the mirror had not synced an artifact yet; for the same reason, the Maven Wrapper downloads from the mirror only after checking that it holds the pinned Maven.
Local builds and retained captures keep resolving from Central: a `.mvn/maven.config` entry or a POM repository was rejected because it would change every developer's resolution for a CI failure, and a retained bundle records the resolved tree and jars, not the serving repository.
One shared Maven repository entry, saved by one Verify job on `main`, replaces `setup-java`'s cache, which any job could save under a shared key and which held a 7 MB entry that every Verify job restored.
Saving only on `main` keeps pull requests from creating entries that only their own ref can read, and starting the saving job from an empty repository when its key is new keeps an entry from accumulating superseded dependencies.
The other lanes fetch the runtime-specific dependencies that the entry lacks from the mirror; a warm-up that resolves every lane before the save, or one entry per lane, was rejected because it would enlarge the cache and add steps without changing where those artifacts come from.
The mise, Hugo, and retained-bundle caches are unchanged, because a pull request saves a copy only when it does not restore its exact key, as when `main` has no entry for it, and an unused copy is evicted after seven days without access.

Dependabot proposes individual weekly Maven, GitHub Actions, and uv updates.
Flink major/minor changes follow ADR-0003, and Protobuf major changes require an explicit compatibility decision.
JUnit major upgrades follow the supported Flink floor: adopt JUnit 6 when that upstream Flink release's root POM (`flink-parent`) pins it, then verify the complete supported matrix, including LTS.
This uses the adoption condition recorded in [connector issue #906](https://github.com/flink-gcp/flink-connector-gcp/issues/906); minor and patch updates within JUnit 5 remain reviewable.
Keep the test logging binding on the SLF4J API major supplied by Flink and exclude independent binding major updates.
Hold the benchmark Avro dependency on the supported Flink runtimes' exact Avro version and its Jackson result reader on that Avro release's exact Jackson version, excluding those Dependabot updates and failing the benchmark build when either diverges from the selected runtime's dependency graph.
Revisit these exclusions when changing the supported Flink window or when a security alert requires an excluded upgrade.
Keep Dependabot alerts and security-update PRs enabled in repository settings; security updates still require review and verification before merge.
Ignore conditions can also prevent security-update PRs, so alerts requiring an excluded upgrade need manual review and preparation.
Submit the resolved Maven dependency graph for each supported runtime and the `benchmarks` profile from `main`, because static `pom.xml` parsing omits transitive and profile-pinned dependencies and alerts cannot match them otherwise.
The submission replaces the static root `pom.xml` entry, so build plugins leave the graph: plugin advisories raise no alert, and plugins rely on Dependabot version updates.
GitHub's automatic dependency submission was rejected because it resolves only the default profile and would miss Protobuf 3, the ceiling, LTS, and `benchmarks`.

All changes after the initial empty main commit use a dedicated worktree and a Draft PR with the WHAT/WHY template.
Require two distinct self-review rounds, an independent reviewer that did not author the change, and current aggregate CI before Ready.
The restored original workflow permits a recorded exception when the independent reviewer cannot run; a running or uncollected review is incomplete and does not qualify.
Freeze the base and head for review, use range-diff for bounded repairs after a completed pass, and keep feedback inline.
Agent guidance and skills make this procedure discoverable while keeping private memory and credentials outside the repository.

Maintain the four workflow skills and the WHAT/WHY template in [flink-gcp-dev-tools](https://github.com/flink-gcp/flink-gcp-dev-tools).
Track skill copies at the full upstream commit recorded in the justfile and update them through `just skills-sync` and a reviewed PR.
Keep the installation recipe visible in `dev-tools.just`, and project-specific commands and rules in this repository's guidance.
The shared procedures are restored from connector commit `02c4bd594d2b774cc24b0c0194c1834dd5032120`; preserve their detailed instructions, exceptions, concrete examples and historical evidence.
Bind this library's single-module checks and supported profiles through its existing guidance rather than maintaining a second workflow.
This follows the [shared-assets decision](https://github.com/flink-gcp/flink-gcp-dev-tools/blob/f33d4549a3be1790ef26650ba9cc35a94a44b49f/docs/adr/0001-share-development-assets.md) without adding a submodule or a build-time network requirement.

## Consequences

The library remains a single Maven module with no parent reactor.
A standalone application under `examples/datastream` consumes an installed development jar through ordinary Maven coordinates; it is not a reactor module or publication artifact.
Its independent build verifies application use without relying on library source roots, and allows the same consumer to switch to Maven Central coordinates at 1.0.0.
The existing runtime/JDK/Protobuf CI matrix verifies the examples.
Hugo `example` shortcodes render the named marker region directly from that version's application source or YAML.
Markdown stores only the source reference, with no copied excerpt or synchronization step.
GitHub readers follow the adjacent links to the executable sources; Hugo validates the marker references during site generation.
Public API documentation is checked by direct Javadoc generation; [ADR-0006](0006-central-portal-releases.md) configures the Maven release Javadoc jar with the same doclint strictness.
The site uses Hugo Extended and the pinned shared flink-gcp-dev-tools design, with existing Markdown mounted as canonical content.
The consumer owns URLs, source links, version selection, Javadoc and publication; no Java runtime dependency on the connector or shared tooling is introduced.
The bootstrap-only deferral of Hugo is complete.

From the first 1.0.0 release, documentation retains the latest patch of the current and previous released minor series plus Development.
Only published stable `vX.Y.Z` releases from 1.0.0 are eligible for that window; the Flink 1.20 artifact suffix shares its release's documentation slot.
Before the first stable release, the site hosts Development and the latest patch of each 0.x minor published as a GitHub pre-release, labelled as a development milestone.
The 0.2.0 tracker [#14](https://github.com/flink-gcp/flink-datastream-protobuf/issues/14) and its upgrade guide [#17](https://github.com/flink-gcp/flink-datastream-protobuf/issues/17) require attributable 0.1.0 documentation beside the 0.2.0 pages, and the original Development-only rule left tagged milestone pages reachable only as source.
Milestones leave the site with the first stable release.
Keeping them after 1.0.0 was rejected because they are unmaintained, unpublished APIs; the release archive retains their tagged sources.
Each selected source is pinned to a commit and builds its own examples, dependencies and strict Javadoc.
The controller checks provenance and assembles into a fresh output directory; it does not rewrite historical release content or retain generated HTML as an archive.
Development and milestones are excluded from indexing, and source links identify the exact build commit.
Version-free URLs point to Development, never a milestone, before the first stable release and the current release afterwards.

PRs validate and package the complete site without deploying.
Trusted main and release-triggered runs publish only after the same frozen main source passes Verify, Lint and Docs.
Publication is serialized from source selection through deployment, and only the deployment job has Pages write permissions.
The release hook accepts successful tag-push `Release` runs; failed runs and manual dry runs do not deploy.
[ADR-0006](0006-central-portal-releases.md) defines the `Release` workflow.
GitHub Pages is configured and the initial deployment is live; final verification of the deployed site after the search initialization repair remains a rollout gate of [#19](https://github.com/flink-gcp/flink-datastream-protobuf/issues/19).
Current commands, retention details and rollout instructions live in the development guide and documentation versions page.

[Issue #13](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13) owns 1.0.0 publication preparation, candidate verification, actual publication, and consumer verification.
Issue #40's test-only development bundle tooling retains packaged writers, compiled consumers, runtime dependencies, and state outside the source tree.
The capture workflow builds each artifact/profile once and reruns its retained inputs on the supported JDK/Flink combinations without compilation.
CI exercises these as disposable rehearsals: on every push to main, the weekly schedule, and manual runs, and on pull requests that change sources or build inputs.
The final v0.1.0 bundles, captured from source revision `f5063f784bdf6799250c521c39a47701900133ea`, are stored with `FINAL-BASELINE.json`, `INVENTORY.json`, and `SHA256SUMS` as assets of the `v0.1.0` pre-release, as described in the [retained baseline](../validation/development-baselines.md#retained-v010-baseline).
Milestone checks obtain them through a verified retrieval that checks a `SHA256SUMS` digest pinned in the repository and every extracted file, without rebuilding the old tree; CI caches each archive by its checksum and verifies it again on every run.
Milestone-to-milestone API checks are added for 0.2.0 against the fixed 0.1.0 development baseline in [issue #17](https://github.com/flink-gcp/flink-datastream-protobuf/issues/17), separately from saved-state and application gencode/runtime checks.
An opt-in Maven profile compares the `@Public` and `@PublicEvolving` API of a candidate with each retained library using the parent-managed japicmp plugin, and japicmp's build break rejects incompatible changes except members excluded in that profile with a link to their release documentation; a JDK-only check requires a deliberately broken fixture to be reported in the same run.
The parent's defaults compare `org.apache.flink` coordinates and cover `@Public` only, which would leave this library's `@PublicEvolving` API unchecked.
The retained compiled consumers also run against the candidate without recompilation, on dependencies resolved by the current tree because the retained dependency jars are host-specific.
0.3.0 retained those checks and extended them to the fixed 0.2.0 development baseline under ADR-0001's 0.x policy.
The [1.0.0 format restart](0001-native-protobuf-type-integration.md#compatibility-policy-before-100) ends the 0.x checks: CI no longer runs them against the 0.x bundles, and the same restore, API, and consumer checks apply to bundles retained from published releases, starting with 1.0.0.
Until a release is retained, CI runs these checks on the development bundle that the same build captures, so that they keep working.
Intentional API or state breaks require explicit release notes, upgrade guidance and a reviewed compatibility record; tests must prove supported paths and rejection of unsupported state.
Undocumented regressions and failures of claimed supported paths block milestone completion and eventual publication.
The public API and supported forward-restore contract stabilize at 1.0.0; [ADR-0005](0005-descriptor-bound-dynamic-messages.md) selects the DynamicMessage implementation structure.
This bootstrap creates neither a Maven Central release nor a stable public API.
