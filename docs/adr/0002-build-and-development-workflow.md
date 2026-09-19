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
- Development bundle tooling amended: 2026-09-16, [issue #40](https://github.com/flink-gcp/flink-datastream-protobuf/issues/40)
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

CI calls the same just recipes used locally.
The CI orchestrator has no path filter and requires source selection, Verify, Lint and Docs to succeed through one `CI passed` gate.
It freezes the source SHA before calling each reusable workflow, on PRs and trusted publication events.
An unexpected skipped, failed, or cancelled dependency cannot satisfy that gate.
Required-check policy can name this aggregate check without depending on individual matrix job names.
Actions are pinned to commit SHAs; linter versions live in mise.toml, while Java build-tool versions live in Maven.

Dependabot proposes individual weekly Maven and GitHub Actions updates.
Flink major/minor changes follow ADR-0003, and Protobuf major changes require an explicit compatibility decision.
JUnit major upgrades follow the supported Flink floor: adopt JUnit 6 when that upstream Flink release's root POM (`flink-parent`) pins it, then verify the complete supported matrix, including LTS.
This uses the adoption condition recorded in [connector issue #906](https://github.com/flink-gcp/flink-connector-gcp/issues/906); minor and patch updates within JUnit 5 remain reviewable.
Keep the test logging binding on the SLF4J API major supplied by Flink and exclude independent binding major updates.
Revisit these exclusions when changing the supported Flink window or when a security alert requires an excluded upgrade.
Keep Dependabot alerts and security-update PRs enabled in repository settings; security updates still require review and verification before merge.
Ignore conditions can also prevent security-update PRs, so alerts requiring an excluded upgrade need manual review and preparation.

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
Public API documentation is checked by direct Javadoc generation; Maven release Javadoc packaging remains in [#13](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13).
The site uses Hugo Extended and the pinned shared flink-gcp-dev-tools design, with existing Markdown mounted as canonical content.
The consumer owns URLs, source links, version selection, Javadoc and publication; no Java runtime dependency on the connector or shared tooling is introduced.
The bootstrap-only deferral of Hugo is complete.

Documentation follows Development until the first 1.0.0 release, then retains the latest patch of the current and previous released minor series plus Development.
Only published stable `vX.Y.Z` releases from 1.0.0 are eligible; the Flink 1.20 artifact suffix shares its release's documentation slot.
Each selected source is pinned to a commit and builds its own examples, dependencies and strict Javadoc.
The controller checks provenance and assembles into a fresh output directory; it does not rewrite historical release content or retain generated HTML as an archive.
Development is excluded from indexing, and source links identify the exact build commit.
Version-free URLs point to Development before the first release and the current release afterwards.

PRs validate and package the complete site without deploying.
Trusted main and release-triggered runs publish only after the same frozen main source passes Verify, Lint and Docs.
Publication is serialized from source selection through deployment, and only the deployment job has Pages write permissions.
The release hook accepts successful tag-push `Release` runs; failed runs and manual dry runs do not deploy.
The release workflow itself remains part of #13.
Initial Pages configuration and verification of the deployed URL remain post-merge rollout gates of [#19](https://github.com/flink-gcp/flink-datastream-protobuf/issues/19).
Current commands, retention details and rollout instructions live in the development guide and documentation versions page.

[Issue #13](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13) owns 1.0.0 publication preparation, candidate verification, actual publication, and consumer verification.
Issue #40's test-only development bundle tooling retains packaged writers, compiled consumers, runtime dependencies, and state outside the source tree.
The capture workflow builds each artifact/profile once and reruns its retained inputs on the supported JDK/Flink combinations without compilation.
CI exercises these as disposable rehearsals; final milestone capture and long-term storage follow the remaining v0.1.0 work.
Milestone-to-milestone API checks are added for 0.2.0 against the fixed 0.1.0 development baseline in [issue #17](https://github.com/flink-gcp/flink-datastream-protobuf/issues/17), separately from saved-state and application gencode/runtime checks.
Retain those checks in 0.3.0 and extend them to the fixed 0.2.0 development baseline and direct/sequential upgrade outcomes under ADR-0001's 0.x policy.
Intentional API or state breaks require explicit release notes, upgrade guidance and a reviewed compatibility record; tests must prove supported paths and rejection of unsupported state.
Undocumented regressions and failures of claimed supported paths block milestone completion and eventual publication.
The public API and supported forward-restore contract stabilize at 1.0.0; DynamicMessage implementation structure is selected by its design issue rather than fixed here.
This bootstrap creates neither a Maven Central release nor a stable public API.
