# Development guide for coding agents

## Project and commands

This single-module library targets native Protobuf integration with Flink DataStream.
The native serializer, application-facing type integration, and versioned descriptor snapshots are implemented.
MiniCluster tests verify checkpoint recovery and savepoint restoration with HashMap and RocksDB state backends, including supported schema changes.
The executable probes are separate test instruments.
Read the matching ADR before changing behavior, public API, dependencies, packaging, or workflow.
Current design lives in `docs/adr/`; measured feasibility lives in `docs/validation/spike-0.md`.

- `just format`: format Java before committing.
- `just verify`: build and run unit/integration tests and license checks.
- `just verify-protobuf 3` / `just verify-protobuf 4`: clean verification for each runtime/gencode pair.
- `just verify-flink <version> <protobuf-major>`: clean verification with the matching Flink adapter.
- `just binary-compat flink2 <ceiling>` / `just binary-compat flink1`: run a line's unchanged Protobuf 4 jar at the 2.x ceiling and with Protobuf 3 tests.
- `just baseline-capture <line> <protobuf-major> <new-output>`: retain development writers and state in a new external directory.
- `just baseline-check <bundle>` / `just baseline-read <bundle> <version> <new-output>`: check or restore retained inputs without rebuilding.
- `just baseline-fetch <tag> <new-output> [bundle...] [--archives <dir>]`: download and verify retained release bundles against the pinned inventory; `just baseline-fetch-test` runs its synthetic tests.
- `just baseline-release read|review|assemble|verify ...`: read, review, assemble, and verify a milestone's retained bundles for its pre-release; `just baseline-release-test` runs its synthetic tests.
- `just baseline-upgrade <bundle> <version>`: restore a verified bundle's retained state with the current library and new-schema applications.
- `just api-compat <bundle>`: compare the candidate's `@Public`/`@PublicEvolving` API with a retained library through japicmp, with a negative control.
- `just baseline-consume <bundle> <candidate-jar> <version> <new-output>`: run a retained bundle's compiled consumers against a candidate jar without recompiling them.
- `just release-rehearse <version> <new-output> [--unsigned]`: stage both lines' Central bundles from the committed HEAD without uploading and inspect them under ADR-0006; `just release-artifacts-test` runs its synthetic tests and those of the `Release` workflow.
- `just central-portal status|publish|drop <id>...`: inspect, publish, or drop Central Portal deployments when recovering a release; see "Publishing a release" in `docs/development.md`.
- `just examples-verify <version> <protobuf-major> <external-maven-repository>`: build/install the development jar and verify the standalone application.
- `just docs` / `just docs-serve`: build or preview the shared-design site.
- `just docs-validate` / `just test-doc-versions`: verify source-backed site content and version assembly.
- `just docs-javadoc <flink1|flink2> <3|4>`: generate strict API documentation under `target/apidocs`.
- `just probe-chill`: opt in to the separate comparison, with generic types enabled only there.
- `just benchmark-tools <external-cache>`: build the pinned Thrift compiler for ADR-0004 benchmarks.
- `just benchmark-tools-check <compiler>`: reject a compiler that is not the pinned version or cannot generate the benchmark corpus.
- `just benchmark-check <version> <protobuf-major> <new-output>` / `just benchmark-smoke <version> <protobuf-major> <new-output>`: validate the benchmark corpus, with short JMH forks in smoke mode.
- `just benchmark-run <version> <protobuf-major> <new-output>`: run the opt-in long measurements under ADR-0004 and the evidence-retention procedure in `docs/development.md`.
- `just benchmark-build <version> <protobuf-major>` / `just benchmark-measure <mode> <version> <protobuf-major> <new-output>`: the two halves of those recipes, for runners that check host load between them.
- `just benchmark-pair <version> <protobuf-major> <new-output>`: run the same long protocol for only the native/Chill Protobuf pair when comparing native serializer changes.
- `just benchmark-evidence-check <input> <new-output>`: validate retained benchmark bytes before upload; follow the human review procedure in `docs/development.md`.
- `just benchmark-evidence-staged <new-output>`: validate the exact staged benchmark archives before an evidence commit.
- `just benchmark-evidence-repository <new-output>`: inspect committed benchmark archives and retain member inventories without re-uploading the archives.
- `just benchmark-evidence-test`: run synthetic content-gate tests without Maven or Thrift.
- `just gce-benchmark plan|execute|status|collect|cleanup ...`: run benchmarks on temporary credential-free GCE VMs; `execute` requires `--approve` after the cost and ownership checks in `docs/development.md`, and `just gce-benchmark-test` runs its offline tests.
- `just upstream-versions`: name the Flink and Protobuf pins that newer Maven Central releases overtake; the weekly `Upstream versions` workflow runs it outside PR checks, and `just upstream-versions-test` runs its offline tests.
- `just test-scripts [test-file...]`: run the `scripts/tests` suite through the locked pytest; the per-tool test recipes delegate to it.
- `just lint`: workflow, project shell script, and Markdown checks.
- `just pin-actions`: pin Actions references when adding or updating workflows.

Use `mise x -- just <recipe>` outside a mise-activated shell.
See `docs/development.md` for tool installation, JDK 21, and the test boundary.
Run targeted tests while iterating and full verification before pushing Java/build changes in this single-module project.
Verify both Protobuf profiles for compatibility-sensitive changes across the ADR-0003 Flink matrix, including the binary recipe when changing APIs, dependencies, or build configuration.
CI covers JDK 17/21 on the current and previous Flink 2.x minors and JDK 17 on Flink 1.20 LTS.

## Implementation and documentation

- Production packages use `io.github.flink.gcp.protobuf`; give production types the appropriate Flink API annotation and public/protected APIs Javadoc.
- Use JUnit 5, AssertJ, and handwritten test doubles. Do not add Guava, Mockito, PowerMock, Lombok, or AutoValue.
- Keep TypeInfoFactory registry controls in separate test JVMs. Native integration tests disable generic types and must not rely on `--add-opens`.
- Keep repository Python tools on the standard library and run them with `uv run --locked --isolated --no-dev python`, as the justfile `python` variable does; add test-only packages to the root uv project's `dev` group.
- Write repository Python tests as plain pytest functions with fixtures, `monkeypatch`, and `pytest.mark.parametrize`, not `unittest` test cases, subtests, or mocks.
- Keep generated code under target. Do not promote the probe serializer or its wire format into a public API.
- Keep current behavior in user documentation, decisions and alternatives in ADRs, and imperative development rules here or in skills.
- Update documentation with behavior/API changes. Distinguish planned behavior from measured behavior; support claims with pinned source or tests.
- Write commits, PR text, docs, comments, and Javadoc in English. Use the english-tech-writing skill when available.
- Use Apache-2.0 headers with `The flink-gcp authors`; preserve third-party holders and provenance.
- Create temporary automation outside the repository. Never commit local scripts, credentials, personal memory, or generated artifacts.

## GitHub workflow

- The four workflow skills are tracked copies from `flink-gcp/flink-gcp-dev-tools`; `just skills-sync` installs the commit pinned in `justfile`.
- The shared skills retain the original connector procedures, decision conditions, examples and exceptions. Keep project-specific command and compatibility bindings here and in the development guide, outside managed skill directories. Read `docs/development.md` before updating the pin or installation recipe.
- Use `gh`; one worktree per PR under `/tmp/worktrees/flink-datastream-protobuf/`. Never switch branches in the main checkout.
- After the initial empty commit, every change goes through a Draft PR using `.github/PULL_REQUEST_TEMPLATE.md` with filled WHAT and WHY.
- Use `$push-pr-branch` before every branch push. Commit local work, fetch, rebase before squashing, and inspect the explicit deletion list.
- Apply the verification and closing-reference bindings in `docs/development.md` when running the shared skills, including the case with no assigned issue.
- After Draft creation, use `$self-review`, then `$self-review-round-two`, then `$independent-review`. Their instructions define the distinct passes and bounded repairs.
- Record feedback inline on changed lines using the GitHub review API comments array. Do not post standalone review summaries.
- Route out-of-scope findings with the user; do not create issues without a routing decision. Existing authorization persists across the workflow.
- Wait for current aggregate `CI passed` and the required review flow before Ready. The restored shared skill retains the original recorded unavailable-reviewer exception; running or uncollected reviews are incomplete. The user merges.
- After confirming the merge, clean up that PR's own worktree, local branch, and review worktrees without a separate request. First check that no worktree has tracked or untracked changes, that the branch tip is contained in `origin/main`, that each review worktree still names its recorded reviewed SHA, and that ignored files are only build and tool outputs such as `target/` and `.venv/`. Keep anything else, such as ignored local configuration, unexpected commits, fetched bundles, or other sessions' worktrees, and ask before removing it.

## Optional tools

Use Serena for Java symbols and references, activating the current worktree; use rg for text and configuration.
Use Context7 for version-specific third-party documentation when available, and confirm claims against pinned upstream sources or tests.
Both are optional development tools; builds must not require an MCP server.
Keep shared decisions in tracked documentation, not Serena memories.
