# Development guide for coding agents

## Project and commands

This single-module library targets native Protobuf integration with Flink DataStream.
Production serializers are not implemented yet; the executable probes are test instruments.
Read the matching ADR before changing behavior, public API, dependencies, packaging, or workflow.
Current design lives in `docs/adr/`; measured feasibility lives in `docs/validation/spike-0.md`.

- `just format`: format Java before committing.
- `just verify`: build and run unit/integration tests and license checks.
- `just verify-protobuf 3` / `just verify-protobuf 4`: clean verification for each runtime/gencode pair.
- `just probe-chill`: opt in to the separate comparison, with generic types enabled only there.
- `just lint`: workflow and Markdown checks.
- `just pin-actions`: pin Actions references when adding or updating workflows.

Use `mise x -- just <recipe>` outside a mise-activated shell.
See `docs/development.md` for tool installation, JDK 21, and the test boundary.
Run targeted tests while iterating and full verification before pushing Java/build changes in this single-module project.
Verify both Protobuf profiles for compatibility-sensitive changes; CI covers JDK 17 and 21.

## Implementation and documentation

- Production packages use `io.github.flink.gcp.protobuf`; give production types the appropriate Flink API annotation and public/protected APIs Javadoc.
- Use JUnit 5, AssertJ, and handwritten test doubles. Do not add Guava, Mockito, PowerMock, Lombok, or AutoValue.
- Keep TypeInfoFactory registry controls in separate test JVMs. Native integration tests disable generic types and must not rely on `--add-opens`.
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
- Wait for current aggregate `CI passed` and the required review flow before Ready. The restored shared skill retains the original recorded unavailable-reviewer exception; running or uncollected reviews are incomplete. The user merges; clean up only the verified target when requested.

## Optional tools

Use Serena for Java symbols and references, activating the current worktree; use rg for text and configuration.
Use Context7 for version-specific third-party documentation when available, and confirm claims against pinned upstream sources or tests.
Both are optional development tools; builds must not require an MCP server.
Keep shared decisions in tracked documentation, not Serena memories.
