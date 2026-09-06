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

# Contributing

For a substantial change, agree on its problem and approach before implementation.
An issue can hold that discussion; do not open an issue solely to mirror an already agreed task without the maintainer's routing decision.
Use [Development](docs/development.md) to prepare the toolchain.

## Pull requests

1. Create a dedicated worktree from `origin/main` under `/tmp/worktrees/flink-datastream-protobuf/`.
2. Implement the agreed change and update the relevant documentation.
3. Run `just format`, the affected tests, and `just lint` when documentation or workflows change.
4. Commit, fetch, and rebase onto `origin/main` before squashing or pushing. Inspect the explicit deletion list against that base.
5. Use `gh pr create --draft` and fill in the repository template's WHAT and WHY sections, including validation and material limits.
6. Run both self-review rounds and an independent review before calling the PR ready. Record reviewed base and head SHAs, findings, and justified deferrals as inline comments on the relevant changed lines. Do not post a standalone review summary.
7. Resolve verified findings and wait for the current PR's aggregate `CI passed` check before marking it Ready. The maintainer merges.

Keep review rounds distinct: the first checks implementation against the stated behavior; the second verifies the claims in the documentation, tests, and description.
The independent reviewer must not have authored the change or its repairs and reviews the diff without the PR description or private agent memories.
The detailed procedures are in the repository's [agent skills](.agents/skills/).
If independent review cannot run, report the reason and retain Draft status until the maintainer chooses a substitute.

Write commit messages, PR text, documentation, code comments, and Javadoc in English.
Preserve third-party license headers and update NOTICE when adapting third-party material.
After the maintainer confirms a merge and requests cleanup, remove only that PR's verified worktree and branch.

## Design records

An ADR records a settled design choice whose rationale would otherwise be lost.
Routine changes following an existing design do not need a new ADR.
Update an existing ADR for a refinement; add a superseding ADR for a reversal.
Keep current user instructions in the README or development documentation, and link them to the relevant decision.
