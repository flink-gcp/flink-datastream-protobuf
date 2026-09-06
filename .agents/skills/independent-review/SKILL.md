---
name: independent-review
description: Obtain a diff-only review from a model or human that authored none of the change, after both self-review rounds and before Ready.
license: Apache-2.0
---

# Independent review

Use a second model or human that wrote none of the change or its repairs.
A same-model subagent is not this independent round.
Choose an available authenticated reviewer without publishing personal account configuration into the repository.
From a Codex-authored implementation, an available Claude Code reviewer can perform this pass.

Create a detached review worktree at the pushed commit and retain it until Ready.
Give the reviewer literal full base and head SHAs and ask for `git diff <base>..<head>`.
Do not give a PR number, commit message, description, prior review conclusions, or private memory.
Require read-only work: no files changed, no builds/tests/lints, no commits/pushes or other external writes, and no reading the PR or memory stores.
Ask for severity, exact file:line, a concrete failure scenario, and tracing changed hunks into callers, tests, public contracts, and factual claims.

Collect and read the actual result; a launched but uncollected review is incomplete.
Verify every finding before changing the implementation, and record a no-findings result explicitly when appropriate.
For a narrow repair after the initial full pass, supply `git range-diff <old-base>..<old-head> <new-base>..<new-head>` and limit the pass to affected invariants.
Verify the old objects still exist; use a full pass for expanded scope or contracts.
After review, verify the detached worktree is clean and still at the requested head.

Record reviewer identity, literal reviewed SHAs, examined surfaces, verified findings and their disposition inline on the reviewed diff using the comments array.
If no independent reviewer is available, state the specific reason and retain Draft status until the maintainer chooses a substitute.
Ready requires the completed rounds and a successful current aggregate CI check.
