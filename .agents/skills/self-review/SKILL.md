---
name: self-review
description: Run the first review after creating a Draft PR, checking implementation against its stated behavior with separate correctness, API, and test-quality lenses.
license: Apache-2.0
---

# Self-review round one

Read the PR's specification, resolve its actual base and pushed head to full SHAs, and freeze a changed-surface inventory covering behavior, public contracts, tests, build rules, and factual claims.
Review `git diff <base>..<head>` with three distinct lenses: correctness and failure paths; API and simplification; test quality and flakiness.
Use separate parallel subagents when available, each limited to its lens and asked for file:line and a concrete failure scenario; otherwise perform the passes sequentially and record that fact.
Do not combine the lenses and report them as independent passes.

Verify each finding against the changed code and pinned dependency before acting; agreement between reviewers is not additional evidence.
Apply verified correctness and simplification fixes and rerun the affected checks.
Commit before any mutation-testing batch and require named failing tests, not compilation or formatting errors, as evidence of a killed mutant.
Mutation testing is for behavior the change actually introduces, not a requirement to manufacture mutations for a documentation-only change.

After a completed full pass, a narrow fix uses `git range-diff <old-base>..<old-head> <new-base>..<new-head>` and the affected inventory.
Verify that previous objects exist before using bounded mode; expand to the full pass if scope or contracts changed.
Record base/head, inventory coverage, findings, checks and justified deferrals inline on the relevant changed lines, using an empty review body with a comments array.
If there are no defects, say so in an inline record on the reviewed surface.
Route out-of-scope findings with the user and then run round two.
