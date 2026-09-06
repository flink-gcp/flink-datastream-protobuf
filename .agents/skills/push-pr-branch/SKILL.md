---
name: push-pr-branch
description: Push a PR branch after rebasing and checking the complete patch, without reverting newer main changes. Use before each PR-branch push or squash.
license: Apache-2.0
---

# Push a PR branch

Commit this task's local changes before rebasing.
Fetch origin, rebase onto origin/main, and stop if rebase fails.
Only then squash this PR's commits if needed; never reset softly onto a base the branch has not incorporated.
Before pushing, require the base to be an ancestor and inspect `git diff --diff-filter=D --name-only origin/main..HEAD`, the diffstat, and the full patch.
Every deletion must belong to the intended change.
Use an ordinary initial push and force-with-lease only for an intentional rewrite of this PR's branch.

If main has moved, record old/new base and head SHAs and compare the patch series with range-diff.
A proven unchanged, unaffected base refresh retains completed reviews; conflicts need checks for their resolved paths.
Authored fixes use the bounded repair procedure in the review skills after the round's initial full pass.
An expanded contract or scope needs a new full review.
If an issue was assigned, verify its closing reference through gh rather than guessing from the body text.
