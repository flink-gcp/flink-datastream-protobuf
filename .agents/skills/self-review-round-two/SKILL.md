---
name: self-review-round-two
description: Audit the factual claims in a change after round one, including user-facing documentation, dependency behavior, and the scope of test evidence.
license: Apache-2.0
---

# Self-review round two

Freeze this round's base/head SHAs and changed/published-surface inventory before the first audit.
List the factual claims in the PR description, changed docs and comments, commit message, and any restated task premise.
Check them against the pinned sources and actual test observations, rather than only the diff.

Use distinct outward lenses: the user following the instructions; the operator diagnosing failure; affected callers and deployment contexts; an adversarial boundary case; and a documentation reader with no conversation context.
Pay deferred measurements and distinguish a compiled configuration, a successful transport, and a verified state restore.
Search sibling and unchanged prose for claims invalidated by the change.
For small wording changes, a claim audit and surrounding-prose scan suffice; build or framework claims require the full pass.

Verify each finding, repair it, and rerun affected checks.
After the full initial audit, narrow repairs may use the same base-to-head range-diff procedure and object checks as round one.
Record reviewed SHAs, inventory, corrected claims, measurements, and justified deferrals inline using the review API comments array.
Verify current mergeability before treating prior CI as current evidence, and run independent review next.
