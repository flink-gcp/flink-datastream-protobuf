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

# Retained benchmark evidence

These archives preserve the 0.1.0 development-milestone measurements from source commit `f1fdd36137b8baa59202652da83a7a8a94d98c3f`.
They contain measurement data and provenance, with no compiled library, generated Java, or automation scripts.
The milestone is not a Maven Central release.
See the [report](../../serializer-benchmarks-v0.1.0.md) for interpretation, exclusions, and limitations.

| Archive | SHA-256 | Bytes |
|---|---|---:|
| [protobuf-3.tar.gz](protobuf-3.tar.gz) | `b280109b2bf4875b69bfa6414eb1c937cb2bb9e7da46893eb279a06154021c20` | 1370761 |
| [protobuf-4.tar.gz](protobuf-4.tar.gz) | `2d80c1953ca50d769d6239dbe2958c92ea40a498d288eb5cb3e0ebd531f1b082` | 1374674 |
| [diagnostics-protobuf-4.tar.gz](diagnostics-protobuf-4.tar.gz) | `b401969142fd8cbde54a408d1a98c61a0f37699248214e7d1efb9d6b21755b42` | 1228981 |

Each primary archive contains 1,605 JMH JSON results and their logs, the completed inventory, support and encoded-size properties, dependency hashes, configuration/source provenance, power/load observations, and CSV summaries.
`measurements.csv` contains per-fork values; `parity.csv` contains all 40 native/Chill intervals for that profile; `summary.csv` adds arithmetic five-fork means, allocation, and corpus encoded sizes for all 320 primary cells.
The five deterministic collection-serialization results are a separate supplement and do not enter `parity.csv` or `summary.csv`.
The retained `measurement-source.patch` and `source-base.txt` permit reconstruction after the PR branch is squashed.
Personal home-directory prefixes in retained evidence are replaced with `/benchmark-home`.
Toolchain versions and relative path suffixes remain intact; these paths are provenance rather than portable invocation paths.
Archive owner names and numeric IDs are cleared, and archive timestamps are normalized.
These publication-only transformations leave all measurement values, event records apart from matching path strings, source patches, and result inventories unchanged.

From the repository root, run `just benchmark-evidence-repository <new-output-directory>` to inspect these committed archives and retain member inventories and checksums.
This reads the archives without extracting their paths or changing their bytes.
After checking the inventory and completing human review, verify a hash with `shasum -a 256 protobuf-3.tar.gz`, then extract into a temporary directory:

```sh
mkdir -p /tmp/flink-protobuf-baseline-evidence
tar -xzf protobuf-3.tar.gz -C /tmp/flink-protobuf-baseline-evidence
tar -xzf protobuf-4.tar.gz -C /tmp/flink-protobuf-baseline-evidence
```

The diagnostic archive contains allocation/execution event JSON exported from five separate Protobuf 4 JFR recordings, JMH JSON/logs, recorded invocation arguments with the home-prefix replacement described above, and a compact stack summary.
Those runs use three one-second warmups and three one-second measurements with one fork and both GC and JFR profilers.
They are excluded from the 3,210-result primary/supplement inventory and from all baseline tables.
The stack summary filters `jdk.ObjectAllocationSample` and `jdk.ExecutionSample` events to JMH worker threads, groups allocation sample weights by object class and the first four stack frames, and retains the twelve largest groups and execution-sample leaf counts per case.
JFR sampling weights are not exact per-operation allocation totals; use the baseline GC metrics for those totals.
Only `jdk.ObjectAllocationSample` and `jdk.ExecutionSample` events are retained in the exported JSON, including their original stack traces.
Full JFR recordings are excluded because they capture unrelated host environment metadata, which can include credentials.
Inspect truncated stacks and sampling limitations in the retained event JSON.
The [diagnostic evidence procedure](../../../development.md#retaining-diagnostic-evidence) defines the review required before retaining or publishing another archive.
