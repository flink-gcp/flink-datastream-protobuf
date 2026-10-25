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

# Retained large-read evidence

These archives preserve the paired measurements behind the [large-read report](../../serializer-large-read.md), from GCE plan `issue113-read-3`.
They contain measurement data and provenance, with no compiled library, generated Java, or automation scripts.

| Archive | SHA-256 | Bytes |
|---|---|---:|
| [issue113-read-3-protobuf-3-baseline.tar.gz](issue113-read-3-protobuf-3-baseline.tar.gz) | `d26778c990182bce69eb8e661bcd5ccd49bd7ca504a2448bc85a6cbfa398e47d` | 369536 |
| [issue113-read-3-protobuf-3-candidate.tar.gz](issue113-read-3-protobuf-3-candidate.tar.gz) | `28b00fb927ad9f593b06b6089cda874c92a2ec75207bfaf89323b51aae29d3b2` | 370158 |
| [issue113-read-3-protobuf-4-baseline.tar.gz](issue113-read-3-protobuf-4-baseline.tar.gz) | `00ae06c4a4e337cbc8433719b83b7893ae86ccea4dcc22b2051d1f5d5d2af67d` | 368198 |
| [issue113-read-3-protobuf-4-candidate.tar.gz](issue113-read-3-protobuf-4-candidate.tar.gz) | `3707df55e1b90a117fbb10cba7367b5c5253d4ceed353e33fa01ba187a858fae` | 368930 |

Each archive holds one variant's 405 JMH JSON results and logs, the completed inventory, support properties, `manifest.properties`, dependency hashes, `measurements.csv`, and `parity.csv`, under a `protobuf-3/` or `protobuf-4/` prefix.
It also holds the GCE run records: `vm-manifest.txt`, `maintenance-events.log`, the two load-gate logs, the build and console logs, the invocation, the start and end times, `source-base.txt`, and `measurement-source.patch`.
`parity.csv` contains the ADR-0004 native/Chill intervals; the report's variant-to-variant intervals are computed from `measurements.csv` as described in its method.

The results name source commit `ed66955bf8e168759a540882cad661fcc934dc5c` and base `ea2cae62b344c9f53a1edf7be132491d08b10946`.
The source commit was a local commit of the pull-request branch that a later squash replaced; `measurement-source.patch` is the difference from the base to the measured tree, including the variant patch.
Applied to the base with `git apply --exclude='*.tar.gz'`, it reconstructs the measured `src` tree and the text documentation; the measured tree also contained the archives of an earlier plan, `issue113-read-2`, which the patch names only as changed binary files, so plain `git apply` rejects it, and which are not retained.
The baseline tree equals the base under `src`; the candidate tree is the source commit unchanged.
The merged tree equals the candidate tree under `src`; it differs only in documentation: this directory and the report, which now hold the results of this plan.
The VM paths under `/srv/benchmark` contain no operator account names, and no path substitution was applied.
The archives are normalized POSIX ustar in single-stream gzip, with zero owner IDs and timestamps and empty owner names; member contents are the bytes collected from the VMs.

From the repository root, run `just benchmark-evidence-repository <new-output-directory>` to inspect these archives and retain member inventories and checksums.
After human review, verify a hash with `shasum -a 256 <archive>` and extract into a new temporary directory.
