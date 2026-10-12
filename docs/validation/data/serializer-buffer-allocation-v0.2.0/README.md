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

# Retained buffer-allocation evidence

These archives preserve the paired measurements behind the [buffer-allocation report](../../serializer-buffer-allocation.md), from GCE plan `issue42-ablation-2`.
They contain measurement data and provenance, with no compiled library, generated Java, or automation scripts.

| Archive | SHA-256 | Bytes |
|---|---|---:|
| [protobuf-3-baseline.tar.gz](protobuf-3-baseline.tar.gz) | `6a402c60db3ac3f5c259eef4fdd34d669a2679f6e4dd114a9fde594583e8f13e` | 377110 |
| [protobuf-3-candidate.tar.gz](protobuf-3-candidate.tar.gz) | `48ba545f92416f226b8ae12d11e1774fbe8e9e4b13c325fab3378895dc081223` | 381901 |
| [protobuf-4-baseline.tar.gz](protobuf-4-baseline.tar.gz) | `f8df11d7cd6fa2f3800750da0660c49a58942ae0d1b23c1ab5cce728c42b3987` | 376702 |
| [protobuf-4-candidate.tar.gz](protobuf-4-candidate.tar.gz) | `d16e5ad53d7ccb84e16e565b2caba17c410b98a1befc253221c144ced2e0b63c` | 381426 |
| [protobuf-4-input.tar.gz](protobuf-4-input.tar.gz) | `d05e8c574df595bf9b28006cddfd121f98574d994bd9a93e9f6ed705ff2fadd2` | 381317 |
| [protobuf-4-output.tar.gz](protobuf-4-output.tar.gz) | `93cccd1fc0e8f5207dd10c6242ab4409c0e152793061a512816a6971b981764a` | 380934 |

Each archive holds one variant's 405 JMH JSON results and logs, the completed inventory, support properties, `manifest.properties`, dependency hashes, `measurements.csv`, and `parity.csv`, under a `protobuf-3/` or `protobuf-4/` prefix.
It also holds the GCE run records: `vm-manifest.txt`, `maintenance-events.log`, the two load-gate logs, the build and console logs, the invocation, the start and end times, `source-base.txt`, and `measurement-source.patch`.
`parity.csv` contains the ADR-0004 native/Chill intervals; the report's variant-versus-baseline intervals are computed from `measurements.csv` as described in its method.

Every result names source commit `2b9d952880df15d45ad6e85178f0fc647093924d` and base `93f96bfa88cd4f48ef3e018fa278b884cc606fe2`.
The source commit belongs to an unmerged pull-request branch; `measurement-source.patch` is the complete difference from the base to the measured tree, including the variant patch, so `git apply` on the base reconstructs that tree after the branch is squashed.
The merged serializer differs from that tree only on its inconsistent-writer failure path and in comments, as the report describes.
The VM paths under `/srv/benchmark` contain no operator account names, and no path substitution was applied.
The archives are normalized POSIX ustar in single-stream gzip, with zero owner IDs and timestamps and empty owner names; member contents are the bytes collected from the VMs.

From the repository root, run `just benchmark-evidence-repository <new-output-directory>` to inspect these archives and retain member inventories and checksums.
After human review, verify a hash with `shasum -a 256 <archive>` and extract into a new temporary directory.
