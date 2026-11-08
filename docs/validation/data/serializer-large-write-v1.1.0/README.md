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

# Retained large-write evidence

These archives preserve the paired measurements behind the [large-write report](../../serializer-large-write.md), from GCE plan `issue176-write-1`.
They contain measurement data and provenance, with no compiled library, generated Java, or automation scripts.

| Archive | SHA-256 | Bytes |
|---|---|---:|
| [issue176-write-1-protobuf-3-baseline.tar.gz](issue176-write-1-protobuf-3-baseline.tar.gz) | `90a3ec1ce3142eed626f560c071406063575de067c27cc30a53167ce064b916c` | 354753 |
| [issue176-write-1-protobuf-3-candidate.tar.gz](issue176-write-1-protobuf-3-candidate.tar.gz) | `d8338ec6cce97cb46b0913dc1bc1cce39b6d71d13a061ccd066d18dadc0cf0d8` | 353747 |
| [issue176-write-1-protobuf-4-baseline.tar.gz](issue176-write-1-protobuf-4-baseline.tar.gz) | `2e3e0fcf5e3fa2edb9b49d5e4507d1c6cdfd628cb4c9f7a07a4025e24547430a` | 354322 |
| [issue176-write-1-protobuf-4-candidate.tar.gz](issue176-write-1-protobuf-4-candidate.tar.gz) | `f5011ca129d51816c2fc3b5730831570413f9fc9ecbcfcf49be7bed42b41b497` | 352616 |

Each archive holds one variant's JMH JSON results and logs for the native and Chill Protobuf lanes, the plain-run logs and properties of the other lanes, the completed inventory, `manifest.properties`, dependency hashes, `measurements.csv`, and `parity.csv`, under a `protobuf-3/` or `protobuf-4/` prefix.
It also holds the GCE run records: `vm-manifest.txt`, `maintenance-events.log`, the two load-gate logs, the build and console logs, the invocation, the start and end times, `source-base.txt`, and `measurement-source.patch`.
`parity.csv` contains the ADR-0004 native/Chill intervals; the report's variant-to-variant intervals are computed from the `deterministic=false` rows of `measurements.csv` as described in its method.

The results name source commit `7c84fa0df9d65dfaf61d8c768084239ff3720f0f` and base `fc64b421e4a9485c4179b1577fd5f7bb16fc2f31`.
`measurement-source.patch` is the difference from the base to the measured tree, including the variant patch; applied to the base with `git apply`, it reconstructs that tree.
The baseline tree equals the base under `src` and differs from it only in ADR-0001; the candidate tree is the source commit unchanged.
The merged tree equals the candidate tree under `src/main`, and under `src/test` it adds one test of a large frame written after a target failure. Outside `src`, it adds this directory, the report, and the guide entries that name them, and it contains the `main` commits merged after the base, including build-plugin updates in `pom.xml`, which the measured builds did not use.
The VM paths under `/srv/benchmark` contain no operator account names, and no path substitution was applied.
The archives are normalized POSIX ustar in single-stream gzip, with zero owner IDs and timestamps and empty owner names; member contents are the bytes collected from the VMs.

From the repository root, run `just benchmark-evidence-repository <new-output-directory>` to inspect these archives and retain member inventories and checksums.
After human review, verify a hash with `shasum -a 256 <archive>` and extract into a new temporary directory.
