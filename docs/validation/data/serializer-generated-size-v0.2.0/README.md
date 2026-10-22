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

# Retained generated-size evidence

These archives preserve the paired measurements behind the [generated-size report](../../serializer-generated-size.md), from GCE plan `issue112-sizing-2`.
They contain measurement data and provenance, with no compiled library, generated Java, or automation scripts.

| Archive | SHA-256 | Bytes |
|---|---|---:|
| [issue112-sizing-2-protobuf-3-baseline.tar.gz](issue112-sizing-2-protobuf-3-baseline.tar.gz) | `a3ec28d3d21bbe8f705ec2c37366802477a3c9d0786f7ffa12f01c021363039c` | 363674 |
| [issue112-sizing-2-protobuf-3-candidate.tar.gz](issue112-sizing-2-protobuf-3-candidate.tar.gz) | `cfa9eceadf52b70d4a42afaff30d76e9b85ed57586d2daacb663454964e73dc0` | 368521 |
| [issue112-sizing-2-protobuf-4-baseline.tar.gz](issue112-sizing-2-protobuf-4-baseline.tar.gz) | `494fedd53444548fcc5a0d96fad7d6ea9f15023258dc3bc7b3104b112216c018` | 363850 |
| [issue112-sizing-2-protobuf-4-candidate.tar.gz](issue112-sizing-2-protobuf-4-candidate.tar.gz) | `2dcdd380b934df06deb1be439011da30527978b513732748890744935df7ac01` | 367434 |

Each archive holds one variant's 405 JMH JSON results and logs, the completed inventory, support properties, `manifest.properties`, dependency hashes, `measurements.csv`, and `parity.csv`, under a `protobuf-3/` or `protobuf-4/` prefix.
It also holds the GCE run records: `vm-manifest.txt`, `maintenance-events.log`, the two load-gate logs, the build and console logs, the invocation, the start and end times, `source-base.txt`, and `measurement-source.patch`.
`parity.csv` contains the ADR-0004 native/Chill intervals; the report's variant-to-variant intervals are computed from `measurements.csv` as described in its method.

The results name source commit `ee63f1c8da68cdcad4c961afb71b7754acfbbd3d` and base `0c40c0bf5975964f302038e2a7debd709e761a38`.
The source commit was a pushed head of the pull-request branch that a later squash replaced; `measurement-source.patch` is the complete difference from the base to the measured tree, including the variant patch, so `git apply` on the base reconstructs that tree.
The baseline tree equals the base under `src/main` and lacks the generated-size test class; the candidate tree is the source commit unchanged.
The merged tree equals the candidate tree except for documentation: the report, this directory, and the links to them from the development guide and the sizing-allocation report.
The VM paths under `/srv/benchmark` contain no operator account names, and no path substitution was applied.
The archives are normalized POSIX ustar in single-stream gzip, with zero owner IDs and timestamps and empty owner names; member contents are the bytes collected from the VMs.

From the repository root, run `just benchmark-evidence-repository <new-output-directory>` to inspect these archives and retain member inventories and checksums.
After human review, verify a hash with `shasum -a 256 <archive>` and extract into a new temporary directory.
