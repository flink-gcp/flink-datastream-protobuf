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

# Retained 1.1.0 release-candidate benchmark evidence

These archives preserve the measurements behind the [1.1.0 release-candidate benchmark report](../../serializer-benchmarks-v1.1.0.md), from GCE plan `issue213-rc-1`.
They contain measurement data and provenance, with no compiled library, generated Java, or automation scripts.

| Archive | SHA-256 | Bytes |
|---|---|---:|
| [protobuf-3.tar.gz](protobuf-3.tar.gz) | `3cde30f1b768e9f45a720513d30cad86e279371ca0c111e988cc4c4558b82532` | 1581916 |
| [protobuf-4.tar.gz](protobuf-4.tar.gz) | `3db83e4490f7467b23a7cde0cf994d3d1286fa419c71b2e790e1eff16e82aabd` | 1586050 |

Each archive holds one profile's 2,005 JMH JSON results and logs, the completed inventory, support properties, `manifest.properties`, dependency hashes, `measurements.csv`, `parity.csv`, and `dynamic.csv`, under a `protobuf-3/` or `protobuf-4/` prefix.
It also holds the GCE run records: `vm-manifest.txt`, `maintenance-events.log`, the two load-gate logs, the build and console logs, the invocation, the start and end times, `source-base.txt`, and `measurement-source.patch`.
`parity.csv` contains the ADR-0004 native/Chill intervals and `dynamic.csv` the descriptive DynamicMessage ratios.
Each JMH JSON result records its fork's JVM arguments, including the pinned compilation flags of the Protobuf 3 text write cells.

The results name source commit and base `85657151ecb69a7a8c44ffae651d8150aef99dcf`, the merge of pull request #216 on `main`, so `measurement-source.patch` is empty and the measured tree is that commit unchanged.
The first line of `classpath-sha256.txt` records the release-configuration library jar that both profiles measured.
Collection used a plan regenerated from the same specification after the original plan directory was lost; the run records above are the VMs' own and do not depend on the plan.
The VM paths under `/srv/benchmark` contain no operator account names, and no path substitution was applied.
The archives are normalized POSIX ustar in single-stream gzip, with zero owner IDs and timestamps and empty owner names; member contents are the bytes collected from the VMs, which the evidence gate checked after collection.

From the repository root, run `just benchmark-evidence-repository <new-output-directory>` to inspect these archives and retain member inventories and checksums.
After human review, verify a hash with `shasum -a 256 <archive>` and extract into a new temporary directory.
