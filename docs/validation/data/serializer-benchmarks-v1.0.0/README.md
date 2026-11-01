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

# Retained 1.0.0 release-candidate benchmark evidence

These archives preserve the measurements behind the [1.0.0 release-candidate benchmark report](../../serializer-benchmarks-v1.0.0.md), from GCE plan `issue45-rc-1`.
They contain measurement data and provenance, with no compiled library, generated Java, or automation scripts.

| Archive | SHA-256 | Bytes |
|---|---|---:|
| [protobuf-3.tar.gz](protobuf-3.tar.gz) | `d32bec76970b28e5425eb73acc0f75e67114f9ff328ba4f445d863ab86e51645` | 1582614 |
| [protobuf-4.tar.gz](protobuf-4.tar.gz) | `5746039483dda14491966920e802b3e33ec5776206bde66b2a0605e2c6a14ede` | 1586456 |

Each archive holds one profile's 2,005 JMH JSON results and logs, the completed inventory, support properties, `manifest.properties`, dependency hashes, `measurements.csv`, `parity.csv`, and `dynamic.csv`, under a `protobuf-3/` or `protobuf-4/` prefix.
It also holds the GCE run records: `vm-manifest.txt`, `maintenance-events.log`, the two load-gate logs, the build and console logs, the invocation, the start and end times, `source-base.txt`, and `measurement-source.patch`.
`parity.csv` contains the ADR-0004 native/Chill intervals and `dynamic.csv` the descriptive DynamicMessage ratios.

The results name source commit and base `9e7d175341fc0e418a97f199168d63c1a0f8cb33`, the merge of pull request #173 on `main`, so `measurement-source.patch` is empty and the measured tree is that commit unchanged.
`classpath-sha256.txt` records the measured library jar of each profile; the report explains how those jars relate to the release configuration.
The VM paths under `/srv/benchmark` contain no operator account names, and no path substitution was applied.
The archives are normalized POSIX ustar in single-stream gzip, with zero owner IDs and timestamps and empty owner names; member contents are the bytes collected from the VMs, which the evidence gate checked after collection.

From the repository root, run `just benchmark-evidence-repository <new-output-directory>` to inspect these archives and retain member inventories and checksums.
After human review, verify a hash with `shasum -a 256 <archive>` and extract into a new temporary directory.
