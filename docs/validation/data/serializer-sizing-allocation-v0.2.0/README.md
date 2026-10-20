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

# Retained sizing-traversal evidence

These archives preserve the paired measurements behind the [sizing-allocation report](../../serializer-sizing-allocation.md), from GCE plans `issue43-sizing-2` and `issue43-sizing-1`.
They contain measurement data and provenance, with no compiled library, generated Java, or automation scripts.

Plan `issue43-sizing-2` compares the final change with the previous sizing:

| Archive | SHA-256 | Bytes |
|---|---|---:|
| [issue43-sizing-2-protobuf-3-baseline.tar.gz](issue43-sizing-2-protobuf-3-baseline.tar.gz) | `121c34bbcbdd421c0bcfd7d202abe6c935746036d4d1da507a593e7471f7fe9a` | 368455 |
| [issue43-sizing-2-protobuf-3-candidate.tar.gz](issue43-sizing-2-protobuf-3-candidate.tar.gz) | `5a2dbe1fc85f61c20d8cca2b0c710707cfc81ef175fb3c42b2daf30ac84f9175` | 375503 |
| [issue43-sizing-2-protobuf-4-baseline.tar.gz](issue43-sizing-2-protobuf-4-baseline.tar.gz) | `d982d3c64b61367dca75048f02d84ec0dcc8194102fd08834cea50b1f68106f5` | 368205 |
| [issue43-sizing-2-protobuf-4-candidate.tar.gz](issue43-sizing-2-protobuf-4-candidate.tar.gz) | `0b8a23839b693581a3ae5341ec9fee8c91a890f19a714e9e2f7cf15ba0121cd5` | 376422 |

Plan `issue43-sizing-1` separates the sources of the first candidate:

| Archive | SHA-256 | Bytes |
|---|---|---:|
| [issue43-sizing-1-protobuf-3-baseline.tar.gz](issue43-sizing-1-protobuf-3-baseline.tar.gz) | `4da313294984bd63c4d0261bebd65be67217b9164e5dbb2ff75bb8dd5984374f` | 358724 |
| [issue43-sizing-1-protobuf-3-candidate.tar.gz](issue43-sizing-1-protobuf-3-candidate.tar.gz) | `93e3b1761e7f8f2d245b62d5bc5e20c15f922752555506f1e20ed71fe54ea74a` | 365629 |
| [issue43-sizing-1-protobuf-3-descriptors.tar.gz](issue43-sizing-1-protobuf-3-descriptors.tar.gz) | `bf7def5c1582e34ff641bf9dc88feaa1b274fc830213b114c546faf69513d361` | 365401 |
| [issue43-sizing-1-protobuf-3-floor.tar.gz](issue43-sizing-1-protobuf-3-floor.tar.gz) | `2225457db6354ffadaa78f9b1b615b121c916c165e26e5850f8cd147a1aefb67` | 364988 |
| [issue43-sizing-1-protobuf-4-baseline.tar.gz](issue43-sizing-1-protobuf-4-baseline.tar.gz) | `30f76ebb35568f95820047668dc90c216ec546e8511e803241645207c5720b00` | 359864 |
| [issue43-sizing-1-protobuf-4-candidate.tar.gz](issue43-sizing-1-protobuf-4-candidate.tar.gz) | `998b30760637d871447a393b6bddeaf8ab60da1abe52f958538b0d29e14c26c8` | 365161 |
| [issue43-sizing-1-protobuf-4-descriptors.tar.gz](issue43-sizing-1-protobuf-4-descriptors.tar.gz) | `78d449ae72e69ba3707b726fc02efc679d8da52dec17d59e5af44e1a717a83b3` | 363777 |
| [issue43-sizing-1-protobuf-4-floor.tar.gz](issue43-sizing-1-protobuf-4-floor.tar.gz) | `d3b4feada1ecf4cfd14bac62b3335844764ec74bbe4d6322cf80a9b2302a770a` | 363674 |

Each archive holds one variant's 405 JMH JSON results and logs, the completed inventory, support properties, `manifest.properties`, dependency hashes, `measurements.csv`, and `parity.csv`, under a `protobuf-3/` or `protobuf-4/` prefix.
It also holds the GCE run records: `vm-manifest.txt`, `maintenance-events.log`, the two load-gate logs, the build and console logs, the invocation, the start and end times, `source-base.txt`, and `measurement-source.patch`.
`parity.csv` contains the ADR-0004 native/Chill intervals; the report's variant-to-variant intervals are computed from `measurements.csv` as described in its method.

The results of plan `issue43-sizing-2` name source commit `1e3a6c1b97e41f520b3ff1d9d1f0ea2db416e2a7` and base `aefae28a69595e623e89b487ee25ff92c1ee7e3a`; those of plan `issue43-sizing-1` name source `dcd061701678409cfb2c137445d5346012557227` and base `eef658f25227a089ea386b0426879d99c5c0b4aa`.
Both source commits were local commits of the pull-request branch and were never pushed; `measurement-source.patch` is the complete difference from the base to the measured tree, including the variant patch, so `git apply` on the base reconstructs that tree after the branch is squashed.
The later source already contained the eight `issue43-sizing-1` archives, which its patch lists only as changed binary files; apply it with `--exclude='docs/validation/data/serializer-sizing-allocation-v0.2.0/*.tar.gz'`, and take those archives from this directory, where they are byte-identical under their plan-prefixed names.
In both plans, the baseline tree equals the base under `src/`; the floor variant is a diagnostic that skips safe sizing.
The merged sizing and write path equal those of the `issue43-sizing-2` candidate tree; the merged tree also contains the later [#95](https://github.com/flink-gcp/flink-datastream-protobuf/pull/95), which changes snapshot restoration, its tests, and Javadoc but not writing, and the sizing tests differ from the candidate's only in comments; the `issue43-sizing-1` candidate lacks the two sizing changes the report names and otherwise differs only in private member names, comments, and tests.
The VM paths under `/srv/benchmark` contain no operator account names, and no path substitution was applied.
The archives are normalized POSIX ustar in single-stream gzip, with zero owner IDs and timestamps and empty owner names; member contents are the bytes collected from the VMs.

From the repository root, run `just benchmark-evidence-repository <new-output-directory>` to inspect these archives and retain member inventories and checksums.
After human review, verify a hash with `shasum -a 256 <archive>` and extract into a new temporary directory.
