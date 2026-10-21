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

# Retained placement-calibration evidence

These archives preserve the paired measurements behind the [placement-calibration report](../../gce-placement-calibration.md), from GCE plan `issue109-calibration-2`.
They contain measurement data and provenance, with no compiled library, generated Java, or automation scripts.

Each archive is one run of the unchanged source; the letter in its name is the VM group and the digit the run's position on that VM:

| Archive | SHA-256 | Bytes |
|---|---|---:|
| [issue109-calibration-2-protobuf-3-a1.tar.gz](issue109-calibration-2-protobuf-3-a1.tar.gz) | `fe101de9ed7c240a025f7ad9424c9edfe88e1c34b6da7a4504bd364f81f940b6` | 356389 |
| [issue109-calibration-2-protobuf-3-a2.tar.gz](issue109-calibration-2-protobuf-3-a2.tar.gz) | `f90018ea54716d8d4859894cd3e735f914676f8b4711d9278855a7754a0f4863` | 350815 |
| [issue109-calibration-2-protobuf-3-b1.tar.gz](issue109-calibration-2-protobuf-3-b1.tar.gz) | `23a854b0b7edbd7c9d32bde0fdba2377b30bf24cf799df0257f0b9e39ddd5b3a` | 355001 |
| [issue109-calibration-2-protobuf-3-b2.tar.gz](issue109-calibration-2-protobuf-3-b2.tar.gz) | `ace6174d842b55a713ffc33dc74ec499f88de36935d643a54104a3c263d8dc97` | 350823 |
| [issue109-calibration-2-protobuf-3-c1.tar.gz](issue109-calibration-2-protobuf-3-c1.tar.gz) | `6a242bf63c827acc7705cd75503ea14931acb3711f7b2cfe92a18ea297b572ba` | 357417 |
| [issue109-calibration-2-protobuf-3-c2.tar.gz](issue109-calibration-2-protobuf-3-c2.tar.gz) | `4674e84f64c3bed84229d321746d990d066ce6250d243ed7f42cd5598cbdae86` | 351711 |
| [issue109-calibration-2-protobuf-4-a1.tar.gz](issue109-calibration-2-protobuf-4-a1.tar.gz) | `f3cc863ec0c1bbe28aba1417bc1ba74afad72e93c0d4fd5f060ad0acc6fd6e1a` | 356332 |
| [issue109-calibration-2-protobuf-4-a2.tar.gz](issue109-calibration-2-protobuf-4-a2.tar.gz) | `edcab2e3371a08b26c1ffeeb20438a77c756159ac0b819bf540b1a1f5847fba1` | 351842 |
| [issue109-calibration-2-protobuf-4-b1.tar.gz](issue109-calibration-2-protobuf-4-b1.tar.gz) | `3acddabf2fd8d727fb12cc1a30aaf93a36e8a4af8ca68349f1ae2aabd56c3170` | 354436 |
| [issue109-calibration-2-protobuf-4-b2.tar.gz](issue109-calibration-2-protobuf-4-b2.tar.gz) | `fb7db26ad7e7a4a192f247d916c83b028f07d4e281c21500c8e9f44e98fa5b91` | 350291 |
| [issue109-calibration-2-protobuf-4-c1.tar.gz](issue109-calibration-2-protobuf-4-c1.tar.gz) | `ee25e8e27cc6fea1d2bce558fffbdbc41f2cb069dd78738348360c521e3b98e3` | 354900 |
| [issue109-calibration-2-protobuf-4-c2.tar.gz](issue109-calibration-2-protobuf-4-c2.tar.gz) | `8566cb6951962d49a9cbd1f2ede0df54ff577302fbe2a0af23f21e866c7c1a6d` | 351429 |

Each archive holds one run's 405 JMH JSON results and logs, the completed inventory, support properties, `manifest.properties`, dependency hashes, `measurements.csv`, and `parity.csv`, under a `protobuf-3/` or `protobuf-4/` prefix.
It also holds the GCE run records: `vm-manifest.txt`, `maintenance-events.log`, the two load-gate logs, the build and console logs, the invocation, the start and end times, `source-base.txt`, and `measurement-source.patch`.
`vm-manifest.txt` names the instance in `gce.instance`, which identifies the VM shared by the two runs of a group.
`parity.csv` contains the ADR-0004 native/Chill intervals; the report's run-to-run intervals are computed from `measurements.csv` as described in its method.

Every result names source and base commit `844fd45e700040dbe036acea9f98f95922897d3e`, the merge of [#108](https://github.com/flink-gcp/flink-datastream-protobuf/pull/108) on `main`, and its `measurement-source.patch` is empty, so the measured tree is that commit.
The runner hash `c7075a51a5c3cb63e33a229c285110bcddcd2a94c312cab69aeb2ddd18019682` in each VM manifest is the SHA-256 of `scripts/gce-benchmark-vm.sh` as changed by the pull request that retains these archives; the measured tree's own copy of the runner is older and was not used.
The VM paths under `/srv/benchmark` contain no operator account names, and no path substitution was applied.
The archives are normalized POSIX ustar in single-stream gzip, with zero owner IDs and timestamps and empty owner names; member contents are the bytes collected from the VMs.

From the repository root, run `just benchmark-evidence-repository <new-output-directory>` to inspect these archives and retain member inventories and checksums.
After human review, verify a hash with `shasum -a 256 <archive>` and extract into a new temporary directory.
