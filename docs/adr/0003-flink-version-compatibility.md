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

# ADR-0003: Support current/previous Flink 2.x minors and Flink 1.20 LTS

- Status: Accepted
- Date: 2026-09-10
- Supersedes: The Flink 2.3-only runtime scope in [ADR-0001](0001-native-protobuf-type-integration.md)
- Amends: The build matrix in [ADR-0002](0002-build-and-development-workflow.md)
- Current instructions: [Development](../development.md)

## Context

The maintainer wants Flink 1.20 and 2.x support under the same policy as flink-connector-gcp.
Compiling separately on each runtime establishes source compatibility but cannot prove that a released jar works on both supported 2.x minors.
The 1.20 TypeInformation API still requires `createSerializer(ExecutionConfig)`, while Flink 2.x removed it.
The existing test factory therefore needs a small adapter, even though the production serializer currently compiles unchanged.
Flink 1.20 also does not infer native list element information from the probe's TypeHint.

## Decision

### Supported runtimes and artifacts

Support the current and immediately previous Flink 2.x minors with one jar compiled against the older minor, plus Flink 1.20 LTS through a separate build from the same source tree.
The initial supported 2.x range is 2.2/2.3, pinned to 2.2.1/2.3.0; LTS is pinned to 1.20.4.
Use JDK 17 and 21 for Flink 2.x, JDK 17 for 1.20, and Java release 17 for all output.
Java 11 is unsupported.

Use Maven coordinates `io.github.flink-gcp:flink-datastream-protobuf` for both lines.
The bare release version `X.Y.Z` is built at the 2.x floor, with no Flink-minor suffix.
The version `X.Y.Z-1.20` is compiled against Flink 1.20 with `flink.compat=flink1`.
There is no cross-major binary compatibility guarantee.
The 0.1.0, 0.2.0, and 0.3.0 capability milestones and 0.x breaking-change policy in ADR-0001 apply to both lines; this decision does not advance the release number or publish an artifact.

Keep one Maven module and shared sources.
Use `flink.compat=flink2` by default and select alternate roots only for actual API differences.
Production TypeInformation uses a package-private adapter for the legacy 1.20 serializer entry point.
The probe retains its own test-only adapters and is not part of the public API.
Clean when switching Flink or Protobuf profiles.
Validate that the selected Flink version belongs to the adapter's major, with the 1.x adapter restricted to 1.20.

### Evidence and CI

Require full clean verification on every supported Flink/JDK combination with each paired Protobuf runtime and gencode profile.
Also build once at the 2.x floor and rerun all tests at the ceiling without recompiling production or test classes.
The rerun must load the packaged floor jar, verify the actual Flink runtime, compare the complete class/name test inventory, reject skips or failures, and verify unchanged jar and class hashes.
Run this binary check separately for each Protobuf profile on JDK 17.
The source-build matrix covers JDK 21; it does not substitute for the binary check.

This single-module library runs the supported matrix and binary checks on every PR as well as main pushes, manual runs, and a weekly schedule.
This is more frequent than the connector's weekly compatibility lanes while preserving its support contract.
The scheduled run checks the pinned supported versions; it is not automatic discovery of new Flink releases or a next-SNAPSHOT early-warning lane.
Before moving the supported window, explicitly review the new release, advance the floor and ceiling together, update documentation, and pass the matrix and binary checks.
Dependabot must not independently advance Flink minor or major versions; patch updates remain reviewable.
Support for a newly released minor starts with that reviewed range update, not merely its upstream publication.

The tests cover the production serializer, TypeInformation and factory selection, and native transport, alongside a separate test-only factory/transport serializer.
They do not establish checkpoint/savepoint recovery.
All supported runtimes must retain native top-level, POJO, tuple, Row, and explicit List transport with generic types disabled.
The inferred List control must retain its version-specific result: native element information on 2.x, generic fallback and serializer rejection on 1.20.
Users on 1.20 must supply explicit list element TypeInformation.

### State and release acceptance

Issues #9, #10, and #11 must apply their production integration and unchanged-schema restore acceptance to each supported runtime and artifact line.
The remaining release work in #13 must publish and verify both version lines, retain per-line published-artifact provenance, and keep released-state fixtures for each supported runtime.
Before publication, guard the effective Maven model: bare versions must use the exact recorded 2.x floor and `flink2`; `-1.20` versions must use the pinned 1.20 toolchain and `flink1`.
Checking only command-line substrings is insufficient because Maven properties can be overridden.
Both staged artifacts must validate before either is published; publish both before announcing the GitHub release.
The two publications are not transactional, so the future release workflow must record partial failures and recovery instructions.
These are requirements for #13, not implemented publication automation.

Source and binary compatibility do not imply saved-state compatibility across Flink versions.
Library-only upgrade fixtures keep the Flink version fixed and use the matching artifact line.
Cross-Flink-minor or cross-major savepoint upgrades require a separately declared path and fixtures proving restored values and continued processing; no such path is claimed here.
The existing intermediate `snapshotConfiguration()` failure must be replaced and all claimed restore paths verified before 0.1.0 publication.

## Source basis and alternatives

The policy follows flink-connector-gcp at commit `40967023bcd27adbd81b7c659207d6fe791dbdef`:

- [ADR-0053: One artifact covers current and previous Flink minors](https://github.com/flink-gcp/flink-connector-gcp/blob/40967023bcd27adbd81b7c659207d6fe791dbdef/docs/adr/0053-one-artifact-covers-the-current-and-previous-flink-minor.md).
- [ADR-0054: Flink 1.20 from the same source tree](https://github.com/flink-gcp/flink-connector-gcp/blob/40967023bcd27adbd81b7c659207d6fe791dbdef/docs/adr/0054-flink-1-20-is-supported-from-the-same-source-tree-at-source-level.md).
- [ADR-0147: Two version lines and publication validation](https://github.com/flink-gcp/flink-connector-gcp/blob/40967023bcd27adbd81b7c659207d6fe791dbdef/docs/adr/0147-releases-stage-on-the-central-portal-and-publish-by-hand.md).

Separate branches or modules would duplicate the shared serializer before there is enough incompatible code to justify them.
A single jar promised across Flink majors would claim more than the source-build checks establish.
Compiling only against the newest 2.x minor would permit accidental use of APIs absent from the older supported runtime.
