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

# Architecture decisions

| Record | Decision | Status |
|---|---|---|
| [ADR-0001](0001-native-protobuf-type-integration.md) | Native Protobuf type integration for DataStream | Accepted |
| [ADR-0002](0002-build-and-development-workflow.md) | A single Maven module with a shared local and CI workflow | Accepted |
| [ADR-0003](0003-flink-version-compatibility.md) | Current/previous Flink 2.x minors and a separate 1.20 LTS build | Accepted; replaces ADR-0001's runtime scope |

These records distinguish accepted design from implemented behavior.
The native serializer and application-facing type integration are implemented; state snapshots remain pending.
A refinement updates its existing record; a reversal adds a new record and marks the old one as superseded.
