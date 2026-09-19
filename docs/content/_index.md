---
title: Overview
---
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

# Protobuf for Apache Flink DataStream

Native Protocol Buffers type integration for the Apache Flink DataStream API.
Use generated Protobuf messages with explicit type information or factory registration, with generic types disabled.

This is **{{< param DocsVersion >}}** documentation.
Development follows `main`; 0.x milestones are unpublished and their APIs and state formats may change.
The first Maven Central release is planned for 1.0.0.

Start with the [quickstart](docs/quickstart.md), then read [usage and configuration](docs/usage.md).
The [compatibility guide](docs/compatibility.md) distinguishes wire compatibility from saved-state compatibility.
[Supported types](docs/common-types.md) covers Google Well-Known Types and OpenTelemetry messages.

The menu's API reference describes the same source revision as the selected documentation.
See [documentation versions](docs/versions.md) for retention and source provenance, and [development](docs/development.md) for local verification.
