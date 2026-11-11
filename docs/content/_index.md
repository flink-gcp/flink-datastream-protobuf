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

Maven Central publishes `1.1.0` for Flink 2.x and `1.1.0-1.20` for Flink 1.20 LTS; Development follows `main`.

Start with the [quickstart]({{< relref "docs/quickstart" >}}), then read [usage and configuration]({{< relref "docs/usage" >}}).
The [compatibility guide]({{< relref "docs/compatibility" >}}) distinguishes wire compatibility from saved-state compatibility.
[Supported types]({{< relref "docs/common-types" >}}) covers Google Well-Known Types and OpenTelemetry messages.

The menu's API reference describes the same source revision as the selected documentation.
See [documentation versions]({{< relref "docs/versions" >}}) for retention and source provenance, and [development]({{< param BookRepo >}}/blob/main/docs/development.md) for local verification.
