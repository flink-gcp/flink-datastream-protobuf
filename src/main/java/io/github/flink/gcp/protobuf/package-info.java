/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Native Protocol Buffers serialization for Flink DataStream generated full-runtime messages.
 *
 * <p>Use {@link io.github.flink.gcp.protobuf.ProtobufTypeInformation} for explicit construction and
 * per-type settings, or register {@link io.github.flink.gcp.protobuf.ProtobufTypeInfoFactory}
 * against AbstractMessage before type extraction. The factory registry is process-global. Disable
 * generic types to reject accidental fallback and extract stable scalar keys instead of message
 * keys. Versioned snapshots support checkpoint/savepoint recovery with unchanged schemas and
 * supported schema changes (field additions, field removals that reserve the number, and enum value
 * additions), verified with HashMap and RocksDB state backends in MiniCluster tests. Unsupported
 * schema changes and decreased reader limits fail at restore. No Maven Central release is available
 * yet. Connector boundary serialization schemas and Table/SQL formats are outside this package.
 */
package io.github.flink.gcp.protobuf;
