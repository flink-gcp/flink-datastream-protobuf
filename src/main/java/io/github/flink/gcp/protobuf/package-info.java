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
 * <p>The current serializer is internal machinery with package-private construction. Public type
 * information, factory registration, and managed-state snapshots are separate implementation steps;
 * the current package is not a complete release API. Connector boundary serialization schemas and
 * Table/SQL formats are outside this package's scope.
 */
package io.github.flink.gcp.protobuf;
