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

package io.github.flink.gcp.protobuf;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * Immutable settings shared by native type integration and its serializer.
 *
 * @param deterministicSerialization whether generated writing uses deterministic mode; this does
 *     not promise canonical bytes or support for message keys
 * @param maxMessageSize maximum payload bytes, excluding the four-byte frame prefix
 * @param recursionLimit maximum nested parser depth, with the root at depth zero
 */
@Internal
record ProtobufSerializerSettings(
        boolean deterministicSerialization, int maxMessageSize, int recursionLimit)
        implements Serializable {

    /**
     * Defaults disable deterministic mode and set a 64 MiB payload limit and depth limit of 100.
     */
    static final ProtobufSerializerSettings DEFAULT =
            new ProtobufSerializerSettings(false, 64 * 1024 * 1024, 100);

    /**
     * Validates positive size and depth limits.
     *
     * @throws IllegalArgumentException if either numeric limit is nonpositive
     */
    ProtobufSerializerSettings {
        if (maxMessageSize <= 0) {
            throw new IllegalArgumentException(
                    "maxMessageSize must be positive: " + maxMessageSize);
        }
        if (recursionLimit <= 0) {
            throw new IllegalArgumentException(
                    "recursionLimit must be positive: " + recursionLimit);
        }
    }
}
