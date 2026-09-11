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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.protobuf.Empty;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProtobufLegacySerializerTest {
    @Test
    @SuppressWarnings("deprecation")
    void legacyAndCurrentSerializerEntryPointsUseTheSameNativeSettings() {
        var config = new ExecutionConfig();
        var info =
                ProtobufTypeInformation.newBuilder(Empty.class)
                        .deterministicSerialization(true)
                        .maxMessageSize(1)
                        .recursionLimit(2)
                        .build();
        TypeInformation<Empty> throughFlink = info;
        var expected =
                new ProtobufTypeSerializer<>(
                        Empty.class, new ProtobufSerializerSettings(true, 1, 2));
        assertThat(info.createSerializer(config)).isEqualTo(expected);
        assertThat(throughFlink.createSerializer(config)).isEqualTo(expected);
        assertThat(info.createSerializer(config.getSerializerConfig())).isEqualTo(expected);
    }
}
