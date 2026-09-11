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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.protobuf.generated.ScalarMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufExplicitITCase {
    @Test
    @Timeout(120)
    void explicitReturnsTransportsNativeValuesWithoutRegisteringAFactory() throws Exception {
        var config = new Configuration();
        config.set(PipelineOptions.GENERIC_TYPES, false);
        try (var env = StreamExecutionEnvironment.createLocalEnvironment(2, config)) {
            env.disableOperatorChaining();
            var serializerConfig = env.getConfig().getSerializerConfig();
            assertThatThrownBy(
                            () ->
                                    TypeInformation.of(ScalarMessage.class)
                                            .createSerializer(serializerConfig))
                    .isInstanceOf(UnsupportedOperationException.class);
            var info =
                    ProtobufTypeInformation.newBuilder(ScalarMessage.class)
                            .deterministicSerialization(true)
                            .maxMessageSize(128)
                            .recursionLimit(4)
                            .build();
            var stream =
                    env.fromData("payload")
                            .map(text -> ScalarMessage.newBuilder().setStringValue(text).build())
                            .returns(info);
            assertThat(stream.getType()).isSameAs(info);
            assertThat(stream.getType().createSerializer(serializerConfig))
                    .isEqualTo(
                            new ProtobufTypeSerializer<>(
                                    ScalarMessage.class,
                                    new ProtobufSerializerSettings(true, 128, 4)));
            var direct =
                    env.fromData(info, ScalarMessage.newBuilder().setStringValue("direct").build());
            var values =
                    stream.union(direct)
                            .rebalance()
                            .map(ScalarMessage::getStringValue)
                            .returns(Types.STRING)
                            .executeAndCollect(2);
            assertThat(values).containsExactlyInAnyOrder("payload", "direct");
            assertThatThrownBy(
                            () ->
                                    TypeInformation.of(ScalarMessage.class)
                                            .createSerializer(serializerConfig))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
