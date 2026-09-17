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

package io.github.flink.gcp.protobuf.examples;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.protobuf.ProtobufTypeInformation;
import io.github.flink.gcp.protobuf.examples.generated.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(120)
class ExplicitExampleTest {
    @Test
    void transportsExplicitValuesWithoutRegisteringTheFactory() throws Exception {
        try (var env =
                StreamExecutionEnvironment.createLocalEnvironment(
                        2, ExplicitExample.configuration())) {
            env.disableOperatorChaining();
            var serializerConfig = env.getConfig().getSerializerConfig();
            assertThat(serializerConfig.hasGenericTypesDisabled()).isTrue();
            assertThatThrownBy(
                            () ->
                                    TypeInformation.of(Event.class)
                                            .createSerializer(serializerConfig))
                    .isInstanceOf(UnsupportedOperationException.class);
            var values = ExplicitExample.messages(env);
            assertThat(values.getType()).isEqualTo(ProtobufTypeInformation.of(Event.class));
            assertThat(
                            values.rebalance()
                                    .map(event -> event.getAccount() + ":" + event.getAmount())
                                    .returns(Types.STRING)
                                    .executeAndCollect(2))
                    .containsExactlyInAnyOrder("alice:3", "bob:8");
            assertThatThrownBy(
                            () ->
                                    TypeInformation.of(Event.class)
                                            .createSerializer(serializerConfig))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
