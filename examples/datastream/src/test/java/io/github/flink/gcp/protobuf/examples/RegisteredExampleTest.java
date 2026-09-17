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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.protobuf.ProtobufTypeInformation;
import io.github.flink.gcp.protobuf.examples.generated.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(120)
class RegisteredExampleTest {
    @Test
    void loadsTheDocumentedYamlBeforeExtractionAndTransportsValues() throws Exception {
        var config = GlobalConfiguration.loadConfiguration("config");
        try (var env = StreamExecutionEnvironment.createLocalEnvironment(2, config)) {
            env.disableOperatorChaining();
            assertThat(env.getConfig().getSerializerConfig().hasGenericTypesDisabled()).isTrue();
            var values = RegisteredExample.messages(env);
            assertThat(values.getType()).isEqualTo(ProtobufTypeInformation.of(Event.class));
            assertThat(
                            values.rebalance()
                                    .map(event -> event.getAccount() + ":" + event.getAmount())
                                    .returns(Types.STRING)
                                    .executeAndCollect(2))
                    .containsExactlyInAnyOrder("alice:2", "bob:7");
        }
    }
}
