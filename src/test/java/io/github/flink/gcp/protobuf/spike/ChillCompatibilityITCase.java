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

package io.github.flink.gcp.protobuf.spike;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("chill")
class ChillCompatibilityITCase {
    @Test
    @Timeout(120)
    void observesChillTransport() throws Exception {
        for (String name :
                List.of(
                        "com.esotericsoftware.kryo.Kryo",
                        "com.twitter.chill.protobuf.ProtobufSerializer",
                        "com.google.protobuf.StringValue")) {
            Class<?> type = Class.forName(name);
            System.out.println(
                    name
                            + " loaded from "
                            + type.getProtectionDomain().getCodeSource().getLocation());
        }
        Configuration config = new Configuration();
        config.set(
                PipelineOptions.SERIALIZATION_CONFIG,
                List.of(
                        "com.google.protobuf.Message: {type: kryo, kryo-type: default, class: com.twitter.chill.protobuf.ProtobufSerializer}"));
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2, config)) {
            env.disableOperatorChaining();
            System.out.println("StringValue type: " + TypeInformation.of(StringValue.class));
            assertThat(
                            env.fromData(StringValue.of("chill-probe"))
                                    .rebalance()
                                    .map(StringValue::getValue)
                                    .executeAndCollect(1))
                    .containsExactly("chill-probe");
            System.out.println("H1 OBSERVATION: transport succeeded");
        }
    }
}
