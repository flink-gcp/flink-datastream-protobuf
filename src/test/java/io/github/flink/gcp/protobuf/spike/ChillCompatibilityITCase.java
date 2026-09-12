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
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("chill")
@Timeout(120)
class ChillCompatibilityITCase {
    @Test
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
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2, chillConfiguration())) {
            env.disableOperatorChaining();
            System.out.println("StringValue type: " + TypeInformation.of(StringValue.class));
            assertThat(
                            env.fromData(
                                            StringValue.of("chill-probe"),
                                            StringValue.getDefaultInstance())
                                    .rebalance()
                                    .map(StringValue::getValue)
                                    .executeAndCollect(2))
                    .containsExactlyInAnyOrder("chill-probe", "");
            System.out.println("H1 OBSERVATION: transport succeeded");
        }
    }

    @Test
    void copiesAndSerializesEmptyAndNonemptyMessages() throws Exception {
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2, chillConfiguration())) {
            var serializer =
                    TypeInformation.of(StringValue.class)
                            .createSerializer(env.getConfig().getSerializerConfig());
            assertThat(serializer)
                    .isInstanceOfSatisfying(
                            KryoSerializer.class,
                            kryo ->
                                    assertThat(
                                                    kryo.getKryo()
                                                            .getSerializer(StringValue.class)
                                                            .getClass()
                                                            .getName())
                                            .isEqualTo(
                                                    "com.twitter.chill.protobuf.ProtobufSerializer"));
            for (StringValue value :
                    List.of(StringValue.getDefaultInstance(), StringValue.of("chill-copy"))) {
                assertThat(serializer.copy(value)).isEqualTo(value);
                DataOutputSerializer output = new DataOutputSerializer(64);
                serializer.serialize(value, output);
                assertThat(
                                serializer.deserialize(
                                        new DataInputDeserializer(output.getCopyOfBuffer())))
                        .isEqualTo(value);
            }
        }
    }

    private static Configuration chillConfiguration() {
        Configuration config = new Configuration();
        config.set(
                PipelineOptions.SERIALIZATION_CONFIG,
                List.of(
                        "com.google.protobuf.Message: {type: kryo, kryo-type: default, class: com.twitter.chill.protobuf.ProtobufSerializer}"));
        return config;
    }
}
