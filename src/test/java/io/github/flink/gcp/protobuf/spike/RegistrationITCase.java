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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import io.github.flink.gcp.protobuf.spike.generated.ProbeMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RegistrationITCase {
    @Test
    @Timeout(120)
    void registersGeneratedMessagesAndTransportsAllFiveShapes() throws Exception {
        String flinkVersion = System.getProperty("protobuf.test.flink.version");
        assertThat(flinkVersion)
                .as("protobuf.test.flink.version (set by the Maven verification recipes)")
                .isNotBlank();
        Configuration config = new Configuration();
        config.set(PipelineOptions.GENERIC_TYPES, false);
        config.set(
                PipelineOptions.SERIALIZATION_CONFIG,
                List.of(
                        "com.google.protobuf.AbstractMessage: {type: typeinfo, class: io.github.flink.gcp.protobuf.spike.ProbeTypeInfoFactory}"));
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2, config)) {
            env.disableOperatorChaining();
            var serializerConfig = env.getConfig().getSerializerConfig();
            assertThat(serializerConfig.hasGenericTypesDisabled()).isTrue();
            TypeInformation<ProbeMessage> messageInfo = TypeInformation.of(ProbeMessage.class);
            assertThat(messageInfo).isInstanceOf(ProbeTypeInfoFactory.ProbeTypeInformation.class);
            assertThat(messageInfo.getTypeClass()).isEqualTo(ProbeMessage.class);
            assertThat(messageInfo.createSerializer(serializerConfig))
                    .isInstanceOf(ProbeTypeInfoFactory.ProbeSerializer.class);
            ProbeMessage message = ProbeMessage.newBuilder().setId(7).setText("payload").build();
            DataStream<ProbeMessage> top = env.fromData(message);
            assertThat(top.getType()).isEqualTo(messageInfo);
            DataStream<Envelope> pojo = env.fromData(new Envelope(message));
            assertThat(pojo.getType()).isInstanceOf(PojoTypeInfo.class);
            assertThat(((PojoTypeInfo<?>) pojo.getType()).getTypeAt("message"))
                    .isEqualTo(messageInfo);
            DataStream<Tuple2<String, ProbeMessage>> tuple =
                    env.fromData(Tuple2.of("tuple", message));
            assertThat(tuple.getType()).isInstanceOf(TupleTypeInfo.class);
            assertThat(((TupleTypeInfo<?>) tuple.getType()).getTypeAt(1)).isEqualTo(messageInfo);
            TypeInformation<List<ProbeMessage>> inferredList =
                    TypeInformation.of(new TypeHint<List<ProbeMessage>>() {});
            ListTypeInfo<ProbeMessage> listInfo = new ListTypeInfo<>(messageInfo);
            if (flinkVersion.startsWith("1.20.")) {
                // Flink 1.20 needs explicit list element information with generic types disabled.
                assertThat(inferredList).isInstanceOf(GenericTypeInfo.class);
                assertThatThrownBy(() -> inferredList.createSerializer(serializerConfig))
                        .isInstanceOf(UnsupportedOperationException.class);
            } else {
                assertThat(inferredList).isInstanceOf(ListTypeInfo.class);
                assertThat(((ListTypeInfo<?>) inferredList).getElementTypeInfo())
                        .isEqualTo(messageInfo);
            }
            DataStream<Row> row = env.fromData(Types.ROW(messageInfo), Row.of(message));
            DataStream<List<ProbeMessage>> list = env.fromData(listInfo, List.of(message));
            var results =
                    top.rebalance()
                            .map(v -> "top:" + v.getId() + ":" + v.getText())
                            .union(
                                    pojo.rebalance()
                                            .map(
                                                    v ->
                                                            "pojo:"
                                                                    + v.message.getId()
                                                                    + ":"
                                                                    + v.message.getText()),
                                    tuple.rebalance()
                                            .map(
                                                    v ->
                                                            v.f0
                                                                    + ":"
                                                                    + v.f1.getId()
                                                                    + ":"
                                                                    + v.f1.getText()),
                                    row.rebalance()
                                            .map(
                                                    v ->
                                                            "row:"
                                                                    + ((ProbeMessage) v.getField(0))
                                                                            .getId()),
                                    list.rebalance().map(v -> "list:" + v.get(0).getId()))
                            .executeAndCollect(5);
            assertThat(results)
                    .containsExactlyInAnyOrder(
                            "top:7:payload",
                            "pojo:7:payload",
                            "tuple:7:payload",
                            "row:7",
                            "list:7");
        }
    }

    public static class Envelope {
        public ProbeMessage message;

        public Envelope() {}

        public Envelope(ProbeMessage message) {
            this.message = message;
        }
    }
}
