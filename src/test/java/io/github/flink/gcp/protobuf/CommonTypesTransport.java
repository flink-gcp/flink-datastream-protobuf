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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import io.github.flink.gcp.protobuf.generated.CommonTypesEnvelope;
import io.opentelemetry.proto.common.v1.AnyValue;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Shared transport assertions; registration modes run in separate test JVMs. */
final class CommonTypesTransport {
    private CommonTypesTransport() {}

    static void verify(boolean registered) throws Exception {
        var config = new Configuration();
        config.set(PipelineOptions.GENERIC_TYPES, false);
        if (registered) {
            config.set(
                    PipelineOptions.SERIALIZATION_CONFIG,
                    List.of(
                            "com.google.protobuf.AbstractMessage: {type: typeinfo, class: io.github.flink.gcp.protobuf.ProtobufTypeInfoFactory}"));
        }
        try (var env = StreamExecutionEnvironment.createLocalEnvironment(2, config)) {
            env.disableOperatorChaining();
            var serializerConfig = env.getConfig().getSerializerConfig();
            assertThat(serializerConfig.hasGenericTypesDisabled()).isTrue();
            for (Message sample : CommonTypeValues.samples()) {
                var info = information(sample.getClass(), registered);
                assertThat(info).isInstanceOf(ProtobufTypeInformation.class);
                assertThat(info.createSerializer(serializerConfig))
                        .isInstanceOf(ProtobufTypeSerializer.class);
                assertThat(info.createSerializer(serializerConfig).snapshotConfiguration())
                        .isInstanceOf(ProtobufTypeSerializerSnapshot.class);
            }
            var info = information(CommonTypesEnvelope.class, registered);
            var value = CommonTypeValues.envelope(4);
            var top = env.fromData(info, value);
            assertThat(top.getType()).isEqualTo(info);
            var tupleInfo =
                    new TupleTypeInfo<Tuple2<String, CommonTypesEnvelope>>(Types.STRING, info);
            var tuple =
                    env.fromData(
                            registered
                                    ? TypeInformation.of(
                                            new org.apache.flink.api.common.typeinfo.TypeHint<
                                                    Tuple2<String, CommonTypesEnvelope>>() {})
                                    : tupleInfo,
                            Tuple2.of("tuple", value));
            assertThat(((TupleTypeInfo<?>) tuple.getType()).getTypeAt(1)).isEqualTo(info);
            var row = env.fromData(Types.ROW(info), Row.of(value));
            var list = env.fromData(new ListTypeInfo<>(info), List.of(value));
            var result =
                    top.rebalance()
                            .map(v -> checked("top", v, CommonTypeValues.envelope(4)))
                            .union(
                                    tuple.rebalance()
                                            .map(
                                                    v ->
                                                            checked(
                                                                    "tuple",
                                                                    v.f1,
                                                                    CommonTypeValues.envelope(4))),
                                    row.rebalance()
                                            .map(
                                                    v ->
                                                            checked(
                                                                    "row",
                                                                    (Message) v.getField(0),
                                                                    CommonTypeValues.envelope(4))),
                                    list.rebalance()
                                            .map(
                                                    v ->
                                                            checked(
                                                                    "list",
                                                                    v.get(0),
                                                                    CommonTypeValues.envelope(4))),
                                    env.fromData(
                                                    information(Struct.class, registered),
                                                    value.getStructValue())
                                            .rebalance()
                                            .map(
                                                    v ->
                                                            checked(
                                                                    "struct",
                                                                    v,
                                                                    CommonTypeValues.struct(4))),
                                    env.fromData(information(Any.class, registered), value.getAny())
                                            .rebalance()
                                            .map(
                                                    v ->
                                                            checked(
                                                                    "any",
                                                                    v,
                                                                    CommonTypeValues.envelope(4)
                                                                            .getAny())),
                                    env.fromData(
                                                    information(AnyValue.class, registered),
                                                    value.getOtelValue())
                                            .rebalance()
                                            .map(
                                                    v ->
                                                            checked(
                                                                    "otel",
                                                                    v,
                                                                    CommonTypeValues.otel(4))));
            if (registered) {
                var pojo = env.fromData(new Envelope(value));
                assertThat(pojo.getType()).isInstanceOf(PojoTypeInfo.class);
                assertThat(((PojoTypeInfo<?>) pojo.getType()).getTypeAt("message")).isEqualTo(info);
                result =
                        result.union(
                                pojo.rebalance()
                                        .map(
                                                v ->
                                                        checked(
                                                                "pojo",
                                                                v.message,
                                                                CommonTypeValues.envelope(4))));
            }
            assertThat(result.executeAndCollect(registered ? 8 : 7))
                    .containsExactlyInAnyOrderElementsOf(
                            registered
                                    ? List.of(
                                            "top", "tuple", "row", "list", "struct", "any", "otel",
                                            "pojo")
                                    : List.of(
                                            "top", "tuple", "row", "list", "struct", "any",
                                            "otel"));
            if (!registered) {
                assertThatThrownBy(
                                () ->
                                        TypeInformation.of(CommonTypesEnvelope.class)
                                                .createSerializer(serializerConfig))
                        .isInstanceOf(UnsupportedOperationException.class);
            }
        }
    }

    private static <T extends Message> TypeInformation<T> information(
            Class<T> type, boolean registered) {
        return registered ? TypeInformation.of(type) : ProtobufTypeInformation.of(type);
    }

    private static String checked(String name, Message actual, Message expected) {
        if (!actual.equals(expected)) {
            throw new IllegalStateException("Changed " + name + " value: " + actual);
        }
        return name;
    }

    public static class Envelope {
        public CommonTypesEnvelope message;

        public Envelope() {}

        public Envelope(CommonTypesEnvelope message) {
            this.message = message;
        }
    }
}
