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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * DynamicMessage transport and RocksDB keyed state in jobs with generic types disabled, with
 * descriptors that no generated class on the classpath backs.
 */
@Timeout(120)
class ProtobufDynamicMessageTransportITCase {
    private static final String ENTRY = "flink.protobuf.runtime.Entry";

    /**
     * Two schemas with the same Java class and Protobuf full name keep separate serializers and
     * state in one job.
     */
    @Test
    void independentSchemasOfTheSameJavaClassKeepTheirOwnSerializers() throws Exception {
        ProtobufTypeInformation<DynamicMessage> original =
                ProtobufTypeInformation.of(Schemas.of("original"));
        ProtobufTypeInformation<DynamicMessage> enumOriginal =
                ProtobufTypeInformation.of(Schemas.of("enum-original"));
        assertThat(original.getTypeClass()).isSameAs(enumOriginal.getTypeClass());
        assertThat(original).isNotEqualTo(enumOriginal);
        try (StreamExecutionEnvironment env = environment()) {
            DataStream<String> first = counted(env, "original", original);
            DataStream<String> second = counted(env, "enum-original", enumOriginal);
            assertThat(first.union(second).executeAndCollect(16))
                    .containsExactlyInAnyOrder(
                            "original:0:1:none",
                            "original:1:1:none",
                            "original:2:2:none",
                            "original:3:2:none",
                            "original:4:3:none",
                            "original:5:3:none",
                            "original:6:4:none",
                            "original:7:4:none",
                            "enum-original:0:1:KIND_FIRST",
                            "enum-original:1:1:KIND_FIRST",
                            "enum-original:2:2:KIND_FIRST",
                            "enum-original:3:2:KIND_FIRST",
                            "enum-original:4:3:KIND_FIRST",
                            "enum-original:5:3:KIND_FIRST",
                            "enum-original:6:4:KIND_FIRST",
                            "enum-original:7:4:KIND_FIRST");
        }
    }

    @Test
    void valuesOfAnotherRootFailTheJob() throws Exception {
        assertTransportFails(
                "other",
                "Protobuf message changes from "
                        + DynamicDescriptors.PACKAGE
                        + ".Other to "
                        + ENTRY);
    }

    @Test
    void valuesOfAnIncompatibleSchemaWithTheSameRootNameFailTheJob() throws Exception {
        assertTransportFails(
                "enum-original",
                "Field 6 (detail) of " + ENTRY + " is removed without reserving its number");
    }

    @Test
    void nestedValuesOfAnIncompatibleSchemaFailTheJob() throws Exception {
        assertTransportFails(
                "nested-incompatible",
                "Cannot write DynamicMessage value at "
                        + ENTRY
                        + ".child: Field 2 (count) of "
                        + ENTRY
                        + " changes type from SINT32 to INT32");
    }

    /** Sends values of a schema variant through a stream typed with the original schema. */
    private static void assertTransportFails(String variant, String reason) throws Exception {
        try (StreamExecutionEnvironment env = environment()) {
            var values =
                    env.fromData(0, 1)
                            .map(new ToEntry(variant))
                            .returns(ProtobufTypeInformation.of(Schemas.of("original")))
                            .rebalance()
                            .map(value -> value.getAllFields().size())
                            .returns(Types.INT);
            assertThatThrownBy(() -> values.executeAndCollect(2))
                    .hasStackTraceContaining("IOException")
                    .hasStackTraceContaining(reason);
        }
    }

    /** Counts values of one schema per stream key in ValueState after the keyed exchange. */
    private static DataStream<String> counted(
            StreamExecutionEnvironment env,
            String variant,
            ProtobufTypeInformation<DynamicMessage> info) {
        // One upstream subtask keeps each key's input order through the keyed exchange.
        return env.fromData(0, 1, 2, 3, 4, 5, 6, 7)
                .map(new ToEntry(variant))
                .returns(info)
                .setParallelism(1)
                .keyBy(
                        value ->
                                (Integer)
                                                value.getField(
                                                        value.getDescriptorForType()
                                                                .findFieldByName("ordinal"))
                                        % 2,
                        Types.INT)
                .process(new Count(variant, info))
                .returns(Types.STRING);
    }

    private static StreamExecutionEnvironment environment() {
        Configuration config = new Configuration();
        config.set(PipelineOptions.GENERIC_TYPES, false);
        // RocksDB writes and reads every state value with the state serializer.
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.setString("state.backend.rocksdb.memory.managed", "false");
        config.setString("state.backend.rocksdb.memory.fixed-per-slot", "32mb");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2, config);
        env.disableOperatorChaining();
        return env;
    }

    /** Creates a value of a schema variant, with the descriptor of the running JVM. */
    public static final class ToEntry implements MapFunction<Integer, DynamicMessage> {
        private static final long serialVersionUID = 1L;
        private final String variant;

        ToEntry(String variant) {
            this.variant = variant;
        }

        @Override
        public DynamicMessage map(Integer ordinal) throws Exception {
            if (variant.equals("other")) {
                return DynamicMessage.getDefaultInstance(Schemas.of("other"));
            }
            if (variant.equals("nested-incompatible")) {
                Descriptor entry = Schemas.of("original");
                return DynamicMessage.newBuilder(entry)
                        .setField(entry.findFieldByName("ordinal"), ordinal)
                        .setField(entry.findFieldByName("child"), entry(Schemas.of("incompatible")))
                        .build();
            }
            Descriptor entry = Schemas.of(variant);
            DynamicMessage.Builder value =
                    DynamicMessage.newBuilder(entry)
                            .setField(entry.findFieldByName("ordinal"), ordinal);
            FieldDescriptor detail = entry.findFieldByName("detail");
            if (detail != null) {
                FieldDescriptor kind = detail.getMessageType().findFieldByName("kind");
                value.setField(
                        detail,
                        DynamicMessage.newBuilder(detail.getMessageType())
                                .setField(kind, kind.getEnumType().findValueByName("KIND_FIRST"))
                                .build());
            }
            return value.build();
        }

        private static DynamicMessage entry(Descriptor descriptor) {
            return DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("count"), 7)
                    .build();
        }
    }

    /** Counts values per key and reports the nested enum through the value's own descriptor. */
    public static final class Count extends KeyedProcessFunction<Integer, DynamicMessage, String> {
        private static final long serialVersionUID = 1L;
        private final String variant;
        private final ProtobufTypeInformation<DynamicMessage> info;
        private transient ValueState<DynamicMessage> last;

        Count(String variant, ProtobufTypeInformation<DynamicMessage> info) {
            this.variant = variant;
            this.info = info;
        }

        @Override
        public void open(OpenContext context) {
            last = getRuntimeContext().getState(new ValueStateDescriptor<>("last", info));
        }

        @Override
        public void processElement(DynamicMessage value, Context context, Collector<String> out)
                throws Exception {
            Descriptor type = value.getDescriptorForType();
            DynamicMessage previous = last.value();
            int count =
                    previous == null
                            ? 1
                            : (Integer)
                                            previous.getField(
                                                    previous.getDescriptorForType()
                                                            .findFieldByName("count"))
                                    + 1;
            last.update(value.toBuilder().setField(type.findFieldByName("count"), count).build());
            FieldDescriptor detail = type.findFieldByName("detail");
            String kind = "none";
            if (detail != null) {
                DynamicMessage nested = (DynamicMessage) value.getField(detail);
                kind =
                        ((EnumValueDescriptor)
                                        nested.getField(
                                                nested.getDescriptorForType()
                                                        .findFieldByName("kind")))
                                .getName();
            }
            out.collect(
                    variant
                            + ":"
                            + value.getField(type.findFieldByName("ordinal"))
                            + ":"
                            + count
                            + ":"
                            + kind);
        }
    }

    /** Holds one descriptor per schema in the JVM, as an application would. */
    private static final class Schemas {
        private static final Map<String, Descriptor> DESCRIPTORS = new HashMap<>();

        static synchronized Descriptor of(String variant) throws Exception {
            Descriptor descriptor = DESCRIPTORS.get(variant);
            if (descriptor == null) {
                descriptor =
                        variant.equals("other")
                                ? DynamicDescriptors.build(
                                        DynamicDescriptors.PACKAGE + ".Other",
                                        DynamicDescriptors.file("other.proto")
                                                .addMessageType(
                                                        DynamicDescriptors.message(
                                                                "Other",
                                                                DynamicDescriptors.field(
                                                                        "name",
                                                                        1,
                                                                        FieldDescriptorProto.Type
                                                                                .TYPE_STRING)))
                                                .build())
                                : RuntimeRecoveryHarness.descriptor(variant);
                DESCRIPTORS.put(variant, descriptor);
            }
            return descriptor;
        }
    }
}
