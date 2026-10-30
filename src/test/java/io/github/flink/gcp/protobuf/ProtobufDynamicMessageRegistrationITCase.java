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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The registered factory with DynamicMessage outputs, which need explicit type information. */
class ProtobufDynamicMessageRegistrationITCase {
    private static final String LIBRARY_MESSAGE = "ProtobufTypeInformation.of(Descriptor)";

    /** One method, because the process-global factory registry accepts one registration. */
    @Test
    @Timeout(120)
    void dynamicMessageOutputsAndCompositeFieldsNeedExplicitTypeInformation() throws Exception {
        try (StreamExecutionEnvironment env = environment()) {
            env.disableOperatorChaining();
            var serializerConfig = env.getConfig().getSerializerConfig();
            assertThat(serializerConfig.hasGenericTypesDisabled()).isTrue();

            ProtobufTypeInformation<DynamicMessage> info =
                    ProtobufTypeInformation.of(Entries.descriptor());
            var values = env.fromData("first", "second").map(new ToEntry()).returns(info);
            assertThat(values.getType()).isSameAs(info);
            // The rebalanced values are written by the task's serializer, whose rebuilt
            // descriptors differ from the instance that created them.
            List<String> results =
                    values.rebalance()
                            .map(
                                    value ->
                                            (String)
                                                    value.getField(
                                                            value.getDescriptorForType()
                                                                    .findFieldByName("payload")))
                            .returns(Types.STRING)
                            .executeAndCollect(2);
            assertThat(results).containsExactlyInAnyOrder("first", "second");

            // A POJO field of type DynamicMessage becomes a generic type, which is disabled.
            TypeInformation<Holder> inferred = TypeInformation.of(Holder.class);
            assertThat(inferred).isInstanceOf(PojoTypeInfo.class);
            assertThat(((PojoTypeInfo<Holder>) inferred).getTypeAt("message"))
                    .isInstanceOf(GenericTypeInfo.class);
            assertThatThrownBy(() -> inferred.createSerializer(serializerConfig))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining(DynamicMessage.class.getName());
            // Explicit composite type information with the descriptor-bound type works.
            DynamicMessage value = new ToEntry().map("composite");
            TypeInformation<Holder> pojo = Types.POJO(Holder.class, Map.of("message", info));
            Holder holder = new Holder();
            holder.message = value;
            assertThat(roundTrip(pojo, holder, serializerConfig).message.toByteString())
                    .isEqualTo(value.toByteString());
            TypeInformation<Tuple2<String, DynamicMessage>> tuple = Types.TUPLE(Types.STRING, info);
            assertThat(
                            roundTrip(tuple, Tuple2.of("key", value), serializerConfig)
                                    .f1
                                    .toByteString())
                    .isEqualTo(value.toByteString());

            // Without explicit type information, the library's reason reaches the failure. This
            // leaves an operator without a type in the environment, so no job runs afterwards.
            var missing = env.fromData("missing").map(new ToEntry());
            assertThatThrownBy(missing::getType)
                    .satisfies(
                            failure ->
                                    assertThat(causes(failure))
                                            .anyMatch(
                                                    message ->
                                                            message != null
                                                                    && message.contains(
                                                                            LIBRARY_MESSAGE)));
        }
    }

    private static StreamExecutionEnvironment environment() {
        Configuration config = new Configuration();
        config.set(PipelineOptions.GENERIC_TYPES, false);
        config.set(
                PipelineOptions.SERIALIZATION_CONFIG,
                List.of(
                        "com.google.protobuf.AbstractMessage: {type: typeinfo, class: io.github.flink.gcp.protobuf.ProtobufTypeInfoFactory}"));
        return StreamExecutionEnvironment.createLocalEnvironment(2, config);
    }

    private static <T> T roundTrip(
            TypeInformation<T> type,
            T value,
            org.apache.flink.api.common.serialization.SerializerConfig config)
            throws Exception {
        var serializer = type.createSerializer(config);
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
    }

    private static List<String> causes(Throwable failure) {
        List<String> messages = new java.util.ArrayList<>();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            messages.add(cause.getMessage());
        }
        return messages;
    }

    /** Creates values with a descriptor that no generated class backs. */
    public static final class ToEntry implements MapFunction<String, DynamicMessage> {
        private static final long serialVersionUID = 1L;

        @Override
        public DynamicMessage map(String payload) throws Exception {
            Descriptor descriptor = Entries.descriptor();
            return DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("payload"), payload)
                    .build();
        }
    }

    /** A POJO with a DynamicMessage field. */
    public static final class Holder {
        public DynamicMessage message;

        public Holder() {}
    }

    /** Holds one application descriptor per JVM, as an application would. */
    private static final class Entries {
        private static Descriptor descriptor;

        static synchronized Descriptor descriptor() throws Exception {
            if (descriptor == null) {
                descriptor = RuntimeRecoveryHarness.descriptor("original");
            }
            return descriptor;
        }
    }
}
