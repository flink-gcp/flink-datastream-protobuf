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

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import io.github.flink.gcp.protobuf.generated.RecursiveMessage;
import io.github.flink.gcp.protobuf.generated.ScalarMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufTypeInformationTest {
    @Test
    void defaultsDescribeOneOpaqueNonKeyValueAndCreateIndependentNativeSerializers() {
        var info = ProtobufTypeInformation.of(ScalarMessage.class);
        assertThat(info.getTypeClass()).isSameAs(ScalarMessage.class);
        assertThat(info.isBasicType()).isFalse();
        assertThat(info.isTupleType()).isFalse();
        assertThat(info.getArity()).isOne();
        assertThat(info.getTotalFields()).isOne();
        assertThat(info.isKeyType()).isFalse();
        assertThat(info.toString())
                .contains(ScalarMessage.class.getName(), "67108864", "100", "false");
        var config = new SerializerConfigImpl();
        config.setGenericTypes(false);
        var first = info.createSerializer(config);
        assertThat(first)
                .isInstanceOf(ProtobufTypeSerializer.class)
                .isEqualTo(new ProtobufTypeSerializer<>(ScalarMessage.class));
        assertThat(info.createSerializer(config)).isEqualTo(first).isNotSameAs(first);
        assertThat(info).isEqualTo(ProtobufTypeInformation.newBuilder(ScalarMessage.class).build());
    }

    @Test
    void builderReuseCapturesEverySettingWithoutChangingEarlierResults() {
        var builder = ProtobufTypeInformation.newBuilder(ScalarMessage.class);
        var defaults = builder.build();
        var serializer = defaults.createSerializer(new SerializerConfigImpl());
        assertThat(builder.deterministicSerialization(true)).isSameAs(builder);
        assertThat(builder.maxMessageSize(128)).isSameAs(builder);
        assertThat(builder.recursionLimit(3)).isSameAs(builder);
        var configured = builder.build();
        assertThat(configured).isNotEqualTo(defaults);
        assertThat(configured.createSerializer(new SerializerConfigImpl()))
                .isEqualTo(
                        new ProtobufTypeSerializer<>(
                                ScalarMessage.class, new ProtobufSerializerSettings(true, 128, 3)));
        assertThat(defaults).isEqualTo(ProtobufTypeInformation.of(ScalarMessage.class));
        assertThat(serializer).isEqualTo(new ProtobufTypeSerializer<>(ScalarMessage.class));
        assertThat(builder.build()).isEqualTo(configured).isNotSameAs(configured);
        builder.deterministicSerialization(false).maxMessageSize(1).recursionLimit(1);
        assertThat(configured.createSerializer(new SerializerConfigImpl()))
                .isEqualTo(
                        new ProtobufTypeSerializer<>(
                                ScalarMessage.class, new ProtobufSerializerSettings(true, 128, 3)));
    }

    @Test
    void identityIncludesEachSettingAndTheConcreteClass() {
        var defaults = ProtobufTypeInformation.of(ScalarMessage.class);
        var equal = ProtobufTypeInformation.of(ScalarMessage.class);
        assertThat(defaults).isEqualTo(equal).hasSameHashCodeAs(equal).isNotEqualTo(null);
        for (Object other :
                List.of(
                        ProtobufTypeInformation.newBuilder(ScalarMessage.class)
                                .deterministicSerialization(true)
                                .build(),
                        ProtobufTypeInformation.newBuilder(ScalarMessage.class)
                                .maxMessageSize(128)
                                .build(),
                        ProtobufTypeInformation.newBuilder(ScalarMessage.class)
                                .recursionLimit(101)
                                .build(),
                        ProtobufTypeInformation.of(Empty.class))) {
            assertThat(defaults).isNotEqualTo(other);
            assertThat(defaults.canEqual(other)).isTrue();
            assertThat(((ProtobufTypeInformation<?>) other).canEqual(defaults)).isTrue();
            assertThat(((ProtobufTypeInformation<?>) other).isKeyType()).isFalse();
        }
        assertThat(defaults.canEqual(null)).isFalse();
        assertThat(defaults.canEqual(Types.STRING)).isFalse();
        assertThat(defaults).isNotEqualTo(Types.STRING);
    }

    @Test
    void validatesSettingsAtBuildAndAcceptsPositiveIntegerBoundaries() {
        for (int invalid : List.of(0, -1, Integer.MIN_VALUE)) {
            var size = ProtobufTypeInformation.newBuilder(Empty.class).maxMessageSize(invalid);
            assertThatThrownBy(size::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxMessageSize");
            var depth = ProtobufTypeInformation.newBuilder(Empty.class).recursionLimit(invalid);
            assertThatThrownBy(depth::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("recursionLimit");
        }
        for (int valid : List.of(1, Integer.MAX_VALUE)) {
            for (boolean deterministic : List.of(false, true)) {
                assertThat(
                                ProtobufTypeInformation.newBuilder(Empty.class)
                                        .maxMessageSize(valid)
                                        .recursionLimit(valid)
                                        .deterministicSerialization(deterministic)
                                        .build()
                                        .isKeyType())
                        .isFalse();
            }
        }
    }

    @Test
    void configuredLimitsAndDeterministicModeReachTheWireOperations() throws Exception {
        var config = new SerializerConfigImpl();
        var limited =
                ProtobufTypeInformation.newBuilder(RecursiveMessage.class)
                        .maxMessageSize(8)
                        .recursionLimit(1)
                        .build()
                        .createSerializer(config);
        var oversized =
                RecursiveMessage.newBuilder().setPayload(ByteString.copyFrom(new byte[9])).build();
        assertThatThrownBy(() -> encode(limited, oversized)).isInstanceOf(IOException.class);
        var deep =
                RecursiveMessage.newBuilder()
                        .setChild(
                                RecursiveMessage.newBuilder()
                                        .setChild(RecursiveMessage.getDefaultInstance()))
                        .build();
        assertThatThrownBy(() -> encode(limited, deep)).isInstanceOf(IOException.class);
        var defaults = ProtobufTypeInformation.of(RecursiveMessage.class).createSerializer(config);
        assertThatThrownBy(
                        () ->
                                limited.deserialize(
                                        new DataInputDeserializer(encode(defaults, deep))))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(
                        () ->
                                limited.deserialize(
                                        new DataInputDeserializer(encode(defaults, oversized))))
                .isInstanceOf(IOException.class);
        var value = RecursiveMessage.newBuilder().putCounts("z", 1).putCounts("a", 2).build();
        var deterministic =
                ProtobufTypeInformation.newBuilder(RecursiveMessage.class)
                        .deterministicSerialization(true)
                        .build()
                        .createSerializer(config);
        byte[] expected = new byte[value.getSerializedSize()];
        CodedOutputStream output = CodedOutputStream.newInstance(expected);
        output.useDeterministicSerialization();
        value.writeTo(output);
        output.checkNoSpaceLeft();
        var frame = new DataInputDeserializer(encode(deterministic, value));
        assertThat(frame.readInt()).isEqualTo(expected.length);
        byte[] actual = new byte[expected.length];
        frame.readFully(actual);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void javaSerializationPreservesSettingsAndRebuildsMetadataForTheSuppliedClassloader()
            throws Exception {
        var info =
                ProtobufTypeInformation.newBuilder(ScalarMessage.class)
                        .deterministicSerialization(true)
                        .maxMessageSize(256)
                        .recursionLimit(4)
                        .build();
        byte[] serialized = InstantiationUtil.serializeObject(info);
        ProtobufTypeInformation<?> same =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        assertThat(same).isEqualTo(info).hasSameHashCodeAs(info);
        try (var loader = new GeneratedMessageClassLoader()) {
            ProtobufTypeInformation<?> isolated =
                    InstantiationUtil.deserializeObject(serialized, loader);
            assertThat(isolated.getTypeClass().getClassLoader()).isSameAs(loader);
            assertThat(isolated).isNotEqualTo(info);
            assertThat(isolated.canEqual(info)).isTrue();
            verifyIsolatedRoundTrip(isolated, loader);
        }
    }

    private static <T> void verifyIsolatedRoundTrip(
            ProtobufTypeInformation<T> info, ClassLoader loader) throws Exception {
        var serializer = info.createSerializer(new SerializerConfigImpl());
        var expected =
                new ProtobufTypeSerializer<>(
                        info.getTypeClass().asSubclass(Message.class),
                        new ProtobufSerializerSettings(true, 256, 4));
        assertThat(serializer).isEqualTo(expected);
        byte[] wire = ScalarMessage.newBuilder().setInt32Value(42).build().toByteArray();
        T value =
                info.getTypeClass()
                        .cast(
                                info.getTypeClass()
                                        .getMethod("parseFrom", byte[].class)
                                        .invoke(null, wire));
        T result = serializer.deserialize(new DataInputDeserializer(encode(serializer, value)));
        assertThat(result).isEqualTo(value);
        assertThat(result.getClass().getClassLoader()).isSameAs(loader);
    }

    private static <T> byte[] encode(TypeSerializer<T> serializer, T value) throws IOException {
        var output = new DataOutputSerializer(32);
        serializer.serialize(value, output);
        return output.getCopyOfBuffer();
    }
}
