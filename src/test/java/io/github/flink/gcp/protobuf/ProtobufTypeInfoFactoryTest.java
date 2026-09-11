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

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import io.github.flink.gcp.protobuf.generated.ExtensionDeclaration;
import io.github.flink.gcp.protobuf.generated.ExtensionHolder;
import io.github.flink.gcp.protobuf.generated.ExtensionTarget;
import io.github.flink.gcp.protobuf.generated.MessageSetRoot;
import io.github.flink.gcp.protobuf.generated.OptionMessage;
import io.github.flink.gcp.protobuf.generated.ScalarMessage;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufTypeInfoFactoryTest {
    @Test
    void publicNoArgumentFactoryPreservesTheConcreteGeneratedTypeAndDefaults() throws Exception {
        var factory = ProtobufTypeInfoFactory.class.getConstructor().newInstance();
        var info = factory.createTypeInfo(ScalarMessage.class, Map.of());
        assertThat(info).isEqualTo(ProtobufTypeInformation.of(ScalarMessage.class));
        assertThat(info.getTypeClass()).isEqualTo(ScalarMessage.class);
        assertThat(info.createSerializer(new SerializerConfigImpl()))
                .isEqualTo(new ProtobufTypeSerializer<>(ScalarMessage.class));
        assertThat(factory.createTypeInfo(OptionMessage.class, Map.of()))
                .isEqualTo(ProtobufTypeInformation.of(OptionMessage.class));
    }

    @Test
    @SuppressWarnings("deprecation")
    void publicConstructionAndFactoryRejectUnsupportedClassesWithTheOffendingType() {
        var factory = new ProtobufTypeInfoFactory();
        for (Class<?> type :
                List.of(
                        AbstractMessage.class,
                        Message.class,
                        GeneratedMessage.class,
                        GeneratedMessageV3.class,
                        DynamicMessage.class,
                        HiddenGenerated.class,
                        String.class,
                        ScalarMessage.Builder.class,
                        ProtobufTypeSerializerTest.LiteValue.class,
                        ProtobufTypeSerializerTest.MissingDefault.class,
                        ProtobufTypeSerializerTest.WrongDefaultReturn.class,
                        ProtobufTypeSerializerTest.NullDefault.class,
                        ProtobufTypeSerializerTest.WrongParser.class,
                        ExtensionTarget.class,
                        ExtensionHolder.class,
                        ExtensionDeclaration.class,
                        MessageSetRoot.class)) {
            assertThatThrownBy(() -> factory.createTypeInfo(type, Map.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(type.getName());
            assertThatThrownBy(() -> constructUnsupported(type, false))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(type.getName());
            assertThatThrownBy(() -> constructUnsupported(type, true))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(type.getName());
        }
        assertThatThrownBy(() -> factory.createTypeInfo(null, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null");
        assertThatThrownBy(() -> ProtobufTypeInformation.of(String.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("java.lang.String");
        assertThatThrownBy(() -> ProtobufTypeInformation.of((Class<Message>) null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("messageClass");
    }

    @Test
    void factoryRejectsNonConcreteReflectiveTypesInsteadOfFallingBack() throws Exception {
        var factory = new ProtobufTypeInfoFactory();
        var parameterized =
                ReflectiveTypes.class.getDeclaredField("parameterized").getGenericType();
        var wildcard =
                ((java.lang.reflect.ParameterizedType)
                                ReflectiveTypes.class.getDeclaredField("wildcard").getGenericType())
                        .getActualTypeArguments()[0];
        var array = ReflectiveTypes.class.getDeclaredField("array").getGenericType();
        for (Type type :
                List.of(
                        parameterized,
                        wildcard,
                        array,
                        ReflectiveTypes.class.getTypeParameters()[0])) {
            assertThatThrownBy(() -> factory.createTypeInfo(type, Map.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(type.toString());
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void constructUnsupported(Class<?> type, boolean builder) {
        if (builder) {
            ProtobufTypeInformation.newBuilder((Class) type).build();
        } else {
            ProtobufTypeInformation.of((Class) type);
        }
    }

    private static final class HiddenGenerated
            extends ProtobufTypeSerializerTest.InvalidGenerated {}

    private static class ReflectiveTypes<T extends Message> {
        List<ScalarMessage> parameterized;
        List<? extends Message> wildcard;
        T[] array;
    }
}
