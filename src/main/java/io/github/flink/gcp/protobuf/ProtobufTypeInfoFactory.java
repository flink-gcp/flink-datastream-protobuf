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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.protobuf.Message;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Creates native type information with the release defaults for full-runtime generated messages.
 *
 * <p>Register against {@code com.google.protobuf.AbstractMessage} through {@code
 * pipeline.serialization-config} before type extraction. Lookup follows superclasses; registering
 * the Message interface does not reach generated message classes. Flink's factory registry is
 * process-global, not isolated per job.
 *
 * <p>This factory has no mutable global settings. Use {@link ProtobufTypeInformation#newBuilder}
 * and explicit type information for custom settings. Unsupported types fail explicitly; the factory
 * never supplies a generic or Kryo fallback. Set {@code pipeline.generic-types: false} to reject
 * Flink's own generic path when native integration is not selected.
 */
@PublicEvolving
public final class ProtobufTypeInfoFactory extends TypeInfoFactory<Message> {

    /** Creates a stateless factory for configuration-based registration. */
    public ProtobufTypeInfoFactory() {}

    /**
     * Validates a concrete generated class and creates type information preserving that class.
     *
     * @param type concrete generated message class; other reflective type forms are unsupported
     * @param genericParameters inferred generic parameters; unused for concrete generated messages
     * @return native type information with the same defaults as {@link ProtobufTypeInformation#of}
     * @throws IllegalArgumentException if the type or its payload descriptor graph is unsupported
     */
    @Override
    public TypeInformation<Message> createTypeInfo(
            Type type, Map<String, TypeInformation<?>> genericParameters) {
        if (!(type instanceof Class<?> messageClass)
                || !Message.class.isAssignableFrom(messageClass)) {
            throw new IllegalArgumentException(
                    "Expected a concrete full-runtime generated message class: " + type);
        }
        return createConcreteTypeInfo(messageClass.asSubclass(Message.class));
    }

    /** Bridges Flink's factory signature while retaining the exact generated class at runtime. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static TypeInformation<Message> createConcreteTypeInfo(Class<? extends Message> type) {
        return (TypeInformation) ProtobufTypeInformation.of(type);
    }
}
