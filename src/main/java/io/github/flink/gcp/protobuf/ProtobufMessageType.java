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

import org.apache.flink.annotation.Internal;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

/**
 * Immutable runtime metadata cached by the actual application class, including its classloader.
 *
 * <p>The class-associated cache avoids repeated reflective lookup, parser validation, and schema
 * normalization across serializers. It does not maintain a global map that keeps application
 * classes alive. Failed resolutions are not cached. Concurrent first lookups may perform redundant
 * validation before ClassValue publishes one result.
 */
@Internal
final class ProtobufMessageType {
    private static final ClassValue<ProtobufMessageType> CACHE =
            new ClassValue<>() {
                @Override
                protected ProtobufMessageType computeValue(Class<?> type) {
                    return inspect(type);
                }
            };

    final Message defaultInstance;
    final Parser<? extends Message> parser;
    final FileDescriptorSet schema;
    final String fullName;

    private ProtobufMessageType(
            Message defaultInstance, Parser<? extends Message> parser, FileDescriptorSet schema) {
        this.defaultInstance = defaultInstance;
        this.parser = parser;
        this.schema = schema;
        this.fullName = defaultInstance.getDescriptorForType().getFullName();
    }

    /**
     * Resolves reusable metadata without caching serializer settings or mutable scratch buffers.
     *
     * @param type concrete generated application class
     * @return immutable metadata associated with that exact class
     * @throws IllegalArgumentException if the class or its generated metadata is unsupported
     */
    static ProtobufMessageType resolve(Class<? extends Message> type) {
        if (type == null) {
            throw new IllegalArgumentException("messageClass must not be null");
        }
        return CACHE.get(type);
    }

    /** Validates generated metadata using only public runtime and generated-class APIs. */
    private static ProtobufMessageType inspect(Class<?> type) {
        try {
            if (!Modifier.isPublic(type.getModifiers())
                    || Modifier.isAbstract(type.getModifiers())
                    || !isGeneratedMessageClass(type)) {
                throw new IllegalArgumentException(
                        "Expected a public, concrete full-runtime generated message");
            }
            Method method = type.getMethod("getDefaultInstance");
            if (!Modifier.isStatic(method.getModifiers()) || method.getReturnType() != type) {
                throw new IllegalArgumentException(
                        "getDefaultInstance must be static and return the concrete message class");
            }
            Object value = method.invoke(null);
            if (value == null || value.getClass() != type) {
                throw new IllegalArgumentException(
                        "getDefaultInstance returned a different message class or null");
            }
            Message instance = Message.class.cast(value);
            Descriptor descriptor = instance.getDescriptorForType();
            if (descriptor == null) {
                throw new IllegalArgumentException("Missing full message descriptor");
            }
            validatePayloadGraph(descriptor);
            Parser<? extends Message> parser = instance.getParserForType();
            if (parser == null) {
                throw new IllegalArgumentException("Missing message parser");
            }
            // Partial parsing also supports proto2 defaults with missing required fields.
            Message parsed = parser.parsePartialFrom(new byte[0]);
            if (parsed == null
                    || parsed.getClass() != type
                    || !parsed.getDescriptorForType().equals(descriptor)) {
                throw new IllegalArgumentException(
                        "Parser does not produce the concrete message type and descriptor");
            }
            return new ProtobufMessageType(
                    instance, parser, ProtobufSchema.normalize(descriptor.getFile()));
        } catch (ReflectiveOperationException | IOException | RuntimeException e) {
            throw new IllegalArgumentException(
                    "Unsupported messageClass " + type.getName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Recognizes the two full-runtime generated hierarchies supported by the runtime profiles.
     *
     * <p>Protobuf 3's GeneratedMessageV3 is not a subclass of GeneratedMessage. Protobuf 4 retains
     * it as a deprecated compatibility class, so checking it is necessary for the Protobuf 3 build.
     *
     * @param type candidate message class
     * @return whether the class derives from a supported generated-message base
     */
    @SuppressWarnings("deprecation")
    private static boolean isGeneratedMessageClass(Class<?> type) {
        return GeneratedMessageV3.class.isAssignableFrom(type)
                || GeneratedMessage.class.isAssignableFrom(type);
    }

    /**
     * Rejects extension-dependent messages reachable through fields, including recursive graphs.
     */
    private static void validatePayloadGraph(Descriptor root) {
        Set<Descriptor> visited = new HashSet<>();
        ArrayDeque<Descriptor> remaining = new ArrayDeque<>();
        remaining.add(root);
        while (!remaining.isEmpty()) {
            Descriptor descriptor = remaining.removeFirst();
            if (!visited.add(descriptor)) {
                continue;
            }
            if (!descriptor.getExtensions().isEmpty()
                    || !descriptor.toProto().getExtensionRangeList().isEmpty()
                    || descriptor.getOptions().getMessageSetWireFormat()) {
                throw new IllegalArgumentException(
                        "Extension-dependent payload message: " + descriptor.getFullName());
            }
            for (FieldDescriptor field : descriptor.getFields()) {
                if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    remaining.add(field.getMessageType());
                }
            }
        }
    }
}
