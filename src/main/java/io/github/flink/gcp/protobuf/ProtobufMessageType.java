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
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
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
 * Immutable runtime metadata of a generated message class or a DynamicMessage root descriptor.
 *
 * <p>Generated metadata is cached by the actual application class, including its classloader. The
 * class-associated cache avoids repeated reflective lookup, parser validation, schema
 * normalization, and sizing layout construction across serializers. It does not maintain a global
 * map that keeps application classes alive. Failed resolutions are not cached. Concurrent first
 * lookups may perform redundant validation before ClassValue publishes one result.
 *
 * <p>DynamicMessage metadata is not cached. Construction keeps the application's descriptors, and
 * each rebuild from encoded descriptors creates its own; either way, the metadata has its own
 * default instance, parser, and sizing layouts.
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

    /**
     * Deterministic encoding of {@link #schema} for a DynamicMessage root, or null for a generated
     * class. Never modified after construction.
     */
    final byte[] encodedSchema;

    /** Field layouts of the payload graph, linked before construction completes. */
    final ProtobufMessageSize.Layout layout;

    private ProtobufMessageType(
            Message defaultInstance,
            Parser<? extends Message> parser,
            FileDescriptorSet schema,
            byte[] encodedSchema) {
        this.defaultInstance = defaultInstance;
        this.parser = parser;
        this.schema = schema;
        this.encodedSchema = encodedSchema;
        Descriptor descriptor = defaultInstance.getDescriptorForType();
        this.fullName = descriptor.getFullName();
        this.layout =
                encodedSchema == null
                        ? ProtobufMessageSize.layout(descriptor)
                        : ProtobufMessageSize.dynamicLayout(descriptor);
    }

    /**
     * Returns whether this metadata describes a DynamicMessage root rather than a generated class.
     */
    boolean dynamic() {
        return encodedSchema != null;
    }

    /** Names the generated class or the DynamicMessage root in diagnostics. */
    String typeName() {
        return dynamic() ? "DynamicMessage " + fullName : defaultInstance.getClass().getName();
    }

    /**
     * Resolves the metadata that a Java-serialized type information or serializer describes.
     *
     * @param messageClass serialized class, which is DynamicMessage if descriptor bytes are present
     * @param fullName serialized root name, or null for a generated class
     * @param encodedSchema serialized descriptor bytes, or null for a generated class
     * @return generated or rebuilt DynamicMessage metadata
     * @throws IllegalArgumentException if the serialized form is inconsistent or unsupported
     */
    static ProtobufMessageType resolve(
            Class<?> messageClass, String fullName, byte[] encodedSchema) {
        if (encodedSchema != null) {
            if (messageClass != DynamicMessage.class) {
                throw new IllegalArgumentException(
                        "Descriptor bytes require DynamicMessage, not " + messageClass);
            }
            return dynamic(fullName, encodedSchema);
        }
        if (messageClass == null) {
            throw new IllegalArgumentException("messageClass must not be null");
        }
        if (!Message.class.isAssignableFrom(messageClass)) {
            throw new IllegalArgumentException(
                    "Unsupported messageClass "
                            + messageClass.getName()
                            + ": expected a full-runtime generated message");
        }
        return resolve(messageClass.asSubclass(Message.class));
    }

    /**
     * Validates an application's root descriptor and keeps it for DynamicMessage values.
     *
     * <p>The normalized file closure must have a nonempty deterministic encoding within the
     * snapshot limit that rebuilds into descriptors with exactly one root of the same full name,
     * which satisfies the payload rules, as Java deserialization and snapshot restoration rebuild
     * it. The rebuilt root has the same content, so the payload rules hold for this root too.
     *
     * @param root root message descriptor supplied by the application
     * @return metadata that uses the application's descriptor instances
     * @throws IllegalArgumentException naming the root and the failed rule
     */
    static ProtobufMessageType dynamic(Descriptor root) {
        if (root == null) {
            throw new IllegalArgumentException("rootDescriptor must not be null");
        }
        try {
            FileDescriptorSet schema = ProtobufSchema.normalize(root.getFile());
            byte[] encoded = ProtobufSchema.encode(schema);
            rebuild(root.getFullName(), encoded);
            DynamicMessage defaultInstance = DynamicMessage.getDefaultInstance(root);
            return new ProtobufMessageType(
                    defaultInstance, defaultInstance.getParserForType(), schema, encoded);
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException(
                    "Unsupported root descriptor " + root.getFullName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Rebuilds DynamicMessage metadata from encoded descriptors without loading application
     * classes.
     *
     * @param fullName Protobuf full name of the root message
     * @param encodedSchema deterministic encoding of the normalized descriptor closure; retained
     *     without copying
     * @return metadata with newly built descriptors
     * @throws IllegalArgumentException naming the root and the failed rule
     */
    static ProtobufMessageType dynamic(String fullName, byte[] encodedSchema) {
        try {
            Rebuilt rebuilt = rebuild(fullName, encodedSchema);
            DynamicMessage defaultInstance = DynamicMessage.getDefaultInstance(rebuilt.root());
            return new ProtobufMessageType(
                    defaultInstance,
                    defaultInstance.getParserForType(),
                    rebuilt.schema(),
                    encodedSchema);
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException(
                    "Unsupported DynamicMessage descriptor " + fullName + ": " + e.getMessage(), e);
        }
    }

    /** Normalized descriptor content and the root message built from it. */
    private record Rebuilt(FileDescriptorSet schema, Descriptor root) {}

    /** Decodes, normalizes, and builds descriptors, then applies the payload rules to the root. */
    private static Rebuilt rebuild(String fullName, byte[] encodedSchema) throws IOException {
        ProtobufSchema.validateName(fullName, "Protobuf message full name");
        if (encodedSchema == null) {
            throw new IOException("Missing descriptor bytes");
        }
        ProtobufSchema.validateEncodedLength(encodedSchema.length);
        FileDescriptorSet normalized =
                ProtobufSchema.normalize(ProtobufSchema.decode(encodedSchema), fullName);
        Descriptor root = ProtobufSchema.resolveRootMessage(normalized, fullName);
        validatePayloadGraph(root);
        return new Rebuilt(normalized, root);
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
            if (type == DynamicMessage.class) {
                throw new IllegalArgumentException(
                        "DynamicMessage needs a root descriptor; use"
                                + " ProtobufTypeInformation.of(Descriptor)");
            }
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
            Message parsed = parser.parseFrom(new byte[0]);
            if (parsed == null
                    || parsed.getClass() != type
                    || !parsed.getDescriptorForType().equals(descriptor)) {
                throw new IllegalArgumentException(
                        "Parser does not produce the concrete message type and descriptor");
            }
            return new ProtobufMessageType(
                    instance, parser, ProtobufSchema.normalize(descriptor.getFile()), null);
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
     * Rejects payload graphs that reach a message outside a proto3 file, one that declares
     * extensions, or one that declares a field number above the specification's maximum, visiting
     * referenced message types with cycle detection.
     *
     * <p>Proto3 has no extension ranges or MessageSet wire format, so the syntax check also
     * excludes payloads that depend on them. Proto3 permits extension declarations nested in a
     * message, such as custom options, and those are still rejected. Protoc enforces the maximum
     * field number, but descriptor building checks only that a number is positive, and the writer
     * would wrap the tag of a larger number. Descriptor building also accepts group fields,
     * required fields, and extension ranges in a proto3 file, so a hand-built descriptor is checked
     * for those too.
     */
    static void validatePayloadGraph(Descriptor root) {
        Set<Descriptor> visited = new HashSet<>();
        ArrayDeque<Descriptor> remaining = new ArrayDeque<>();
        remaining.add(root);
        while (!remaining.isEmpty()) {
            Descriptor descriptor = remaining.removeFirst();
            if (!visited.add(descriptor)) {
                continue;
            }
            FileDescriptor file = descriptor.getFile();
            String syntax = file.toProto().getSyntax();
            if (!syntax.equals("proto3")) {
                // protoc leaves the syntax of a proto2 file empty.
                throw new IllegalArgumentException(
                        "Payload message "
                                + descriptor.getFullName()
                                + " is declared in "
                                + file.getName()
                                + " with syntax "
                                + (syntax.isEmpty() ? "proto2" : syntax)
                                + "; only proto3 is supported");
            }
            if (!descriptor.getExtensions().isEmpty()
                    || !descriptor.toProto().getExtensionRangeList().isEmpty()) {
                throw new IllegalArgumentException(
                        "Extension-dependent payload message: " + descriptor.getFullName());
            }
            for (FieldDescriptor field : descriptor.getFields()) {
                // Descriptor building accepts these in a proto3 file, which protoc does not.
                if (field.getType() == FieldDescriptor.Type.GROUP || field.isRequired()) {
                    throw new IllegalArgumentException(
                            "Payload field "
                                    + field.getFullName()
                                    + " is "
                                    + (field.isRequired() ? "required" : "a group")
                                    + "; proto3 declares neither");
                }
                if (field.getNumber() > ProtobufMessageSize.MAX_FIELD_NUMBER) {
                    throw new IllegalArgumentException(
                            "Payload field "
                                    + field.getFullName()
                                    + " declares number "
                                    + field.getNumber()
                                    + " above the maximum field number "
                                    + ProtobufMessageSize.MAX_FIELD_NUMBER);
                }
                if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    remaining.add(field.getMessageType());
                }
            }
        }
    }
}
