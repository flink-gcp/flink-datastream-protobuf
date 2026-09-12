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
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.google.protobuf.Message;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.util.Objects;

/**
 * Native type information for immutable, generated full-runtime Protobuf values.
 *
 * <p>Use {@link #of(Class)} for defaults or {@link #newBuilder(Class)} for per-type settings, for
 * example in {@code .returns(ProtobufTypeInformation.of(MyMessage.class))}. Explicit construction
 * does not register a factory. Disable Flink generic types to reject accidental generic serializer
 * selection elsewhere in the job.
 *
 * <p>Messages are supported as values, not partitioning or MapState keys. Extract stable scalar
 * keys instead. {@link #isKeyType()} is a declaration, not a universal rejection hook: both
 * inferred and explicitly typed {@code keyBy} can bypass it. Deterministic serialization does not
 * make message hashes stable across classloaders or provide canonical bytes across builds.
 *
 * <p>Versioned snapshots support unchanged-schema checkpoint/savepoint recovery, verified with
 * HashMap and RocksDB state backends in MiniCluster tests.
 *
 * <p>The class and {@code of(Class)} have an unbounded type parameter to match Flink's inherited
 * static {@code TypeInformation.of(Class)} signature. Construction still requires a supported
 * generated Message class and rejects other classes with IllegalArgumentException. The builder
 * retains its compile-time Message bound. The inherited {@code of(TypeHint)} overload instead
 * follows Flink's ordinary type extraction and can select a generic type without factory
 * registration. Use {@link #of(Class)} or {@link #newBuilder(Class)} for explicit native selection.
 *
 * @param <T> concrete generated message type, validated at construction
 */
@PublicEvolving
public final class ProtobufTypeInformation<T> extends CrossVersionProtobufTypeInformation<T> {
    private static final long serialVersionUID = 1L;

    /**
     * @serial Concrete generated application class, resolved by the job deserializer.
     */
    private final Class<T> messageClass;

    /**
     * @serial Immutable wire and reader settings.
     */
    private final ProtobufSerializerSettings settings;

    private transient ProtobufMessageType messageType;

    private ProtobufTypeInformation(Class<T> messageClass, ProtobufSerializerSettings settings) {
        this.messageClass = messageClass;
        this.settings = settings;
        messageType = resolveMessageType();
    }

    /**
     * Creates type information with deterministic writing disabled, a 64 MiB payload limit, and a
     * recursion limit of 100.
     *
     * @param messageClass public concrete full-runtime generated message class
     * @param <T> concrete generated message type
     * @return new immutable type information
     * @throws IllegalArgumentException if the class or its payload descriptor graph is unsupported
     */
    public static <T> ProtobufTypeInformation<T> of(Class<T> messageClass) {
        return new ProtobufTypeInformation<>(messageClass, ProtobufSerializerSettings.DEFAULT);
    }

    /**
     * Creates a reusable builder with the same defaults as {@link #of(Class)}.
     *
     * @param messageClass class to validate when {@link Builder#build()} is called
     * @param <T> concrete generated message type
     * @return new builder
     */
    public static <T extends Message> Builder<T> newBuilder(Class<T> messageClass) {
        return new Builder<>(messageClass);
    }

    /**
     * @return false; messages are not Flink basic types
     */
    @Override
    public boolean isBasicType() {
        return false;
    }

    /**
     * @return false; messages are not Flink tuples
     */
    @Override
    public boolean isTupleType() {
        return false;
    }

    /**
     * @return one opaque message value
     */
    @Override
    public int getArity() {
        return 1;
    }

    /**
     * @return one; Protobuf fields are not flattened into Flink fields
     */
    @Override
    public int getTotalFields() {
        return 1;
    }

    /**
     * @return the exact generated class, including its defining classloader
     */
    @Override
    public Class<T> getTypeClass() {
        return messageClass;
    }

    /**
     * Declares message keys unsupported with every setting. Flink keyBy overloads can bypass this
     * declaration; applications must extract supported scalar keys.
     *
     * @return always false
     */
    @Override
    public boolean isKeyType() {
        return false;
    }

    /**
     * Creates a native serializer with this type information's immutable settings.
     *
     * @param config Flink configuration; native settings come from this type information
     * @return a new serializer with independent mutable scratch storage
     */
    @Override
    @SuppressWarnings("unchecked")
    public TypeSerializer<T> createSerializer(SerializerConfig config) {
        // Construction and Java deserialization validate this exact class before it reaches here.
        return (TypeSerializer<T>)
                new ProtobufTypeSerializer<>(messageClass.asSubclass(Message.class), settings);
    }

    /**
     * @return the generated class name and all native serializer settings
     */
    @Override
    public String toString() {
        return "Protobuf(" + messageClass.getName() + ", " + settings + ")";
    }

    /**
     * Compares the exact class, full message name, normalized descriptor content, and all settings.
     * Instances of this final implementation share the same framing and normalization versions.
     *
     * @param other candidate type information
     * @return whether both objects describe the same native serialization
     */
    @Override
    public boolean equals(Object other) {
        return other instanceof ProtobufTypeInformation<?> that
                && messageClass == that.messageClass
                && settings.equals(that.settings)
                && messageType.fullName.equals(that.messageType.fullName)
                && messageType.schema.equals(that.messageType.schema);
    }

    /**
     * @return a hash of class, complete schema, versions, and settings
     */
    @Override
    public int hashCode() {
        return Objects.hash(
                messageClass,
                messageType.fullName,
                messageType.schema,
                ProtobufTypeSerializer.FRAMING_VERSION,
                ProtobufSchema.NORMALIZATION_VERSION,
                settings);
    }

    /**
     * Restricts equality to this final implementation.
     *
     * @param other candidate type information
     * @return whether the candidate is native Protobuf type information
     */
    @Override
    public boolean canEqual(Object other) {
        return other instanceof ProtobufTypeInformation<?>;
    }

    /** Applies the runtime Message boundary required by the inherited unbounded static factory. */
    private ProtobufMessageType resolveMessageType() {
        if (messageClass == null) {
            throw new IllegalArgumentException("messageClass must not be null");
        }
        if (!Message.class.isAssignableFrom(messageClass)) {
            throw new IllegalArgumentException(
                    "Unsupported messageClass "
                            + messageClass.getName()
                            + ": expected a full-runtime generated message");
        }
        return ProtobufMessageType.resolve(messageClass.asSubclass(Message.class));
    }

    /**
     * Reconstructs class-associated metadata without serializing parsers or classloaders.
     *
     * @param input serialized job configuration
     * @throws IOException if the configuration is corrupt or the resolved type is unsupported
     * @throws ClassNotFoundException if a serialized class cannot be resolved
     */
    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        try {
            if (settings == null) {
                throw new IllegalArgumentException("Missing serializer settings");
            }
            messageType = resolveMessageType();
        } catch (IllegalArgumentException e) {
            InvalidObjectException failure = new InvalidObjectException(e.getMessage());
            failure.initCause(e);
            throw failure;
        }
    }

    /**
     * Reusable builder for immutable native type information. Validation occurs at {@link
     * #build()}. Builders are not intended for concurrent use.
     *
     * @param <T> concrete generated message type
     */
    @PublicEvolving
    public static final class Builder<T extends Message> {
        private final Class<T> messageClass;
        private boolean deterministicSerialization =
                ProtobufSerializerSettings.DEFAULT.deterministicSerialization();
        private int maxMessageSize = ProtobufSerializerSettings.DEFAULT.maxMessageSize();
        private int recursionLimit = ProtobufSerializerSettings.DEFAULT.recursionLimit();

        private Builder(Class<T> messageClass) {
            this.messageClass = messageClass;
        }

        /**
         * Enables deterministic payload writing, without promising canonical bytes or message keys.
         *
         * @param enabled whether to enable deterministic writing; default false
         * @return this builder
         */
        public Builder<T> deterministicSerialization(boolean enabled) {
            deterministicSerialization = enabled;
            return this;
        }

        /**
         * Sets the payload byte limit, excluding the four-byte frame prefix. This is not a heap
         * bound.
         *
         * @param bytes value from 1 through Integer.MAX_VALUE; default 67108864 (64 MiB)
         * @return this builder
         */
        public Builder<T> maxMessageSize(int bytes) {
            maxMessageSize = bytes;
            return this;
        }

        /**
         * Sets the parser recursion budget, also enforced before writing a message.
         *
         * @param depth positive recursion limit with the root at depth zero; default 100
         * @return this builder
         */
        public Builder<T> recursionLimit(int depth) {
            recursionLimit = depth;
            return this;
        }

        /**
         * Validates the generated class and settings and captures an independent immutable result.
         *
         * @return new type information unaffected by subsequent builder changes
         * @throws IllegalArgumentException if the class, payload graph, or settings are unsupported
         */
        public ProtobufTypeInformation<T> build() {
            return new ProtobufTypeInformation<>(
                    messageClass,
                    new ProtobufSerializerSettings(
                            deterministicSerialization, maxMessageSize, recursionLimit));
        }
    }
}
