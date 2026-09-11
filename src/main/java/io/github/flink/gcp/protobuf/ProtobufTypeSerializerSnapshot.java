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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.Message;

import java.io.EOFException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Version-1 metadata for unchanged-schema restoration of generated Protobuf value serializers.
 *
 * <p>The persisted class name and payload layout are state-format identities. Reading metadata does
 * not load a generated class. Restoration uses Flink's supplied user-code classloader.
 * Compatibility compares complete normalized descriptors and permits only nondecreasing reader
 * limits; deterministic writing may change in either direction.
 *
 * @param <T> concrete generated message type
 */
@Internal
public final class ProtobufTypeSerializerSnapshot<T extends Message>
        implements TypeSerializerSnapshot<T> {
    static final int MAX_DESCRIPTOR_BYTES = 64 * 1024 * 1024;
    private static final int VERSION = 1;

    private String className;
    private String fullName;
    private ProtobufSerializerSettings settings;
    private FileDescriptorSet schema;
    private ClassLoader userCodeClassLoader;

    /** Creates an empty snapshot for Flink's versioned metadata reader. */
    public ProtobufTypeSerializerSnapshot() {}

    /** Captures immutable metadata without retaining the generated class or parser. */
    ProtobufTypeSerializerSnapshot(Class<T> type, ProtobufSerializerSettings settings) {
        ProtobufMessageType metadata = ProtobufMessageType.resolve(type);
        this.className = type.getName();
        this.fullName = metadata.fullName;
        this.settings = settings;
        this.schema = metadata.schema;
        this.userCodeClassLoader = type.getClassLoader();
    }

    /**
     * @return the version of the payload following Flink's class/version envelope
     */
    @Override
    public int getCurrentVersion() {
        return VERSION;
    }

    /**
     * Writes the fixed version-1 payload with deterministic descriptor bytes and their SHA-256.
     *
     * @param out destination immediately after Flink's snapshot version
     * @throws IOException if metadata is uninitialized, exceeds format limits, or cannot be written
     */
    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        if (schema == null) {
            throw new IOException("Protobuf snapshot has not been initialized");
        }
        validateName(className, "Java binary class name");
        validateName(fullName, "Protobuf message full name");
        int size = schema.getSerializedSize();
        validateDescriptorLength(size);
        byte[] bytes = new byte[size];
        CodedOutputStream coded = CodedOutputStream.newInstance(bytes);
        coded.useDeterministicSerialization();
        schema.writeTo(coded);
        coded.checkNoSpaceLeft();
        out.writeInt(ProtobufTypeSerializer.FRAMING_VERSION);
        out.writeInt(ProtobufSchema.NORMALIZATION_VERSION);
        out.writeUTF(className);
        out.writeUTF(fullName);
        out.writeByte(settings.deterministicSerialization() ? 1 : 0);
        out.writeInt(settings.maxMessageSize());
        out.writeInt(settings.recursionLimit());
        out.writeInt(bytes.length);
        out.write(bytes);
        out.write(digest(bytes));
    }

    /**
     * Validates metadata without loading the saved generated class.
     *
     * @param readVersion version read from Flink's envelope
     * @param in source positioned at the snapshot payload
     * @param userCodeClassLoader loader to use later for serializer restoration
     * @throws IOException for unsupported versions, malformed metadata, corruption, or truncation
     */
    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        // A failed read must not leave a previously initialized snapshot usable.
        schema = null;
        this.userCodeClassLoader = null;
        try {
            requireVersion(readVersion, VERSION, "snapshot");
            requireVersion(in.readInt(), ProtobufTypeSerializer.FRAMING_VERSION, "framing");
            requireVersion(in.readInt(), ProtobufSchema.NORMALIZATION_VERSION, "normalization");
            String savedClass = in.readUTF();
            String savedName = in.readUTF();
            validateName(savedClass, "Java binary class name");
            validateName(savedName, "Protobuf message full name");
            int deterministic = in.readUnsignedByte();
            if (deterministic > 1) {
                throw new IOException("Invalid deterministic serialization flag: " + deterministic);
            }
            ProtobufSerializerSettings savedSettings =
                    new ProtobufSerializerSettings(deterministic == 1, in.readInt(), in.readInt());
            int size = in.readInt();
            validateDescriptorLength(size);
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            byte[] fingerprint = new byte[32];
            in.readFully(fingerprint);
            if (!MessageDigest.isEqual(fingerprint, digest(bytes))) {
                throw new IOException("Descriptor SHA-256 fingerprint mismatch");
            }
            CodedInputStream descriptorInput = CodedInputStream.newInstance(bytes);
            // The set adds one level around files already accepted by per-file normalization.
            descriptorInput.setRecursionLimit(ProtobufSchema.DESCRIPTOR_RECURSION_LIMIT + 1);
            FileDescriptorSet decoded =
                    FileDescriptorSet.parseFrom(
                            descriptorInput, ExtensionRegistryLite.getEmptyRegistry());
            // The CodedInputStream overload leaves the top-level end-tag check to its caller.
            descriptorInput.checkLastTagWas(0);
            FileDescriptorSet savedSchema = ProtobufSchema.normalize(decoded, savedName);
            className = savedClass;
            fullName = savedName;
            settings = savedSettings;
            this.userCodeClassLoader = userCodeClassLoader;
            schema = savedSchema;
        } catch (EOFException e) {
            throw new IOException("Truncated Protobuf snapshot", e);
        } catch (IOException | IllegalArgumentException e) {
            throw new IOException("Cannot read Protobuf snapshot: " + e.getMessage(), e);
        }
    }

    /**
     * Resolves and validates the generated class using the loader retained by this snapshot.
     *
     * @return a reader with this snapshot's recorded settings
     * @throws IllegalStateException if metadata is uninitialized or the class is missing,
     *     unsupported, or has a different schema
     */
    @Override
    public TypeSerializer<T> restoreSerializer() {
        requireInitialized();
        try {
            Class<? extends Message> type =
                    Class.forName(className, false, userCodeClassLoader).asSubclass(Message.class);
            ProtobufMessageType metadata = ProtobufMessageType.resolve(type);
            if (!fullName.equals(metadata.fullName) || !schema.equals(metadata.schema)) {
                throw new IllegalArgumentException(
                        "Generated class has a different Protobuf schema");
            }
            @SuppressWarnings("unchecked")
            Class<T> restoredType = (Class<T>) type;
            return new ProtobufTypeSerializer<>(restoredType, settings);
        } catch (ClassNotFoundException | LinkageError | RuntimeException e) {
            throw new IllegalStateException(
                    "Cannot restore Protobuf serializer for "
                            + className
                            + " ("
                            + fullName
                            + "): supply a supported generated class with the saved schema through "
                            + "Flink's user-code classloader. Cause: "
                            + e.getMessage(),
                    e);
        }
    }

    /**
     * Checks whether this new configuration can read values permitted by an old configuration.
     *
     * @param oldSerializerSnapshot saved metadata, without requiring its generated class
     * @return compatible as is for identical schemas and nondecreasing limits, otherwise
     *     incompatible
     * @throws IllegalStateException if either Protobuf snapshot is uninitialized
     */
    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializerSnapshot<T> oldSerializerSnapshot) {
        requireInitialized();
        if (!(oldSerializerSnapshot instanceof ProtobufTypeSerializerSnapshot<?> old)) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
        old.requireInitialized();
        if (className.equals(old.className)
                && fullName.equals(old.fullName)
                && schema.equals(old.schema)
                && settings.maxMessageSize() >= old.settings.maxMessageSize()
                && settings.recursionLimit() >= old.settings.recursionLimit()) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
        return TypeSerializerSchemaCompatibility.incompatible();
    }

    private void requireInitialized() {
        if (schema == null) {
            throw new IllegalStateException("Protobuf snapshot has not been initialized");
        }
    }

    private static void requireVersion(int actual, int supported, String component)
            throws IOException {
        if (actual != supported) {
            throw new IOException("Unsupported Protobuf " + component + " version: " + actual);
        }
    }

    private static void validateDescriptorLength(int size) throws IOException {
        if (size <= 0 || size > MAX_DESCRIPTOR_BYTES) {
            throw new IOException(
                    "Invalid descriptor byte length: "
                            + size
                            + "; maximum is "
                            + MAX_DESCRIPTOR_BYTES);
        }
    }

    private static void validateName(String name, String field) throws IOException {
        if (name == null || name.isEmpty()) {
            throw new IOException("Empty " + field);
        }
        long length = 0;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            length += c >= 1 && c <= 127 ? 1 : c <= 2047 ? 2 : 3;
        }
        if (length > 65535) {
            throw new IOException(field + " exceeds writeUTF's 65535-byte limit");
        }
    }

    private static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Required SHA-256 algorithm is unavailable", e);
        }
    }
}
