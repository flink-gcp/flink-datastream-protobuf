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

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DynamicMessage;

import java.io.EOFException;
import java.io.IOException;

/**
 * Version-1 metadata for restoring DynamicMessage value serializers from their saved descriptors.
 *
 * <p>The payload is the generated snapshot's version-1 layout without the class name: framing and
 * normalization versions, the root's full name, the four settings, and the normalized descriptor
 * closure with its SHA-256 fingerprint. Restoration rebuilds the saved descriptors without loading
 * application classes or consulting the application's descriptor. Compatibility resolution
 * delegates to the directional schema evaluator, which accepts unchanged descriptors and the
 * supported schema changes and permits only nondecreasing reader limits; deterministic writing and
 * large-payload aliasing may change in either direction. A generated-message snapshot is never
 * compatible.
 */
@Internal
public final class ProtobufDynamicSerializerSnapshot
        implements TypeSerializerSnapshot<DynamicMessage> {
    private static final int VERSION = 1;

    private String fullName;
    private ProtobufSerializerSettings settings;
    private FileDescriptorSet schema;

    /** Creates an empty snapshot for Flink's versioned metadata reader. */
    public ProtobufDynamicSerializerSnapshot() {}

    /** Captures immutable metadata without retaining descriptors or parsers. */
    ProtobufDynamicSerializerSnapshot(
            String fullName, ProtobufSerializerSettings settings, FileDescriptorSet schema) {
        this.fullName = fullName;
        this.settings = settings;
        this.schema = schema;
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
        ProtobufSchema.validateName(fullName, "Protobuf message full name");
        byte[] bytes = ProtobufSchema.encode(schema);
        out.writeInt(ProtobufTypeSerializer.FRAMING_VERSION);
        out.writeInt(ProtobufSchema.NORMALIZATION_VERSION);
        out.writeUTF(fullName);
        out.writeByte(settings.deterministicSerialization() ? 1 : 0);
        out.writeInt(settings.maxMessageSize());
        out.writeInt(settings.recursionLimit());
        out.writeByte(settings.largePayloadAliasing() ? 1 : 0);
        ProtobufGeneratedSerializerSnapshot.writeDescriptors(out, bytes);
    }

    /**
     * Validates metadata and normalizes the saved descriptors against their root, without loading
     * any application class.
     *
     * @param readVersion version read from Flink's envelope
     * @param in source positioned at the snapshot payload
     * @param userCodeClassLoader unused; restoration needs no application class
     * @throws IOException for unsupported versions, malformed metadata, corruption, or truncation
     */
    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        // A failed read must not leave a previously initialized snapshot usable.
        schema = null;
        try {
            if (readVersion != VERSION) {
                throw new IOException(
                        "Unsupported Protobuf DynamicMessage snapshot version: " + readVersion);
            }
            ProtobufGeneratedSerializerSnapshot.requireVersion(
                    in.readInt(), ProtobufTypeSerializer.FRAMING_VERSION, "framing");
            ProtobufGeneratedSerializerSnapshot.requireVersion(
                    in.readInt(), ProtobufSchema.NORMALIZATION_VERSION, "normalization");
            String savedName = in.readUTF();
            ProtobufSchema.validateName(savedName, "Protobuf message full name");
            boolean deterministic =
                    ProtobufGeneratedSerializerSnapshot.readFlag(in, "deterministic serialization");
            int maxMessageSize = in.readInt();
            int recursionLimit = in.readInt();
            boolean aliasing =
                    ProtobufGeneratedSerializerSnapshot.readFlag(in, "large-payload aliasing");
            ProtobufSerializerSettings savedSettings =
                    new ProtobufSerializerSettings(
                            deterministic, maxMessageSize, recursionLimit, aliasing);
            FileDescriptorSet decoded = ProtobufGeneratedSerializerSnapshot.readDescriptors(in);
            FileDescriptorSet savedSchema = ProtobufSchema.normalize(decoded, savedName);
            fullName = savedName;
            settings = savedSettings;
            schema = savedSchema;
        } catch (EOFException e) {
            throw new IOException("Truncated Protobuf snapshot", e);
        } catch (IOException | IllegalArgumentException e) {
            throw new IOException("Cannot read Protobuf snapshot: " + e.getMessage(), e);
        }
    }

    /**
     * Rebuilds the saved descriptors and returns a reader with the saved settings.
     *
     * @return a DynamicMessage serializer bound to the saved root descriptor
     * @throws IllegalStateException if metadata is uninitialized or the saved descriptors fail the
     *     construction rules; the message names the full name and the failed rule
     */
    @Override
    public TypeSerializer<DynamicMessage> restoreSerializer() {
        requireInitialized();
        try {
            return new ProtobufTypeSerializer<>(
                    ProtobufMessageType.dynamic(fullName, ProtobufSchema.encode(schema)), settings);
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException(
                    "Cannot restore Protobuf DynamicMessage serializer for "
                            + fullName
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }

    /**
     * Checks whether this new configuration can read values permitted by an old configuration.
     *
     * @param oldSerializerSnapshot saved metadata
     * @return compatible as is when the old snapshot is a DynamicMessage snapshot whose schema and
     *     settings the evaluator accepts, otherwise incompatible
     * @throws IllegalStateException if either Protobuf snapshot is uninitialized
     */
    @Override
    public TypeSerializerSchemaCompatibility<DynamicMessage> resolveSchemaCompatibility(
            TypeSerializerSnapshot<DynamicMessage> oldSerializerSnapshot) {
        requireInitialized();
        if (!(oldSerializerSnapshot instanceof ProtobufDynamicSerializerSnapshot old)) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
        old.requireInitialized();
        // Flink's result carries no reason; the evaluator rules are documented for users.
        return ProtobufSchemaCompatibility.evaluate(old.metadata(), metadata()).compatibleAsIs()
                ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                : TypeSerializerSchemaCompatibility.incompatible();
    }

    private ProtobufSchemaCompatibility.Metadata metadata() {
        return ProtobufSchemaCompatibility.Metadata.dynamic(fullName, settings, schema);
    }

    private void requireInitialized() {
        if (schema == null) {
            throw new IllegalStateException("Protobuf snapshot has not been initialized");
        }
    }
}
