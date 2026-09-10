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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Native serializer for immutable, generated full-runtime Protobuf values.
 *
 * <p>Frames contain a four-byte big-endian payload length and exactly that many wire bytes. The
 * serialization operation rejects null and uninitialized values, and enforces configured payload
 * and parser-depth limits before writing a frame. These limits do not bound total heap usage or
 * stack capacity. Deterministic writing does not provide canonical bytes or make messages supported
 * Flink keys.
 *
 * <p>Data views must honor their DataInput/DataOutput contracts. Runtime views that omit their own
 * bounds checks must be used through Flink's framing and exception-conversion wrappers.
 *
 * <p>Instances are not intended for concurrent use; call {@link #duplicate()} for independent
 * stream-copy buffers. Generated defaults, parsers, and descriptor metadata are shared by exact
 * message class through a ClassValue cache, including after Java deserialization.
 *
 * <p>Construction is internal to native type integration. Managed-state snapshots remain
 * unsupported until issue #10 is implemented; this intermediate implementation must not be
 * published as 0.1.0.
 *
 * @param <T> concrete generated message type
 */
@Internal
public final class ProtobufTypeSerializer<T extends Message> extends TypeSerializer<T> {
    private static final long serialVersionUID = 1L;
    static final int FRAMING_VERSION = 1;

    /**
     * @serial Concrete generated application class, resolved by the job deserializer.
     */
    private final Class<T> messageClass;

    /**
     * @serial Immutable wire and reader settings.
     */
    private final ProtobufSerializerSettings settings;

    private transient ProtobufMessageType messageType;
    private transient Parser<? extends Message> parser;
    private transient ProtobufMessageSize messageSize;
    private transient byte[] copyBuffer;

    /**
     * Creates an internal serializer with the default size, depth, and writing settings.
     *
     * @param messageClass public concrete full-runtime generated message class
     * @throws IllegalArgumentException if the class or its payload descriptor graph is unsupported
     */
    ProtobufTypeSerializer(Class<T> messageClass) {
        this(messageClass, ProtobufSerializerSettings.DEFAULT);
    }

    /**
     * Creates a serializer using class-associated metadata and the supplied immutable settings.
     *
     * @param messageClass public concrete full-runtime generated message class
     * @param settings writing and reader limits
     * @throws IllegalArgumentException if either argument or the generated metadata is unsupported
     */
    ProtobufTypeSerializer(Class<T> messageClass, ProtobufSerializerSettings settings) {
        if (settings == null) {
            throw new IllegalArgumentException("settings must not be null");
        }
        this.messageClass = messageClass;
        this.settings = settings;
        initialize();
    }

    /** Shares immutable metadata while leaving mutable stream-copy storage independent. */
    private ProtobufTypeSerializer(ProtobufTypeSerializer<T> original) {
        messageClass = original.messageClass;
        settings = original.settings;
        messageType = original.messageType;
        parser = messageType.parser;
        messageSize = new ProtobufMessageSize(settings);
    }

    /**
     * Reports that generated message values are immutable.
     *
     * @return always {@code true}
     */
    @Override
    public boolean isImmutableType() {
        return true;
    }

    /**
     * Creates a serializer sharing immutable metadata with independent mutable scratch storage.
     *
     * @return a distinct serializer with the same class and settings
     */
    @Override
    public ProtobufTypeSerializer<T> duplicate() {
        return new ProtobufTypeSerializer<>(this);
    }

    /**
     * Returns the generated default instance.
     *
     * <p>A proto2 default with missing required fields is allowed here but cannot be serialized.
     *
     * @return the default instance of the exact configured class
     */
    @Override
    public T createInstance() {
        return messageClass.cast(messageType.defaultInstance);
    }

    /**
     * Returns the input reference because supported message values are immutable.
     *
     * <p>This operation does not validate the value or enforce wire limits.
     *
     * @param from value to copy, possibly null
     * @return {@code from}, including null
     */
    @Override
    public T copy(T from) {
        return from;
    }

    /**
     * Returns the input reference without modifying the reuse value or validating either value.
     *
     * @param from immutable value to copy, possibly null
     * @param reuse ignored reuse candidate
     * @return {@code from}, including null
     */
    @Override
    public T copy(T from, T reuse) {
        return from;
    }

    /**
     * Reports variable-length framing, including the four-byte length prefix.
     *
     * @return {@code -1} because payload sizes vary
     */
    @Override
    public int getLength() {
        return -1;
    }

    /**
     * Writes one length-prefixed Protobuf payload after validating its type, size, and depth.
     *
     * <p>Validation failures leave the target untouched. An I/O failure or inconsistent generated
     * writer can leave a partial frame. No payload-sized temporary byte array is created by this
     * serializer; generated runtime operations may allocate their own storage.
     *
     * @param value non-null initialized message of the exact configured class
     * @param target destination for the frame
     * @throws IOException if validation fails, writing fails, or the writer emits an unexpected
     *     size
     */
    @Override
    public void serialize(T value, DataOutputView target) throws IOException {
        if (value == null || value.getClass() != messageClass) {
            throw new IOException("Expected non-null message of type " + messageClass.getName());
        }
        int size = messageSize.check(value);
        if (!value.isInitialized()) {
            throw new IOException("Uninitialized message: " + messageClass.getName());
        }
        target.writeInt(size);
        ViewOutputStream frame = new ViewOutputStream(target, size);
        CodedOutputStream output = CodedOutputStream.newInstance(frame);
        if (settings.deterministicSerialization()) {
            output.useDeterministicSerialization();
        }
        value.writeTo(output);
        output.flush();
        frame.checkComplete();
    }

    /**
     * Reads exactly one frame with an empty extension registry and configured size/depth limits.
     *
     * <p>The bounded input prevents reading into an adjacent frame. A failed read may consume only
     * part of the current frame; callers must not assume input resynchronization after failure.
     *
     * @param source source positioned at a frame length prefix
     * @return an initialized message of the configured class
     * @throws IOException if the prefix or payload is invalid, truncated, exceeds limits, or cannot
     *     be read
     */
    @Override
    public T deserialize(DataInputView source) throws IOException {
        int size = readLength(source);
        FrameInputStream frame = new FrameInputStream(source, size);
        CodedInputStream input = CodedInputStream.newInstance(frame);
        input.setSizeLimit(settings.maxMessageSize());
        input.setRecursionLimit(settings.recursionLimit());
        try {
            T result =
                    messageClass.cast(
                            parser.parseFrom(input, ExtensionRegistryLite.getEmptyRegistry()));
            input.checkLastTagWas(0);
            if (!input.isAtEnd() || input.getTotalBytesRead() != size || frame.remaining != 0) {
                throw new IOException("Protobuf parser did not consume the complete frame");
            }
            return result;
        } catch (IOException e) {
            throw new IOException(
                    "Invalid Protobuf frame for " + messageClass.getName() + ": " + e.getMessage(),
                    e);
        }
    }

    /**
     * Reads one frame using {@link #deserialize(DataInputView)} without modifying the reuse value.
     *
     * @param reuse ignored immutable reuse candidate
     * @param source source positioned at a frame length prefix
     * @return the parsed message
     * @throws IOException if frame validation or reading fails
     */
    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    /**
     * Copies the original frame without parsing its payload.
     *
     * <p>Length and truncation are checked; payload semantics and nesting are not validated. A
     * failed copy may have written a partial frame. No input resynchronization is promised after
     * failure. A private 4 KiB buffer is allocated lazily and reused by this serializer only.
     *
     * @param source source positioned at a frame prefix
     * @param target destination for the unchanged prefix and payload bytes
     * @throws IOException if the length is invalid, the payload is truncated, or copying fails
     */
    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int size = readLength(source);
        target.writeInt(size);
        int remaining = size;
        if (remaining > 0 && copyBuffer == null) {
            copyBuffer = new byte[4096];
        }
        while (remaining > 0) {
            int count = Math.min(remaining, copyBuffer.length);
            try {
                source.readFully(copyBuffer, 0, count);
            } catch (EOFException e) {
                throw new IOException("Truncated Protobuf frame during stream copy", e);
            }
            // DataOutputSerializer grows for byte-array writes, but not write(DataInputView, int).
            target.write(copyBuffer, 0, count);
            remaining -= count;
        }
    }

    /**
     * Compares exact class identity, normalized descriptor content, and every serializer setting.
     *
     * <p>Class identity includes the defining classloader. Equality does not establish persisted
     * state compatibility; that decision belongs to the snapshot implementation.
     *
     * @param other object to compare
     * @return whether both serializers have the same configuration and message identity
     */
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ProtobufTypeSerializer<?> that)) {
            return false;
        }
        return messageClass == that.messageClass
                && settings.equals(that.settings)
                && messageType.fullName.equals(that.messageType.fullName)
                && messageType.schema.equals(that.messageType.schema);
    }

    /**
     * Hashes message identity, settings, and the framing/normalization versions.
     *
     * @return a hash consistent with {@link #equals(Object)}
     */
    @Override
    public int hashCode() {
        return Objects.hash(
                messageClass,
                messageType.fullName,
                messageType.schema,
                FRAMING_VERSION,
                ProtobufSchema.NORMALIZATION_VERSION,
                settings);
    }

    /**
     * Rejects snapshot creation until the versioned snapshot implementation in issue #10 is
     * available.
     *
     * @return never returns normally
     * @throws UnsupportedOperationException because managed-state snapshots are not implemented yet
     */
    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        throw new UnsupportedOperationException(
                "Protobuf managed-state snapshots require issue #10; unavailable before that implementation");
    }

    /** Reads and validates a frame prefix before allocating payload-dependent storage. */
    private int readLength(DataInputView source) throws IOException {
        final int size;
        try {
            size = source.readInt();
        } catch (EOFException e) {
            throw new IOException("Truncated Protobuf frame length", e);
        }
        if (size < 0 || size > settings.maxMessageSize()) {
            throw new IOException(
                    "Invalid Protobuf frame length "
                            + size
                            + "; maxMessageSize="
                            + settings.maxMessageSize());
        }
        return size;
    }

    /**
     * Resolves metadata for the application class and creates serializer-local validation state.
     */
    private void initialize() {
        messageType = ProtobufMessageType.resolve(messageClass);
        parser = messageType.parser;
        messageSize = new ProtobufMessageSize(settings);
    }

    /**
     * Reconstructs runtime metadata from the generated class resolved by the object input stream.
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
            initialize();
        } catch (IllegalArgumentException e) {
            InvalidObjectException failure = new InvalidObjectException(e.getMessage());
            failure.initCause(e);
            throw failure;
        }
    }

    /** Enforces the exact prevalidated payload length while writing directly to a Flink view. */
    private static final class ViewOutputStream extends OutputStream {
        private final DataOutputView target;
        private final int expectedSize;
        private long written;

        private ViewOutputStream(DataOutputView target, int expectedSize) {
            this.target = target;
            this.expectedSize = expectedSize;
        }

        @Override
        public void write(int value) throws IOException {
            checkCapacity(1);
            target.write(value);
            written++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            checkCapacity(length);
            target.write(bytes, offset, length);
            written += length;
        }

        private void checkCapacity(int count) throws IOException {
            if (written + count > expectedSize) {
                throw new IOException("Protobuf output exceeds calculated payload size");
            }
        }

        private void checkComplete() throws IOException {
            if (written != expectedSize) {
                throw new IOException("Protobuf output is shorter than calculated payload size");
            }
        }
    }

    /**
     * Bounds reads to one payload and turns premature view exhaustion into a truncation failure.
     */
    private static final class FrameInputStream extends InputStream {
        private final DataInputView source;
        private int remaining;

        private FrameInputStream(DataInputView source, int remaining) {
            this.source = source;
            this.remaining = remaining;
        }

        @Override
        public int read() throws IOException {
            if (remaining == 0) {
                return -1;
            }
            try {
                int value = source.readUnsignedByte();
                remaining--;
                return value;
            } catch (EOFException e) {
                throw new EOFException(
                        "Truncated Protobuf payload; missing " + remaining + " bytes");
            }
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            if (length == 0) {
                return 0;
            }
            if (remaining == 0) {
                return -1;
            }
            int count = source.read(bytes, offset, Math.min(length, remaining));
            // Some Flink runtime views return zero, rather than -1, at exhaustion.
            if (count <= 0) {
                throw new EOFException(
                        "Truncated Protobuf payload; missing " + remaining + " bytes");
            }
            remaining -= count;
            return count;
        }
    }
}
