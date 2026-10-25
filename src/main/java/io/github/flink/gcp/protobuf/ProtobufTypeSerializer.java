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
import com.google.protobuf.UnsafeByteOperations;

import java.io.EOFException;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.Arrays;
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
 * <p>Payloads of at most 4 KiB are encoded and decoded through one lazily allocated 4 KiB array
 * that each instance retains and also uses as the chunk for stream copy and larger reads. Larger
 * payloads are written through per-record Protobuf coded-stream buffers without payload-sized
 * storage in this serializer, and each is read into a new array of its length that decoded values
 * may share.
 *
 * <p>Each instance also retains a bounded stack of sizing frames, which hold record references only
 * during a write. Instances are not intended for concurrent use; call {@link #duplicate()} for
 * independent scratch storage and sizing frames. Generated defaults, parsers, descriptor metadata,
 * and sizing field layouts are shared by exact message class through a ClassValue cache, including
 * after Java deserialization.
 *
 * <p>Construction is internal to native type integration. Versioned snapshots support restoration
 * across unchanged schemas and supported schema changes with nondecreasing reader limits.
 *
 * @param <T> concrete generated message type
 */
@Internal
public final class ProtobufTypeSerializer<T extends Message> extends TypeSerializer<T> {
    private static final long serialVersionUID = 1L;
    static final int FRAMING_VERSION = 1;

    /**
     * Largest payload handled in the retained scratch array; also the copy and large-read chunk.
     */
    static final int SCRATCH_SIZE = 4096;

    /** First array size for a larger payload, which then doubles until it reaches the length. */
    static final int LARGE_READ_INITIAL_SIZE = 1 << 20;

    /**
     * The generated class represented by this instance.
     *
     * @serial Concrete generated application class, resolved by the job deserializer.
     */
    private final Class<T> messageClass;

    /**
     * The serialization settings retained across Java serialization.
     *
     * @serial Immutable wire and reader settings.
     */
    private final ProtobufSerializerSettings settings;

    private transient ProtobufMessageType messageType;
    private transient Parser<? extends Message> parser;
    private transient ProtobufMessageSize messageSize;
    private transient byte[] scratch;

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
        messageSize = new ProtobufMessageSize(settings, messageType.layout);
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
     * <p>Validation failures leave the target untouched. A payload of at most 4 KiB is encoded into
     * the retained scratch array before the frame is written, so an inconsistent generated writer
     * also leaves the target untouched. For a statically bounded message type, the size of such a
     * payload may come from the runtime's generated size, which this encoding confirms. Larger
     * payloads are written incrementally, and an inconsistent generated writer can leave a partial
     * frame. An I/O failure of the target can leave a partial frame for any payload. Generated
     * runtime operations may allocate their own storage.
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
            // A generated size skips required fields; the traversal names the first missing one.
            messageSize.traverse(value);
            throw new IOException("Uninitialized message: " + messageClass.getName());
        }
        if (size <= SCRATCH_SIZE) {
            byte[] payload = scratch();
            CodedOutputStream output = CodedOutputStream.newInstance(payload, 0, size);
            try {
                write(value, output);
                // The array encoder is bounded by the 4 KiB array, not the payload size, so it can
                // overrun a smaller payload without failing; negative remaining space reports that.
                int remaining = output.spaceLeft();
                if (remaining != 0) {
                    throw new IOException(
                            remaining < 0
                                    ? "Protobuf output exceeds calculated payload size"
                                    : "Protobuf output is shorter than calculated payload size");
                }
            } catch (IOException e) {
                // A generated size can hide an int overflow, which the traversal reports as the
                // exceeded limit; otherwise the writer is inconsistent with its own size.
                try {
                    messageSize.traverse(value);
                } catch (IOException limit) {
                    limit.addSuppressed(e);
                    throw limit;
                }
                throw e;
            }
            target.writeInt(size);
            target.write(payload, 0, size);
            return;
        }
        target.writeInt(size);
        ViewOutputStream frame = new ViewOutputStream(target, size);
        write(value, CodedOutputStream.newInstance(frame));
        frame.checkComplete();
    }

    /**
     * Reads exactly one frame with an empty extension registry and configured size/depth limits.
     *
     * <p>The bounded input prevents reading into an adjacent frame. A payload of at most 4 KiB is
     * read completely into the retained scratch array before parsing; parsed values copy their
     * bytes and do not refer to that array. A larger payload is copied through the scratch array
     * into a new array that the source never receives. That array starts at no more than 1 MiB and
     * doubles as bytes arrive until it has the declared length, so the arrays allocated for a
     * corrupt prefix total at most 1 MiB or less than four times the bytes present; a growth step
     * that the heap or the JVM's array-size limit cannot satisfy still fails with an error rather
     * than {@code IOException}. The serializer never writes that array again, reuses it, or exposes
     * it. Values that the runtime decodes as {@code ByteString}, such as {@code bytes} fields,
     * proto2 strings, and unknown length-delimited fields, may share that array instead of copying
     * it, so keeping any of them keeps the whole array reachable. A failed read may consume only
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
        try {
            CodedInputStream input;
            if (size <= SCRATCH_SIZE) {
                byte[] payload = scratch();
                readPayload(source, payload, 0, size, size);
                // This factory never aliases its mutable input, so decoded values copy their bytes.
                input = CodedInputStream.newInstance(payload, 0, size);
            } else {
                byte[] payload = readLargePayload(source, size);
                // Neither the source nor this serializer holds this array, so decoded bytes may
                // share it; only a decoder created from a ByteString treats its input as immutable.
                input = UnsafeByteOperations.unsafeWrap(payload).newCodedInput();
                input.enableAliasing(true);
            }
            input.setSizeLimit(settings.maxMessageSize());
            input.setRecursionLimit(settings.recursionLimit());
            T result =
                    messageClass.cast(
                            parser.parseFrom(input, ExtensionRegistryLite.getEmptyRegistry()));
            input.checkLastTagWas(0);
            // Both decoders are limited to the payload, which was read completely before parsing.
            if (!input.isAtEnd()) {
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
     * failure. The retained 4 KiB scratch array is allocated lazily and reused by this serializer
     * only.
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
        byte[] buffer = remaining > 0 ? scratch() : null;
        while (remaining > 0) {
            int count = Math.min(remaining, buffer.length);
            try {
                source.readFully(buffer, 0, count);
            } catch (EOFException e) {
                throw new IOException("Truncated Protobuf frame during stream copy", e);
            }
            // DataOutputSerializer grows for byte-array writes, but not write(DataInputView, int).
            target.write(buffer, 0, count);
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
     * Captures versioned descriptors and settings for restoration by a compatible reader.
     *
     * @return an independent snapshot of this serializer's immutable configuration
     */
    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new ProtobufTypeSerializerSnapshot<>(messageClass, settings);
    }

    /** Writes the complete generated payload, reporting writes beyond the calculated size. */
    private void write(T value, CodedOutputStream output) throws IOException {
        if (settings.deterministicSerialization()) {
            output.useDeterministicSerialization();
        }
        try {
            value.writeTo(output);
            output.flush();
        } catch (CodedOutputStream.OutOfSpaceException e) {
            throw new IOException("Protobuf output exceeds calculated payload size", e);
        }
    }

    /** Returns the lazily allocated scratch array owned by this serializer instance. */
    private byte[] scratch() {
        if (scratch == null) {
            scratch = new byte[SCRATCH_SIZE];
        }
        return scratch;
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
        messageSize = new ProtobufMessageSize(settings, messageType.layout);
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
     * Fills {@code bytes[offset, end)} from the frame, naming the bytes it lacks when truncated.
     */
    private static void readPayload(
            DataInputView source, byte[] bytes, int offset, int end, int size) throws IOException {
        while (offset < end) {
            int count = source.read(bytes, offset, end - offset);
            if (count <= 0) {
                throw truncated(size - offset);
            }
            offset += count;
        }
    }

    /**
     * Reads a payload above the scratch size through the scratch array into a new array, which
     * grows only as bytes arrive and which the source never receives.
     */
    private byte[] readLargePayload(DataInputView source, int size) throws IOException {
        byte[] chunk = scratch();
        byte[] payload = new byte[Math.min(size, LARGE_READ_INITIAL_SIZE)];
        int filled = 0;
        while (filled < size) {
            if (filled == payload.length) {
                payload = Arrays.copyOf(payload, (int) Math.min(size, 2L * filled));
            }
            int count = source.read(chunk, 0, Math.min(chunk.length, payload.length - filled));
            if (count <= 0) {
                throw truncated(size - filled);
            }
            System.arraycopy(chunk, 0, payload, filled, count);
            filled += count;
        }
        return payload;
    }

    /**
     * Reports premature exhaustion, which some Flink runtime views signal by reading zero bytes.
     */
    private static EOFException truncated(int missing) {
        return new EOFException("Truncated Protobuf payload; missing " + missing + " bytes");
    }
}
