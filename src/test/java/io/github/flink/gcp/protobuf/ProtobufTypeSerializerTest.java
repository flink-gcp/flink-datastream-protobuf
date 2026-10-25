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

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.AbstractParser;
import com.google.protobuf.ByteOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.GeneratedMessageLite;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.StringValue;
import com.google.protobuf.TextFormat;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnsafeByteOperations;
import io.github.flink.gcp.protobuf.generated.BoundedBranch;
import io.github.flink.gcp.protobuf.generated.BoundedLeaf;
import io.github.flink.gcp.protobuf.generated.BoundedMessage;
import io.github.flink.gcp.protobuf.generated.ExtensionDeclaration;
import io.github.flink.gcp.protobuf.generated.ExtensionHolder;
import io.github.flink.gcp.protobuf.generated.LazyMessage;
import io.github.flink.gcp.protobuf.generated.LazyValue;
import io.github.flink.gcp.protobuf.generated.OptionMessage;
import io.github.flink.gcp.protobuf.generated.Proto2Message;
import io.github.flink.gcp.protobuf.generated.ReachesDescriptor;
import io.github.flink.gcp.protobuf.generated.ReachesProto2;
import io.github.flink.gcp.protobuf.generated.RecursiveMessage;
import io.github.flink.gcp.protobuf.generated.ScalarMessage;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.github.flink.gcp.protobuf.ProtobufTypeSerializer.LARGE_READ_INITIAL_SIZE;
import static io.github.flink.gcp.protobuf.ProtobufTypeSerializer.SCRATCH_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufTypeSerializerTest {
    @Test
    void emptyAndAdjacentFramesDoNotReadAhead() throws Exception {
        ProtobufTypeSerializer<Empty> serializer = new ProtobufTypeSerializer<>(Empty.class);
        byte[] frame = encode(serializer, Empty.getDefaultInstance());
        assertThat(frame).containsExactly(0, 0, 0, 0);
        ByteArrayInputStream bytes =
                new ByteArrayInputStream(concat(frame, frame, new byte[] {42}));
        DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(bytes);
        assertThat(serializer.deserialize(input)).isEqualTo(Empty.getDefaultInstance());
        assertThat(bytes.available()).isEqualTo(5);
        assertThat(serializer.deserialize(Empty.getDefaultInstance(), input))
                .isEqualTo(Empty.getDefaultInstance());
        assertThat(input.readByte()).isEqualTo((byte) 42);
    }

    @Test
    void exactSizingMatchesEveryScalarAndRepeatedEncoding() throws Exception {
        Random random = new Random(819);
        ProtobufTypeSerializer<ScalarMessage> serializer =
                new ProtobufTypeSerializer<>(ScalarMessage.class);
        for (int iteration = 0; iteration < 40; iteration++) {
            ScalarMessage.Builder builder = ScalarMessage.newBuilder();
            for (FieldDescriptor field : ScalarMessage.getDescriptor().getFields()) {
                if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    continue;
                }
                if (field.isRepeated()) {
                    for (int i = 0; i < 3; i++) {
                        builder.addRepeatedField(field, scalarValue(field, random));
                    }
                } else {
                    builder.setField(field, scalarValue(field, random));
                }
            }
            assertRoundTrip(serializer, builder.build());
        }
        assertRoundTrip(
                serializer,
                ScalarMessage.newBuilder()
                        .setSelectedMessage(ScalarMessage.newBuilder().setInt32Value(-1))
                        .build());
    }

    @Test
    void unicodeStringsRetainTheirWireSize() throws Exception {
        ProtobufTypeSerializer<ScalarMessage> serializer =
                new ProtobufTypeSerializer<>(ScalarMessage.class);
        ByteString value = ByteString.copyFromUtf8("ascii é 中 😀 �");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream wire = CodedOutputStream.newInstance(bytes);
        for (FieldDescriptor field : ScalarMessage.getDescriptor().getFields()) {
            if (field.getType() == FieldDescriptor.Type.STRING) {
                wire.writeBytes(field.getNumber(), value);
                if (field.isRepeated()) {
                    wire.writeBytes(field.getNumber(), value);
                }
            }
        }
        wire.flush();
        ScalarMessage message = ScalarMessage.parseFrom(bytes.toByteArray());
        byte[] expected = message.toByteArray();
        byte[] actual = encode(serializer, message);
        assertThat(Arrays.copyOfRange(actual, 4, actual.length)).isEqualTo(expected);
        assertThat(serializer.deserialize(input(actual)).toByteArray()).isEqualTo(expected);
        // Java strings with unpaired surrogates use the runtime replacement-byte fallback.
        String broken = "a" + (char) 0xd800 + "b";
        assertPayload(new ProtobufTypeSerializer<>(StringValue.class), StringValue.of(broken));
    }

    @Test
    void independentlyConstructedSerializersReuseValidatedClassMetadata() {
        ProtobufMessageType first = ProtobufMessageType.resolve(ScalarMessage.class);
        assertThat(ProtobufMessageType.resolve(ScalarMessage.class)).isSameAs(first);
        assertThat(ProtobufMessageType.resolve(RecursiveMessage.class)).isNotSameAs(first);
        assertThat(new ProtobufTypeSerializer<>(ScalarMessage.class).createInstance())
                .isSameAs(first.defaultInstance);
    }

    @Test
    void lazyOptionsUseThePinnedGeneratedMessageEncoding() throws Exception {
        // Duplicate singular fields are parsed with last-value-wins semantics by both generators.
        byte[] duplicated = new byte[] {10, 6, 10, 1, 97, 10, 1, 98, 18, 6, 10, 1, 99, 10, 1, 100};
        LazyMessage value = LazyMessage.parseFrom(duplicated);
        assertThat(value.getChild().getValue()).isEqualTo("b");
        assertThat(value.getUnchecked().getValue()).isEqualTo("d");
        assertThat(value.toByteArray()).containsExactly(10, 3, 10, 1, 98, 18, 3, 10, 1, 100);
        assertRoundTrip(new ProtobufTypeSerializer<>(LazyMessage.class), value);
    }

    @Test
    void flinkMemoryViewsGrowAndPreserveAdjacentFramesDuringCopy() throws Exception {
        ProtobufTypeSerializer<StringValue> serializer =
                new ProtobufTypeSerializer<>(StringValue.class);
        List<StringValue> values =
                List.of(
                        StringValue.getDefaultInstance(),
                        StringValue.of("x".repeat(20000)),
                        StringValue.of("after"));
        DataOutputSerializer original = new DataOutputSerializer(1);
        for (StringValue value : values) {
            serializer.serialize(value, original);
        }
        DataInputDeserializer input = new DataInputDeserializer(original.getCopyOfBuffer());
        DataOutputSerializer copy = new DataOutputSerializer(1);
        for (int i = 0; i < values.size(); i++) {
            serializer.copy(input, copy);
        }
        assertThat(input.available()).isZero();
        assertThat(copy.getCopyOfBuffer()).isEqualTo(original.getCopyOfBuffer());
        DataInputDeserializer copiedInput = new DataInputDeserializer(copy.getCopyOfBuffer());
        for (StringValue value : values) {
            assertThat(serializer.deserialize(copiedInput)).isEqualTo(value);
        }
        assertThat(copiedInput.available()).isZero();
    }

    @Test
    void scratchBoundaryFramesMatchRuntimeBytesThroughEveryView() throws Exception {
        ProtobufTypeSerializer<ScalarMessage> serializer =
                new ProtobufTypeSerializer<>(ScalarMessage.class);
        List<ScalarMessage> values = new ArrayList<>();
        values.add(ScalarMessage.getDefaultInstance());
        for (int size :
                List.of(3, SCRATCH_SIZE - 1, SCRATCH_SIZE, SCRATCH_SIZE + 1, 5 * SCRATCH_SIZE)) {
            values.add(payloadOfSize(size));
        }
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputSerializer memory = new DataOutputSerializer(1);
        for (ScalarMessage value : values) {
            new DataOutputViewStreamWrapper(expected).writeInt(value.getSerializedSize());
            expected.write(value.toByteArray());
            serializer.serialize(value, new DataOutputViewStreamWrapper(stream));
            serializer.serialize(value, memory);
        }
        assertThat(stream.toByteArray()).isEqualTo(expected.toByteArray());
        assertThat(memory.getCopyOfBuffer()).isEqualTo(expected.toByteArray());

        byte[] frames = concat(expected.toByteArray(), new byte[] {42});
        DataInputViewStreamWrapper streamInput = input(frames);
        DataInputDeserializer memoryInput = new DataInputDeserializer(frames);
        for (ScalarMessage value : values) {
            assertThat(serializer.deserialize(streamInput)).isEqualTo(value);
            assertThat(serializer.deserialize(memoryInput)).isEqualTo(value);
        }
        assertThat(streamInput.readByte()).isEqualTo((byte) 42);
        assertThat(memoryInput.readByte()).isEqualTo((byte) 42);
    }

    @Test
    void freshAndRepeatedlyEncodedMessagesWriteIdenticalFrames() throws Exception {
        for (boolean deterministic : List.of(false, true)) {
            ProtobufTypeSerializer<RecursiveMessage> serializer =
                    configured(RecursiveMessage.class, deterministic, 64 * 1024 * 1024, 100);
            for (int bytes : List.of(16, 2 * SCRATCH_SIZE)) {
                // Generated writers memoize nested sizes during the first write of a fresh value.
                RecursiveMessage fresh = nestedPayload(bytes);
                byte[] first = encode(serializer, fresh);
                byte[] repeated = encode(serializer, fresh);
                byte[] independent = encode(serializer, nestedPayload(bytes));

                ByteArrayOutputStream runtime = new ByteArrayOutputStream();
                CodedOutputStream output = CodedOutputStream.newInstance(runtime);
                if (deterministic) {
                    output.useDeterministicSerialization();
                }
                fresh.writeTo(output);
                output.flush();
                assertThat(Arrays.copyOfRange(first, 4, first.length))
                        .isEqualTo(runtime.toByteArray());
                assertThat(repeated).isEqualTo(first);
                assertThat(independent).isEqualTo(first);
                assertThat(serializer.deserialize(input(first))).isEqualTo(fresh);
            }
        }
    }

    @Test
    void decodedValuesShareNoReusedOrCallerStorage() throws Exception {
        // Scale 1 parses from the scratch array; scale 16 parses from a fresh frame array that
        // decoded bytes may share. Each value then survives later small and large reads.
        for (int scale : List.of(1, 16)) {
            String label = "scale " + scale;
            ScalarMessage scalar =
                    ScalarMessage.newBuilder()
                            .setStringValue("first é 中 😀".repeat(scale))
                            .setBytesValue(randomBytes(512 * scale, 1))
                            .addBlobs(randomBytes(64 * scale, 2))
                            .addStrings("listed".repeat(scale))
                            .setSelectedMessage(
                                    ScalarMessage.newBuilder()
                                            .setBytesValue(randomBytes(32 * scale, 3)))
                            .setUnknownFields(
                                    UnknownFieldSet.newBuilder()
                                            .addField(
                                                    200,
                                                    UnknownFieldSet.Field.newBuilder()
                                                            .addLengthDelimited(
                                                                    randomBytes(48 * scale, 4))
                                                            .addGroup(unknownGroups(2))
                                                            .addGroup(
                                                                    lengthDelimited(
                                                                            201,
                                                                            randomBytes(
                                                                                    16 * scale, 5)))
                                                            .build())
                                            .build())
                            .build();
            assertThat(scalar.getSerializedSize() > SCRATCH_SIZE).as(label).isEqualTo(scale > 1);
            ProtobufTypeSerializer<ScalarMessage> serializer =
                    new ProtobufTypeSerializer<>(ScalarMessage.class);
            ScalarMessage decoded =
                    serializer.deserialize(new DataInputDeserializer(encode(serializer, scalar)));
            // Only the large path aliases, so its bytes values share one frame array.
            assertThat(backingArray(decoded.getBytesValue()) == backingArray(decoded.getBlobs(0)))
                    .as("%s shares the frame array", label)
                    .isEqualTo(scale > 1);
            assertDecodedValueSurvivesLaterReads(
                    label,
                    serializer,
                    scalar,
                    ScalarMessage.newBuilder().setBytesValue(filledBytes(SCRATCH_SIZE - 3)).build(),
                    ScalarMessage.newBuilder()
                            .setBytesValue(filledBytes(2 * SCRATCH_SIZE))
                            .build());
            // The full runtime parses lazy fields eagerly, so they are decoded with the frame.
            LazyMessage lazy =
                    LazyMessage.newBuilder()
                            .setChild(
                                    LazyValue.newBuilder()
                                            .setValue("lazy child ".repeat(32 * scale)))
                            .setUnchecked(
                                    LazyValue.newBuilder()
                                            .setValue("unverified ".repeat(32 * scale)))
                            .build();
            assertThat(lazy.getSerializedSize() > SCRATCH_SIZE).as(label).isEqualTo(scale > 1);
            assertDecodedValueSurvivesLaterReads(
                    label,
                    new ProtobufTypeSerializer<>(LazyMessage.class),
                    lazy,
                    LazyMessage.newBuilder()
                            .setChild(
                                    LazyValue.newBuilder().setValue("x".repeat(SCRATCH_SIZE - 16)))
                            .build(),
                    LazyMessage.newBuilder()
                            .setChild(LazyValue.newBuilder().setValue("x".repeat(2 * SCRATCH_SIZE)))
                            .build());
        }
    }

    @Test
    void framesAboveTheInitialLargeReadSizeGrowToTheirExactLength() throws Exception {
        ProtobufTypeSerializer<ScalarMessage> serializer =
                new ProtobufTypeSerializer<>(ScalarMessage.class);
        for (int size :
                List.of(
                        LARGE_READ_INITIAL_SIZE - 1,
                        LARGE_READ_INITIAL_SIZE,
                        LARGE_READ_INITIAL_SIZE + 1,
                        3 * LARGE_READ_INITIAL_SIZE + 5)) {
            ScalarMessage value = bytesPayload(size, size);
            assertThat(value.getSerializedSize()).isEqualTo(size);
            byte[] frame = encode(serializer, value);
            byte[] adjacent =
                    encode(serializer, ScalarMessage.newBuilder().setInt32Value(9).build());
            byte[] both = Arrays.copyOf(frame, frame.length + adjacent.length);
            System.arraycopy(adjacent, 0, both, frame.length, adjacent.length);
            DataInputDeserializer source = new DataInputDeserializer(both);
            ScalarMessage decoded = serializer.deserialize(source);
            assertThat(decoded).as("%d-byte payload", size).isEqualTo(value);
            assertThat(backingArray(decoded.getBytesValue())).hasSize(size);
            assertThat(serializer.deserialize(source).getInt32Value()).isEqualTo(9);
            assertThat(source.available()).isZero();
        }
    }

    @Test
    void decodedValuesSurviveSourcesThatKeepTheirReadDestinations() throws Exception {
        // Protobuf's stream decoder copies arrays that its input received for the same reason.
        ProtobufTypeSerializer<ScalarMessage> serializer =
                new ProtobufTypeSerializer<>(ScalarMessage.class);
        for (int size : List.of(64, 2 * SCRATCH_SIZE, LARGE_READ_INITIAL_SIZE + 5)) {
            ScalarMessage value = bytesPayload(size, 6);
            byte[] expected = value.toByteArray();
            List<byte[]> destinations = new ArrayList<>();
            DataInputViewStreamWrapper source =
                    new DataInputViewStreamWrapper(
                            new ByteArrayInputStream(encode(serializer, value)) {
                                @Override
                                public synchronized int read(byte[] bytes, int offset, int length) {
                                    destinations.add(bytes);
                                    return super.read(bytes, offset, length);
                                }
                            });
            ScalarMessage decoded = serializer.deserialize(source);
            for (byte[] destination : destinations) {
                Arrays.fill(destination, (byte) 0x55);
            }
            assertThat(decoded.toByteArray()).as("%d-byte payload", size).isEqualTo(expected);
        }
    }

    @Test
    void truncatedLargeFramesAllocateOnlyAroundTheBytesPresent() throws Exception {
        // Allocating each declared length up front would exceed the JVM's array limit here.
        ProtobufTypeSerializer<ScalarMessage> serializer =
                new ProtobufTypeSerializer<>(
                        ScalarMessage.class,
                        new ProtobufSerializerSettings(false, Integer.MAX_VALUE, 100));
        for (int present : List.of(0, 1, LARGE_READ_INITIAL_SIZE, 3 * LARGE_READ_INITIAL_SIZE)) {
            byte[] truncated = new byte[4 + present];
            truncated[0] = 0x7f;
            truncated[1] = truncated[2] = truncated[3] = (byte) 0xff;
            assertThatThrownBy(() -> serializer.deserialize(new DataInputDeserializer(truncated)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(
                            "Truncated Protobuf payload; missing "
                                    + (Integer.MAX_VALUE - present)
                                    + " bytes");
        }
    }

    @Test
    void duplicatesKeepIndependentScratchStorageAcrossThreads() throws Exception {
        ProtobufTypeSerializer<ScalarMessage> original =
                new ProtobufTypeSerializer<>(ScalarMessage.class);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> tasks = new ArrayList<>();
            for (int thread = 0; thread < 4; thread++) {
                ProtobufTypeSerializer<ScalarMessage> serializer = original.duplicate();
                List<ScalarMessage> values =
                        List.of(
                                ScalarMessage.newBuilder()
                                        .setInt32Value(thread)
                                        .setBytesValue(randomBytes(1000, thread))
                                        .build(),
                                ScalarMessage.newBuilder()
                                        .setInt32Value(thread)
                                        .setBytesValue(randomBytes(3 * SCRATCH_SIZE, thread))
                                        .build());
                tasks.add(
                        executor.submit(
                                () -> {
                                    for (int i = 0; i < 500; i++) {
                                        ScalarMessage value = values.get(i % values.size());
                                        byte[] frame = encode(serializer, value);
                                        DataOutputSerializer copy = new DataOutputSerializer(1);
                                        serializer.copy(input(frame), copy);
                                        assertThat(copy.getCopyOfBuffer()).isEqualTo(frame);
                                        assertThat(serializer.deserialize(input(frame)))
                                                .isEqualTo(value);
                                    }
                                    return null;
                                }));
            }
            for (Future<?> task : tasks) {
                task.get();
            }
        } finally {
            executor.shutdownNow();
            // Let a failed task's siblings stop before later tests run in this JVM.
            assertThat(executor.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
        }
    }

    @Test
    void mismatchedGeneratedWriterCannotCompleteACorruptFrame() {
        ProtobufTypeSerializer<BrokenWriter> serializer =
                new ProtobufTypeSerializer<>(BrokenWriter.class);
        // Declared bytes-field lengths giving payloads of 3, 4096 (the last array-encoded size),
        // 4097 (the first streamed size), and 8195 bytes. An empty proto3 bytes value is absent.
        for (int declared : List.of(1, SCRATCH_SIZE - 3, SCRATCH_SIZE - 2, 2 * SCRATCH_SIZE)) {
            int size =
                    CodedOutputStream.computeBytesSize(10, ByteString.copyFrom(new byte[declared]));
            for (int count : List.of(0, size - 1, size + 1)) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                assertThatThrownBy(
                                () ->
                                        serializer.serialize(
                                                new BrokenWriter(declared, count),
                                                new DataOutputViewStreamWrapper(bytes)))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(
                                count > size
                                        ? "exceeds calculated payload size"
                                        : "shorter than calculated payload size");
                if (size <= SCRATCH_SIZE) {
                    // Small frames are completed in scratch storage before any target write.
                    assertThat(bytes.size()).isZero();
                } else {
                    assertThat(bytes.size()).isLessThanOrEqualTo(4 + size);
                }
            }
        }
    }

    @Test
    void unknownFieldsAndGroupsRoundTripWithoutLoss() throws Exception {
        UnknownFieldSet unknown =
                UnknownFieldSet.newBuilder()
                        .addField(
                                200,
                                UnknownFieldSet.Field.newBuilder()
                                        .addVarint(-1)
                                        .addFixed32(-1)
                                        .addFixed64(-1)
                                        .addLengthDelimited(ByteString.copyFromUtf8("unknown"))
                                        .addGroup(unknownGroups(2))
                                        .build())
                        .build();
        ScalarMessage value =
                ScalarMessage.newBuilder().setInt32Value(12).setUnknownFields(unknown).build();
        assertRoundTrip(new ProtobufTypeSerializer<>(ScalarMessage.class), value);
    }

    @Test
    void unknownFieldsThatTheParserReadsAsMessagesAreRejectedBeforeWriting() throws Exception {
        ProtobufTypeSerializer<BoundedMessage> bounded =
                new ProtobufTypeSerializer<>(BoundedMessage.class);
        String leaf = collision(7, "BoundedMessage", "BoundedMessage.leaf");
        // Issue #120: the bytes of field 7 hold groups nested 150 deep, which the reader parses
        // as BoundedMessage.leaf and rejects at the default recursion limit of 100.
        BoundedMessage deep =
                BoundedMessage.newBuilder()
                        .setUnknownFields(lengthDelimited(7, unknownGroups(150).toByteString()))
                        .build();
        assertThatThrownBy(() -> BoundedMessage.parseFrom(deep.toByteArray()))
                .hasMessageContaining("too many levels of nesting");
        assertRejectedBeforeWrite(bounded, deep, leaf);
        // A shallow collision is rejected too, at the root, in a nested value, and in a repeated
        // value, whether or not the generated size would otherwise decide.
        BoundedMessage shallow =
                BoundedMessage.newBuilder()
                        .setId(1)
                        .setUnknownFields(lengthDelimited(7, ByteString.EMPTY))
                        .build();
        assertRejectedBeforeWrite(bounded, shallow, leaf);
        // Reading such bytes, as from state that an earlier build wrote, merges them into the
        // declared field, so the decoded value no longer collides.
        BoundedMessage decoded = BoundedMessage.parseFrom(shallow.toByteArray());
        assertThat(decoded.hasLeaf()).isTrue();
        assertRoundTrip(bounded, decoded);
        assertRejectedBeforeWrite(
                bounded,
                BoundedMessage.newBuilder()
                        .setUnknownFields(lengthDelimited(9, ByteString.EMPTY))
                        .build(),
                collision(9, "BoundedMessage", "BoundedMessage.branches"));
        assertRejectedBeforeWrite(
                bounded,
                BoundedMessage.newBuilder()
                        .setUnknownFields(lengthDelimited(11, ByteString.EMPTY))
                        .build(),
                collision(11, "BoundedMessage", "BoundedMessage.selected"));
        BoundedBranch branch =
                BoundedBranch.newBuilder()
                        .setUnknownFields(lengthDelimited(1, ByteString.EMPTY))
                        .build();
        String branchLeaf = collision(1, "BoundedBranch", "BoundedBranch.leaf");
        assertRejectedBeforeWrite(
                bounded, BoundedMessage.newBuilder().setBranch(branch).build(), branchLeaf);
        assertRejectedBeforeWrite(
                bounded,
                BoundedMessage.newBuilder()
                        .addBranches(BoundedBranch.getDefaultInstance())
                        .addBranches(branch)
                        .build(),
                branchLeaf);
        // Map entries are parsed as messages, so a map field's number collides whatever its value
        // type, and so does a message field of a map value.
        ProtobufTypeSerializer<RecursiveMessage> recursive =
                new ProtobufTypeSerializer<>(RecursiveMessage.class);
        assertRejectedBeforeWrite(
                recursive,
                RecursiveMessage.newBuilder()
                        .setUnknownFields(lengthDelimited(4, ByteString.EMPTY))
                        .build(),
                collision(4, "RecursiveMessage", "RecursiveMessage.counts"));
        assertRejectedBeforeWrite(
                recursive,
                RecursiveMessage.newBuilder()
                        .putNodes(
                                "key",
                                RecursiveMessage.newBuilder()
                                        .setUnknownFields(lengthDelimited(1, ByteString.EMPTY))
                                        .build())
                        .build(),
                collision(1, "RecursiveMessage", "RecursiveMessage.child"));

        // The parser keeps other wire types, undeclared numbers, and group content unknown.
        UnknownFieldSet kept =
                UnknownFieldSet.newBuilder()
                        .addField(
                                7,
                                UnknownFieldSet.Field.newBuilder()
                                        .addVarint(1)
                                        .addFixed32(2)
                                        .addFixed64(3)
                                        .addGroup(lengthDelimited(7, ByteString.copyFromUtf8("x")))
                                        .build())
                        .mergeFrom(lengthDelimited(100, ByteString.copyFromUtf8("y")))
                        .build();
        BoundedMessage opaque = BoundedMessage.newBuilder().setUnknownFields(kept).build();
        assertRoundTrip(bounded, opaque);
        assertThat(BoundedMessage.parseFrom(opaque.toByteArray()).getUnknownFields())
                .isEqualTo(kept);
        // Numbers of string, bytes, and packed fields are written unchanged, and the reader moves
        // valid content into those fields (issue #127).
        BoundedMessage scalar =
                BoundedMessage.newBuilder()
                        .setUnknownFields(
                                UnknownFieldSet.newBuilder()
                                        .mergeFrom(lengthDelimited(2, ByteString.copyFromUtf8("x")))
                                        .mergeFrom(lengthDelimited(4, ByteString.copyFromUtf8("p")))
                                        .mergeFrom(
                                                lengthDelimited(
                                                        5, ByteString.copyFrom(new byte[] {1})))
                                        .mergeFrom(
                                                lengthDelimited(13, ByteString.copyFromUtf8("c")))
                                        .build())
                        .build();
        byte[] frame = encode(bounded, scalar);
        assertThat(Arrays.copyOfRange(frame, 4, frame.length)).isEqualTo(scalar.toByteArray());
        assertThat(bounded.deserialize(input(frame)))
                .isEqualTo(
                        BoundedMessage.newBuilder()
                                .setText("x")
                                .setPayload(ByteString.copyFromUtf8("p"))
                                .addPacked(1)
                                .addChunks(ByteString.copyFromUtf8("c"))
                                .build());

        // A set shared with a type that does not declare the number is still checked at the
        // container that does, after the traversal has summarized it under the other type.
        UnknownFieldSet.Builder sharedBuilder = UnknownFieldSet.newBuilder();
        for (int number = 100; number <= 100 + ProtobufMessageSize.CACHE_THRESHOLD; number++) {
            sharedBuilder.addField(
                    number, UnknownFieldSet.Field.newBuilder().addVarint(number).build());
        }
        UnknownFieldSet shared =
                sharedBuilder.mergeFrom(lengthDelimited(7, ByteString.EMPTY)).build();
        BoundedLeaf holder = BoundedLeaf.newBuilder().setUnknownFields(shared).build();
        assertRoundTrip(bounded, BoundedMessage.newBuilder().setLeaf(holder).build());
        assertRejectedBeforeWrite(
                bounded,
                BoundedMessage.newBuilder().setLeaf(holder).setUnknownFields(shared).build(),
                leaf);
    }

    @Test
    void unknownFieldNumbersAboveTheSpecificationMaximumAreRejectedBeforeWriting()
            throws Exception {
        int maximum = (1 << 29) - 1;
        String rejected = "exceeds the maximum field number " + maximum;
        // The writer's tag shift wraps 2^29 + 1 to field 1, RecursiveMessage.child, so the
        // reader parses these groups as nested messages and exceeds its recursion limit.
        RecursiveMessage aliased =
                RecursiveMessage.newBuilder()
                        .setUnknownFields(
                                lengthDelimited(maximum + 2, unknownGroups(150).toByteString()))
                        .build();
        assertThatThrownBy(() -> RecursiveMessage.parseFrom(aliased.toByteArray()))
                .hasMessageContaining("too many levels of nesting");
        assertRejectedBeforeWrite(
                new ProtobufTypeSerializer<>(RecursiveMessage.class),
                aliased,
                "Unknown field number " + (maximum + 2) + " " + rejected);
        // 2^29 wraps to field zero, which the reader rejects as an invalid tag.
        RecursiveMessage zero =
                RecursiveMessage.newBuilder()
                        .setUnknownFields(lengthDelimited(maximum + 1, ByteString.EMPTY))
                        .build();
        assertThatThrownBy(() -> RecursiveMessage.parseFrom(zero.toByteArray()))
                .hasMessageContaining("invalid tag");
        assertRejectedBeforeWrite(
                new ProtobufTypeSerializer<>(RecursiveMessage.class),
                zero,
                "Unknown field number " + (maximum + 1) + " " + rejected);
        // Any wire type is rejected, also where the generated size would otherwise decide and
        // inside an unknown group.
        ProtobufTypeSerializer<BoundedMessage> bounded =
                new ProtobufTypeSerializer<>(BoundedMessage.class);
        UnknownFieldSet varint =
                UnknownFieldSet.newBuilder()
                        .addField(
                                maximum + 100,
                                UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                        .build();
        assertRejectedBeforeWrite(
                bounded, BoundedMessage.newBuilder().setUnknownFields(varint).build(), rejected);
        assertRejectedBeforeWrite(
                bounded,
                BoundedMessage.newBuilder()
                        .setUnknownFields(
                                UnknownFieldSet.newBuilder()
                                        .addField(
                                                200,
                                                UnknownFieldSet.Field.newBuilder()
                                                        .addGroup(varint)
                                                        .build())
                                        .build())
                        .build(),
                rejected);
        // The maximum itself is a valid number, through the generated size and through the
        // traversal that the recursive type always uses.
        UnknownFieldSet largest =
                UnknownFieldSet.newBuilder()
                        .addField(maximum, UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                        .build();
        BoundedMessage valid = BoundedMessage.newBuilder().setUnknownFields(largest).build();
        assertRoundTrip(bounded, valid);
        assertThat(BoundedMessage.parseFrom(valid.toByteArray()).getUnknownFields())
                .isEqualTo(largest);
        assertRoundTrip(
                new ProtobufTypeSerializer<>(RecursiveMessage.class),
                RecursiveMessage.newBuilder().setUnknownFields(largest).build());
    }

    private static String collision(int number, String type, String field) {
        return "Unknown length-delimited field "
                + number
                + " of flink.protobuf.serializer."
                + type
                + " would be parsed as message field flink.protobuf.serializer."
                + field;
    }

    private static UnknownFieldSet lengthDelimited(int number, ByteString bytes) {
        return UnknownFieldSet.newBuilder()
                .addField(
                        number,
                        UnknownFieldSet.Field.newBuilder().addLengthDelimited(bytes).build())
                .build();
    }

    @Test
    void deterministicMapWritingMatchesRuntimeWithoutCanonicalClaims() throws Exception {
        RecursiveMessage first =
                RecursiveMessage.newBuilder()
                        .putCounts("z", 1)
                        .putCounts("a", 2)
                        .putCounts("", 0)
                        .putNodes("z", recursive(1))
                        .putNodes("a", RecursiveMessage.getDefaultInstance())
                        .build();
        RecursiveMessage second =
                RecursiveMessage.newBuilder()
                        .putCounts("a", 2)
                        .putCounts("", 0)
                        .putCounts("z", 1)
                        .putNodes("a", RecursiveMessage.getDefaultInstance())
                        .putNodes("z", recursive(1))
                        .build();
        ProtobufTypeSerializer<RecursiveMessage> serializer =
                configured(RecursiveMessage.class, true, 1024, 10);
        assertThat(encode(serializer, first)).isEqualTo(encode(serializer, second));
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(expected);
        output.useDeterministicSerialization();
        first.writeTo(output);
        output.flush();
        assertThat(Arrays.copyOfRange(encode(serializer, first), 4, first.getSerializedSize() + 4))
                .isEqualTo(expected.toByteArray());
        assertRoundTrip(serializer, first);

        RecursiveMessage replacementKey =
                RecursiveMessage.newBuilder().putCounts("\ufffd", 1).build();
        assertRoundTrip(
                configured(RecursiveMessage.class, false, replacementKey.getSerializedSize(), 1),
                replacementKey);
        assertRejectedBeforeWrite(
                configured(RecursiveMessage.class, false, 8, 1),
                RecursiveMessage.newBuilder().putCounts("\ufffd" + "x".repeat(1024), 1).build(),
                "maxMessageSize");
    }

    @Test
    void sizeLimitsAreCheckedBeforeWritingOrReadingPayload() throws Exception {
        StringValue value = StringValue.of("boundary");
        int size = value.getSerializedSize();
        assertRoundTrip(configured(StringValue.class, false, size, 100), value);
        ProtobufTypeSerializer<StringValue> small =
                configured(StringValue.class, false, size - 1, 100);
        assertRejectedBeforeWrite(small, value, "maxMessageSize");
        ByteArrayInputStream bytes =
                new ByteArrayInputStream(
                        encode(new ProtobufTypeSerializer<>(StringValue.class), value));
        assertThatThrownBy(() -> small.deserialize(new DataInputViewStreamWrapper(bytes)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("frame length");
        assertThat(bytes.available()).isEqualTo(size);
    }

    @Test
    void logicalPayloadAboveFourGiBIsRejectedWithoutEncodingIt() throws Exception {
        ByteString shared = ByteString.copyFrom(new byte[1024 * 1024]);
        RecursiveMessage.Builder builder = RecursiveMessage.newBuilder();
        for (int i = 0; i < 4096; i++) {
            builder.addChunks(shared);
        }
        RecursiveMessage huge = builder.build();
        // The runtime returns a small positive int even though the logical payload exceeds 4 GiB.
        assertThat(huge.getSerializedSize()).isBetween(0, 64 * 1024 * 1024);
        assertRejectedBeforeWrite(
                new ProtobufTypeSerializer<>(RecursiveMessage.class), huge, "maxMessageSize");
    }

    @Test
    void depthLimitsMatchNestedMessagesMapsAndUnknownGroups() throws Exception {
        ProtobufTypeSerializer<RecursiveMessage> limited =
                configured(RecursiveMessage.class, false, 4096, 3);
        assertRoundTrip(limited, recursive(3));
        assertRejectedBeforeWrite(limited, recursive(4), "recursionLimit");
        assertReadRejected(
                limited,
                encode(new ProtobufTypeSerializer<>(RecursiveMessage.class), recursive(4)));
        RecursiveMessage map = RecursiveMessage.newBuilder().putNodes("node", recursive(1)).build();
        assertRoundTrip(limited, map);
        assertRejectedBeforeWrite(
                configured(RecursiveMessage.class, false, 4096, 2), map, "recursionLimit");
        RecursiveMessage defaultMap =
                RecursiveMessage.newBuilder()
                        .putNodes("node", RecursiveMessage.getDefaultInstance())
                        .build();
        assertRoundTrip(configured(RecursiveMessage.class, false, 4096, 2), defaultMap);
        assertRejectedBeforeWrite(
                configured(RecursiveMessage.class, false, 4096, 1), defaultMap, "recursionLimit");
        RecursiveMessage unknown =
                RecursiveMessage.newBuilder().setUnknownFields(unknownGroups(3)).build();
        assertRoundTrip(limited, unknown);
        RecursiveMessage deeper = RecursiveMessage.newBuilder().setChild(unknown).build();
        assertRejectedBeforeWrite(limited, deeper, "recursionLimit");
        assertReadRejected(
                limited, encode(new ProtobufTypeSerializer<>(RecursiveMessage.class), deeper));
        // Reusing a cached child at a deeper path must still consume that paths recursion budget.
        RecursiveMessage shared = recursive(2);
        RecursiveMessage reused =
                RecursiveMessage.newBuilder()
                        .addChildren(shared)
                        .addChildren(RecursiveMessage.newBuilder().setChild(shared))
                        .build();
        assertRejectedBeforeWrite(limited, reused, "recursionLimit");
        assertRoundTrip(new ProtobufTypeSerializer<>(RecursiveMessage.class), reused);
    }

    @Test
    void veryDeepValuesFailBeforeRecursiveRuntimeSize() throws Exception {
        assertRejectedBeforeWrite(
                new ProtobufTypeSerializer<>(RecursiveMessage.class),
                recursive(10000),
                "recursionLimit");
    }

    @Test
    void invalidFramesFailWithIOException() throws Exception {
        ProtobufTypeSerializer<ScalarMessage> serializer =
                new ProtobufTypeSerializer<>(ScalarMessage.class);
        for (byte[] bytes :
                List.of(
                        new byte[] {},
                        new byte[] {0, 0},
                        frame(-1),
                        frame(Integer.MAX_VALUE),
                        frame(3, 8),
                        frame(1, 0),
                        frame(1, 8),
                        frame(1, 12),
                        frame(2, 10, 127),
                        frame(1, 15))) {
            assertReadRejected(serializer, bytes);
        }
        // A frame can end at a valid tag boundary but still be shorter than its declared length.
        assertReadRejected(serializer, frame(3, 40, 1));
        assertRejectedBeforeWrite(serializer, null, "non-null");
    }

    @Test
    void zeroByteReadAtExhaustionStillReportsTruncationAsIOException() throws Exception {
        ProtobufTypeSerializer<ScalarMessage> serializer =
                new ProtobufTypeSerializer<>(ScalarMessage.class);
        for (byte[] truncated : List.of(frame(3, 40, 1), frame(SCRATCH_SIZE + 1, 40, 1))) {
            assertZeroByteExhaustionIsTruncation(serializer, truncated);
        }
    }

    private static void assertZeroByteExhaustionIsTruncation(
            ProtobufTypeSerializer<ScalarMessage> serializer, byte[] truncated) {
        // Flink 2.3.0 NonSpanningWrapper returns zero, rather than -1, when exhausted.
        DataInputViewStreamWrapper source =
                new DataInputViewStreamWrapper(
                        new ByteArrayInputStream(truncated) {
                            @Override
                            public synchronized int read() {
                                if (available() == 0) {
                                    // NonSpanningWrapper's primitive reads do not check its limit.
                                    throw new IndexOutOfBoundsException(
                                            "Primitive read past input");
                                }
                                return super.read();
                            }

                            @Override
                            public synchronized int read(byte[] bytes, int offset, int length) {
                                int count = super.read(bytes, offset, length);
                                return count < 0 ? 0 : count;
                            }
                        });
        assertThatThrownBy(() -> serializer.deserialize(source))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Truncated Protobuf payload");
    }

    @Test
    void streamCopyPreservesNoncanonicalAndMalformedPayloadBytes() throws Exception {
        ProtobufTypeSerializer<ScalarMessage> serializer =
                new ProtobufTypeSerializer<>(ScalarMessage.class);
        for (byte[] first : List.of(frame(3, 40, 129, 0), frame(1, 0), frame(0))) {
            byte[] second = frame(2, 40, 42);
            ByteArrayInputStream bytes = new ByteArrayInputStream(concat(first, second));
            DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(bytes);
            ByteArrayOutputStream copy = new ByteArrayOutputStream();
            serializer.copy(input, new DataOutputViewStreamWrapper(copy));
            assertThat(copy.toByteArray()).isEqualTo(first);
            assertThat(bytes.available()).isEqualTo(second.length);
            assertThat(serializer.deserialize(input).getInt32Value()).isEqualTo(42);
        }
        for (byte[] invalid :
                List.of(frame(-1), frame(Integer.MAX_VALUE), frame(4, 1), new byte[] {0})) {
            assertThatThrownBy(
                            () ->
                                    serializer.copy(
                                            input(invalid),
                                            new DataOutputViewStreamWrapper(
                                                    new ByteArrayOutputStream())))
                    .isInstanceOf(IOException.class);
        }
        RecursiveMessage deep = recursive(4);
        byte[] frame = encode(new ProtobufTypeSerializer<>(RecursiveMessage.class), deep);
        ByteArrayOutputStream copy = new ByteArrayOutputStream();
        configured(RecursiveMessage.class, false, 4096, 1)
                .copy(input(frame), new DataOutputViewStreamWrapper(copy));
        assertThat(copy.toByteArray()).isEqualTo(frame);
    }

    @Test
    void immutableCopiesDuplicatesAndJavaSerializationRetainSettings() throws Exception {
        ProtobufTypeSerializer<ScalarMessage> serializer =
                configured(ScalarMessage.class, true, 128, 3);
        ScalarMessage value = ScalarMessage.newBuilder().setInt32Value(42).build();
        assertThat(serializer.isImmutableType()).isTrue();
        assertThat(serializer.getLength()).isEqualTo(-1);
        assertThat(serializer.createInstance()).isSameAs(ScalarMessage.getDefaultInstance());
        assertThat(serializer.copy(value)).isSameAs(value);
        assertThat(serializer.copy(value, ScalarMessage.getDefaultInstance())).isSameAs(value);
        assertThat(serializer.duplicate()).isEqualTo(serializer).isNotSameAs(serializer);
        byte[] frame = encode(serializer, value);
        serializer.copy(input(frame), new DataOutputSerializer(1));
        ProtobufTypeSerializer<ScalarMessage> restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(serializer), getClass().getClassLoader());
        assertThat(restored).isEqualTo(serializer).hasSameHashCodeAs(serializer);
        assertRoundTrip(restored, value);
        for (ProtobufTypeSerializer<ScalarMessage> copySerializer :
                List.of(serializer.duplicate(), restored)) {
            DataOutputSerializer copied = new DataOutputSerializer(1);
            copySerializer.copy(input(frame), copied);
            assertThat(copied.getCopyOfBuffer()).isEqualTo(frame);
        }
        assertRejectedBeforeWrite(
                restored,
                ScalarMessage.newBuilder()
                        .setBytesValue(ByteString.copyFrom(new byte[129]))
                        .build(),
                "maxMessageSize");
        assertThat(serializer.snapshotConfiguration().restoreSerializer()).isEqualTo(serializer);
    }

    @Test
    void equalityIncludesClassAndEverySetting() {
        ProtobufTypeSerializer<ScalarMessage> defaults =
                new ProtobufTypeSerializer<>(ScalarMessage.class);
        assertThat(defaults)
                .isEqualTo(new ProtobufTypeSerializer<>(ScalarMessage.class))
                .hasSameHashCodeAs(new ProtobufTypeSerializer<>(ScalarMessage.class));
        for (Object other :
                List.of(
                        configured(ScalarMessage.class, true, 64 * 1024 * 1024, 100),
                        configured(ScalarMessage.class, false, 128, 100),
                        configured(ScalarMessage.class, false, 64 * 1024 * 1024, 101),
                        new ProtobufTypeSerializer<>(Empty.class),
                        "other")) {
            assertThat(defaults).isNotEqualTo(other);
        }
        assertThat(defaults).isNotEqualTo(null);
    }

    @Test
    void unsupportedClassesAndInvalidSettingsFailAtConstruction() {
        for (Class<?> type :
                List.of(
                        DynamicMessage.class,
                        Message.class,
                        String.class,
                        ScalarMessage.Builder.class,
                        GeneratedMessageLite.class,
                        LiteValue.class,
                        MissingDefault.class,
                        WrongDefaultReturn.class,
                        NullDefault.class,
                        WrongParser.class)) {
            assertThatThrownBy(() -> constructUnsupported(type))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(type.getName());
        }
        assertThatThrownBy(() -> new ProtobufTypeSerializer<>(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProtobufTypeSerializer<>(Empty.class, null))
                .isInstanceOf(IllegalArgumentException.class);
        for (int invalid : List.of(0, -1, Integer.MIN_VALUE)) {
            assertThatThrownBy(() -> new ProtobufSerializerSettings(false, invalid, 100))
                    .hasMessageContaining("maxMessageSize");
            assertThatThrownBy(() -> new ProtobufSerializerSettings(false, 100, invalid))
                    .hasMessageContaining("recursionLimit");
        }
        assertThat(new ProtobufSerializerSettings(false, Integer.MAX_VALUE, Integer.MAX_VALUE))
                .isNotNull();
        assertThat(new ProtobufTypeSerializer<>(OptionMessage.class)).isNotNull();
    }

    /** Bypasses compile-time bounds deliberately to exercise constructor validation itself. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void constructUnsupported(Class<?> type) {
        new ProtobufTypeSerializer((Class) type);
    }

    @Test
    void payloadGraphsReachingMessagesOutsideProto3FilesNameTheTypeAndFile() throws Exception {
        String proto2 =
                "Payload message flink.protobuf.serializer.Proto2Message is declared in"
                        + " proto2.proto with syntax proto2; only proto3 is supported";
        for (Class<? extends Message> type : List.of(Proto2Message.class, ReachesProto2.class)) {
            assertThatThrownBy(() -> new ProtobufTypeSerializer<>(type))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(type.getName())
                    .hasMessageEndingWith(proto2);
        }
        // protoc leaves the syntax of a proto2 file empty, including descriptor.proto's.
        assertThatThrownBy(() -> new ProtobufTypeSerializer<>(ReachesDescriptor.class))
                .hasMessageEndingWith(
                        "Payload message google.protobuf.DescriptorProto is declared in"
                                + " google/protobuf/descriptor.proto with syntax proto2; only"
                                + " proto3 is supported");
        // Both runtimes build editions descriptors without generated editions classes.
        FileDescriptor editions =
                FileDescriptor.buildFrom(
                        TextFormat.parse(
                                "name: 'editions.proto' package: 't' syntax: 'editions'"
                                        + " edition: EDITION_2023 message_type { name: 'Leaf' }",
                                FileDescriptorProto.class),
                        new FileDescriptor[0]);
        FileDescriptor reaching =
                FileDescriptor.buildFrom(
                        TextFormat.parse(
                                "name: 'root.proto' package: 't' syntax: 'proto3'"
                                        + " dependency: 'editions.proto' message_type { name: 'Root'"
                                        + " field { name: 'leaf' number: 1 label: LABEL_OPTIONAL"
                                        + " type: TYPE_MESSAGE type_name: '.t.Leaf' } }",
                                FileDescriptorProto.class),
                        new FileDescriptor[] {editions});
        for (Descriptor root :
                List.of(
                        editions.findMessageTypeByName("Leaf"),
                        reaching.findMessageTypeByName("Root"))) {
            assertThatThrownBy(() -> ProtobufMessageType.validatePayloadGraph(root))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(
                            "Payload message t.Leaf is declared in editions.proto with syntax"
                                    + " editions; only proto3 is supported");
        }
        // Proto3 permits extension declarations nested in a message, which are still rejected.
        for (Class<? extends Message> type :
                List.of(ExtensionDeclaration.class, ExtensionHolder.class)) {
            assertThatThrownBy(() -> new ProtobufTypeSerializer<>(type))
                    .hasMessageEndingWith(
                            "Extension-dependent payload message:"
                                    + " flink.protobuf.serializer.ExtensionDeclaration");
        }
        assertThatCode(
                        () ->
                                ProtobufMessageType.validatePayloadGraph(
                                        OptionMessage.getDescriptor()))
                .doesNotThrowAnyException();
    }

    @Test
    void javaSerializationReconstructsParserWithIsolatedUserCodeClassloader() throws Exception {
        URL location = ScalarMessage.class.getProtectionDomain().getCodeSource().getLocation();
        try (URLClassLoader loader =
                new URLClassLoader(new URL[] {location}, getClass().getClassLoader()) {
                    @Override
                    protected synchronized Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (!name.startsWith("io.github.flink.gcp.protobuf.generated.")) {
                            return super.loadClass(name, resolve);
                        }
                        Class<?> result = findLoadedClass(name);
                        if (result == null) {
                            result = findClass(name);
                        }
                        if (resolve) {
                            resolveClass(result);
                        }
                        return result;
                    }
                }) {
            verifyIsolatedSerializer(
                    loader.loadClass(ScalarMessage.class.getName()).asSubclass(Message.class),
                    loader);
        }
    }

    private static <T extends Message> void verifyIsolatedSerializer(
            Class<T> isolated, ClassLoader loader) throws Exception {
        ProtobufTypeSerializer<T> serializer = new ProtobufTypeSerializer<>(isolated);
        ProtobufTypeSerializer<T> restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(serializer), loader);
        T expected =
                isolated.cast(
                        isolated.getMethod("parseFrom", byte[].class)
                                .invoke(
                                        null,
                                        ScalarMessage.newBuilder()
                                                .setInt32Value(42)
                                                .build()
                                                .toByteArray()));
        T actual = restored.deserialize(input(encode(restored, expected)));
        assertThat(actual.getClass().getClassLoader()).isSameAs(loader);
        assertThat(actual).isEqualTo(expected);
        assertThat(restored)
                .isEqualTo(serializer)
                .isNotEqualTo(new ProtobufTypeSerializer<>(ScalarMessage.class));
        assertThat(restored.createInstance().getClass()).isEqualTo(isolated);
        ProtobufMessageType metadata = ProtobufMessageType.resolve(isolated);
        assertThat(metadata)
                .isSameAs(ProtobufMessageType.resolve(isolated))
                .isNotSameAs(ProtobufMessageType.resolve(ScalarMessage.class));
        assertThat(metadata.defaultInstance.getClass()).isSameAs(isolated);
    }

    private static Object scalarValue(FieldDescriptor field, Random random) {
        return switch (field.getJavaType()) {
            case INT -> random.nextInt();
            case LONG -> random.nextLong();
            case FLOAT -> Float.intBitsToFloat(random.nextInt());
            case DOUBLE -> Double.longBitsToDouble(random.nextLong());
            case BOOLEAN -> random.nextBoolean();
            case STRING -> "text é 中 😀 " + random.nextInt();
            case BYTE_STRING -> ByteString.copyFromUtf8("bytes " + random.nextInt());
            case ENUM ->
                    field.getEnumType()
                            .getValues()
                            .get(random.nextInt(field.getEnumType().getValues().size()));
            default -> throw new AssertionError(field);
        };
    }

    private static RecursiveMessage recursive(int depth) {
        RecursiveMessage result = RecursiveMessage.getDefaultInstance();
        for (int i = 0; i < depth; i++) {
            result = RecursiveMessage.newBuilder().setChild(result).build();
        }
        return result;
    }

    /** Returns a value whose only field is a {@code bytes} value giving an exact payload size. */
    private static ScalarMessage bytesPayload(int size, long seed) {
        for (int prefix = 1; prefix <= 5; prefix++) {
            int length = size - 1 - prefix;
            if (CodedOutputStream.computeUInt32SizeNoTag(length) == prefix) {
                return ScalarMessage.newBuilder().setBytesValue(randomBytes(length, seed)).build();
            }
        }
        throw new IllegalArgumentException("No bytes length gives payload size " + size);
    }

    private static UnknownFieldSet unknownGroups(int depth) {
        UnknownFieldSet result = UnknownFieldSet.getDefaultInstance();
        for (int i = 0; i < depth; i++) {
            result =
                    UnknownFieldSet.newBuilder()
                            .addField(
                                    200,
                                    UnknownFieldSet.Field.newBuilder().addGroup(result).build())
                            .build();
        }
        return result;
    }

    private static <T extends Message> ProtobufTypeSerializer<T> configured(
            Class<T> type, boolean deterministic, int size, int depth) {
        return new ProtobufTypeSerializer<>(
                type, new ProtobufSerializerSettings(deterministic, size, depth));
    }

    /** Returns a single bytes-field message whose payload has exactly the requested size. */
    private static ScalarMessage payloadOfSize(int size) {
        for (int length = size - 2; length > 0; length--) {
            ScalarMessage value =
                    ScalarMessage.newBuilder().setBytesValue(randomBytes(length, size)).build();
            if (value.getSerializedSize() == size) {
                return value;
            }
        }
        throw new AssertionError("No bytes-field payload of size " + size);
    }

    /** Builds new nested values, maps, and repeated bytes without computing any serialized size. */
    private static RecursiveMessage nestedPayload(int bytes) {
        RecursiveMessage leaf =
                RecursiveMessage.newBuilder().setPayload(randomBytes(bytes, 5)).build();
        return RecursiveMessage.newBuilder()
                .setChild(RecursiveMessage.newBuilder().setChild(leaf))
                .addChildren(leaf)
                .putNodes("z", leaf)
                .putNodes("a", RecursiveMessage.getDefaultInstance())
                .putCounts("z", 1)
                .putCounts("a", 2)
                .addChunks(randomBytes(bytes / 2, 6))
                .build();
    }

    private static ByteString randomBytes(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return ByteString.copyFrom(bytes);
    }

    private static ByteString filledBytes(int length) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) 0x55);
        return ByteString.copyFrom(bytes);
    }

    /**
     * Decodes a value, then overwrites the caller's frame and the serializer's arrays through every
     * operation that reads a small and a large frame. No data view exposes the caller's array, so
     * overwriting it is defensive; the large frame is shorter than a large value, so an array that
     * the serializer kept for large frames would be reused.
     */
    private static <T extends Message> void assertDecodedValueSurvivesLaterReads(
            String label, ProtobufTypeSerializer<T> serializer, T value, T small, T large)
            throws Exception {
        byte[] expected = value.toByteArray();
        byte[] frame = encode(serializer, value);
        byte[] smallFrame = encode(serializer, small);
        byte[] largeFrame = encode(serializer, large);
        assertThat(smallFrame.length - 4).as(label).isLessThanOrEqualTo(SCRATCH_SIZE);
        assertThat(largeFrame.length - 4).as(label).isGreaterThan(SCRATCH_SIZE);
        if (expected.length > SCRATCH_SIZE) {
            assertThat(largeFrame.length).as(label).isLessThan(frame.length);
        }
        T decoded = serializer.deserialize(new DataInputDeserializer(frame));
        Arrays.fill(frame, (byte) 0x55);
        for (byte[] overwrite : List.of(smallFrame, largeFrame)) {
            serializer.deserialize(new DataInputDeserializer(overwrite));
            serializer.copy(input(overwrite), new DataOutputSerializer(1));
        }
        assertThat(decoded.toByteArray())
                .as("%s %s after later reads", label, value.getDescriptorForType().getName())
                .isEqualTo(expected);
        assertThat(decoded).isEqualTo(value);
    }

    /** Returns the array that a ByteString passes to a lazy writer, which is its storage. */
    private static byte[] backingArray(ByteString value) throws IOException {
        byte[][] storage = new byte[1][];
        UnsafeByteOperations.unsafeWriteTo(
                value,
                new ByteOutput() {
                    @Override
                    public void write(byte value) {
                        throw new AssertionError("Expected a lazy array write");
                    }

                    @Override
                    public void write(byte[] value, int offset, int length) {
                        throw new AssertionError("Expected a lazy array write");
                    }

                    @Override
                    public void writeLazy(byte[] value, int offset, int length) {
                        storage[0] = value;
                    }

                    @Override
                    public void write(ByteBuffer value) {
                        throw new AssertionError("Expected a lazy array write");
                    }

                    @Override
                    public void writeLazy(ByteBuffer value) {
                        throw new AssertionError("Expected a lazy array write");
                    }
                });
        return storage[0];
    }

    private static <T extends Message> byte[] encode(
            ProtobufTypeSerializer<T> serializer, T message) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        serializer.serialize(message, new DataOutputViewStreamWrapper(bytes));
        return bytes.toByteArray();
    }

    private static <T extends Message> void assertPayload(
            ProtobufTypeSerializer<T> serializer, T value) throws Exception {
        byte[] bytes = encode(serializer, value);
        DataInputViewStreamWrapper input = input(bytes);
        assertThat(input.readInt()).isEqualTo(value.toByteArray().length);
        assertThat(Arrays.copyOfRange(bytes, 4, bytes.length)).isEqualTo(value.toByteArray());
    }

    private static <T extends Message> void assertRoundTrip(
            ProtobufTypeSerializer<T> serializer, T value) throws Exception {
        byte[] bytes = encode(serializer, value);
        assertThat(input(bytes).readInt()).isEqualTo(bytes.length - 4);
        assertThat(serializer.deserialize(input(bytes))).isEqualTo(value);
        assertThat(bytes.length - 4).isEqualTo(value.toByteArray().length);
    }

    private static <T extends Message> void assertRejectedBeforeWrite(
            ProtobufTypeSerializer<T> serializer, T value, String condition) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        assertThatThrownBy(
                        () -> serializer.serialize(value, new DataOutputViewStreamWrapper(bytes)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(condition);
        assertThat(bytes.size()).isZero();
    }

    private static void assertReadRejected(ProtobufTypeSerializer<?> serializer, byte[] bytes) {
        assertThatThrownBy(() -> serializer.deserialize(input(bytes)))
                .isInstanceOf(IOException.class);
    }

    private static DataInputViewStreamWrapper input(byte[] bytes) {
        return new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes));
    }

    private static byte[] frame(int length, int... payload) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(bytes);
        output.writeInt(length);
        for (int value : payload) {
            output.writeByte(value);
        }
        return bytes.toByteArray();
    }

    private static byte[] concat(byte[]... chunks) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (byte[] chunk : chunks) {
            bytes.write(chunk);
        }
        return bytes.toByteArray();
    }

    /** Handwritten lite class: rejection must happen before any lite runtime operation. */
    public static final class LiteValue extends GeneratedMessageLite<LiteValue, LiteBuilder> {
        @Override
        protected Object dynamicMethod(MethodToInvoke method, Object first, Object second) {
            throw new AssertionError("Lite runtime must not be invoked");
        }
    }

    /** Builder required by the lite generic signature. */
    public static final class LiteBuilder
            extends GeneratedMessageLite.Builder<LiteValue, LiteBuilder> {
        private LiteBuilder() {
            super(new LiteValue());
        }
    }

    /**
     * Minimal malformed full-runtime fixture. GeneratedMessageV3 is required by Protobuf 3 and
     * retained as a deprecated compatibility base by Protobuf 4.
     */
    @SuppressWarnings("deprecation")
    public abstract static class InvalidGenerated extends GeneratedMessageV3 {
        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            throw new AssertionError("No field access expected during class validation");
        }

        @Override
        public Descriptor getDescriptorForType() {
            return ScalarMessage.getDescriptor();
        }

        @Override
        protected Message.Builder newBuilderForType(BuilderParent parent) {
            throw new AssertionError("No builder expected during class validation");
        }

        @Override
        public Message.Builder newBuilderForType() {
            throw new AssertionError("No builder expected during class validation");
        }

        @Override
        public Message.Builder toBuilder() {
            throw new AssertionError("No builder expected during class validation");
        }

        @Override
        public Message getDefaultInstanceForType() {
            return this;
        }

        /**
         * Deliberately returns the wrong generated type so constructor validation must reject it.
         */
        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public Parser<? extends GeneratedMessageV3> getParserForType() {
            return (Parser) ScalarMessage.parser();
        }
    }

    /** Missing public static default accessor. */
    public static final class MissingDefault extends InvalidGenerated {}

    /** Deliberately inconsistent writer for testing the final frame-length guard. */
    public static final class BrokenWriter extends InvalidGenerated {
        private final ByteString declared;
        private final int writtenSize;

        private BrokenWriter(int declaredBytes, int writtenSize) {
            this.declared = ByteString.copyFrom(new byte[declaredBytes]);
            this.writtenSize = writtenSize;
        }

        public static BrokenWriter getDefaultInstance() {
            return new BrokenWriter(0, 2);
        }

        @Override
        public Parser<BrokenWriter> getParserForType() {
            return new AbstractParser<>() {
                @Override
                public BrokenWriter parsePartialFrom(
                        CodedInputStream input, ExtensionRegistryLite registry) {
                    return getDefaultInstance();
                }
            };
        }

        @Override
        public boolean hasField(FieldDescriptor field) {
            return field.getName().equals("bytes_value");
        }

        @Override
        public Object getField(FieldDescriptor field) {
            if (field.isRepeated()) {
                return List.of();
            }
            // Like the runtime, an absent field reads as its default value.
            return hasField(field) ? declared : field.getDefaultValue();
        }

        @Override
        public int getRepeatedFieldCount(FieldDescriptor field) {
            return 0;
        }

        @Override
        public UnknownFieldSet getUnknownFields() {
            return UnknownFieldSet.getDefaultInstance();
        }

        @Override
        public boolean isInitialized() {
            return true;
        }

        @Override
        public void writeTo(CodedOutputStream output) throws IOException {
            output.writeRawBytes(new byte[writtenSize]);
        }
    }

    /** Accessor returns a different class. */
    public static final class WrongDefaultReturn extends InvalidGenerated {
        public static ScalarMessage getDefaultInstance() {
            return ScalarMessage.getDefaultInstance();
        }
    }

    /** Accessor has the correct signature but returns null. */
    public static final class NullDefault extends InvalidGenerated {
        public static NullDefault getDefaultInstance() {
            return null;
        }
    }

    /** Default instance is valid but its parser belongs to another message. */
    public static final class WrongParser extends InvalidGenerated {
        public static WrongParser getDefaultInstance() {
            return new WrongParser();
        }
    }
}
