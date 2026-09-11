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
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.GeneratedMessageLite;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.StringValue;
import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.protobuf.generated.AmbiguousStringNames;
import io.github.flink.gcp.protobuf.generated.ExtensionDeclaration;
import io.github.flink.gcp.protobuf.generated.ExtensionHolder;
import io.github.flink.gcp.protobuf.generated.ExtensionTarget;
import io.github.flink.gcp.protobuf.generated.LazyMessage;
import io.github.flink.gcp.protobuf.generated.LegacyMessage;
import io.github.flink.gcp.protobuf.generated.MessageSetRoot;
import io.github.flink.gcp.protobuf.generated.OptionMessage;
import io.github.flink.gcp.protobuf.generated.RecursiveMessage;
import io.github.flink.gcp.protobuf.generated.RequiredMessage;
import io.github.flink.gcp.protobuf.generated.ScalarMessage;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
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
    void unicodeAndProto2RawStringsRetainTheirWireSize() throws Exception {
        ProtobufTypeSerializer<LegacyMessage> serializer =
                new ProtobufTypeSerializer<>(LegacyMessage.class);
        for (ByteString value :
                List.of(
                        ByteString.copyFromUtf8("ascii é 中 😀 �"),
                        ByteString.copyFrom(new byte[] {(byte) 0xff, (byte) 0xfe}))) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CodedOutputStream wire = CodedOutputStream.newInstance(bytes);
            for (FieldDescriptor field : LegacyMessage.getDescriptor().getFields()) {
                if (field.getType() == FieldDescriptor.Type.STRING) {
                    wire.writeBytes(field.getNumber(), value);
                    if (field.isRepeated()) {
                        wire.writeBytes(field.getNumber(), value);
                    }
                }
            }
            wire.flush();
            LegacyMessage message = LegacyMessage.parseFrom(bytes.toByteArray());
            byte[] expected = message.toByteArray();
            byte[] actual = encode(serializer, message);
            assertThat(Arrays.copyOfRange(actual, 4, actual.length)).isEqualTo(expected);
            assertThat(serializer.deserialize(input(actual)).toByteArray()).isEqualTo(expected);
        }
        // Java strings with unpaired surrogates use the runtime replacement-byte fallback.
        String broken = "a" + (char) 0xd800 + "b";
        assertPayload(new ProtobufTypeSerializer<>(StringValue.class), StringValue.of(broken));
    }

    @Test
    void ambiguousFieldNamesRetainTheirRawWireBytes() throws Exception {
        // Construct wire fields directly, independently of protoc's Java accessor naming rules.
        byte[] wire = {26, 2, (byte) 0xff, (byte) 0xfe, 34, 1, (byte) 0xff, 42, 1, 97};
        AmbiguousStringNames value = AmbiguousStringNames.parseFrom(wire);
        ProtobufTypeSerializer<AmbiguousStringNames> serializer =
                new ProtobufTypeSerializer<>(AmbiguousStringNames.class);
        assertPayload(serializer, value);
        assertThat(serializer.deserialize(input(encode(serializer, value))).toByteArray())
                .isEqualTo(value.toByteArray());
    }

    @Test
    void rawProto2StringsRespectExactSizeAndNestedDepthLimits() throws Exception {
        byte[] wire = {10, 2, (byte) 0xff, (byte) 0xfe};
        LegacyMessage leaf = LegacyMessage.parseFrom(wire);
        assertRoundTrip(configured(LegacyMessage.class, false, wire.length, 2), leaf);
        assertRejectedBeforeWrite(
                configured(LegacyMessage.class, false, wire.length - 1, 2), leaf, "maxMessageSize");
        LegacyMessage nested =
                LegacyMessage.newBuilder(leaf)
                        .setGroupValue(LegacyMessage.GroupValue.newBuilder().setChild(leaf))
                        .build();
        assertRoundTrip(configured(LegacyMessage.class, false, 4096, 2), nested);
        assertRejectedBeforeWrite(
                configured(LegacyMessage.class, false, 4096, 1), nested, "recursionLimit");
        assertPayload(new ProtobufTypeSerializer<>(LegacyMessage.class), nested);
    }

    @Test
    void independentlyConstructedSerializersReuseValidatedClassMetadata() {
        ProtobufMessageType first = ProtobufMessageType.resolve(ScalarMessage.class);
        assertThat(ProtobufMessageType.resolve(ScalarMessage.class)).isSameAs(first);
        assertThat(ProtobufMessageType.resolve(LegacyMessage.class)).isNotSameAs(first);
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
    void mismatchedGeneratedWriterCannotCompleteACorruptFrame() {
        ProtobufTypeSerializer<BrokenWriter> serializer =
                new ProtobufTypeSerializer<>(BrokenWriter.class);
        for (int count : List.of(0, 3)) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            assertThatThrownBy(
                            () ->
                                    serializer.serialize(
                                            new BrokenWriter(count),
                                            new DataOutputViewStreamWrapper(bytes)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("calculated payload size");
            assertThat(bytes.size()).isLessThanOrEqualTo(6);
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
        LegacyMessage group =
                LegacyMessage.newBuilder()
                        .setGroupValue(
                                LegacyMessage.GroupValue.newBuilder()
                                        .setNumber(-1)
                                        .setChild(LegacyMessage.newBuilder().setText("nested")))
                        .build();
        assertRoundTrip(new ProtobufTypeSerializer<>(LegacyMessage.class), group);
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
    void veryDeepValuesFailBeforeRecursiveRuntimeSizeOrInitialization() throws Exception {
        assertRejectedBeforeWrite(
                new ProtobufTypeSerializer<>(RecursiveMessage.class),
                recursive(10000),
                "recursionLimit");
        RequiredMessage required = RequiredMessage.newBuilder().setValue("leaf").buildPartial();
        for (int i = 0; i < 10000; i++) {
            // build() would recursively initialize and memoize the fixture before the test.
            required =
                    RequiredMessage.newBuilder().setValue("node").setChild(required).buildPartial();
        }
        assertRejectedBeforeWrite(
                new ProtobufTypeSerializer<>(RequiredMessage.class), required, "recursionLimit");
        LegacyMessage raw = LegacyMessage.parseFrom(new byte[] {10, 2, (byte) 0xff, (byte) 0xfe});
        LegacyMessage deepRaw = raw;
        for (int i = 0; i < 10000; i++) {
            deepRaw =
                    LegacyMessage.newBuilder(raw)
                            .setGroupValue(
                                    LegacyMessage.GroupValue.newBuilder()
                                            .setChild(deepRaw)
                                            .buildPartial())
                            .buildPartial();
        }
        // Root wire counting must wait until the iterative depth check has rejected this graph.
        assertRejectedBeforeWrite(
                new ProtobufTypeSerializer<>(LegacyMessage.class), deepRaw, "recursionLimit");
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
        assertReadRejected(new ProtobufTypeSerializer<>(RequiredMessage.class), frame(0));
        assertRejectedBeforeWrite(
                new ProtobufTypeSerializer<>(RequiredMessage.class),
                RequiredMessage.newBuilder().buildPartial(),
                "Uninitialized");
        assertRejectedBeforeWrite(serializer, null, "non-null");
    }

    @Test
    void zeroByteReadAtExhaustionStillReportsTruncationAsIOException() throws Exception {
        ProtobufTypeSerializer<ScalarMessage> serializer =
                new ProtobufTypeSerializer<>(ScalarMessage.class);
        // Flink 2.3.0 NonSpanningWrapper returns zero, rather than -1, when exhausted.
        DataInputViewStreamWrapper source =
                new DataInputViewStreamWrapper(
                        new ByteArrayInputStream(frame(3, 40, 1)) {
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
        assertThatThrownBy(serializer::snapshotConfiguration)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("#10");
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
                        WrongParser.class,
                        ExtensionTarget.class,
                        ExtensionHolder.class,
                        ExtensionDeclaration.class,
                        MessageSetRoot.class)) {
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
        private final int writtenSize;

        private BrokenWriter(int writtenSize) {
            this.writtenSize = writtenSize;
        }

        public static BrokenWriter getDefaultInstance() {
            return new BrokenWriter(2);
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
        public Map<FieldDescriptor, Object> getAllFields() {
            return Map.of(ScalarMessage.getDescriptor().findFieldByName("int32_value"), 1);
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
