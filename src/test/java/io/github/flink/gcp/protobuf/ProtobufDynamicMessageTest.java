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

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.protobuf.generated.BoundedLeaf;
import io.github.flink.gcp.protobuf.generated.BoundedMessage;
import io.github.flink.gcp.protobuf.generated.ExtensionDeclaration;
import io.github.flink.gcp.protobuf.generated.ExtensionHolder;
import io.github.flink.gcp.protobuf.generated.Proto2Message;
import io.github.flink.gcp.protobuf.generated.ReachesProto2;
import io.github.flink.gcp.protobuf.generated.RecursiveMessage;
import io.github.flink.gcp.protobuf.generated.ScalarMessage;
import io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotMessages.Envelope;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static io.github.flink.gcp.protobuf.DynamicDescriptors.field;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.file;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.javaRoundTrip;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.message;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.messageField;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.rebuild;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufDynamicMessageTest {
    private static final SerializerConfigImpl CONFIG = new SerializerConfigImpl();

    @Test
    void typeInformationIsIdentifiedByDescriptorContentAndKeepsTheApplicationDescriptor()
            throws Exception {
        Descriptor root = rebuild(Envelope.getDescriptor());
        ProtobufTypeInformation<DynamicMessage> info = ProtobufTypeInformation.of(root);
        assertThat(info.getTypeClass()).isEqualTo(DynamicMessage.class);
        assertThat(info.isKeyType()).isFalse();
        assertThat(info.getArity()).isOne();
        assertThat(info.toString())
                .contains("DynamicMessage flink.protobuf.snapshot.Envelope")
                .contains("maxMessageSize=67108864");
        // Equal content behind another instance is the same type; the defaults match of(Class).
        ProtobufTypeInformation<DynamicMessage> other =
                ProtobufTypeInformation.newBuilder(rebuild(root)).build();
        assertThat(other).isEqualTo(info).hasSameHashCodeAs(info);
        assertThat(ProtobufTypeInformation.newBuilder(root).maxMessageSize(128).build())
                .isNotEqualTo(info);
        // The generated type of the same descriptor content is a different type.
        assertThat(info).isNotEqualTo(ProtobufTypeInformation.of(Envelope.class));
        assertThat(ProtobufTypeInformation.of(Envelope.class)).isNotEqualTo(info);

        TypeSerializer<DynamicMessage> serializer = info.createSerializer(CONFIG);
        assertThat(serializer)
                .isInstanceOf(ProtobufTypeSerializer.class)
                .isEqualTo(other.createSerializer(CONFIG))
                .hasSameHashCodeAs(other.createSerializer(CONFIG))
                .isNotEqualTo(ProtobufTypeInformation.of(Envelope.class).createSerializer(CONFIG))
                .isNotEqualTo(
                        ProtobufTypeInformation.newBuilder(root)
                                .deterministicSerialization(true)
                                .build()
                                .createSerializer(CONFIG));
        assertThat(serializer.isImmutableType()).isTrue();
        DynamicMessage instance = serializer.createInstance();
        assertThat(instance.getDescriptorForType()).isSameAs(root);
        assertThat(serializer.copy(instance)).isSameAs(instance);
        assertThat(serializer.duplicate()).isEqualTo(serializer).isNotSameAs(serializer);
    }

    @Test
    void constructionRejectsMissingAndUnsupportedDescriptorsNamingTheRule() throws Exception {
        assertConstructionFails(null, "rootDescriptor must not be null");
        assertConstructionFails(
                Proto2Message.getDescriptor(), "Proto2Message", "only proto3 is supported");
        assertConstructionFails(
                ReachesProto2.getDescriptor(), "ReachesProto2", "only proto3 is supported");
        assertConstructionFails(
                ExtensionDeclaration.getDescriptor(),
                "ExtensionDeclaration",
                "Extension-dependent payload message");
        assertConstructionFails(
                ExtensionHolder.getDescriptor(),
                "ExtensionHolder",
                "Extension-dependent payload message");

        // Descriptor building accepts a number whose tag would wrap; construction does not.
        FileDescriptorProto large =
                file("dynamic/large.proto")
                        .addMessageType(
                                message(
                                        "Large",
                                        field(
                                                "value",
                                                ProtobufMessageSize.MAX_FIELD_NUMBER + 1,
                                                FieldDescriptorProto.Type.TYPE_INT32)))
                        .addMessageType(message("Holder", messageField("large", 1, "Large")))
                        .build();
        for (String name : List.of("Large", "Holder")) {
            assertConstructionFails(
                    DynamicDescriptors.build(DynamicDescriptors.PACKAGE + "." + name, large),
                    name,
                    "Payload field flink.protobuf.dynamic.Large.value declares number 536870912"
                            + " above the maximum field number 536870911");
        }
        // Descriptor building accepts these in a proto3 file, which protoc does not.
        FileDescriptorProto.Builder invalid =
                file("dynamic/invalid.proto")
                        .addMessageType(
                                message(
                                                "Grouped",
                                                field(
                                                                "group",
                                                                1,
                                                                FieldDescriptorProto.Type
                                                                        .TYPE_GROUP)
                                                        .setTypeName(
                                                                ".flink.protobuf.dynamic.Grouped.Group"))
                                        .addNestedType(message("Group")))
                        .addMessageType(
                                message(
                                        "Required",
                                        field("value", 1, FieldDescriptorProto.Type.TYPE_INT32)
                                                .setLabel(
                                                        FieldDescriptorProto.Label.LABEL_REQUIRED)))
                        .addMessageType(
                                message("Extensible")
                                        .addExtensionRange(
                                                DescriptorProto.ExtensionRange.newBuilder()
                                                        .setStart(100)
                                                        .setEnd(200)));
        assertConstructionFails(
                DynamicDescriptors.build(DynamicDescriptors.PACKAGE + ".Grouped", invalid.build()),
                "Payload field flink.protobuf.dynamic.Grouped.group is a group");
        assertConstructionFails(
                DynamicDescriptors.build(DynamicDescriptors.PACKAGE + ".Required", invalid.build()),
                "Payload field flink.protobuf.dynamic.Required.value is required");
        assertConstructionFails(
                DynamicDescriptors.build(
                        DynamicDescriptors.PACKAGE + ".Extensible", invalid.build()),
                "Extension-dependent payload message: flink.protobuf.dynamic.Extensible");

        // The largest number in the specification is accepted.
        ProtobufTypeInformation.of(
                DynamicDescriptors.build(
                        DynamicDescriptors.PACKAGE + ".Largest",
                        file("dynamic/largest.proto")
                                .addMessageType(
                                        message(
                                                "Largest",
                                                field(
                                                        "value",
                                                        ProtobufMessageSize.MAX_FIELD_NUMBER,
                                                        FieldDescriptorProto.Type.TYPE_INT32)))
                                .build()));

        // A placeholder for an unresolved import cannot be rebuilt from the descriptor content.
        Descriptor placeholder =
                FileDescriptor.buildFrom(
                                file("dynamic/placeholder.proto")
                                        .addDependency("dynamic/missing.proto")
                                        .addMessageType(
                                                message(
                                                        "Placeholder",
                                                        messageField("missing", 1, "Missing")))
                                        .build(),
                                new FileDescriptor[0],
                                true)
                        .findMessageTypeByName("Placeholder");
        assertConstructionFails(
                placeholder,
                "Placeholder",
                "Unresolved descriptor import: dynamic/placeholder.proto -> dynamic/missing.proto");

        String longName = "N" + "n".repeat(65535);
        assertConstructionFails(
                DynamicDescriptors.build(
                        DynamicDescriptors.PACKAGE + "." + longName,
                        file("dynamic/long.proto").addMessageType(message(longName)).build()),
                longName,
                "exceeds writeUTF's 65535-byte limit");

        FileOptions oversized =
                FileOptions.newBuilder()
                        .setUnknownFields(
                                UnknownFieldSet.newBuilder()
                                        .addField(
                                                50000,
                                                UnknownFieldSet.Field.newBuilder()
                                                        .addLengthDelimited(
                                                                ByteString.copyFrom(
                                                                        new byte
                                                                                [ProtobufSchema
                                                                                        .MAX_DESCRIPTOR_BYTES]))
                                                        .build())
                                        .build())
                        .build();
        assertConstructionFails(
                DynamicDescriptors.build(
                        DynamicDescriptors.PACKAGE + ".Oversized",
                        file("dynamic/oversized.proto")
                                .setOptions(oversized)
                                .addMessageType(message("Oversized"))
                                .build()),
                "Oversized",
                "Invalid descriptor byte length");
    }

    @Test
    void javaSerializationRebuildsDescriptorsWithoutGeneratedClasses() throws Exception {
        Descriptor root = rebuild(Envelope.getDescriptor());
        ProtobufTypeInformation<DynamicMessage> info =
                ProtobufTypeInformation.newBuilder(root).recursionLimit(10).build();
        ProtobufTypeInformation<DynamicMessage> restoredInfo = javaRoundTrip(info);
        assertThat(restoredInfo).isEqualTo(info).hasSameHashCodeAs(info);
        Descriptor restoredRoot =
                restoredInfo.createSerializer(CONFIG).createInstance().getDescriptorForType();
        assertThat(restoredRoot).isNotSameAs(root);
        assertThat(restoredRoot.getFullName()).isEqualTo(root.getFullName());
        // Imported dependencies are rebuilt as well.
        assertThat(restoredRoot.findFieldByName("detail").getMessageType().getFile().getName())
                .isEqualTo("snapshot_dependency.proto");

        TypeSerializer<DynamicMessage> serializer = info.createSerializer(CONFIG);
        TypeSerializer<DynamicMessage> restored = javaRoundTrip(serializer);
        assertThat(restored).isEqualTo(serializer).hasSameHashCodeAs(serializer);
        Descriptor rebuilt = restored.createInstance().getDescriptorForType();
        assertThat(rebuilt).isNotSameAs(root).isNotSameAs(restoredRoot);
        // A duplicate shares the rebuilt descriptors, and decoded values carry them.
        TypeSerializer<DynamicMessage> duplicate = restored.duplicate();
        assertThat(duplicate.createInstance().getDescriptorForType()).isSameAs(rebuilt);
        DynamicMessage value = dynamic(root, envelope());
        DynamicMessage decoded = roundTrip(restored, value);
        assertThat(decoded.getDescriptorForType()).isSameAs(rebuilt);
        assertThat(decoded.toByteString()).isEqualTo(value.toByteString());
        assertThat(roundTrip(duplicate, decoded).getDescriptorForType()).isSameAs(rebuilt);
    }

    @Test
    void javaDeserializationRejectsCorruptDescriptorMetadata() throws Exception {
        ProtobufTypeInformation<DynamicMessage> info =
                ProtobufTypeInformation.of(rebuild(Envelope.getDescriptor()));
        ProtobufTypeSerializer<DynamicMessage> serializer =
                (ProtobufTypeSerializer<DynamicMessage>) info.createSerializer(CONFIG);
        for (Object original : List.of(info, serializer)) {
            assertCorruptFails(
                    original,
                    "fullName",
                    "flink.protobuf.snapshot.Missing",
                    "Missing root message: flink.protobuf.snapshot.Missing");
            assertCorruptFails(original, "fullName", null, "Empty Protobuf message full name");
            assertCorruptFails(
                    original,
                    "descriptorSet",
                    new byte[] {(byte) 0xff},
                    "Unsupported DynamicMessage descriptor flink.protobuf.snapshot.Envelope: While"
                            + " parsing a protocol message, the input ended unexpectedly");
            assertCorruptFails(
                    original, "descriptorSet", new byte[0], "Invalid descriptor byte length: 0");
            assertCorruptFails(
                    original,
                    "messageClass",
                    ScalarMessage.class,
                    "Descriptor bytes require DynamicMessage");
            assertCorruptFails(original, "settings", null, "Missing serializer settings");
        }
    }

    @Test
    void roundTripsRecursiveImportedNestedMapOneofAndUnknownFieldValues() throws Exception {
        List<Message> values =
                List.of(
                        envelope(),
                        RecursiveMessage.newBuilder()
                                .setChild(RecursiveMessage.newBuilder().setPayload(bytes(3)))
                                .addChildren(RecursiveMessage.getDefaultInstance())
                                .putNodes(
                                        "node",
                                        RecursiveMessage.newBuilder().putCounts("a", 1).build())
                                .putCounts("b", 2)
                                .addChunks(bytes(2))
                                .build(),
                        ScalarMessage.newBuilder()
                                .setStringValue("text")
                                .setChoice(ScalarMessage.Choice.NEGATIVE)
                                .addPackedValues(-1)
                                .addUnpackedValues(5)
                                .setSelectedMessage(ScalarMessage.newBuilder().setBoolValue(true))
                                .build(),
                        BoundedMessage.newBuilder()
                                .setNote("")
                                .putCounts("c", 3)
                                .setSelectedText("selected")
                                .setPayload(bytes(5000))
                                .build());
        for (Message generated : values) {
            Descriptor root = rebuild(generated.getDescriptorForType());
            TypeSerializer<DynamicMessage> serializer =
                    ProtobufTypeInformation.of(root).createSerializer(CONFIG);
            Descriptor own = serializer.createInstance().getDescriptorForType();
            for (Descriptor descriptor : List.of(own, rebuild(root))) {
                DynamicMessage value = dynamic(descriptor, generated);
                DynamicMessage decoded = roundTrip(serializer, value);
                assertThat(decoded.getDescriptorForType()).isSameAs(own);
                assertThat(decoded.toByteString()).isEqualTo(generated.toByteString());
                assertThat(decoded).isEqualTo(dynamic(own, generated));
            }
        }
    }

    @Test
    void writesAndSizesByteArraysInBytesFields() throws Exception {
        // BoundedLeaf may use the generated size; RecursiveMessage is always traversed.
        for (Descriptor descriptor :
                List.of(
                        rebuild(BoundedLeaf.getDescriptor()),
                        rebuild(RecursiveMessage.getDescriptor()))) {
            boolean leaf = descriptor.getName().equals("BoundedLeaf");
            FieldDescriptor single = descriptor.findFieldByName(leaf ? "data" : "payload");
            FieldDescriptor repeated = descriptor.findFieldByName("chunks");
            TypeSerializer<DynamicMessage> serializer =
                    ProtobufTypeInformation.of(descriptor).createSerializer(CONFIG);
            for (int length : List.of(0, 3, 5000)) {
                DynamicMessage value =
                        DynamicMessage.newBuilder(descriptor)
                                .setField(single, new byte[length])
                                .addRepeatedField(repeated, new byte[] {1, 2})
                                .addRepeatedField(repeated, new byte[0])
                                .build();
                byte[] frame = frame(serializer, value);
                assertThat(new DataInputDeserializer(frame).readInt())
                        .isEqualTo(value.getSerializedSize())
                        .isEqualTo(frame.length - 4);
                DynamicMessage decoded = serializer.deserialize(new DataInputDeserializer(frame));
                assertThat(((ByteString) decoded.getField(single)).size()).isEqualTo(length);
                assertThat(decoded.getRepeatedFieldCount(repeated)).isEqualTo(2);
                // The frame holds the value's own encoding, which writes an empty array set on an
                // implicit-presence field; the reader then leaves that field at its default.
                assertThat(Arrays.copyOfRange(frame, 4, frame.length))
                        .isEqualTo(value.toByteArray());
            }
        }
    }

    @Test
    void readingRejectsMalformedFramesNamingTheRoot() throws Exception {
        Descriptor root = rebuild(RecursiveMessage.getDescriptor());
        TypeSerializer<DynamicMessage> serializer =
                ProtobufTypeInformation.newBuilder(root)
                        .recursionLimit(2)
                        .build()
                        .createSerializer(CONFIG);
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        new DataInputDeserializer(new byte[] {0, 0, 0, 2, 0x0a})))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("missing 1 bytes");
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        new DataInputDeserializer(new byte[] {-1, -1, -1, -1})))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid Protobuf frame length -1");
        // Tag zero is invalid, and a nested value deeper than the limit is rejected.
        byte[] deep =
                RecursiveMessage.newBuilder()
                        .setChild(
                                RecursiveMessage.newBuilder()
                                        .setChild(
                                                RecursiveMessage.newBuilder()
                                                        .setChild(
                                                                RecursiveMessage.newBuilder()
                                                                        .setPayload(bytes(1)))))
                        .build()
                        .toByteArray();
        for (byte[] payload : List.of(new byte[] {0, 0}, deep)) {
            DataOutputSerializer out = new DataOutputSerializer(16);
            out.writeInt(payload.length);
            out.write(payload);
            assertThatThrownBy(
                            () ->
                                    serializer.deserialize(
                                            new DataInputDeserializer(out.getCopyOfBuffer())))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(
                            "Invalid Protobuf frame for DynamicMessage "
                                    + RecursiveMessage.getDescriptor().getFullName());
        }
        // Writing enforces the same depth limit and the exact type.
        assertThatThrownBy(() -> frame(serializer, dynamic(root, RecursiveMessage.parseFrom(deep))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Message depth exceeds recursionLimit 2");
        TypeSerializer<Message> raw = rawSerializer(serializer);
        assertThatThrownBy(() -> frame(raw, RecursiveMessage.getDefaultInstance()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Expected non-null message of type DynamicMessage");
        assertThatThrownBy(() -> frame(serializer, null)).isInstanceOf(IOException.class);
    }

    static Envelope envelope() {
        return Envelope.newBuilder()
                .setId(42)
                .setKind(Envelope.Kind.READY)
                .setDetail(
                        io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotDependencies.Detail
                                .newBuilder()
                                .setText("saved"))
                .setChild(
                        Envelope.newBuilder()
                                .setId(7)
                                .setNested(Envelope.Nested.newBuilder().setText("inner")))
                .setNested(Envelope.Nested.newBuilder().setText("nested"))
                .setUnknownFields(
                        UnknownFieldSet.newBuilder()
                                .addField(
                                        100,
                                        UnknownFieldSet.Field.newBuilder().addVarint(99).build())
                                .addField(
                                        101,
                                        UnknownFieldSet.Field.newBuilder()
                                                .addLengthDelimited(bytes(4))
                                                .build())
                                .build())
                .build();
    }

    /** Parses a generated value's bytes as a DynamicMessage of the given descriptor. */
    static DynamicMessage dynamic(Descriptor descriptor, Message value) throws IOException {
        return DynamicMessage.parseFrom(descriptor, value.toByteString());
    }

    static byte[] frame(TypeSerializer<? super DynamicMessage> serializer, Message value)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        @SuppressWarnings("unchecked")
        TypeSerializer<Message> raw = (TypeSerializer<Message>) serializer;
        raw.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    static DynamicMessage roundTrip(TypeSerializer<DynamicMessage> serializer, DynamicMessage value)
            throws IOException {
        return serializer.deserialize(new DataInputDeserializer(frame(serializer, value)));
    }

    private static ByteString bytes(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) i;
        }
        return ByteString.copyFrom(bytes);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static TypeSerializer<Message> rawSerializer(
            TypeSerializer<DynamicMessage> serializer) {
        return (TypeSerializer) serializer;
    }

    private static void assertConstructionFails(Descriptor descriptor, String... parts) {
        List<Function<Descriptor, ProtobufTypeInformation<DynamicMessage>>> constructions =
                List.of(
                        ProtobufTypeInformation::of,
                        value -> ProtobufTypeInformation.newBuilder(value).build());
        for (var construction : constructions) {
            var failure =
                    assertThatThrownBy(() -> construction.apply(descriptor))
                            .isInstanceOf(IllegalArgumentException.class);
            for (String part : parts) {
                failure.hasMessageContaining(part);
            }
        }
    }

    private static void assertCorruptFails(
            Object original, String name, Object value, String reason) throws Exception {
        Object copy =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(original),
                        ProtobufDynamicMessageTest.class.getClassLoader());
        Field field = copy.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(copy, value);
        byte[] corrupt = InstantiationUtil.serializeObject(copy);
        assertThatThrownBy(
                        () ->
                                InstantiationUtil.deserializeObject(
                                        corrupt, ProtobufDynamicMessageTest.class.getClassLoader()))
                .isInstanceOf(InvalidObjectException.class)
                .hasMessageContaining(reason);
    }
}
