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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotMessages.Envelope;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static io.github.flink.gcp.protobuf.DynamicDescriptors.field;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.file;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.message;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.rebuild;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.withoutGeneratedClasses;
import static io.github.flink.gcp.protobuf.ProtobufDynamicMessageTest.dynamic;
import static io.github.flink.gcp.protobuf.ProtobufDynamicMessageTest.envelope;
import static io.github.flink.gcp.protobuf.ProtobufDynamicMessageTest.roundTrip;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufDynamicMessageSnapshotTest {
    private static final ProtobufSerializerSettings DEFAULT = ProtobufSerializerSettings.DEFAULT;

    @Test
    void flinkEnvelopeAndPayloadHaveTheSpecifiedLayout() throws Exception {
        Descriptor root = rebuild(Envelope.getDescriptor());
        DataInputDeserializer in = new DataInputDeserializer(versioned(serializer(root, DEFAULT)));
        assertThat(in.readUTF())
                .isEqualTo("io.github.flink.gcp.protobuf.ProtobufDynamicSerializerSnapshot");
        assertThat(in.readInt()).isOne();
        assertThat(in.readInt()).as("framing").isOne();
        assertThat(in.readInt()).as("normalization").isOne();
        assertThat(in.readUTF()).isEqualTo("flink.protobuf.snapshot.Envelope");
        assertThat(in.readUnsignedByte()).as("deterministic").isZero();
        assertThat(in.readInt()).isEqualTo(67108864);
        assertThat(in.readInt()).isEqualTo(100);
        assertThat(in.readUnsignedByte()).as("aliasing").isOne();
        byte[] descriptors = new byte[in.readInt()];
        in.readFully(descriptors);
        byte[] fingerprint = new byte[32];
        in.readFully(fingerprint);
        assertThat(in.available()).isZero();
        assertThat(fingerprint).isEqualTo(MessageDigest.getInstance("SHA-256").digest(descriptors));
        FileDescriptorSet schema = FileDescriptorSet.parseFrom(descriptors);
        assertThat(schema).isEqualTo(ProtobufMessageType.resolve(Envelope.class).schema);
        assertThat(schema.getFileList())
                .extracting(FileDescriptorProto::getName)
                .containsExactly("snapshot_dependency.proto", "snapshot.proto");
    }

    /** Pins the frozen snapshot bytes, which both Protobuf profiles must write identically. */
    @Test
    void snapshotBytesAreFrozen() throws Exception {
        byte[] snapshot = versioned(serializer(rebuild(Envelope.getDescriptor()), DEFAULT));
        assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(snapshot)))
                .isEqualTo("b59bb144783c5d7b5dd731f893f291b0db987dcf6238bc2ccedc1547db0bb215");
    }

    @Test
    void restoresSavedDescriptorsAndSettingsWithoutApplicationInput() throws Exception {
        Descriptor root = rebuild(Envelope.getDescriptor());
        for (ProtobufSerializerSettings settings :
                List.of(DEFAULT, new ProtobufSerializerSettings(true, 1024, 8, false))) {
            TypeSerializer<DynamicMessage> original = serializer(root, settings);
            TypeSerializerSnapshot<DynamicMessage> snapshot =
                    readVersioned(versioned(original), withoutGeneratedClasses());
            TypeSerializer<DynamicMessage> restored = snapshot.restoreSerializer();
            assertThat(restored).isEqualTo(original).hasSameHashCodeAs(original);
            Descriptor saved = restored.createInstance().getDescriptorForType();
            assertThat(saved).isNotSameAs(root);
            DynamicMessage value = dynamic(root, envelope());
            DataOutputSerializer bytes = new DataOutputSerializer(64);
            original.serialize(value, bytes);
            DynamicMessage decoded =
                    restored.deserialize(new DataInputDeserializer(bytes.getCopyOfBuffer()));
            assertThat(decoded.getDescriptorForType()).isSameAs(saved);
            assertThat(decoded.toByteString()).isEqualTo(value.toByteString());
            assertThat(roundTrip(restored, value).getDescriptorForType()).isSameAs(saved);
            assertThat(versioned(restored)).isEqualTo(versioned(original));
            // Restoring twice rebuilds independent descriptors.
            assertThat(snapshot.restoreSerializer().createInstance().getDescriptorForType())
                    .isNotSameAs(saved);
        }
    }

    @Test
    void resolvesSavedDynamicSnapshotsWithTheDirectionalEvaluator() throws Exception {
        Descriptor original = RuntimeRecoveryHarness.descriptor("original");
        Descriptor changed = RuntimeRecoveryHarness.descriptor("changed");
        assertCompatible(original, DEFAULT, rebuild(original), DEFAULT, true);
        assertCompatible(original, DEFAULT, changed, DEFAULT, true);
        assertCompatible(
                original, DEFAULT, RuntimeRecoveryHarness.descriptor("reserved"), DEFAULT, true);
        assertCompatible(
                RuntimeRecoveryHarness.descriptor("enum-original"),
                DEFAULT,
                RuntimeRecoveryHarness.descriptor("enum-added"),
                DEFAULT,
                true);
        assertCompatible(changed, DEFAULT, original, DEFAULT, false);
        assertCompatible(
                original,
                DEFAULT,
                RuntimeRecoveryHarness.descriptor("incompatible"),
                DEFAULT,
                false);
        assertCompatible(
                RuntimeRecoveryHarness.descriptor("enum-added"),
                DEFAULT,
                RuntimeRecoveryHarness.descriptor("enum-original"),
                DEFAULT,
                false);
        // Reader limits may only grow; writing modes may change either way.
        ProtobufSerializerSettings raised =
                new ProtobufSerializerSettings(true, 1 << 27, 101, false);
        assertCompatible(original, DEFAULT, original, raised, true);
        assertCompatible(original, raised, original, DEFAULT, false);
        // Another root message is incompatible.
        assertCompatible(original, DEFAULT, rebuild(Envelope.getDescriptor()), DEFAULT, false);
    }

    @Test
    void generatedAndDynamicSnapshotsAreNeverCompatible() throws Exception {
        TypeSerializerSnapshot<DynamicMessage> dynamic =
                readVersioned(versioned(serializer(Envelope.getDescriptor(), DEFAULT)), loader());
        @SuppressWarnings({"unchecked", "rawtypes"})
        TypeSerializerSnapshot<DynamicMessage> generated =
                (TypeSerializerSnapshot)
                        new ProtobufTypeSerializer<>(Envelope.class).snapshotConfiguration();
        assertThat(dynamic.resolveSchemaCompatibility(generated).isIncompatible()).isTrue();
        @SuppressWarnings({"unchecked", "rawtypes"})
        TypeSerializerSnapshot<Envelope> generatedNew = (TypeSerializerSnapshot) generated;
        @SuppressWarnings({"unchecked", "rawtypes"})
        TypeSerializerSnapshot<Envelope> dynamicOld = (TypeSerializerSnapshot) dynamic;
        assertThat(generatedNew.resolveSchemaCompatibility(dynamicOld).isIncompatible()).isTrue();
    }

    @Test
    void readingRejectsMalformedPayloads() throws Exception {
        byte[] payload = payload(serializer(Envelope.getDescriptor(), DEFAULT));
        for (int length = 0; length < payload.length; length++) {
            assertReadFails(1, Arrays.copyOf(payload, length), "Truncated Protobuf snapshot");
        }
        assertReadFails(2, payload, "Unsupported Protobuf DynamicMessage snapshot version: 2");
        assertReadFails(1, replace(payload, 3, 2), "Unsupported Protobuf framing version: 2");
        assertReadFails(1, replace(payload, 7, 2), "Unsupported Protobuf normalization version: 2");
        int name = 8 + 2 + "flink.protobuf.snapshot.Envelope".length();
        assertReadFails(1, replace(payload, name, 2), "Invalid deterministic serialization flag");
        assertReadFails(1, replace(payload, name + 9, 2), "Invalid large-payload aliasing flag");
        byte[] corrupted = payload.clone();
        corrupted[corrupted.length - 1] ^= 1;
        assertReadFails(1, corrupted, "Descriptor SHA-256 fingerprint mismatch");
        // A failed read leaves the snapshot uninitialized.
        ProtobufDynamicSerializerSnapshot snapshot = new ProtobufDynamicSerializerSnapshot();
        snapshot.readSnapshot(1, new DataInputDeserializer(payload), loader());
        assertThatThrownBy(
                        () ->
                                snapshot.readSnapshot(
                                        1, new DataInputDeserializer(corrupted), loader()))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(snapshot::restoreSerializer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("has not been initialized");
        assertThatThrownBy(
                        () ->
                                new ProtobufDynamicSerializerSnapshot()
                                        .writeSnapshot(new DataOutputSerializer(8)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("has not been initialized");
    }

    @Test
    void restorationAppliesTheConstructionRulesToSavedDescriptors() throws Exception {
        // Saved descriptors that descriptor building accepts but construction rejects.
        FileDescriptorSet large =
                FileDescriptorSet.newBuilder()
                        .addFile(
                                file("dynamic/large.proto")
                                        .addMessageType(
                                                message(
                                                        "Saved",
                                                        field(
                                                                "value",
                                                                ProtobufMessageSize.MAX_FIELD_NUMBER
                                                                        + 1,
                                                                FieldDescriptorProto.Type
                                                                        .TYPE_INT32))))
                        .build();
        FileDescriptorSet proto2 =
                FileDescriptorSet.newBuilder()
                        .addFile(
                                file("dynamic/proto2.proto")
                                        .setSyntax("proto2")
                                        .addMessageType(message("Saved")))
                        .build();
        // Descriptor building dereferences a missing field type before validating it.
        FileDescriptorSet untyped =
                FileDescriptorSet.newBuilder()
                        .addFile(
                                file("dynamic/untyped.proto")
                                        .addMessageType(
                                                DescriptorProto.newBuilder()
                                                        .setName("Saved")
                                                        .addField(
                                                                FieldDescriptorProto.newBuilder()
                                                                        .setName("value")
                                                                        .setNumber(1))))
                        .build();
        assertThatThrownBy(
                        () ->
                                new ProtobufDynamicSerializerSnapshot()
                                        .readSnapshot(
                                                1,
                                                new DataInputDeserializer(payload(untyped)),
                                                loader()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid descriptor graph");
        for (FileDescriptorSet schema : List.of(large, proto2)) {
            ProtobufDynamicSerializerSnapshot snapshot = new ProtobufDynamicSerializerSnapshot();
            snapshot.readSnapshot(1, new DataInputDeserializer(payload(schema)), loader());
            assertThatThrownBy(snapshot::restoreSerializer)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "Cannot restore Protobuf DynamicMessage serializer for"
                                    + " flink.protobuf.dynamic.Saved")
                    .hasMessageContaining(
                            schema == large
                                    ? "above the maximum field number 536870911"
                                    : "only proto3 is supported");
        }
    }

    private static void assertCompatible(
            Descriptor saved,
            ProtobufSerializerSettings savedSettings,
            Descriptor current,
            ProtobufSerializerSettings currentSettings,
            boolean compatible)
            throws Exception {
        TypeSerializerSnapshot<DynamicMessage> old =
                readVersioned(versioned(serializer(saved, savedSettings)), loader());
        var result =
                serializer(current, currentSettings)
                        .snapshotConfiguration()
                        .resolveSchemaCompatibility(old);
        assertThat(result.isCompatibleAsIs())
                .as("%s -> %s", saved.getFile().toProto(), current.getFile().toProto())
                .isEqualTo(compatible);
        assertThat(result.isIncompatible()).isEqualTo(!compatible);
    }

    private static void assertReadFails(int version, byte[] payload, String reason) {
        assertThatThrownBy(
                        () ->
                                new ProtobufDynamicSerializerSnapshot()
                                        .readSnapshot(
                                                version,
                                                new DataInputDeserializer(payload),
                                                loader()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(reason);
    }

    private static TypeSerializer<DynamicMessage> serializer(
            Descriptor descriptor, ProtobufSerializerSettings settings) {
        return ProtobufTypeInformation.newBuilder(descriptor)
                .deterministicSerialization(settings.deterministicSerialization())
                .maxMessageSize(settings.maxMessageSize())
                .recursionLimit(settings.recursionLimit())
                .largePayloadAliasing(settings.largePayloadAliasing())
                .build()
                .createSerializer(new SerializerConfigImpl());
    }

    private static byte[] versioned(TypeSerializer<DynamicMessage> serializer) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, serializer.snapshotConfiguration());
        return out.getCopyOfBuffer();
    }

    private static TypeSerializerSnapshot<DynamicMessage> readVersioned(
            byte[] bytes, ClassLoader loader) throws IOException {
        return TypeSerializerSnapshot.readVersionedSnapshot(
                new DataInputDeserializer(bytes), loader);
    }

    private static byte[] payload(TypeSerializer<DynamicMessage> serializer) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.snapshotConfiguration().writeSnapshot(out);
        return out.getCopyOfBuffer();
    }

    /** Writes a payload for saved descriptors without the construction rules. */
    private static byte[] payload(FileDescriptorSet schema) throws Exception {
        ProtobufDynamicSerializerSnapshot snapshot = new ProtobufDynamicSerializerSnapshot();
        for (Object[] value :
                new Object[][] {
                    {"fullName", "flink.protobuf.dynamic.Saved"},
                    {"settings", DEFAULT},
                    {"schema", schema}
                }) {
            Field field = snapshot.getClass().getDeclaredField((String) value[0]);
            field.setAccessible(true);
            field.set(snapshot, value[1]);
        }
        DataOutputSerializer out = new DataOutputSerializer(64);
        snapshot.writeSnapshot(out);
        return out.getCopyOfBuffer();
    }

    private static byte[] replace(byte[] payload, int index, int value) {
        byte[] result = payload.clone();
        result[index] = (byte) value;
        return result;
    }

    private static ClassLoader loader() {
        return ProtobufDynamicMessageSnapshotTest.class.getClassLoader();
    }
}
