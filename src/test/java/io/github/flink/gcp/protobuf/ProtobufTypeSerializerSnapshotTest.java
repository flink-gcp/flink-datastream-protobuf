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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.SourceCodeInfo;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.protobuf.generated.OptionMessage;
import io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotDependencies.Detail;
import io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotMessages.Envelope;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufTypeSerializerSnapshotTest {
    private static final ClassLoader LOADER =
            ProtobufTypeSerializerSnapshotTest.class.getClassLoader();
    private static final ProtobufSerializerSettings DEFAULT = ProtobufSerializerSettings.DEFAULT;

    @Test
    void flinkEnvelopeAndPayloadHaveTheSpecifiedLayout() throws Exception {
        ProtobufTypeSerializer<Envelope> serializer = serializer(DEFAULT);
        DataInputDeserializer in =
                new DataInputDeserializer(versioned(serializer.snapshotConfiguration()));
        assertThat(in.readUTF())
                .isEqualTo("io.github.flink.gcp.protobuf.ProtobufTypeSerializerSnapshot");
        assertThat(in.readInt()).isEqualTo(1);
        Payload payload = Payload.read(in);
        assertThat(payload.framing).isEqualTo(1);
        assertThat(payload.normalization).isEqualTo(1);
        assertThat(payload.className).isEqualTo(Envelope.class.getName());
        assertThat(payload.fullName).isEqualTo(Envelope.getDescriptor().getFullName());
        assertThat(payload.flag).isZero();
        assertThat(payload.sizeLimit).isEqualTo(67108864);
        assertThat(payload.depthLimit).isEqualTo(100);
        assertThat(payload.fingerprint).hasSize(32).isEqualTo(sha256(payload.descriptors));
        assertThat(in.available()).isZero();
        assertThat(payload.schema()).isEqualTo(ProtobufMessageType.resolve(Envelope.class).schema);
        assertThat(payload.schema().getFileList())
                .extracting(FileDescriptorProto::getName)
                .containsExactly("snapshot_dependency.proto", "snapshot.proto");
    }

    @Test
    void restoredSerializerReadsSavedValuesAndKeepsItsSettings() throws Exception {
        for (ProtobufSerializerSettings settings :
                List.of(DEFAULT, new ProtobufSerializerSettings(true, 1024, 8))) {
            ProtobufTypeSerializer<Envelope> original = serializer(settings);
            TypeSerializer<Envelope> restored =
                    readVersioned(versioned(original.snapshotConfiguration()), LOADER)
                            .restoreSerializer();
            assertThat(restored).isEqualTo(original).hasSameHashCodeAs(original);
            Envelope value = value();
            DataOutputSerializer bytes = new DataOutputSerializer(32);
            original.serialize(value, bytes);
            assertThat(restored.deserialize(new DataInputDeserializer(bytes.getCopyOfBuffer())))
                    .isEqualTo(value);
            assertThat(versioned(restored.snapshotConfiguration()))
                    .isEqualTo(versioned(original.snapshotConfiguration()));
        }
    }

    @Test
    void nestedRootAndImportedOptionDescriptorsRoundTrip() throws Exception {
        for (Class<? extends Message> type : List.of(Envelope.Nested.class, OptionMessage.class)) {
            verifyType(type);
        }
    }

    private static <T extends Message> void verifyType(Class<T> type) throws Exception {
        ProtobufTypeSerializer<T> original = new ProtobufTypeSerializer<>(type);
        TypeSerializerSnapshot<T> restored =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        new DataInputDeserializer(versioned(original.snapshotConfiguration())),
                        LOADER);
        assertThat(restored.restoreSerializer()).isEqualTo(original);
    }

    @Test
    void readerLimitsAreDirectionalAndDeterministicModeCanChangeEitherWay() throws Exception {
        for (boolean before : List.of(false, true)) {
            for (boolean after : List.of(false, true)) {
                TypeSerializerSnapshot<Envelope> old =
                        snapshot(new ProtobufSerializerSettings(before, 100, 10));
                for (int size : List.of(99, 100, 101)) {
                    for (int depth : List.of(9, 10, 11)) {
                        TypeSerializerSnapshot<Envelope> next =
                                snapshot(new ProtobufSerializerSettings(after, size, depth));
                        var result = next.resolveSchemaCompatibility(old);
                        assertThat(result.isCompatibleAsIs())
                                .as("%s -> %s, size=%s depth=%s", before, after, size, depth)
                                .isEqualTo(size >= 100 && depth >= 10);
                        assertThat(result.isIncompatible()).isEqualTo(size < 100 || depth < 10);
                        assertThat(result.isCompatibleAfterMigration()).isFalse();
                        assertThat(result.isCompatibleWithReconfiguredSerializer()).isFalse();
                    }
                }
            }
        }
    }

    @Test
    void aNewSnapshotRecordsIncreasedLimitsRatherThanThePreviouslyRestoredLimits()
            throws Exception {
        TypeSerializerSnapshot<Envelope> old =
                snapshot(new ProtobufSerializerSettings(false, 100, 10));
        ProtobufTypeSerializer<Envelope> next =
                serializer(new ProtobufSerializerSettings(true, 200, 20));
        assertThat(next.snapshotConfiguration().resolveSchemaCompatibility(old).isCompatibleAsIs())
                .isTrue();
        TypeSerializerSnapshot<Envelope> savedAgain =
                readVersioned(versioned(next.snapshotConfiguration()), LOADER);
        for (ProtobufSerializerSettings reduced :
                List.of(
                        new ProtobufSerializerSettings(false, 100, 20),
                        new ProtobufSerializerSettings(false, 200, 10),
                        new ProtobufSerializerSettings(false, 100, 10))) {
            assertThat(snapshot(reduced).resolveSchemaCompatibility(savedAgain).isIncompatible())
                    .isTrue();
        }
        assertThat(savedAgain.restoreSerializer()).isEqualTo(next);
    }

    @Test
    void javaClassAndMessageRenamesAreIncompatible() throws Exception {
        Payload saved = payload();
        saved.className = "renamed.GeneratedClass";
        assertIncompatible(saved);
        saved = payload();
        saved.fullName = "flink.protobuf.snapshot.Envelope.Nested";
        assertIncompatible(saved);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void otherSnapshotImplementationsAreIncompatible() throws Exception {
        assertThat(
                        ((TypeSerializerSnapshot) snapshot(DEFAULT))
                                .resolveSchemaCompatibility(
                                        StringSerializer.INSTANCE.snapshotConfiguration())
                                .isIncompatible())
                .isTrue();
    }

    @Test
    void descriptorChangesIncludingImportedMetadataAreIncompatible() throws Exception {
        FileDescriptorSet original = payload().schema();
        FileDescriptorProto dependency = original.getFile(0);
        FileDescriptorProto root = original.getFile(1);
        DescriptorProto message = root.getMessageType(0);
        for (FileDescriptorSet changed :
                List.of(
                        original.toBuilder()
                                .setFile(
                                        0,
                                        dependency.toBuilder()
                                                .setOptions(
                                                        dependency.getOptions().toBuilder()
                                                                .setDeprecated(true)))
                                .build(),
                        original.toBuilder()
                                .setFile(
                                        1,
                                        root.toBuilder()
                                                .setMessageType(
                                                        0,
                                                        message.toBuilder()
                                                                .setField(
                                                                        0,
                                                                        message
                                                                                .getField(0)
                                                                                .toBuilder()
                                                                                .setName(
                                                                                        "renamed"))))
                                .build(),
                        original.toBuilder()
                                .setFile(
                                        1,
                                        root.toBuilder()
                                                .setMessageType(
                                                        0, message.toBuilder().removeField(0)))
                                .build(),
                        original.toBuilder()
                                .setFile(
                                        1,
                                        root.toBuilder()
                                                .setMessageType(
                                                        0,
                                                        message.toBuilder()
                                                                .setEnumType(
                                                                        0,
                                                                        message
                                                                                .getEnumType(0)
                                                                                .toBuilder()
                                                                                .addValue(
                                                                                        message
                                                                                                .getEnumType(
                                                                                                        0)
                                                                                                .getValue(
                                                                                                        1)
                                                                                                .toBuilder()
                                                                                                .setName(
                                                                                                        "ADDED")
                                                                                                .setNumber(
                                                                                                        2)))))
                                .build(),
                        original.toBuilder()
                                .setUnknownFields(
                                        UnknownFieldSet.newBuilder()
                                                .addField(
                                                        1000,
                                                        UnknownFieldSet.Field.newBuilder()
                                                                .addVarint(7)
                                                                .build())
                                                .build())
                                .build())) {
            Payload saved = payload();
            saved.setSchema(changed);
            assertIncompatible(saved);
        }
    }

    @Test
    void sourceInformationAndFileIterationOrderNormalizeAway() throws Exception {
        Payload saved = payload();
        FileDescriptorSet schema = saved.schema();
        saved.setSchema(
                schema.toBuilder()
                        .clearFile()
                        .addFile(
                                schema.getFile(1).toBuilder()
                                        .setSourceCodeInfo(
                                                SourceCodeInfo.newBuilder()
                                                        .addLocation(
                                                                SourceCodeInfo.Location.newBuilder()
                                                                        .setLeadingComments(
                                                                                "ignored"))))
                        .addFile(schema.getFile(0))
                        .build());
        assertThat(
                        snapshot(DEFAULT)
                                .resolveSchemaCompatibility(read(saved, LOADER))
                                .isCompatibleAsIs())
                .isTrue();
    }

    @Test
    void matchingFingerprintCannotHideDifferentDescriptorContent() throws Exception {
        Payload saved = payload();
        byte[] originalFingerprint = saved.fingerprint.clone();
        FileDescriptorSet schema = saved.schema();
        saved.setSchema(
                schema.toBuilder()
                        .setFile(
                                0,
                                schema.getFile(0).toBuilder()
                                        .setOptions(FileOptions.newBuilder().setDeprecated(true)))
                        .build());
        saved.fingerprint = originalFingerprint;
        assertReadFails(saved, "fingerprint mismatch");
    }

    @Test
    void differentFingerprintsDoNotRejectEqualDecodedDescriptorContent() throws Exception {
        Payload saved = payload();
        byte[] canonical = saved.descriptors;
        assertThat(canonical[0]).isEqualTo((byte) 10);
        byte[] alternative = new byte[canonical.length + 1];
        // A nonminimal varint encodes the same FileDescriptorSet.file tag.
        alternative[0] = (byte) 0x8a;
        alternative[1] = 0;
        System.arraycopy(canonical, 1, alternative, 2, canonical.length - 1);
        saved.descriptors = alternative;
        saved.declaredLength = alternative.length;
        saved.fingerprint = sha256(alternative);
        assertThat(saved.fingerprint).isNotEqualTo(sha256(canonical));
        assertThat(saved.schema()).isEqualTo(FileDescriptorSet.parseFrom(canonical));
        assertThat(
                        snapshot(DEFAULT)
                                .resolveSchemaCompatibility(read(saved, LOADER))
                                .isCompatibleAsIs())
                .isTrue();
    }

    @Test
    void metadataReadingAndCompatibilityDoNotLoadGeneratedClasses() throws Exception {
        ClassLoader absent = absentGeneratedClasses();
        TypeSerializerSnapshot<Envelope> old =
                readVersioned(versioned(serializer(DEFAULT).snapshotConfiguration()), absent);
        assertThat(snapshot(DEFAULT).resolveSchemaCompatibility(old).isCompatibleAsIs()).isTrue();
        assertThatThrownBy(old::restoreSerializer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(Envelope.class.getName())
                .hasMessageContaining("user-code classloader")
                .hasCauseInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void restorationUsesTheSuppliedLoaderEvenWhenTheContextLoaderCannotLoadTheClass()
            throws Exception {
        URL location = Envelope.class.getProtectionDomain().getCodeSource().getLocation();
        ClassLoader parent = absentGeneratedClasses();
        assertThatThrownBy(() -> parent.loadClass(Envelope.class.getName()))
                .isInstanceOf(ClassNotFoundException.class);
        ClassLoader oldContext = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader isolated = new URLClassLoader(new URL[] {location}, parent)) {
            Thread.currentThread().setContextClassLoader(parent);
            TypeSerializerSnapshot<Envelope> saved =
                    readVersioned(versioned(serializer(DEFAULT).snapshotConfiguration()), isolated);
            TypeSerializer<Envelope> restored = saved.restoreSerializer();
            DataOutputSerializer bytes = new DataOutputSerializer(32);
            serializer(DEFAULT).serialize(value(), bytes);
            Message value =
                    restored.deserialize(new DataInputDeserializer(bytes.getCopyOfBuffer()));
            assertThat(value.getClass().getClassLoader()).isSameAs(isolated);
            assertThat(value.toByteString()).isEqualTo(value().toByteString());
            assertThat(
                            restored.snapshotConfiguration()
                                    .resolveSchemaCompatibility(snapshot(DEFAULT))
                                    .isCompatibleAsIs())
                    .isTrue();
        } finally {
            Thread.currentThread().setContextClassLoader(oldContext);
        }
    }

    @Test
    void unsupportedAndMismatchedClassesFailWithAnActionableRestoreError() throws Exception {
        for (String name :
                List.of(String.class.getName(), Message.class.getName(), Detail.class.getName())) {
            Payload saved = payload();
            saved.className = name;
            TypeSerializerSnapshot<Envelope> snapshot = read(saved, LOADER);
            assertThatThrownBy(snapshot::restoreSerializer)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(name)
                    .hasMessageContaining("saved schema");
        }
        Payload changed = payload();
        FileDescriptorSet schema = changed.schema();
        changed.setSchema(
                schema.toBuilder()
                        .setFile(
                                0,
                                schema.getFile(0).toBuilder()
                                        .setOptions(FileOptions.newBuilder().setDeprecated(true)))
                        .build());
        assertThatThrownBy(read(changed, LOADER)::restoreSerializer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("different Protobuf schema");
    }

    @Test
    void unsupportedVersionsFailBeforeDecodingFurtherData() throws Exception {
        for (int version : List.of(-1, 0, 2, Integer.MAX_VALUE)) {
            assertThatThrownBy(
                            () ->
                                    new ProtobufTypeSerializerSnapshot<>()
                                            .readSnapshot(
                                                    version,
                                                    new DataInputDeserializer(new byte[0]),
                                                    LOADER))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("snapshot version: " + version);
            Payload saved = payload();
            saved.framing = version;
            assertReadFails(saved, "framing version");
            saved = payload();
            saved.normalization = version;
            assertReadFails(saved, "normalization version");
        }
    }

    @Test
    void invalidNamesFlagsSettingsAndLengthsFailExplicitly() throws Exception {
        Payload saved = payload();
        saved.className = "";
        assertReadFails(saved, "Empty Java");
        saved = payload();
        saved.fullName = "";
        assertReadFails(saved, "Empty Protobuf");
        for (int value : List.of(2, 127, 255)) {
            saved = payload();
            saved.flag = value;
            assertReadFails(saved, "flag");
        }
        for (int value : List.of(0, -1, Integer.MIN_VALUE)) {
            saved = payload();
            saved.sizeLimit = value;
            assertReadFails(saved, "maxMessageSize");
            saved = payload();
            saved.depthLimit = value;
            assertReadFails(saved, "recursionLimit");
        }
        for (int size :
                List.of(
                        -1,
                        0,
                        ProtobufTypeSerializerSnapshot.MAX_DESCRIPTOR_BYTES + 1,
                        Integer.MAX_VALUE)) {
            saved = payload();
            saved.declaredLength = size;
            assertReadFails(saved, "descriptor byte length");
        }
    }

    @Test
    void everyTruncatedPrefixOfASnapshotFails() throws Exception {
        byte[] complete = payload().bytes();
        for (int length = 0; length < complete.length; length++) {
            byte[] truncated = Arrays.copyOf(complete, length);
            assertThatThrownBy(
                            () ->
                                    new ProtobufTypeSerializerSnapshot<>()
                                            .readSnapshot(
                                                    1,
                                                    new DataInputDeserializer(truncated),
                                                    LOADER))
                    .as("length %s", length)
                    .isInstanceOf(IOException.class)
                    .hasMessage("Truncated Protobuf snapshot")
                    .hasCauseInstanceOf(EOFException.class);
        }
    }

    @Test
    void malformedDescriptorsAndInvalidGraphsFailDuringMetadataRead() throws Exception {
        Payload saved = payload();
        saved.descriptors = new byte[] {10, 10, 1};
        saved.declaredLength = 3;
        saved.fingerprint = sha256(saved.descriptors);
        assertReadFails(saved, "Cannot read Protobuf snapshot");
        FileDescriptorSet original = payload().schema();
        FileDescriptorProto root = original.getFile(1);
        for (FileDescriptorSet invalid :
                List.of(
                        FileDescriptorSet.getDefaultInstance(),
                        original.toBuilder().removeFile(0).build(),
                        original.toBuilder()
                                .addFile(original.getFile(0).toBuilder().setPackage("conflict"))
                                .build(),
                        original.toBuilder()
                                .setFile(
                                        0,
                                        original.getFile(0).toBuilder()
                                                .addDependency("snapshot.proto"))
                                .build(),
                        original.toBuilder()
                                .setFile(1, root.toBuilder().addWeakDependency(3))
                                .build(),
                        original.toBuilder()
                                .setFile(1, root.toBuilder().addPublicDependency(-1))
                                .build(),
                        original.toBuilder()
                                .setFile(
                                        1,
                                        root.toBuilder()
                                                .setMessageType(
                                                        0,
                                                        root.getMessageType(0).toBuilder()
                                                                .setField(
                                                                        1,
                                                                        root
                                                                                .getMessageType(0)
                                                                                .getField(1)
                                                                                .toBuilder()
                                                                                .setTypeName(
                                                                                        ".missing.Message"))))
                                .build(),
                        original.toBuilder()
                                .addFile(
                                        FileDescriptorProto.newBuilder().setName("unrelated.proto"))
                                .build())) {
            saved = payload();
            saved.setSchema(invalid);
            assertReadFails(saved, "Cannot read Protobuf snapshot");
        }
        saved = payload();
        saved.fullName = "missing.Root";
        assertReadFails(saved, "Missing root message");
    }

    @Test
    void topLevelEndGroupCannotHideAnIncompleteDescriptorParse() throws Exception {
        for (byte[] suffix : List.of(new byte[] {12}, new byte[] {12, 0})) {
            Payload saved = payload();
            int length = saved.descriptors.length;
            saved.descriptors = Arrays.copyOf(saved.descriptors, length + suffix.length);
            System.arraycopy(suffix, 0, saved.descriptors, length, suffix.length);
            saved.declaredLength = saved.descriptors.length;
            saved.fingerprint = sha256(saved.descriptors);
            assertReadFails(saved, "Cannot read Protobuf snapshot");
        }
    }

    @Test
    void readConsumesExactlyOneSnapshot() throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(32);
        serializer(DEFAULT).snapshotConfiguration().writeSnapshot(out);
        out.writeInt(0x12345678);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        new ProtobufTypeSerializerSnapshot<>().readSnapshot(1, in, LOADER);
        assertThat(in.readInt()).isEqualTo(0x12345678);
    }

    @Test
    void failedReadInvalidatesPreviouslyInitializedSnapshot() throws Exception {
        ProtobufTypeSerializerSnapshot<Envelope> snapshot =
                new ProtobufTypeSerializerSnapshot<>(Envelope.class, DEFAULT);
        assertThatThrownBy(
                        () ->
                                snapshot.readSnapshot(
                                        2, new DataInputDeserializer(new byte[0]), LOADER))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(snapshot::restoreSerializer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not been initialized");
        assertThatThrownBy(() -> snapshot.writeSnapshot(new DataOutputSerializer(32)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void descriptorSetWrapperPreservesTheExistingFileNestingBoundary() throws Exception {
        DescriptorProto nested =
                DescriptorProto.newBuilder()
                        .setName("Leaf")
                        .addField(
                                FieldDescriptorProto.newBuilder()
                                        .setName("value")
                                        .setNumber(1)
                                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                        .setType(FieldDescriptorProto.Type.TYPE_INT32))
                        .build();
        for (int depth = 1; depth < 99; depth++) {
            nested =
                    DescriptorProto.newBuilder().setName("N" + depth).addNestedType(nested).build();
        }
        FileDescriptor originalFile = Envelope.getDescriptor().getFile();
        FileDescriptorProto deepFile =
                originalFile.toProto().toBuilder().addMessageType(nested).build();
        // A shallow root still retains deeply nested, unrelated declarations in its file.
        FileDescriptorSet accepted =
                ProtobufSchema.normalize(
                        FileDescriptor.buildFrom(
                                deepFile,
                                originalFile.getDependencies().toArray(new FileDescriptor[0])));
        ProtobufTypeSerializerSnapshot<Envelope> snapshot =
                new ProtobufTypeSerializerSnapshot<>(Envelope.class, DEFAULT);
        setField(snapshot, "schema", accepted);
        byte[] saved = versioned(snapshot);
        TypeSerializerSnapshot<Envelope> read = readVersioned(saved, LOADER);
        assertThat(read.resolveSchemaCompatibility(snapshot).isCompatibleAsIs()).isTrue();
        assertThat(versioned(read)).isEqualTo(saved);
    }

    @Test
    void writingRejectsModifiedUtfOverflowAndOversizedDescriptorsBeforeEmittingBytes()
            throws Exception {
        ProtobufTypeSerializerSnapshot<Envelope> snapshot =
                new ProtobufTypeSerializerSnapshot<>(Envelope.class, DEFAULT);
        setField(snapshot, "className", "\u0800".repeat(21846));
        DataOutputSerializer out = new DataOutputSerializer(32);
        assertThatThrownBy(() -> snapshot.writeSnapshot(out))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("65535");
        assertThat(out.length()).isZero();
        setField(snapshot, "className", Envelope.class.getName());
        FileDescriptorSet large =
                FileDescriptorSet.newBuilder()
                        .setUnknownFields(
                                UnknownFieldSet.newBuilder()
                                        .addField(
                                                1000,
                                                UnknownFieldSet.Field.newBuilder()
                                                        .addLengthDelimited(
                                                                ByteString.copyFrom(
                                                                        new byte
                                                                                [ProtobufTypeSerializerSnapshot
                                                                                        .MAX_DESCRIPTOR_BYTES]))
                                                        .build())
                                        .build())
                        .build();
        setField(snapshot, "schema", large);
        assertThatThrownBy(() -> snapshot.writeSnapshot(out))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("descriptor byte length");
        assertThat(out.length()).isZero();
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    static Envelope value() {
        return Envelope.newBuilder()
                .setId(42)
                .setKind(Envelope.Kind.READY)
                .setDetail(Detail.newBuilder().setText("saved"))
                .setChild(Envelope.newBuilder().setId(7))
                .setNested(Envelope.Nested.newBuilder().setText("nested"))
                .setUnknownFields(
                        UnknownFieldSet.newBuilder()
                                .addField(
                                        100,
                                        UnknownFieldSet.Field.newBuilder().addVarint(99).build())
                                .build())
                .build();
    }

    static ProtobufTypeSerializer<Envelope> serializer(ProtobufSerializerSettings settings) {
        return new ProtobufTypeSerializer<>(Envelope.class, settings);
    }

    private static TypeSerializerSnapshot<Envelope> snapshot(ProtobufSerializerSettings settings)
            throws Exception {
        return readVersioned(versioned(serializer(settings).snapshotConfiguration()), LOADER);
    }

    static byte[] versioned(TypeSerializerSnapshot<?> snapshot) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, snapshot);
        return out.getCopyOfBuffer();
    }

    static TypeSerializerSnapshot<Envelope> readVersioned(byte[] bytes, ClassLoader loader)
            throws IOException {
        return TypeSerializerSnapshot.readVersionedSnapshot(
                new DataInputDeserializer(bytes), loader);
    }

    private static Payload payload() throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(32);
        serializer(DEFAULT).snapshotConfiguration().writeSnapshot(out);
        return Payload.read(new DataInputDeserializer(out.getCopyOfBuffer()));
    }

    private static TypeSerializerSnapshot<Envelope> read(Payload payload, ClassLoader loader)
            throws Exception {
        ProtobufTypeSerializerSnapshot<Envelope> result = new ProtobufTypeSerializerSnapshot<>();
        result.readSnapshot(1, new DataInputDeserializer(payload.bytes()), loader);
        return result;
    }

    private static void assertIncompatible(Payload saved) throws Exception {
        assertThat(
                        snapshot(DEFAULT)
                                .resolveSchemaCompatibility(read(saved, LOADER))
                                .isIncompatible())
                .isTrue();
    }

    private static void assertReadFails(Payload saved, String condition) {
        assertThatThrownBy(() -> read(saved, LOADER))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(condition);
    }

    private static ClassLoader absentGeneratedClasses() {
        return new ClassLoader(LOADER) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                    throws ClassNotFoundException {
                if (name.startsWith("io.github.flink.gcp.protobuf.snapshotgenerated.")) {
                    throw new ClassNotFoundException(
                            "Generated classes intentionally absent: " + name);
                }
                return super.loadClass(name, resolve);
            }
        };
    }

    private static byte[] sha256(byte[] bytes) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes);
    }

    /** Independent field-level encoder for deliberate mutations of persisted metadata. */
    private static final class Payload {
        int framing;
        int normalization;
        String className;
        String fullName;
        int flag;
        int sizeLimit;
        int depthLimit;
        int declaredLength;
        byte[] descriptors;
        byte[] fingerprint;

        static Payload read(DataInputDeserializer in) throws IOException {
            Payload p = new Payload();
            p.framing = in.readInt();
            p.normalization = in.readInt();
            p.className = in.readUTF();
            p.fullName = in.readUTF();
            p.flag = in.readUnsignedByte();
            p.sizeLimit = in.readInt();
            p.depthLimit = in.readInt();
            p.declaredLength = in.readInt();
            p.descriptors = new byte[p.declaredLength];
            in.readFully(p.descriptors);
            p.fingerprint = new byte[32];
            in.readFully(p.fingerprint);
            return p;
        }

        FileDescriptorSet schema() throws IOException {
            return FileDescriptorSet.parseFrom(descriptors);
        }

        void setSchema(FileDescriptorSet schema) throws Exception {
            descriptors = schema.toByteArray();
            declaredLength = descriptors.length;
            fingerprint = sha256(descriptors);
        }

        byte[] bytes() throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(32);
            out.writeInt(framing);
            out.writeInt(normalization);
            out.writeUTF(className);
            out.writeUTF(fullName);
            out.writeByte(flag);
            out.writeInt(sizeLimit);
            out.writeInt(depthLimit);
            out.writeInt(declaredLength);
            out.write(descriptors);
            out.write(fingerprint);
            return out.getCopyOfBuffer();
        }
    }
}
