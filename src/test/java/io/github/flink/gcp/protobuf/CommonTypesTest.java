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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.github.flink.gcp.protobuf.generated.CommonTypesEnvelope;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CommonTypesTest {
    @Test
    void documentedConstructionExamplesUseGeneratedTypes() throws Exception {
        var attributes =
                Struct.newBuilder()
                        .putFields("service", Value.newBuilder().setStringValue("checkout").build())
                        .putFields("healthy", Value.newBuilder().setBoolValue(true).build())
                        .build();
        var structType = ProtobufTypeInformation.of(Struct.class);
        var packed = Any.pack(attributes);
        var anyType = ProtobufTypeInformation.of(Any.class);
        var values =
                AnyValue.newBuilder()
                        .setArrayValue(
                                ArrayValue.newBuilder()
                                        .addValues(AnyValue.newBuilder().setIntValue(42))
                                        .addValues(
                                                AnyValue.newBuilder().setStringValue("checkout")))
                        .build();
        var otelType = ProtobufTypeInformation.of(AnyValue.class);
        assertThat(structType.getTypeClass()).isEqualTo(attributes.getClass());
        assertThat(anyType.getTypeClass()).isEqualTo(packed.getClass());
        assertThat(otelType.getTypeClass()).isEqualTo(values.getClass());
        verify(attributes);
        verify(packed);
        verify(values);
    }

    static Stream<Arguments> values() {
        var samples = CommonTypeValues.samples();
        return IntStream.range(0, samples.size())
                .mapToObj(
                        i ->
                                Arguments.of(
                                        i
                                                + ":"
                                                + samples.get(i)
                                                        .getDescriptorForType()
                                                        .getFullName(),
                                        samples.get(i)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("values")
    void productionSerializerCopiesTransportsAndRestoresEveryValue(String name, Message value)
            throws Exception {
        verify(value);
        verify(value.toBuilder().setUnknownFields(CommonTypeValues.unknown()).build());
    }

    @SuppressWarnings("unchecked")
    private static <T extends Message> void verify(T value) throws Exception {
        for (boolean deterministic : new boolean[] {false, true}) {
            var info =
                    ProtobufTypeInformation.newBuilder((Class<T>) value.getClass())
                            .deterministicSerialization(deterministic)
                            .build();
            var serializer = info.createSerializer(new ExecutionConfig().getSerializerConfig());
            assertThat(serializer).isInstanceOf(ProtobufTypeSerializer.class);
            assertThat(serializer.isImmutableType()).isTrue();
            assertThat(serializer.copy(value)).isSameAs(value);
            assertThat(serializer.copy(value, serializer.createInstance())).isSameAs(value);
            var wire = new DataOutputSerializer(64);
            serializer.serialize(value, wire);
            var frame = new DataInputDeserializer(wire.getCopyOfBuffer());
            assertThat(frame.readInt()).isEqualTo(value.getSerializedSize());
            byte[] payload = new byte[frame.available()];
            frame.readFully(payload);
            assertThat(value.getParserForType().parseFrom(payload)).isEqualTo(value);
            assertThat(serializer.deserialize(new DataInputDeserializer(wire.getCopyOfBuffer())))
                    .isEqualTo(value);
            var copied = new DataOutputSerializer(64);
            serializer.copy(new DataInputDeserializer(wire.getCopyOfBuffer()), copied);
            assertThat(copied.getCopyOfBuffer()).isEqualTo(wire.getCopyOfBuffer());
            var snapshotBytes = new DataOutputSerializer(64);
            TypeSerializerSnapshot.writeVersionedSnapshot(
                    snapshotBytes, serializer.snapshotConfiguration());
            TypeSerializerSnapshot<T> snapshot =
                    TypeSerializerSnapshot.readVersionedSnapshot(
                            new DataInputDeserializer(snapshotBytes.getCopyOfBuffer()),
                            CommonTypesTest.class.getClassLoader());
            assertThat(
                            snapshot.resolveSchemaCompatibility(serializer.snapshotConfiguration())
                                    .isCompatibleAsIs())
                    .isTrue();
            assertThat(
                            snapshot.restoreSerializer()
                                    .deserialize(new DataInputDeserializer(wire.getCopyOfBuffer())))
                    .isEqualTo(value);
        }
    }

    @Test
    void descriptorClosureContainsEveryDeclaredImportAndNoOpaquePayloadSchema() throws Exception {
        var schema = ProtobufSchema.normalize(CommonTypesEnvelope.getDescriptor().getFile());
        assertThat(schema.getFileList())
                .extracting(FileDescriptorProto::getName)
                .containsExactly(
                        "google/protobuf/any.proto",
                        "google/protobuf/duration.proto",
                        "google/protobuf/empty.proto",
                        "google/protobuf/field_mask.proto",
                        "google/protobuf/struct.proto",
                        "google/protobuf/timestamp.proto",
                        "google/protobuf/wrappers.proto",
                        "opentelemetry/proto/common/v1/common.proto",
                        "common_types.proto");
        Map<String, Descriptors.FileDescriptor> resolved = new HashMap<>();
        for (var file : schema.getFileList()) {
            assertThat(file.hasSourceCodeInfo()).isFalse();
            resolved.put(
                    file.getName(),
                    Descriptors.FileDescriptor.buildFrom(
                            file,
                            file.getDependencyList().stream()
                                    .map(resolved::get)
                                    .toArray(Descriptors.FileDescriptor[]::new)));
        }
        assertThat(
                        ProtobufSchema.normalize(
                                schema, CommonTypesEnvelope.getDescriptor().getFullName()))
                .isEqualTo(schema);
        assertThat(ProtobufSchema.normalize(Any.getDescriptor().getFile()).getFileList())
                .extracting(FileDescriptorProto::getName)
                .containsExactly("google/protobuf/any.proto");
        var incomplete = schema.toBuilder().removeFile(4).build();
        assertThatThrownBy(
                        () ->
                                ProtobufSchema.normalize(
                                        incomplete,
                                        CommonTypesEnvelope.getDescriptor().getFullName()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unresolved descriptor import");
    }

    @Test
    void dataVariantsLeaveFullDescriptorIdentityAndSnapshotsUnchanged() throws Exception {
        assertThat(CommonTypeValues.struct(0).getFieldsMap().keySet())
                .isNotEqualTo(CommonTypeValues.struct(1).getFieldsMap().keySet());
        assertThat(CommonTypeValues.otel(0).getValueCase())
                .isNotEqualTo(CommonTypeValues.otel(1).getValueCase());
        for (int i = 0; i < 9; i++) {
            assertThat(
                            ProtobufSchema.normalize(
                                    CommonTypeValues.struct(i).getDescriptorForType().getFile()))
                    .isEqualTo(ProtobufSchema.normalize(Struct.getDescriptor().getFile()));
            assertThat(
                            ProtobufSchema.normalize(
                                    CommonTypeValues.otel(i).getDescriptorForType().getFile()))
                    .isEqualTo(ProtobufSchema.normalize(AnyValue.getDescriptor().getFile()));
            verify(CommonTypeValues.envelope(i));
        }
    }

    @Test
    void transportsWireValuesWithoutApplicationSemanticValidation() throws Exception {
        verify(
                Any.newBuilder()
                        .setTypeUrl("https://unresolvable.invalid/unknown.Type")
                        .setValue(ByteString.copyFrom(new byte[] {(byte) 255}))
                        .build());
        verify(Timestamp.newBuilder().setSeconds(Long.MAX_VALUE).setNanos(-1).build());
        verify(Duration.newBuilder().setSeconds(1).setNanos(-1).build());
        verify(FieldMask.newBuilder().addPaths("not.a.field").addPaths("").build());
        var duplicate = KeyValue.newBuilder().setKey("same").build();
        verify(KeyValueList.newBuilder().addValues(duplicate).addValues(duplicate).build());
    }

    @Test
    void officialSchemaIsUnmodifiedAndIncludesEveryPinnedOneofCase() throws Exception {
        byte[] source =
                Files.readAllBytes(
                        Path.of("src/test/proto/opentelemetry/proto/common/v1/common.proto"));
        assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(source)))
                .isEqualTo("0429795169e29089cc53490b541a0ae89fa7c80492ac58ed2b3c4171cce4ecee");
        assertThat(AnyValue.getDescriptor().getOneofs().get(0).getFields())
                .extracting(Descriptors.FieldDescriptor::getName)
                .containsExactly(
                        "string_value",
                        "bool_value",
                        "int_value",
                        "double_value",
                        "array_value",
                        "kvlist_value",
                        "bytes_value",
                        "string_value_strindex");
    }
}
