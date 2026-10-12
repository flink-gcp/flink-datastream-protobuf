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

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.github.flink.gcp.protobuf.ProtobufSchemaCompatibility.Metadata;
import io.github.flink.gcp.protobuf.ProtobufSchemaCompatibility.Result;
import io.github.flink.gcp.protobuf.generated.CommonTypesEnvelope;
import io.github.flink.gcp.protobuf.generated.LegacyMessage;
import io.github.flink.gcp.protobuf.generated.RecursiveMessage;
import io.github.flink.gcp.protobuf.generated.RequiredMessage;
import io.github.flink.gcp.protobuf.generated.ScalarMessage;
import io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotMessages.Envelope;
import org.assertj.core.api.AbstractStringAssert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Generated mutations of every declaration reachable from the test message corpus.
 *
 * <p>Each supported mutation must be accepted in the restore direction and rejected when reversed;
 * each unsupported mutation must be rejected. File order and identity are checked as invariants.
 */
class ProtobufSchemaCompatibilityPropertyTest {
    static Stream<Descriptor> roots() {
        return Stream.of(
                Envelope.getDescriptor(),
                CommonTypesEnvelope.getDescriptor(),
                RecursiveMessage.getDescriptor(),
                ScalarMessage.getDescriptor(),
                LegacyMessage.getDescriptor(),
                RequiredMessage.getDescriptor());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roots")
    void identityAndFileOrderDoNotAffectTheResult(Descriptor root) {
        FileDescriptorSet schema = ProtobufSchema.normalize(root.getFile());
        assertCompatible(root, schema, schema);
        Random random = new Random(root.getFullName().hashCode());
        for (int i = 0; i < 10; i++) {
            FileDescriptorSet shuffled = shuffle(schema, random);
            assertCompatible(root, schema, shuffled);
            assertCompatible(root, shuffled, schema);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roots")
    void freshFieldsAreAcceptedOnlyInTheRestoreDirection(Descriptor root) {
        FileDescriptorSet schema = ProtobufSchema.normalize(root.getFile());
        for (Descriptor message : reachableMessages(root)) {
            FileDescriptorSet added =
                    mutateMessage(
                            schema,
                            message,
                            proto ->
                                    proto.toBuilder()
                                            .addField(
                                                    FieldDescriptorProto.newBuilder()
                                                            .setName("generated_added_field")
                                                            .setNumber(freshNumber(proto))
                                                            .setLabel(
                                                                    FieldDescriptorProto.Label
                                                                            .LABEL_OPTIONAL)
                                                            .setType(
                                                                    FieldDescriptorProto.Type
                                                                            .TYPE_STRING))
                                            .build());
            String reverse = "is removed without reserving its number";
            assertOneWay(root, schema, added, message.getFullName(), reverse);
            // Descriptor file order normalizes away even when the schemas differ.
            Random random = new Random(message.getFullName().hashCode());
            FileDescriptorSet shuffled = shuffle(added, random);
            assertThat(evaluate(root, schema, shuffled).reason()).isNull();
            assertThat(evaluate(root, shuffled, schema).reason()).contains(reverse);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roots")
    void removalsRequireReservationAndCannotBeReversed(Descriptor root) {
        FileDescriptorSet schema = ProtobufSchema.normalize(root.getFile());
        for (Descriptor message : reachableMessages(root)) {
            for (FieldDescriptor field : message.getFields()) {
                String label = field.getFullName();
                FileDescriptorSet unreserved =
                        mutateMessage(schema, message, proto -> removeField(proto, field));
                assertRejected(root, schema, unreserved, label)
                        .contains("is removed without reserving its number");
                FileDescriptorSet reserved =
                        mutateMessage(
                                schema,
                                message,
                                proto ->
                                        removeField(proto, field).toBuilder()
                                                .addReservedRange(
                                                        DescriptorProto.ReservedRange.newBuilder()
                                                                .setStart(field.getNumber())
                                                                .setEnd(field.getNumber() + 1))
                                                .build());
                assertOneWay(root, schema, reserved, label, "no longer reserves field numbers");
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roots")
    void typeChangesOfRetainedFieldsAreRejected(Descriptor root) {
        FileDescriptorSet schema = ProtobufSchema.normalize(root.getFile());
        for (Descriptor message : reachableMessages(root)) {
            for (FieldDescriptor field : message.getFields()) {
                FileDescriptorSet changed =
                        mutateMessage(
                                schema,
                                message,
                                proto -> {
                                    DescriptorProto.Builder builder = proto.toBuilder();
                                    FieldDescriptorProto.Builder target =
                                            builder.getFieldBuilder(field.getIndex());
                                    target.setType(alternative(target.getType()))
                                            .clearTypeName()
                                            .clearDefaultValue();
                                    return builder.build();
                                });
                assertRejected(root, schema, changed, field.getFullName())
                        .contains(" changes type from ");
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roots")
    void appendedEnumValuesAreAcceptedOnlyInTheRestoreDirection(Descriptor root) {
        FileDescriptorSet schema = ProtobufSchema.normalize(root.getFile());
        for (EnumDescriptor type : reachableEnums(root)) {
            FileDescriptorSet appended =
                    mutateEnum(
                            schema,
                            type,
                            proto ->
                                    proto.toBuilder()
                                            .addValue(
                                                    EnumValueDescriptorProto.newBuilder()
                                                            .setName("GENERATED_ADDED_VALUE")
                                                            .setNumber(
                                                                    proto.getValueList().stream()
                                                                                    .mapToInt(
                                                                                            EnumValueDescriptorProto
                                                                                                    ::getNumber)
                                                                                    .max()
                                                                                    .orElseThrow()
                                                                            + 1))
                                            .build());
            assertOneWay(
                    root,
                    schema,
                    appended,
                    type.getFullName(),
                    "removes or renames value GENERATED_ADDED_VALUE");
            String last = type.getValues().get(type.getValues().size() - 1).getName();
            if (type.getValues().size() > 1) {
                FileDescriptorSet removed =
                        mutateEnum(
                                schema,
                                type,
                                proto ->
                                        proto.toBuilder()
                                                .removeValue(proto.getValueCount() - 1)
                                                .build());
                assertRejected(root, schema, removed, type.getFullName())
                        .contains("removes or renames value " + last);
            }
        }
    }

    /** Messages that saved bytes can reach, excluding synthesized map entries. */
    private static List<Descriptor> reachableMessages(Descriptor root) {
        return reachable(root).stream()
                .filter(message -> !message.getOptions().getMapEntry())
                .toList();
    }

    private static List<EnumDescriptor> reachableEnums(Descriptor root) {
        Set<EnumDescriptor> enums = new LinkedHashSet<>();
        for (Descriptor message : reachable(root)) {
            for (FieldDescriptor field : message.getFields()) {
                if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                    enums.add(field.getEnumType());
                }
            }
        }
        return List.copyOf(enums);
    }

    private static Set<Descriptor> reachable(Descriptor root) {
        Set<Descriptor> visited = new LinkedHashSet<>();
        ArrayDeque<Descriptor> remaining = new ArrayDeque<>(List.of(root));
        while (!remaining.isEmpty()) {
            Descriptor message = remaining.removeFirst();
            if (visited.add(message)) {
                for (FieldDescriptor field : message.getFields()) {
                    if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                        remaining.add(field.getMessageType());
                    }
                }
            }
        }
        return visited;
    }

    private static FileDescriptorSet shuffle(FileDescriptorSet set, Random random) {
        List<FileDescriptorProto> files = new ArrayList<>(set.getFileList());
        Collections.shuffle(files, random);
        return set.toBuilder().clearFile().addAllFile(files).build();
    }

    private static DescriptorProto removeField(DescriptorProto proto, FieldDescriptor field) {
        return proto.toBuilder().removeField(field.getIndex()).build();
    }

    /** Selects a different type with the same packability so the descriptor stays valid. */
    private static FieldDescriptorProto.Type alternative(FieldDescriptorProto.Type type) {
        return switch (type) {
            case TYPE_INT32 -> FieldDescriptorProto.Type.TYPE_INT64;
            case TYPE_INT64 -> FieldDescriptorProto.Type.TYPE_INT32;
            case TYPE_UINT32 -> FieldDescriptorProto.Type.TYPE_INT32;
            case TYPE_UINT64 -> FieldDescriptorProto.Type.TYPE_INT64;
            case TYPE_SINT32 -> FieldDescriptorProto.Type.TYPE_INT32;
            case TYPE_SINT64 -> FieldDescriptorProto.Type.TYPE_INT64;
            case TYPE_FIXED32 -> FieldDescriptorProto.Type.TYPE_SFIXED32;
            case TYPE_FIXED64 -> FieldDescriptorProto.Type.TYPE_SFIXED64;
            case TYPE_SFIXED32 -> FieldDescriptorProto.Type.TYPE_FIXED32;
            case TYPE_SFIXED64 -> FieldDescriptorProto.Type.TYPE_FIXED64;
            case TYPE_DOUBLE -> FieldDescriptorProto.Type.TYPE_FLOAT;
            case TYPE_FLOAT -> FieldDescriptorProto.Type.TYPE_DOUBLE;
            case TYPE_BOOL, TYPE_ENUM -> FieldDescriptorProto.Type.TYPE_INT32;
            case TYPE_STRING -> FieldDescriptorProto.Type.TYPE_BYTES;
            case TYPE_BYTES, TYPE_MESSAGE, TYPE_GROUP -> FieldDescriptorProto.Type.TYPE_STRING;
        };
    }

    private static int freshNumber(DescriptorProto proto) {
        int max = 0;
        for (FieldDescriptorProto field : proto.getFieldList()) {
            max = Math.max(max, field.getNumber());
        }
        for (DescriptorProto.ReservedRange range : proto.getReservedRangeList()) {
            max = Math.max(max, range.getEnd() - 1);
        }
        int number = max + 1;
        // Numbers 19000 through 19999 are reserved by the Protobuf implementation.
        return number >= 19000 && number <= 19999 ? 20000 : number;
    }

    private static FileDescriptorSet mutateMessage(
            FileDescriptorSet set, Descriptor target, UnaryOperator<DescriptorProto> mutation) {
        List<Integer> path = new ArrayList<>();
        for (Descriptor scope = target; scope != null; scope = scope.getContainingType()) {
            path.add(0, scope.getIndex());
        }
        return mutateFile(
                set,
                target.getFile().getName(),
                file -> {
                    FileDescriptorProto.Builder builder = file.toBuilder();
                    builder.setMessageType(
                            path.get(0),
                            mutateNested(file.getMessageType(path.get(0)), path, 1, mutation));
                    return builder.build();
                });
    }

    private static FileDescriptorSet mutateEnum(
            FileDescriptorSet set,
            EnumDescriptor target,
            UnaryOperator<EnumDescriptorProto> mutation) {
        Descriptor container = target.getContainingType();
        if (container == null) {
            return mutateFile(
                    set,
                    target.getFile().getName(),
                    file ->
                            file.toBuilder()
                                    .setEnumType(
                                            target.getIndex(),
                                            mutation.apply(file.getEnumType(target.getIndex())))
                                    .build());
        }
        return mutateMessage(
                set,
                container,
                proto ->
                        proto.toBuilder()
                                .setEnumType(
                                        target.getIndex(),
                                        mutation.apply(proto.getEnumType(target.getIndex())))
                                .build());
    }

    private static DescriptorProto mutateNested(
            DescriptorProto proto,
            List<Integer> path,
            int depth,
            UnaryOperator<DescriptorProto> mutation) {
        if (depth == path.size()) {
            return mutation.apply(proto);
        }
        int index = path.get(depth);
        return proto.toBuilder()
                .setNestedType(
                        index, mutateNested(proto.getNestedType(index), path, depth + 1, mutation))
                .build();
    }

    private static FileDescriptorSet mutateFile(
            FileDescriptorSet set, String name, UnaryOperator<FileDescriptorProto> mutation) {
        FileDescriptorSet.Builder builder = set.toBuilder();
        boolean found = false;
        for (int i = 0; i < builder.getFileCount(); i++) {
            if (builder.getFile(i).getName().equals(name)) {
                builder.setFile(i, mutation.apply(builder.getFile(i)));
                found = true;
            }
        }
        assertThat(found).as(name).isTrue();
        return builder.build();
    }

    private static void assertCompatible(
            Descriptor root, FileDescriptorSet saved, FileDescriptorSet current) {
        Result result = evaluate(root, saved, current);
        assertThat(result.reason()).isNull();
        assertThat(result.compatibleAsIs()).isTrue();
    }

    private static void assertOneWay(
            Descriptor root,
            FileDescriptorSet saved,
            FileDescriptorSet current,
            String label,
            String reverse) {
        assertThat(evaluate(root, saved, current).reason()).as(label).isNull();
        assertThat(evaluate(root, current, saved).reason()).as(label).contains(reverse);
    }

    private static AbstractStringAssert<?> assertRejected(
            Descriptor root, FileDescriptorSet saved, FileDescriptorSet current, String label) {
        Result result = evaluate(root, saved, current);
        assertThat(result.compatibleAsIs()).as(label).isFalse();
        return assertThat(result.reason()).as(label);
    }

    private static Result evaluate(
            Descriptor root, FileDescriptorSet saved, FileDescriptorSet current) {
        return ProtobufSchemaCompatibility.evaluate(metadata(root, saved), metadata(root, current));
    }

    private static Metadata metadata(Descriptor root, FileDescriptorSet schema) {
        return new Metadata(
                Message.class.getName(),
                root.getFullName(),
                ProtobufSerializerSettings.DEFAULT,
                schema);
    }
}
