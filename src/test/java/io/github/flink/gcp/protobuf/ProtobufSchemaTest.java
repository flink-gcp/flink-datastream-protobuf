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

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.SourceCodeInfo;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.protobuf.generated.OptionMessage;
import io.github.flink.gcp.protobuf.generated.RecursiveMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufSchemaTest {
    @Test
    void importsAreOrderedTopologicallyWithLexicalTieBreaking() throws Exception {
        FileDescriptor z = build(file("z.proto"));
        FileDescriptor b = build(file("b.proto").toBuilder().addDependency("z.proto").build(), z);
        FileDescriptor a = build(file("a.proto"));
        FileDescriptorProto root =
                file("root.proto").toBuilder()
                        .addDependency("b.proto")
                        .addDependency("a.proto")
                        .build();
        FileDescriptorSet first = ProtobufSchema.normalize(build(root, b, a));
        FileDescriptorSet second = ProtobufSchema.normalize(build(root, a, b));
        assertThat(first).isEqualTo(second);
        assertThat(first.getFileList())
                .extracting(FileDescriptorProto::getName)
                .containsExactly("a.proto", "z.proto", "b.proto", "root.proto");
        assertThat(first.getFile(3).getDependencyList()).containsExactly("b.proto", "a.proto");
    }

    @Test
    void normalizationRemovesOnlySourceCodeInfo() throws Exception {
        FieldDescriptorProto one =
                FieldDescriptorProto.newBuilder()
                        .setName("one")
                        .setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_INT32)
                        .build();
        FieldDescriptorProto two = one.toBuilder().setName("two").setNumber(2).build();
        DescriptorProto message =
                DescriptorProto.newBuilder().setName("Value").addField(one).addField(two).build();
        FileDescriptorProto root = file("value.proto").toBuilder().addMessageType(message).build();
        FileDescriptorSet plain = ProtobufSchema.normalize(build(root));
        FileDescriptorProto source =
                root.toBuilder()
                        .setSourceCodeInfo(
                                SourceCodeInfo.newBuilder()
                                        .addLocation(
                                                SourceCodeInfo.Location.newBuilder()
                                                        .setLeadingComments("ignored")))
                        .build();
        assertThat(ProtobufSchema.normalize(build(source))).isEqualTo(plain);
        for (FileDescriptorProto changed :
                List.of(
                        root.toBuilder()
                                .setOptions(FileOptions.newBuilder().setDeprecated(true))
                                .build(),
                        root.toBuilder()
                                .setMessageType(
                                        0,
                                        message.toBuilder()
                                                .clearField()
                                                .addField(two)
                                                .addField(one))
                                .build(),
                        root.toBuilder()
                                .setUnknownFields(
                                        UnknownFieldSet.newBuilder()
                                                .addField(
                                                        1000,
                                                        UnknownFieldSet.Field.newBuilder()
                                                                .addVarint(42)
                                                                .build())
                                                .build())
                                .build())) {
            assertThat(ProtobufSchema.normalize(build(changed))).isNotEqualTo(plain);
        }
    }

    @Test
    void importedOptionsAndUnrelatedDeclarationsParticipateInIdentity() throws Exception {
        FileDescriptorProto dependency = file("dependency.proto");
        FileDescriptorProto root =
                file("root.proto").toBuilder().addDependency("dependency.proto").build();
        FileDescriptorSet original = ProtobufSchema.normalize(build(root, build(dependency)));
        FileDescriptorProto modified =
                dependency.toBuilder()
                        .addMessageType(DescriptorProto.newBuilder().setName("Unrelated"))
                        .build();
        assertThat(ProtobufSchema.normalize(build(root, build(modified)))).isNotEqualTo(original);
        modified =
                dependency.toBuilder()
                        .setOptions(FileOptions.newBuilder().setDeprecated(true))
                        .build();
        assertThat(ProtobufSchema.normalize(build(root, build(modified)))).isNotEqualTo(original);
    }

    @Test
    void customOptionsBecomeUnknownContentWithoutDiscardingTheirBytes() throws Exception {
        FileDescriptor root = OptionMessage.getDescriptor().getFile();
        FileDescriptorSet normalized = ProtobufSchema.normalize(root);
        FileDescriptorProto options =
                normalized.getFileList().stream()
                        .filter(f -> f.getName().equals("options.proto"))
                        .findFirst()
                        .orElseThrow();
        assertThat(
                        options.getMessageType(0)
                                .getOptions()
                                .getUnknownFields()
                                .getField(50000)
                                .getLengthDelimitedList())
                .containsExactly(ByteString.copyFromUtf8("retained metadata"));
        FileDescriptorProto decodedWithoutRegistry =
                FileDescriptorProto.parseFrom(root.toProto().toByteArray());
        assertThat(
                        ProtobufSchema.normalize(
                                build(
                                        decodedWithoutRegistry,
                                        root.getDependencies().toArray(new FileDescriptor[0]))))
                .isEqualTo(normalized);
        assertThat(normalized.getFileList())
                .extracting(FileDescriptorProto::getName)
                .contains("google/protobuf/descriptor.proto");
    }

    @Test
    void recursivePayloadTypesAreSupported() {
        assertThat(ProtobufMessageType.resolve(RecursiveMessage.class).schema.getFileCount())
                .isEqualTo(1);
    }

    @Test
    void missingConflictingAndCyclicImportsFailExplicitly() throws Exception {
        FileDescriptorProto root =
                file("root.proto").toBuilder().addDependency("missing.proto").build();
        assertThatThrownBy(() -> ProtobufSchema.order(Map.of("root.proto", root)))
                .hasMessageContaining("Unresolved");
        FileDescriptorProto left =
                file("left.proto").toBuilder().addDependency("right.proto").build();
        FileDescriptorProto right =
                file("right.proto").toBuilder().addDependency("left.proto").build();
        assertThatThrownBy(
                        () ->
                                ProtobufSchema.order(
                                        Map.of("left.proto", left, "right.proto", right)))
                .hasMessageContaining("Cyclic");
        assertThatThrownBy(
                        () -> ProtobufSchema.order(Map.of("other.proto", file("original.proto"))))
                .hasMessageContaining("name mismatch");
        FileDescriptor shared = build(file("shared.proto"));
        FileDescriptor different =
                build(
                        file("shared.proto").toBuilder()
                                .setOptions(FileOptions.newBuilder().setDeprecated(true))
                                .build());
        FileDescriptor a =
                build(file("a.proto").toBuilder().addDependency("shared.proto").build(), shared);
        FileDescriptor b =
                build(file("b.proto").toBuilder().addDependency("shared.proto").build(), different);
        FileDescriptor conflict =
                build(
                        file("root.proto").toBuilder()
                                .addDependency("a.proto")
                                .addDependency("b.proto")
                                .build(),
                        a,
                        b);
        assertThatThrownBy(() -> ProtobufSchema.normalize(conflict))
                .hasMessageContaining("Conflicting descriptor file: shared.proto");
    }

    private static FileDescriptorProto file(String name) {
        return FileDescriptorProto.newBuilder().setName(name).setSyntax("proto2").build();
    }

    private static FileDescriptor build(FileDescriptorProto file, FileDescriptor... dependencies)
            throws Exception {
        return FileDescriptor.buildFrom(file, dependencies);
    }
}
