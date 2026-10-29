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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.protobuf.generated.BoundedBranch;
import io.github.flink.gcp.protobuf.generated.BoundedLeaf;
import io.github.flink.gcp.protobuf.generated.BoundedMessage;
import io.github.flink.gcp.protobuf.generated.ScalarMessage;
import io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotMessages.Envelope;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static io.github.flink.gcp.protobuf.DynamicDescriptors.field;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.file;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.message;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.messageField;
import static io.github.flink.gcp.protobuf.DynamicDescriptors.rebuild;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Write acceptance of DynamicMessage values whose descriptors are other instances. */
class ProtobufDynamicMessageWriteTest {
    private static final String ENTRY = "flink.protobuf.runtime.Entry";

    @Test
    void acceptsIdenticalEqualContentAndContainingFieldDescriptorsWithCachedEvaluations()
            throws Exception {
        ProtobufTypeSerializer<DynamicMessage> serializer = serializer("original");
        Descriptor own = serializer.createInstance().getDescriptorForType();
        write(serializer, entry(own, 1, entry(own, 2, null)));
        assertThat(serializer.pairEvaluations()).isZero();

        // An equal-content root from another instance is evaluated once, then cached; a child
        // built from that value's own field type needs no further evaluation.
        Descriptor other = RuntimeRecoveryHarness.descriptor("original");
        DynamicMessage value = entry(other, 1, entry(other, 2, entry(other, 3, null)));
        assertThat(roundTrip(serializer, value)).isEqualTo(reparse(own, value));
        write(serializer, value);
        assertThat(serializer.pairEvaluations()).isOne();

        // A child of the serializer's own instance under that root is its own declaration.
        write(serializer, entry(other, 1, entry(own, 2, entry(own, 3, null))));
        assertThat(serializer.pairEvaluations()).isOne();
        // A child of a third instance is evaluated against the serializer's child declaration.
        Descriptor third = RuntimeRecoveryHarness.descriptor("original");
        write(serializer, entry(own, 1, entry(third, 2, null)));
        assertThat(serializer.pairEvaluations()).isEqualTo(2);
        // A duplicate has its own cache.
        ProtobufTypeSerializer<DynamicMessage> duplicate = serializer.duplicate();
        write(duplicate, value);
        assertThat(duplicate.pairEvaluations()).isOne();
    }

    @Test
    void acceptsEarlierSchemasThatTheEvaluatorAcceptsWithTheirValuesUnchanged() throws Exception {
        for (String[] pair :
                List.of(
                        new String[] {"original", "changed"},
                        new String[] {"original", "reserved"},
                        new String[] {"enum-original", "enum-added"})) {
            ProtobufTypeSerializer<DynamicMessage> serializer = serializer(pair[1]);
            Descriptor saved = RuntimeRecoveryHarness.descriptor(pair[0]);
            DynamicMessage value =
                    entry(saved, 1, entry(saved, 2, null)).toBuilder()
                            .setField(saved.findFieldByName("payload"), "kept")
                            .build();
            DynamicMessage decoded = roundTrip(serializer, value);
            // A reserved field's value moves after the declared fields as an unknown field.
            assertThat(DynamicMessage.parseFrom(saved, decoded.toByteString())).isEqualTo(value);
            assertThat(serializer.pairEvaluations()).isOne();
        }
    }

    @Test
    void rejectsUnsupportedSchemaPairsWithTheFieldPathAndTheEvaluatorReason() throws Exception {
        // The pairs that the runtime schema-evolution suite rejects, with the same reasons.
        assertRejected(
                "original",
                "incompatible",
                ENTRY + ": Field 2 (count) of " + ENTRY + " changes type from INT32 to SINT32");
        assertRejected(
                "changed",
                "original",
                ENTRY
                        + ": Field 5 (added) of "
                        + ENTRY
                        + " is removed without reserving its number");
        assertRejected(
                "enum-added",
                "enum-original",
                ENTRY + ": Enum " + ENTRY + ".Kind removes or renames value KIND_SECOND");

        // A nested value is rejected with its path, under an accepted root.
        ProtobufTypeSerializer<DynamicMessage> serializer = serializer("original");
        Descriptor own = serializer.createInstance().getDescriptorForType();
        Descriptor incompatible = RuntimeRecoveryHarness.descriptor("incompatible");
        assertWriteFails(
                serializer,
                entry(own, 1, entry(own, 2, entry(incompatible, 3, null))),
                "Cannot write DynamicMessage value at "
                        + ENTRY
                        + ".child.child: Field 2 (count) of "
                        + ENTRY
                        + " changes type from SINT32 to INT32");
        // Another message type is rejected by its full name.
        assertWriteFails(
                serializer,
                DynamicMessage.getDefaultInstance(rebuild(ScalarMessage.getDescriptor())),
                "Protobuf message changes from flink.protobuf.serializer.ScalarMessage to "
                        + ENTRY);
        // A rejected pair is not cached, so it is evaluated again.
        int evaluations = serializer.pairEvaluations();
        assertWriteFails(
                serializer, entry(incompatible, 1, null), "changes type from SINT32 to INT32");
        assertThat(serializer.pairEvaluations()).isEqualTo(evaluations + 1);
    }

    @Test
    void rejectsANonProto3TypeReachedThroughAFieldThatTheSerializerReserves() throws Exception {
        DescriptorProto.Builder reserving =
                message("Holder", field("id", 1, FieldDescriptorProto.Type.TYPE_INT32))
                        .addReservedRange(
                                DescriptorProto.ReservedRange.newBuilder().setStart(2).setEnd(3));
        Descriptor current =
                DynamicDescriptors.build(
                        DynamicDescriptors.PACKAGE + ".Holder",
                        file("dynamic/holder.proto").addMessageType(reserving).build());
        Descriptor saved =
                DynamicDescriptors.build(
                        DynamicDescriptors.PACKAGE + ".Holder",
                        file("dynamic/legacy.proto")
                                .setSyntax("proto2")
                                .addMessageType(
                                        message(
                                                "Legacy",
                                                field(
                                                        "value",
                                                        1,
                                                        FieldDescriptorProto.Type.TYPE_INT32)))
                                .build(),
                        file("dynamic/holder.proto")
                                .addDependency("dynamic/legacy.proto")
                                .addMessageType(
                                        message(
                                                "Holder",
                                                field(
                                                        "id",
                                                        1,
                                                        FieldDescriptorProto.Type.TYPE_INT32),
                                                messageField("legacy", 2, "Legacy")))
                                .build());
        ProtobufTypeSerializer<DynamicMessage> serializer = serializer(current);
        assertWriteFails(
                serializer,
                DynamicMessage.getDefaultInstance(saved),
                "Cannot write DynamicMessage value at flink.protobuf.dynamic.Holder: Payload"
                        + " message flink.protobuf.dynamic.Legacy is declared in"
                        + " dynamic/legacy.proto with syntax proto2");
    }

    @Test
    void rejectsUnknownFieldsThatCollideWithAFieldOnlyTheSerializerDeclares() throws Exception {
        Descriptor saved = RuntimeRecoveryHarness.descriptor("original");
        UnknownFieldSet added =
                UnknownFieldSet.newBuilder()
                        .addField(
                                5,
                                UnknownFieldSet.Field.newBuilder()
                                        .addLengthDelimited(ByteString.copyFromUtf8("x"))
                                        .build())
                        .build();
        DynamicMessage root = entry(saved, 1, null).toBuilder().setUnknownFields(added).build();
        DynamicMessage nested = entry(saved, 1, root);
        // The saved schema's own reader keeps the field unknown.
        assertThat(roundTrip(serializer("original"), root).getUnknownFields()).isEqualTo(added);
        String collision =
                "Unknown length-delimited field 5 of "
                        + ENTRY
                        + " would be parsed as declared field "
                        + ENTRY
                        + ".added";
        for (DynamicMessage value : List.of(root, nested)) {
            assertWriteFails(serializer("changed"), value, collision);
        }
    }

    @Test
    void boundedValuesOnTheGeneratedSizePathAreAcceptedOrRejectedLikeTraversedOnes()
            throws Exception {
        // BoundedMessage is acyclic, so small values first try the runtime's generated size.
        Descriptor saved = BoundedMessage.getDescriptor();
        ProtobufTypeSerializer<DynamicMessage> serializer = serializer(rebuild(saved));
        Descriptor own = serializer.createInstance().getDescriptorForType();
        BoundedMessage generated =
                BoundedMessage.newBuilder()
                        .setLeaf(BoundedLeaf.newBuilder().setCode(1))
                        .putCounts("k", 2)
                        .setBranch(BoundedBranch.newBuilder().putLabels(3, "label"))
                        .build();
        Descriptor other = rebuild(saved);
        DynamicMessage value = DynamicMessage.parseFrom(other, generated.toByteString());
        assertThat(roundTrip(serializer, value).toByteString()).isEqualTo(generated.toByteString());
        // Nested values and map entries built from the root's own types need no evaluation.
        assertThat(serializer.pairEvaluations()).isOne();

        Descriptor changed =
                bounded(
                        file -> {
                            for (DescriptorProto.Builder message :
                                    file.getMessageTypeBuilderList()) {
                                if (message.getName().equals("BoundedLeaf")) {
                                    message.getFieldBuilder(0)
                                            .setType(FieldDescriptorProto.Type.TYPE_SINT32);
                                }
                                if (message.getName().equals("BoundedMessage")) {
                                    for (DescriptorProto.Builder entry :
                                            message.getNestedTypeBuilderList()) {
                                        if (entry.getName().equals("CountsEntry")) {
                                            entry.getFieldBuilder(1)
                                                    .setType(FieldDescriptorProto.Type.TYPE_SINT64);
                                        }
                                    }
                                }
                            }
                        });
        Descriptor leaf = changed.findFieldByName("leaf").getMessageType();
        Descriptor counts = changed.findFieldByName("counts").getMessageType();
        DynamicMessage.Builder root = DynamicMessage.newBuilder(own);
        assertWriteFails(
                serializer,
                root.clone()
                        .setField(
                                own.findFieldByName("leaf"),
                                DynamicMessage.newBuilder(leaf)
                                        .setField(leaf.findFieldByName("code"), 1)
                                        .build())
                        .build(),
                "Cannot write DynamicMessage value at flink.protobuf.serializer.BoundedMessage.leaf:"
                        + " Field 1 (code) of flink.protobuf.serializer.BoundedLeaf changes type"
                        + " from SINT32 to INT32");
        assertWriteFails(
                serializer,
                root.clone()
                        .addRepeatedField(
                                own.findFieldByName("counts"),
                                DynamicMessage.newBuilder(counts)
                                        .setField(counts.findFieldByName("key"), "k")
                                        .setField(counts.findFieldByName("value"), -1L)
                                        .build())
                        .build(),
                "Cannot write DynamicMessage value at"
                        + " flink.protobuf.serializer.BoundedMessage.counts[0]: Field 2 (value) of"
                        + " flink.protobuf.serializer.BoundedMessage.CountsEntry changes type from"
                        + " SINT64 to INT64");
        // A map entry of the serializer's own type can still carry a colliding unknown field,
        // for a scalar-valued and for a string-valued map.
        FieldDescriptor countsField = own.findFieldByName("counts");
        Descriptor ownCounts = countsField.getMessageType();
        assertWriteFails(
                serializer,
                root.clone()
                        .addRepeatedField(
                                countsField,
                                DynamicMessage.newBuilder(ownCounts)
                                        .setField(ownCounts.findFieldByName("key"), "k")
                                        .setUnknownFields(
                                                unknown(
                                                        2,
                                                        UnknownFieldSet.Field.newBuilder()
                                                                .addVarint(7)))
                                        .build())
                        .build(),
                "Unknown varint field 2 of flink.protobuf.serializer.BoundedMessage.CountsEntry"
                        + " would be parsed as declared field"
                        + " flink.protobuf.serializer.BoundedMessage.CountsEntry.value");
        FieldDescriptor branchField = own.findFieldByName("branch");
        Descriptor branch = branchField.getMessageType();
        FieldDescriptor labelsField = branch.findFieldByName("labels");
        Descriptor labels = labelsField.getMessageType();
        assertWriteFails(
                serializer,
                root.clone()
                        .setField(
                                branchField,
                                DynamicMessage.newBuilder(branch)
                                        .addRepeatedField(
                                                labelsField,
                                                DynamicMessage.newBuilder(labels)
                                                        .setField(labels.findFieldByName("key"), 3)
                                                        .setUnknownFields(
                                                                unknown(
                                                                        2,
                                                                        UnknownFieldSet.Field
                                                                                .newBuilder()
                                                                                .addLengthDelimited(
                                                                                        ByteString
                                                                                                .copyFromUtf8(
                                                                                                        "x"))))
                                                        .build())
                                        .build())
                        .build(),
                "would be parsed as declared field"
                        + " flink.protobuf.serializer.BoundedBranch.LabelsEntry.value");
        // A default instance of another type is checked like any other value.
        assertWriteFails(
                serializer,
                root.clone()
                        .setField(own.findFieldByName("leaf"), ScalarMessage.getDefaultInstance())
                        .build(),
                "Protobuf message changes from flink.protobuf.serializer.ScalarMessage to"
                        + " flink.protobuf.serializer.BoundedLeaf");
    }

    @Test
    void nestedTypesOfAnAcceptedRootNeedNoFurtherEvaluation() throws Exception {
        // Envelope reaches a nested type, an imported type, and itself.
        Descriptor root = rebuild(Envelope.getDescriptor());
        ProtobufTypeSerializer<DynamicMessage> serializer = serializer(root);
        Descriptor other = rebuild(root);
        DynamicMessage value =
                ProtobufDynamicMessageTest.dynamic(other, ProtobufDynamicMessageTest.envelope());
        assertThat(roundTrip(serializer, value).toByteString()).isEqualTo(value.toByteString());
        assertThat(serializer.pairEvaluations()).isOne();
    }

    @Test
    void appliesThePayloadRulesToAValueUnderAFieldThatTheReaderLacks() throws Exception {
        Descriptor current =
                DynamicDescriptors.build(
                        DynamicDescriptors.PACKAGE + ".Holder",
                        file("dynamic/holder.proto")
                                .addMessageType(
                                        message(
                                                        "Holder",
                                                        field(
                                                                "id",
                                                                1,
                                                                FieldDescriptorProto.Type
                                                                        .TYPE_INT32))
                                                .addReservedRange(
                                                        DescriptorProto.ReservedRange.newBuilder()
                                                                .setStart(2)
                                                                .setEnd(3)))
                                .build());
        Descriptor saved =
                DynamicDescriptors.build(
                        DynamicDescriptors.PACKAGE + ".Holder",
                        file("dynamic/holder.proto")
                                .addMessageType(
                                        message(
                                                "Inner",
                                                field(
                                                        "value",
                                                        1,
                                                        FieldDescriptorProto.Type.TYPE_INT32)))
                                .addMessageType(
                                        message(
                                                "Holder",
                                                field(
                                                        "id",
                                                        1,
                                                        FieldDescriptorProto.Type.TYPE_INT32),
                                                messageField("inner", 2, "Inner")))
                                .build());
        Descriptor legacy =
                DynamicDescriptors.build(
                        DynamicDescriptors.PACKAGE + ".Legacy",
                        file("dynamic/legacy.proto")
                                .setSyntax("proto2")
                                .addMessageType(
                                        message(
                                                "Legacy",
                                                field(
                                                        "value",
                                                        1,
                                                        FieldDescriptorProto.Type.TYPE_INT32)))
                                .build());
        ProtobufTypeSerializer<DynamicMessage> serializer = serializer(current);
        FieldDescriptor inner = saved.findFieldByName("inner");
        // The reader keeps field 2 unknown, so a proto3 value there is written as is.
        Descriptor otherInner = rebuild(inner.getMessageType());
        DynamicMessage accepted =
                DynamicMessage.newBuilder(saved)
                        .setField(
                                inner,
                                DynamicMessage.newBuilder(otherInner)
                                        .setField(otherInner.findFieldByName("value"), 1)
                                        .build())
                        .build();
        assertThat(roundTrip(serializer, accepted).getUnknownFields().hasField(2)).isTrue();
        // Its own unknown fields cannot collide with anything: the reader keeps all of field 2.
        Descriptor ownInner = inner.getMessageType();
        DynamicMessage colliding =
                DynamicMessage.newBuilder(saved)
                        .setField(
                                inner,
                                DynamicMessage.newBuilder(ownInner)
                                        .setUnknownFields(
                                                unknown(
                                                        1,
                                                        UnknownFieldSet.Field.newBuilder()
                                                                .addVarint(5)))
                                        .build())
                        .build();
        assertThat(roundTrip(serializer, colliding).getUnknownFields().hasField(2)).isTrue();
        assertWriteFails(
                serializer,
                DynamicMessage.newBuilder(saved)
                        .setField(inner, DynamicMessage.getDefaultInstance(legacy))
                        .build(),
                "Cannot write DynamicMessage value at flink.protobuf.dynamic.Holder.inner: Payload"
                        + " message flink.protobuf.dynamic.Legacy is declared in"
                        + " dynamic/legacy.proto with syntax proto2");
    }

    @Test
    void rejectsLiteMessageValuesWithAnIOException() throws Exception {
        // DynamicMessage accepts any MessageLite in a message field; buildPartial skips the
        // initialization check, so no method of the lite double runs.
        ProtobufTypeSerializer<DynamicMessage> bounded =
                serializer(rebuild(BoundedMessage.getDescriptor()));
        Descriptor root = bounded.createInstance().getDescriptorForType();
        assertWriteFails(
                bounded,
                DynamicMessage.newBuilder(root)
                        .setField(root.findFieldByName("leaf"), new LiteOnly())
                        .buildPartial(),
                "Cannot write DynamicMessage value at flink.protobuf.serializer.BoundedMessage.leaf:"
                        + " Unsupported message value of");
        ProtobufTypeSerializer<DynamicMessage> recursive = serializer("original");
        Descriptor entry = recursive.createInstance().getDescriptorForType();
        assertWriteFails(
                recursive,
                DynamicMessage.newBuilder(entry)
                        .setField(entry.findFieldByName("child"), new LiteOnly())
                        .buildPartial(),
                "Cannot write DynamicMessage value at "
                        + ENTRY
                        + ".child: Unsupported message value");
    }

    @Test
    void evictsTheLeastRecentlyUsedPairBeyondTheCacheSize() throws Exception {
        assertThat(ProtobufMessageSize.PAIR_CACHE_SIZE).isEqualTo(16);
        ProtobufTypeSerializer<DynamicMessage> serializer = serializer("original");
        List<Descriptor> instances = new ArrayList<>();
        for (int i = 0; i <= ProtobufMessageSize.PAIR_CACHE_SIZE; i++) {
            instances.add(RuntimeRecoveryHarness.descriptor("original"));
        }
        for (int i = 0; i < ProtobufMessageSize.PAIR_CACHE_SIZE; i++) {
            write(serializer, entry(instances.get(i), i, null));
        }
        assertThat(serializer.pairEvaluations()).isEqualTo(ProtobufMessageSize.PAIR_CACHE_SIZE);
        // Using the first pair again makes the second the least recently used.
        write(serializer, entry(instances.get(0), 0, null));
        write(serializer, entry(instances.get(ProtobufMessageSize.PAIR_CACHE_SIZE), 0, null));
        int evaluations = ProtobufMessageSize.PAIR_CACHE_SIZE + 1;
        assertThat(serializer.pairEvaluations()).isEqualTo(evaluations);
        write(serializer, entry(instances.get(0), 0, null));
        assertThat(serializer.pairEvaluations()).isEqualTo(evaluations);
        write(serializer, entry(instances.get(1), 0, null));
        assertThat(serializer.pairEvaluations()).isEqualTo(evaluations + 1);
    }

    /** Builds BoundedMessage from a modified copy of its file, with its imported file. */
    private static Descriptor bounded(
            java.util.function.Consumer<FileDescriptorProto.Builder> change) throws Exception {
        FileDescriptorProto.Builder sizing =
                BoundedMessage.getDescriptor().getFile().toProto().toBuilder();
        change.accept(sizing);
        return DynamicDescriptors.build(
                BoundedMessage.getDescriptor().getFullName(),
                ScalarMessage.getDescriptor().getFile().toProto(),
                sizing.build());
    }

    private static UnknownFieldSet unknown(int number, UnknownFieldSet.Field.Builder field) {
        return UnknownFieldSet.newBuilder().addField(number, field.build()).build();
    }

    private static void assertRejected(String saved, String current, String reason)
            throws Exception {
        Descriptor descriptor = RuntimeRecoveryHarness.descriptor(saved);
        assertWriteFails(
                serializer(current),
                entry(descriptor, 1, null),
                "Cannot write DynamicMessage value at " + reason);
    }

    private static void assertWriteFails(
            ProtobufTypeSerializer<DynamicMessage> serializer,
            DynamicMessage value,
            String reason) {
        DataOutputSerializer out = new DataOutputSerializer(64);
        assertThatThrownBy(() -> serializer.serialize(value, out))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(reason);
        assertThat(out.length()).isZero();
    }

    private static ProtobufTypeSerializer<DynamicMessage> serializer(String variant)
            throws Exception {
        return serializer(RuntimeRecoveryHarness.descriptor(variant));
    }

    private static ProtobufTypeSerializer<DynamicMessage> serializer(Descriptor descriptor) {
        return (ProtobufTypeSerializer<DynamicMessage>)
                ProtobufTypeInformation.of(descriptor).createSerializer(new SerializerConfigImpl());
    }

    /** Builds an entry whose child, if any, keeps its own descriptor instance. */
    private static DynamicMessage entry(Descriptor descriptor, int ordinal, DynamicMessage child) {
        DynamicMessage.Builder builder =
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("ordinal"), ordinal);
        if (child != null) {
            FieldDescriptor field = descriptor.findFieldByName("child");
            builder.setField(field, child);
        }
        return builder.build();
    }

    private static void write(
            ProtobufTypeSerializer<DynamicMessage> serializer, DynamicMessage value)
            throws IOException {
        serializer.serialize(value, new DataOutputSerializer(64));
    }

    private static DynamicMessage roundTrip(
            ProtobufTypeSerializer<DynamicMessage> serializer, DynamicMessage value)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
    }

    private static DynamicMessage reparse(Descriptor descriptor, DynamicMessage value)
            throws IOException {
        return DynamicMessage.parseFrom(descriptor, value.toByteString());
    }

    /** A lite message whose methods the serializer must not call. */
    private static final class LiteOnly implements MessageLite {
        @Override
        public void writeTo(CodedOutputStream output) {
            throw new AssertionError();
        }

        @Override
        public int getSerializedSize() {
            throw new AssertionError();
        }

        @Override
        public Parser<? extends MessageLite> getParserForType() {
            throw new AssertionError();
        }

        @Override
        public ByteString toByteString() {
            throw new AssertionError();
        }

        @Override
        public byte[] toByteArray() {
            throw new AssertionError();
        }

        @Override
        public void writeTo(OutputStream output) {
            throw new AssertionError();
        }

        @Override
        public void writeDelimitedTo(OutputStream output) {
            throw new AssertionError();
        }

        @Override
        public MessageLite.Builder newBuilderForType() {
            throw new AssertionError();
        }

        @Override
        public MessageLite.Builder toBuilder() {
            throw new AssertionError();
        }

        @Override
        public MessageLite getDefaultInstanceForType() {
            throw new AssertionError();
        }

        @Override
        public boolean isInitialized() {
            throw new AssertionError();
        }
    }
}
