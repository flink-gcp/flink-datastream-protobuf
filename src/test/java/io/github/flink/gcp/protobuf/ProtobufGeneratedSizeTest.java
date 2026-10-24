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

import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.SourceCodeInfo.Location;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.protobuf.generated.BoundedBranch;
import io.github.flink.gcp.protobuf.generated.BoundedLeaf;
import io.github.flink.gcp.protobuf.generated.BoundedLegacy;
import io.github.flink.gcp.protobuf.generated.BoundedMessage;
import io.github.flink.gcp.protobuf.generated.BoundedRequired;
import io.github.flink.gcp.protobuf.generated.ReachesRequired;
import io.github.flink.gcp.protobuf.generated.RequiredMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Compares serialization through generated sizes with decisions made by the traversal alone. */
class ProtobufGeneratedSizeTest {
    private static final int MAX_SIZE = 64 * 1024 * 1024;

    @Test
    void boundedProto3GraphsKeepTheTraversalsDecisions() throws Exception {
        Random random = new Random(112);
        for (int iteration = 0; iteration < 150; iteration++) {
            assertBoundaries(
                    BoundedMessage.class,
                    message(random, new ArrayList<>(), new ArrayList<>()),
                    random);
        }
        // The runtime logs every write of an unpaired surrogate, so only one value includes one.
        BoundedMessage surrogate =
                BoundedMessage.newBuilder()
                        .setText("a\ud800")
                        .setLeaf(BoundedLeaf.newBuilder().setName("\udc00b"))
                        .build();
        assertBoundaries(BoundedMessage.class, surrogate, random);
    }

    @Test
    void boundedProto2GraphsKeepTheTraversalsDecisions() throws Exception {
        Random random = new Random(1120);
        for (int iteration = 0; iteration < 150; iteration++) {
            assertBoundaries(BoundedLegacy.class, legacy(random), random);
        }
    }

    @Test
    void overflowingGeneratedSizesAreRejectedAsOversizedBeforeWriting() throws Exception {
        // Each occurrence takes exactly 1 MiB, so 4,096 of them wrap the runtime's int size to 0.
        ByteString chunk = ByteString.copyFrom(new byte[(1 << 20) - 4]);
        BoundedLeaf.Builder wrapped = BoundedLeaf.newBuilder();
        BoundedMessage.Builder direct = BoundedMessage.newBuilder();
        for (int i = 0; i < 4096; i++) {
            wrapped.addChunks(chunk);
            direct.addChunks(chunk);
        }
        BoundedLeaf leaf = wrapped.build();
        List<BoundedMessage> values =
                List.of(
                        direct.build(),
                        BoundedMessage.newBuilder().setId(1).setLeaf(leaf).build(),
                        // A map value at the static height 3 still uses the generated size.
                        BoundedMessage.newBuilder()
                                .setBranch(BoundedBranch.newBuilder().putNamed("key", leaf))
                                .build());
        ProtobufMessageSize sizer = sizer(BoundedMessage.class, MAX_SIZE, 3);
        ProtobufTypeSerializer<BoundedMessage> serializer =
                new ProtobufTypeSerializer<>(BoundedMessage.class);
        for (BoundedMessage value : values) {
            // Only the generated size can be this small; the traversal would reject the value.
            assertThat(sizer.check(value)).isEqualTo(value.getSerializedSize()).isBetween(0, 16);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            assertThatThrownBy(
                            () ->
                                    serializer.serialize(
                                            value, new DataOutputViewStreamWrapper(bytes)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("maxMessageSize");
            assertThat(bytes.size()).isZero();
        }
        // Half as many occurrences wrap to a negative size, which the traversal decides.
        BoundedMessage.Builder half = BoundedMessage.newBuilder();
        for (int i = 0; i < 2048; i++) {
            half.addChunks(chunk);
        }
        BoundedMessage negative = half.build();
        assertThat(negative.getSerializedSize()).isNegative();
        assertThatThrownBy(() -> sizer.check(negative)).hasMessageContaining("maxMessageSize");
    }

    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void sharedRepeatedStringsAreRejectedWithoutMeasuringEveryOccurrence() throws Exception {
        // The runtime would measure a 1 MiB string again at each of 262,144 occurrences,
        // 256 GiB of characters, before the size limit rejects any of these values.
        String shared = "x".repeat(1 << 20);
        BoundedMessage.Builder repeated = BoundedMessage.newBuilder();
        BoundedBranch.Builder mapped = BoundedBranch.newBuilder();
        for (int i = 0; i < 1 << 18; i++) {
            repeated.addStrings(shared);
            mapped.putLabels(i, shared);
        }
        List<BoundedMessage> values =
                List.of(repeated.build(), BoundedMessage.newBuilder().setBranch(mapped).build());
        for (BoundedMessage value : values) {
            assertThatThrownBy(() -> sizer(BoundedMessage.class, MAX_SIZE, 100).check(value))
                    .hasMessageContaining("maxMessageSize");
        }
        // A replacement character can hide retained proto2 bytes, so the traversal counts the
        // root's output, whose writer first sizes packed fields and every string occurrence.
        String ambiguous = "\ufffd" + "x".repeat((1 << 20) - 1);
        Location.Builder location = Location.newBuilder().addPath(1);
        for (int i = 0; i < 1 << 18; i++) {
            location.addLeadingDetachedComments(ambiguous);
        }
        Location raw = location.build();
        assertThatThrownBy(() -> sizer(Location.class, MAX_SIZE, 100).check(raw))
                .hasMessageContaining("maxMessageSize");
    }

    @Test
    void unknownGroupsKeepTheirDepthAtEveryNestedPosition() throws Exception {
        // Three nested groups below a value at depth 1 reach depth 4, above the static height 3.
        UnknownFieldSet groups = groups(3);
        BoundedLeaf leaf = BoundedLeaf.newBuilder().setUnknownFields(groups).build();
        BoundedBranch branch = BoundedBranch.newBuilder().setUnknownFields(groups).build();
        List<BoundedMessage> values =
                List.of(
                        BoundedMessage.newBuilder().setUnknownFields(groups(4)).build(),
                        BoundedMessage.newBuilder().setLeaf(leaf).build(),
                        BoundedMessage.newBuilder().setSelected(leaf).build(),
                        BoundedMessage.newBuilder().setBranch(branch).build(),
                        BoundedMessage.newBuilder().addBranches(branch).build(),
                        BoundedMessage.newBuilder()
                                .setBranch(BoundedBranch.newBuilder().setLeaf(leaf))
                                .build(),
                        BoundedMessage.newBuilder()
                                .addBranches(BoundedBranch.newBuilder().addLeaves(leaf))
                                .build(),
                        BoundedMessage.newBuilder()
                                .setBranch(BoundedBranch.newBuilder().putNamed("key", leaf))
                                .build());
        for (BoundedMessage value : values) {
            assertThatThrownBy(() -> sizer(BoundedMessage.class, MAX_SIZE, 3).check(value))
                    .as("%s", value)
                    .hasMessageContaining("recursionLimit");
            assertThat(sizer(BoundedMessage.class, MAX_SIZE, 100).check(value))
                    .isEqualTo(value.getSerializedSize());
        }
    }

    @Test
    void onlyStaticallyBoundedValuesWithoutUnknownGroupsUseTheGeneratedSize() throws Exception {
        // The generated size omits the required-field check that serialization performs later,
        // so a missing field shows which path decided.
        BoundedRequired missing = BoundedRequired.newBuilder().buildPartial();
        BoundedLegacy grouped =
                BoundedLegacy.newBuilder()
                        .setId("root")
                        .setItem(BoundedLegacy.Item.newBuilder().setValue(missing).buildPartial())
                        .buildPartial();
        assertGenerated(BoundedLegacy.class, grouped, 2);
        // Serialization still names the missing field when the generated size decided.
        assertThatThrownBy(
                        () ->
                                new ProtobufTypeSerializer<>(BoundedLegacy.class)
                                        .serialize(
                                                grouped,
                                                new DataOutputViewStreamWrapper(
                                                        new ByteArrayOutputStream())))
                .hasMessage(
                        "Uninitialized message: missing flink.protobuf.serializer.BoundedRequired.value");
        // The group's value reaches the static height 2, so a lower limit needs a traversal even
        // for a value of depth 1.
        BoundedLegacy shallow =
                BoundedLegacy.newBuilder().setId("root").setChild(missing).buildPartial();
        assertGenerated(BoundedLegacy.class, shallow, 2);
        assertTraversed(BoundedLegacy.class, shallow, 1);

        UnknownFieldSet values =
                UnknownFieldSet.newBuilder()
                        .addField(
                                100,
                                UnknownFieldSet.Field.newBuilder()
                                        .addVarint(1)
                                        .addFixed32(2)
                                        .addFixed64(3)
                                        .addLengthDelimited(ByteString.copyFromUtf8("four"))
                                        .build())
                        .build();
        UnknownFieldSet group =
                UnknownFieldSet.newBuilder()
                        .addField(
                                101,
                                UnknownFieldSet.Field.newBuilder()
                                        .addGroup(UnknownFieldSet.getDefaultInstance())
                                        .build())
                        .build();
        BoundedRequired opaque = missing.toBuilder().setUnknownFields(values).buildPartial();
        BoundedRequired nested = missing.toBuilder().setUnknownFields(group).buildPartial();
        assertGenerated(BoundedLegacy.class, with(grouped, opaque, 0), 2);
        // A singular leaf, a repeated element, and the root are each checked for groups.
        assertTraversed(BoundedLegacy.class, with(grouped, nested, 0), 100);
        assertTraversed(BoundedLegacy.class, with(grouped, missing, 1, nested), 100);
        assertTraversed(
                BoundedLegacy.class,
                grouped.toBuilder().setUnknownFields(group).buildPartial(),
                100);

        // The group, its value, and 62 elements reach the budget; one more element exceeds it.
        int counted = ProtobufMessageSize.GENERATED_SIZE_BUDGET - 2;
        assertGenerated(BoundedLegacy.class, with(grouped, missing, counted), 100);
        assertTraversed(BoundedLegacy.class, with(grouped, missing, counted + 1), 100);

        // A type reaching a recursive type is traversed even when its value is shallow.
        assertTraversed(
                ReachesRequired.class,
                ReachesRequired.newBuilder()
                        .setNode(RequiredMessage.newBuilder().buildPartial())
                        .buildPartial(),
                100);
        // A value reporting another descriptor than its layout's is traversed with its own.
        assertTraversed(BoundedLegacy.class, RequiredMessage.newBuilder().buildPartial(), 100);
        // Generated sizes up to the scratch size and the size limit decide; larger ones do not.
        int scratch = ProtobufTypeSerializer.SCRATCH_SIZE;
        // Raw bytes after a one-byte tag and a two-byte length fill the rest of the scratch size.
        BoundedLegacy full = padded(grouped, scratch - grouped.getSerializedSize() - 3);
        BoundedLegacy over = padded(grouped, scratch - grouped.getSerializedSize() - 2);
        assertThat(full.getSerializedSize()).isEqualTo(scratch);
        assertThat(over.getSerializedSize()).isEqualTo(scratch + 1);
        assertGenerated(BoundedLegacy.class, full, 100);
        assertTraversed(BoundedLegacy.class, over, 100);
        assertThatThrownBy(() -> sizer(BoundedLegacy.class, scratch - 1, 100).check(full))
                .hasMessageContaining("missing");
    }

    /** Compares decisions at size limits around the value and depth limits around its type. */
    private static <T extends Message> void assertBoundaries(Class<T> type, T value, Random random)
            throws IOException {
        int size = value.getSerializedSize();
        TreeSet<Integer> sizes =
                new TreeSet<>(List.of(size + 1, ProtobufTypeSerializer.SCRATCH_SIZE, MAX_SIZE));
        // A size limit must be positive.
        for (int candidate = size; candidate >= Math.max(1, size - 1); candidate--) {
            sizes.add(candidate);
        }
        for (int maxSize : sizes) {
            for (int recursionLimit : List.of(1, 2, 3, 4, 100)) {
                ProtobufSerializerSettings settings =
                        new ProtobufSerializerSettings(
                                random.nextBoolean(), maxSize, recursionLimit);
                assertSameDecision(type, value, settings, "memoized");
                // A parsed copy has no memoized runtime sizes until serialization computes them.
                T fresh =
                        type.cast(
                                ProtobufMessageType.resolve(type)
                                        .parser
                                        .parsePartialFrom(value.toByteString()));
                assertSameDecision(type, fresh, settings, "fresh");
            }
        }
    }

    /**
     * Expects the frame or failure condition that the traversal establishes without generated
     * sizes, and an untouched target after every failure.
     */
    private static <T extends Message> void assertSameDecision(
            Class<T> type, T value, ProtobufSerializerSettings settings, String state)
            throws IOException {
        ProtobufMessageSize reference =
                new ProtobufMessageSize(settings, ProtobufMessageType.resolve(type).layout);
        String failure;
        int size = -1;
        try {
            size = reference.traverse(value);
            failure = value.isInitialized() ? null : "Uninitialized message";
        } catch (IOException e) {
            failure = e.getMessage();
        }
        ProtobufTypeSerializer<T> serializer = new ProtobufTypeSerializer<>(type, settings);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper target = new DataOutputViewStreamWrapper(bytes);
        String description =
                String.format("%s value, traversal size %d, %s", state, size, settings);
        if (failure == null) {
            assertThatCode(() -> serializer.serialize(value, target))
                    .as(description)
                    .doesNotThrowAnyException();
            assertThat(bytes.toByteArray()).as(description).isEqualTo(frame(size, value, settings));
        } else {
            assertThatThrownBy(() -> serializer.serialize(value, target))
                    .as(description)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(failure);
            assertThat(bytes.size()).as(description).isZero();
        }
    }

    private static byte[] frame(int size, Message value, ProtobufSerializerSettings settings)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new DataOutputStream(bytes).writeInt(size);
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        if (settings.deterministicSerialization()) {
            output.useDeterministicSerialization();
        }
        value.writeTo(output);
        output.flush();
        return bytes.toByteArray();
    }

    private static <T extends Message> void assertGenerated(
            Class<T> type, T value, int recursionLimit) throws IOException {
        assertThat(sizer(type, MAX_SIZE, recursionLimit).check(value))
                .isEqualTo(value.getSerializedSize());
    }

    private static <T extends Message> void assertTraversed(
            Class<? extends Message> type, T value, int recursionLimit) {
        assertThatThrownBy(() -> sizer(type, MAX_SIZE, recursionLimit).check(value))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("missing");
    }

    private static BoundedLegacy padded(BoundedLegacy value, int length) {
        return value.toBuilder().setRawBytes(ByteString.copyFrom(new byte[length])).buildPartial();
    }

    /** Returns unknown fields holding groups nested to the given depth. */
    private static UnknownFieldSet groups(int depth) {
        UnknownFieldSet result = UnknownFieldSet.getDefaultInstance();
        for (int i = 0; i < depth; i++) {
            result =
                    UnknownFieldSet.newBuilder()
                            .addField(
                                    200 + i,
                                    UnknownFieldSet.Field.newBuilder().addGroup(result).build())
                            .build();
        }
        return result;
    }

    /** Adds repeated children to a value, optionally followed by one final child. */
    private static BoundedLegacy with(
            BoundedLegacy value, BoundedRequired child, int count, BoundedRequired... last) {
        BoundedLegacy.Builder builder = value.toBuilder();
        if (count == 0 && last.length == 0) {
            return builder.setChild(child).buildPartial();
        }
        for (int i = 0; i < count; i++) {
            builder.addChildren(child);
        }
        for (BoundedRequired element : last) {
            builder.addChildren(element);
        }
        return builder.buildPartial();
    }

    private static BoundedMessage message(
            Random random, List<BoundedLeaf> leaves, List<BoundedBranch> branches) {
        BoundedMessage.Builder builder = BoundedMessage.newBuilder();
        if (random.nextBoolean()) {
            builder.setId(random.nextLong());
        }
        if (random.nextBoolean()) {
            builder.setText(text(random));
        }
        if (random.nextInt(3) == 0) {
            builder.setNote(text(random));
        }
        if (random.nextInt(3) == 0) {
            // Some payloads cross the scratch size, alone or with the other fields.
            builder.setPayload(bytes(random, random.nextInt(4) == 0 ? 4200 : 200));
        }
        for (int i = random.nextInt(4); i > 0; i--) {
            builder.addPacked(random.nextLong() >> random.nextInt(64));
        }
        for (int i = random.nextInt(3); i > 0; i--) {
            builder.addStrings(text(random));
        }
        if (random.nextBoolean()) {
            builder.setLeaf(leaf(random, leaves));
        }
        if (random.nextBoolean()) {
            builder.setBranch(branch(random, leaves, branches));
        }
        for (int i = random.nextInt(3); i > 0; i--) {
            builder.addBranches(branch(random, leaves, branches));
        }
        for (int i = random.nextInt(4); i > 0; i--) {
            builder.putCounts(text(random), random.nextLong());
        }
        switch (random.nextInt(3)) {
            case 0 -> builder.setSelected(leaf(random, leaves));
            case 1 -> builder.setSelectedText(text(random));
            default -> {}
        }
        for (int i = random.nextInt(3); i > 0; i--) {
            builder.addChunks(bytes(random, 50));
        }
        unknown(random, builder);
        return builder.build();
    }

    /** Returns a new leaf or, sometimes, an earlier one of the same graph by identity. */
    private static BoundedLeaf leaf(Random random, List<BoundedLeaf> leaves) {
        if (!leaves.isEmpty() && random.nextInt(4) == 0) {
            return leaves.get(random.nextInt(leaves.size()));
        }
        BoundedLeaf.Builder builder = BoundedLeaf.newBuilder();
        if (random.nextBoolean()) {
            builder.setCode(random.nextInt());
        }
        if (random.nextBoolean()) {
            builder.setName(text(random));
        }
        if (random.nextInt(3) == 0) {
            builder.setData(bytes(random, 100));
        }
        for (int i = random.nextInt(4); i > 0; i--) {
            builder.addValues(random.nextLong() >> random.nextInt(64));
        }
        for (int i = random.nextInt(3); i > 0; i--) {
            builder.addChunks(bytes(random, 20));
        }
        unknown(random, builder);
        BoundedLeaf result = builder.build();
        leaves.add(result);
        return result;
    }

    private static BoundedBranch branch(
            Random random, List<BoundedLeaf> leaves, List<BoundedBranch> branches) {
        if (!branches.isEmpty() && random.nextInt(4) == 0) {
            return branches.get(random.nextInt(branches.size()));
        }
        BoundedBranch.Builder builder = BoundedBranch.newBuilder();
        if (random.nextBoolean()) {
            builder.setLeaf(leaf(random, leaves));
        }
        for (int i = random.nextInt(3); i > 0; i--) {
            builder.addLeaves(leaf(random, leaves));
        }
        for (int i = random.nextInt(3); i > 0; i--) {
            // A map value sits one level below its entry, at the static height of the root.
            builder.putNamed(text(random), leaf(random, leaves));
        }
        for (int i = random.nextInt(3); i > 0; i--) {
            builder.putLabels(random.nextInt(), text(random));
        }
        unknown(random, builder);
        BoundedBranch result = builder.build();
        branches.add(result);
        return result;
    }

    private static BoundedLegacy legacy(Random random) {
        BoundedLegacy.Builder builder = BoundedLegacy.newBuilder();
        if (random.nextInt(8) != 0) {
            builder.setId(text(random));
        }
        if (random.nextBoolean()) {
            builder.setRawBytes(raw(random));
        }
        if (random.nextBoolean()) {
            builder.setChild(required(random));
        }
        for (int i = random.nextInt(4); i > 0; i--) {
            builder.addChildren(required(random));
        }
        if (random.nextBoolean()) {
            BoundedLegacy.Item.Builder item = BoundedLegacy.Item.newBuilder();
            if (random.nextBoolean()) {
                item.setLabelBytes(raw(random));
            }
            if (random.nextBoolean()) {
                item.setValue(required(random));
            }
            unknown(random, item);
            builder.setItem(item.buildPartial());
        }
        unknown(random, builder);
        return builder.buildPartial();
    }

    private static BoundedRequired required(Random random) {
        BoundedRequired.Builder builder = BoundedRequired.newBuilder();
        if (random.nextInt(8) != 0) {
            builder.setValue(random.nextInt());
        }
        if (random.nextBoolean()) {
            builder.setRawBytes(raw(random));
        }
        unknown(random, builder);
        return builder.buildPartial();
    }

    /** Sometimes sets unknown fields, which hold groups nested one to three deep in some cases. */
    private static void unknown(Random random, Message.Builder builder) {
        if (random.nextInt(4) != 0) {
            return;
        }
        UnknownFieldSet.Field.Builder field =
                UnknownFieldSet.Field.newBuilder()
                        .addVarint(random.nextLong())
                        .addFixed32(random.nextInt())
                        .addFixed64(random.nextLong())
                        .addLengthDelimited(bytes(random, 30));
        if (random.nextInt(3) == 0) {
            field.addGroup(groups(random.nextInt(3)));
        }
        builder.setUnknownFields(
                UnknownFieldSet.newBuilder()
                        .addField(1000 + random.nextInt(100), field.build())
                        .build());
    }

    /** Returns string bytes that are sometimes invalid UTF-8, which proto2 retains unchecked. */
    private static ByteString raw(Random random) {
        return random.nextInt(3) == 0
                ? ByteString.copyFrom(new byte[] {'x', (byte) 0xff, (byte) 0xfe})
                : ByteString.copyFromUtf8(text(random));
    }

    private static ByteString bytes(Random random, int bound) {
        byte[] bytes = new byte[random.nextInt(bound)];
        random.nextBytes(bytes);
        return ByteString.copyFrom(bytes);
    }

    /** Returns text with one- to four-byte characters and replacement characters. */
    private static String text(Random random) {
        StringBuilder text = new StringBuilder();
        for (int i = random.nextInt(12); i > 0; i--) {
            text.append(
                    switch (random.nextInt(5)) {
                        case 0 -> "a";
                        case 1 -> "é";
                        case 2 -> "中";
                        case 3 -> "😀";
                        default -> "\ufffd";
                    });
        }
        return text.toString();
    }

    private static ProtobufMessageSize sizer(
            Class<? extends Message> type, int maxSize, int recursionLimit) {
        return new ProtobufMessageSize(
                new ProtobufSerializerSettings(false, maxSize, recursionLimit),
                ProtobufMessageType.resolve(type).layout);
    }
}
