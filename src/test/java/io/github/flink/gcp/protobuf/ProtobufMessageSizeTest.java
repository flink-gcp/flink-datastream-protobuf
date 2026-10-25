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
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.protobuf.generated.RecursiveMessage;
import io.github.flink.gcp.protobuf.generated.ScalarMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufMessageSizeTest {
    private static final int MAX_SIZE = 64 * 1024 * 1024;

    @Test
    void randomGraphsMatchTheRuntimeSize() throws Exception {
        Random random = new Random(4343);
        ProtobufMessageSize sizer = sizer(RecursiveMessage.class, MAX_SIZE, 100);
        for (int iteration = 0; iteration < 300; iteration++) {
            // Sharing stays within one graph, which bounds its depth and size.
            RecursiveMessage value = randomGraph(random, new ArrayList<>(), 4);
            // A rebuilt value has no memoized runtime sizes until the runtime computes them.
            RecursiveMessage fresh = value.toBuilder().build();
            assertThat(sizer.check(fresh)).isEqualTo(value.getSerializedSize());
            assertThat(sizer.check(value)).isEqualTo(value.getSerializedSize());
        }
    }

    @Test
    void sharedChildrenAboveAndBelowTheCacheBoundMatchTheRuntimeSize() throws Exception {
        ProtobufMessageSize sizer = sizer(RecursiveMessage.class, MAX_SIZE, 100);
        RecursiveMessage small = RecursiveMessage.newBuilder().putCounts("small", -1).build();
        RecursiveMessage large = withCounts(small, ProtobufMessageSize.CACHE_THRESHOLD + 1);
        for (RecursiveMessage shared : List.of(small, large)) {
            RecursiveMessage value =
                    RecursiveMessage.newBuilder()
                            .addChildren(shared)
                            .addChildren(shared)
                            .putNodes("first", shared)
                            .putNodes(
                                    "second",
                                    RecursiveMessage.newBuilder().setChild(shared).build())
                            .setChild(RecursiveMessage.newBuilder().addChildren(shared))
                            .build();
            assertThat(sizer.check(value)).isEqualTo(value.getSerializedSize());
        }
    }

    @Test
    void largePackedBlocksUseMultiByteLengthPrefixes() throws Exception {
        Random random = new Random(129);
        ScalarMessage.Builder builder = ScalarMessage.newBuilder();
        for (FieldDescriptor field : ScalarMessage.getDescriptor().getFields()) {
            if (!field.isPacked()) {
                continue;
            }
            // 2,100 elements give every packed width a block above 127 bytes, and 8- and
            // 10-byte elements a block above 16 KiB, so prefixes take two and three bytes.
            for (int i = 0; i < 2100; i++) {
                builder.addRepeatedField(field, packedValue(field, random));
            }
        }
        ScalarMessage value = builder.build();
        assertThat(sizer(ScalarMessage.class, MAX_SIZE, 100).check(value))
                .isEqualTo(value.getSerializedSize());
    }

    @Test
    void implicitPresenceFollowsTheRuntimeDefaultComparison() throws Exception {
        ProtobufMessageSize sizer = sizer(ScalarMessage.class, MAX_SIZE, 100);
        List<ScalarMessage> values =
                List.of(
                        // Explicitly set defaults are absent, as in generated writing.
                        ScalarMessage.newBuilder()
                                .setInt32Value(0)
                                .setStringValue("")
                                .setBytesValue(ByteString.EMPTY)
                                .setBoolValue(false)
                                .setDoubleValue(0.0)
                                .build(),
                        // Negative zero and NaN differ from the default bits and are written.
                        ScalarMessage.newBuilder()
                                .setDoubleValue(-0.0)
                                .setFloatValue(-0.0f)
                                .build(),
                        ScalarMessage.newBuilder()
                                .setDoubleValue(Double.NaN)
                                .setFloatValue(Float.NaN)
                                .build(),
                        // An open enum keeps numbers it does not declare.
                        ScalarMessage.newBuilder().setChoiceValue(7).build(),
                        ScalarMessage.newBuilder().setChoiceValue(-5).setSint64Value(-1).build(),
                        // Every field at its default is absent.
                        ScalarMessage.getDefaultInstance());
        for (ScalarMessage value : values) {
            assertThat(sizer.check(value)).isEqualTo(value.getSerializedSize());
        }
        // A map entry writes its default key and value, which are not absent.
        RecursiveMessage defaults = RecursiveMessage.newBuilder().putCounts("", 0).build();
        assertThat(sizer(RecursiveMessage.class, MAX_SIZE, 100).check(defaults))
                .isEqualTo(defaults.getSerializedSize());
    }

    @Test
    void sharedUnknownFieldsAboveTheCacheBoundKeepTheirSizeAndDepth() throws Exception {
        UnknownFieldSet.Field.Builder varints = UnknownFieldSet.Field.newBuilder();
        for (long i = 0; i <= ProtobufMessageSize.CACHE_THRESHOLD; i++) {
            varints.addVarint(-i);
        }
        // The shared set is summarized after its first occurrence and holds one nested group.
        UnknownFieldSet shared =
                UnknownFieldSet.newBuilder()
                        .addField(300, varints.build())
                        .addField(
                                301,
                                UnknownFieldSet.Field.newBuilder()
                                        .addGroup(UnknownFieldSet.getDefaultInstance())
                                        .build())
                        .build();
        RecursiveMessage child = RecursiveMessage.newBuilder().setUnknownFields(shared).build();
        // The first occurrence ends at depth 2. The deepest path, depth 3, reuses the summary as
        // a grandchild's unknown fields; after the fields, two unknown groups also reach depth 3.
        RecursiveMessage value =
                RecursiveMessage.newBuilder()
                        .setChild(child)
                        .addChildren(RecursiveMessage.newBuilder().setChild(child.toBuilder()))
                        .setUnknownFields(group(400, group(401, shared)))
                        .build();
        assertThat(sizer(RecursiveMessage.class, MAX_SIZE, 3).check(value))
                .isEqualTo(value.getSerializedSize());
        assertThatThrownBy(() -> sizer(RecursiveMessage.class, MAX_SIZE, 2).check(value))
                .hasMessageContaining("recursionLimit");
    }

    @Test
    void messagesReportingAnotherDescriptorUseThatDescriptorsLayout() throws Exception {
        RecursiveMessage value =
                RecursiveMessage.newBuilder()
                        .setChild(recursive(2))
                        .putCounts("count", 7)
                        .addChunks(ByteString.copyFromUtf8("chunk"))
                        .build();
        // The layout passed in describes another type, so sizing derives one from the value.
        assertThat(sizer(ScalarMessage.class, MAX_SIZE, 100).check(value))
                .isEqualTo(value.getSerializedSize());
    }

    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void doublingSharedSubgraphsAreSizedWithoutExponentialTraversal() throws Exception {
        RecursiveMessage exact = doubling(16);
        assertThat(sizer(RecursiveMessage.class, MAX_SIZE, 100).check(exact))
                .isEqualTo(exact.getSerializedSize());
        // Each occurrence still counts, so 64 doublings exceed any int-sized payload.
        RecursiveMessage huge = doubling(64);
        ProtobufMessageSize sizer = sizer(RecursiveMessage.class, Integer.MAX_VALUE, 100);
        assertThatThrownBy(() -> sizer.check(huge))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxMessageSize");
    }

    @Test
    void sharedChildrenAtDeeperPathsUseTheirRecursionBudget() throws Exception {
        RecursiveMessage small = RecursiveMessage.newBuilder().setChild(recursive(1)).build();
        RecursiveMessage large = withCounts(small, ProtobufMessageSize.CACHE_THRESHOLD + 1);
        for (RecursiveMessage shared : List.of(small, large)) {
            // The shallow occurrence completes first; the deeper one reaches depth 4.
            RecursiveMessage repeated =
                    RecursiveMessage.newBuilder()
                            .addChildren(shared)
                            .addChildren(RecursiveMessage.newBuilder().setChild(shared))
                            .build();
            // A map value sits one level below its entry.
            RecursiveMessage mapped =
                    RecursiveMessage.newBuilder().setChild(shared).putNodes("node", shared).build();
            for (RecursiveMessage value : List.of(repeated, mapped)) {
                assertThat(sizer(RecursiveMessage.class, MAX_SIZE, 4).check(value))
                        .isEqualTo(value.getSerializedSize());
                assertThatThrownBy(() -> sizer(RecursiveMessage.class, MAX_SIZE, 3).check(value))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("recursionLimit");
            }
        }
    }

    @Test
    void failedChecksDoNotAffectLaterChecksOnTheSameInstance() throws Exception {
        ProtobufMessageSize sizer = sizer(RecursiveMessage.class, 4096, 100);
        RecursiveMessage valid =
                RecursiveMessage.newBuilder()
                        .setChild(recursive(3))
                        .addChildren(recursive(2))
                        .putCounts("a", 1)
                        .build();
        RecursiveMessage oversizedChild =
                RecursiveMessage.newBuilder()
                        .setChild(
                                RecursiveMessage.newBuilder()
                                        .setChild(recursive(2))
                                        .setPayload(ByteString.copyFrom(new byte[4096])))
                        .build();
        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> sizer.check(recursive(150)))
                    .hasMessageContaining("recursionLimit");
            assertThat(sizer.check(valid)).isEqualTo(valid.getSerializedSize());
            assertThatThrownBy(() -> sizer.check(oversizedChild))
                    .hasMessageContaining("maxMessageSize");
            assertThat(sizer.check(valid)).isEqualTo(valid.getSerializedSize());
        }

        // Traversals deeper than the retained frames leave shallower checks exact.
        ProtobufMessageSize deep = sizer(RecursiveMessage.class, MAX_SIZE, 1000);
        RecursiveMessage nested = recursive(900);
        assertThat(deep.check(nested)).isEqualTo(nested.getSerializedSize());
        assertThat(deep.check(valid)).isEqualTo(valid.getSerializedSize());
    }

    @Test
    void checksDoNotRetainRecordsAfterSuccessOrFailure() throws Exception {
        // Each scenario gets its own instance, so a later check cannot overwrite its frames.
        List<ProtobufMessageSize> sizers = new ArrayList<>();
        List<WeakReference<Object>> released = new ArrayList<>();
        for (int depth : List.of(3, 200)) {
            for (boolean valid : List.of(true, false)) {
                released.add(checkUnreferenced(track(sizers, 1000), depth, valid));
            }
        }
        released.add(failInRepeatedField(track(sizers, 100)));
        released.add(failInMapField(track(sizers, 100)));
        released.add(failInUnknownGroup(track(sizers, 100)));
        for (int attempt = 0;
                attempt < 100 && released.stream().anyMatch(r -> r.get() != null);
                attempt++) {
            System.gc();
            Thread.sleep(10);
        }
        assertThat(released).allSatisfy(reference -> assertThat(reference.get()).isNull());
        // Only the frames retained by these live instances could keep the values reachable.
        Reference.reachabilityFence(sizers);
    }

    @Test
    void unknownFieldsSharedAcrossMessagesMatchTheRuntimeSize() throws Exception {
        UnknownFieldSet unknown =
                UnknownFieldSet.newBuilder()
                        .addField(
                                300,
                                UnknownFieldSet.Field.newBuilder()
                                        .addVarint(Long.MIN_VALUE)
                                        .addFixed32(7)
                                        .addFixed64(-7)
                                        .addLengthDelimited(ByteString.copyFromUtf8("bytes"))
                                        .addGroup(UnknownFieldSet.getDefaultInstance())
                                        .build())
                        .build();
        RecursiveMessage child = RecursiveMessage.newBuilder().setUnknownFields(unknown).build();
        RecursiveMessage value =
                RecursiveMessage.newBuilder()
                        .setUnknownFields(unknown)
                        .setChild(child)
                        .addChildren(child)
                        .build();
        assertThat(sizer(RecursiveMessage.class, MAX_SIZE, 100).check(value))
                .isEqualTo(value.getSerializedSize());
        ScalarMessage scalar = ScalarMessage.newBuilder().setUnknownFields(unknown).build();
        assertThat(sizer(ScalarMessage.class, MAX_SIZE, 100).check(scalar))
                .isEqualTo(scalar.getSerializedSize());
    }

    private static ProtobufMessageSize track(List<ProtobufMessageSize> sizers, int depth) {
        ProtobufMessageSize sizer = sizer(RecursiveMessage.class, 4096, depth);
        sizers.add(sizer);
        return sizer;
    }

    /** Fails on a repeated element, returning a weak reference to the element after it. */
    private static WeakReference<Object> failInRepeatedField(ProtobufMessageSize sizer) {
        RecursiveMessage pending = recursive(1);
        RecursiveMessage value =
                RecursiveMessage.newBuilder()
                        .addChildren(recursive(1))
                        .addChildren(oversized())
                        .addChildren(pending)
                        .build();
        assertThatThrownBy(() -> sizer.check(value)).hasMessageContaining("maxMessageSize");
        return new WeakReference<>(pending);
    }

    /** Fails on a map value, returning a weak reference to the other value. */
    private static WeakReference<Object> failInMapField(ProtobufMessageSize sizer) {
        RecursiveMessage pending = recursive(1);
        RecursiveMessage value =
                RecursiveMessage.newBuilder()
                        .putNodes("a", oversized())
                        .putNodes("b", pending)
                        .build();
        assertThatThrownBy(() -> sizer.check(value)).hasMessageContaining("maxMessageSize");
        return new WeakReference<>(pending);
    }

    /** Fails inside an unknown group, returning a weak reference to the group after it. */
    private static WeakReference<Object> failInUnknownGroup(ProtobufMessageSize sizer) {
        UnknownFieldSet pending = group(500, UnknownFieldSet.getDefaultInstance());
        UnknownFieldSet oversized =
                UnknownFieldSet.newBuilder()
                        .addField(
                                501,
                                UnknownFieldSet.Field.newBuilder()
                                        .addLengthDelimited(ByteString.copyFrom(new byte[8192]))
                                        .build())
                        .build();
        RecursiveMessage value =
                RecursiveMessage.newBuilder()
                        .setUnknownFields(
                                UnknownFieldSet.newBuilder()
                                        .addField(
                                                400,
                                                UnknownFieldSet.Field.newBuilder()
                                                        .addGroup(oversized)
                                                        .addGroup(pending)
                                                        .build())
                                        .build())
                        .build();
        assertThatThrownBy(() -> sizer.check(value)).hasMessageContaining("maxMessageSize");
        return new WeakReference<>(pending);
    }

    private static RecursiveMessage oversized() {
        return RecursiveMessage.newBuilder()
                .setPayload(ByteString.copyFrom(new byte[8192]))
                .build();
    }

    /** Checks a value built here, returning only a weak reference to it. */
    private static WeakReference<Object> checkUnreferenced(
            ProtobufMessageSize sizer, int depth, boolean valid) throws IOException {
        RecursiveMessage value =
                RecursiveMessage.newBuilder()
                        .setChild(recursive(depth))
                        .setPayload(ByteString.copyFrom(new byte[valid ? 16 : 8192]))
                        .putNodes("node", recursive(2))
                        .build();
        if (valid) {
            assertThat(sizer.check(value)).isEqualTo(value.getSerializedSize());
        } else {
            assertThatThrownBy(() -> sizer.check(value)).hasMessageContaining("maxMessageSize");
        }
        return new WeakReference<>(value);
    }

    private static RecursiveMessage randomGraph(
            Random random, List<RecursiveMessage> built, int depth) {
        RecursiveMessage.Builder builder = RecursiveMessage.newBuilder();
        if (depth == 0) {
            return remember(built, builder.build());
        }
        if (random.nextBoolean()) {
            builder.setChild(randomChild(random, built, depth - 1));
        }
        for (int i = random.nextInt(4); i > 0; i--) {
            builder.addChildren(randomChild(random, built, depth - 1));
        }
        for (int i = random.nextInt(4); i > 0; i--) {
            builder.putNodes("node-" + random.nextInt(8), randomChild(random, built, depth - 1));
        }
        for (int i = random.nextInt(6); i > 0; i--) {
            builder.putCounts(randomText(random), random.nextInt());
        }
        if (random.nextInt(3) == 0) {
            builder.setPayload(ByteString.copyFrom(new byte[random.nextInt(300)]));
        }
        for (int i = random.nextInt(3); i > 0; i--) {
            builder.addChunks(ByteString.copyFromUtf8(randomText(random)));
        }
        if (random.nextInt(5) == 0) {
            builder.setUnknownFields(
                    UnknownFieldSet.newBuilder()
                            .addField(
                                    100 + random.nextInt(1000),
                                    UnknownFieldSet.Field.newBuilder()
                                            .addVarint(random.nextLong())
                                            .addFixed64(random.nextLong())
                                            .build())
                            .build());
        }
        return remember(built, builder.build());
    }

    private static RecursiveMessage remember(List<RecursiveMessage> built, RecursiveMessage value) {
        built.add(value);
        return value;
    }

    /** Returns a new child or, sometimes, an earlier subgraph of the same graph by identity. */
    private static RecursiveMessage randomChild(
            Random random, List<RecursiveMessage> built, int depth) {
        if (!built.isEmpty() && random.nextInt(4) == 0) {
            return built.get(random.nextInt(built.size()));
        }
        return randomGraph(random, built, depth);
    }

    private static String randomText(Random random) {
        StringBuilder text = new StringBuilder();
        for (int i = random.nextInt(12); i > 0; i--) {
            text.append(
                    switch (random.nextInt(4)) {
                        case 0 -> 'a';
                        case 1 -> 'é';
                        case 2 -> '中';
                        default -> "😀";
                    });
        }
        return text.toString();
    }

    private static UnknownFieldSet group(int number, UnknownFieldSet content) {
        return UnknownFieldSet.newBuilder()
                .addField(number, UnknownFieldSet.Field.newBuilder().addGroup(content).build())
                .build();
    }

    /** Returns a packed element whose varint, where variable, is often at its longest. */
    private static Object packedValue(FieldDescriptor field, Random random) {
        return switch (field.getJavaType()) {
            case INT -> random.nextInt() | Integer.MIN_VALUE;
            case LONG -> random.nextLong() | Long.MIN_VALUE;
            case FLOAT -> random.nextFloat();
            case DOUBLE -> random.nextDouble();
            case BOOLEAN -> random.nextBoolean();
            case ENUM -> {
                List<EnumValueDescriptor> values = field.getEnumType().getValues();
                yield values.get(random.nextInt(values.size()));
            }
            default -> throw new AssertionError("Unexpected packed field " + field);
        };
    }

    /** Builds a graph whose two children at every level are the same instance. */
    private static RecursiveMessage doubling(int levels) {
        RecursiveMessage result = RecursiveMessage.newBuilder().putCounts("leaf", 1).build();
        for (int i = 0; i < levels; i++) {
            result = RecursiveMessage.newBuilder().addChildren(result).addChildren(result).build();
        }
        return result;
    }

    /** Adds enough map entries that sizing the value examines more than the cache bound. */
    private static RecursiveMessage withCounts(RecursiveMessage value, long entries) {
        RecursiveMessage.Builder builder = value.toBuilder();
        for (int i = 0; i < entries; i++) {
            builder.putCounts("count-" + i, i);
        }
        return builder.build();
    }

    private static RecursiveMessage recursive(int depth) {
        RecursiveMessage result = RecursiveMessage.getDefaultInstance();
        for (int i = 0; i < depth; i++) {
            result = RecursiveMessage.newBuilder().setChild(result).build();
        }
        return result;
    }

    private static ProtobufMessageSize sizer(
            Class<? extends Message> type, int maxSize, int recursionLimit) {
        return new ProtobufMessageSize(
                new ProtobufSerializerSettings(false, maxSize, recursionLimit),
                ProtobufMessageType.resolve(type).layout);
    }
}
