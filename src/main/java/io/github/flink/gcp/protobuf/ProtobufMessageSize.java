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

import org.apache.flink.annotation.Internal;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Internal.IntList;
import com.google.protobuf.Internal.LongList;
import com.google.protobuf.LazyStringList;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.WireFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Overflow-safe wire sizing and parser-depth validation before the caller writes a frame.
 *
 * <p>The parser reads an unknown field into a declared field whose number it reuses with a wire
 * type that field accepts: a length-delimited value of a message or map field as a nested message,
 * whose bytes can nest beyond the recursion limit, and any other such value as that field's value,
 * which can fail to parse or change the value that reads back. Every check rejects such a value,
 * and one with an unknown-field number above the specification's maximum, whose tag the writer
 * would wrap to another number or to field zero. The parser keeps every other unknown field
 * unknown, and it is sized as opaque bytes.
 *
 * <p>When the payload type graph is acyclic and its static height is within the recursion limit, a
 * value without unknown groups or colliding unknown fields cannot exceed that limit. Such a value
 * uses the runtime's memoized generated size if it is at most the scratch size. Until one of the
 * runtime's {@code int} sums first exceeds {@link Integer#MAX_VALUE}, each addend is the exact
 * length that the writer emits, so a size that wrapped implies at least 2 GiB of output; encoding
 * into the scratch array then fails before the caller writes the frame.
 *
 * <p>Otherwise, each check traverses the value graph iteratively over field layouts precomputed for
 * the message class. Values whose subtree sizing examined more than a small constant amount of
 * content are summarized by identity for that invocation only, so shared children are counted at
 * every occurrence without re-traversing large subgraphs. Smaller values are sized again at each
 * occurrence. Fields are sized without encoding them.
 *
 * <p>Traversal frames are reused by later checks on the same instance, which is confined to one
 * serializer and thread. Every check clears their record references before returning.
 *
 * <p>For DynamicMessage values, each message value is paired with the declaration that the reader
 * parses it with, and a value whose descriptor is another instance must be accepted first. A layout
 * describes the value's own fields, which are what the writer emits, and checks unknown fields
 * against the paired declaration. Accepted pairs are cached by descriptor identity.
 */
@Internal
final class ProtobufMessageSize {
    /**
     * Content units (declared fields, elements, unknown values, and string characters) that a
     * subtree may examine before its summary is cached. Below this bound, a repeated occurrence is
     * sized again instead of storing a summary for every completed value.
     */
    static final long CACHE_THRESHOLD = 64;

    /**
     * Counted nested values that a value may contain and still use its generated size. The runtime
     * sizes every distinct value in full, including strings, lists, and unknown fields shared with
     * other values, so this bound limits the repeated work before an oversized value is rejected.
     */
    static final int GENERATED_SIZE_BUDGET = 64;

    /** Frames retained across checks; a deeper traversal releases the additional frames. */
    private static final int RETAINED_FRAMES = 64;

    private static final int UNMEASURED = -3;
    private static final int MEASURING = -2;
    private static final int UNBOUNDED = -1;

    /**
     * Largest field number in the Protobuf specification. A tag is {@code (number << 3) | wire
     * type} in 32 bits, and the runtime's {@code int} shift wraps a larger unknown-field number to
     * another number's tag or to an invalid tag for field zero.
     */
    static final int MAX_FIELD_NUMBER = (1 << 29) - 1;

    /** Accepted DynamicMessage descriptor pairs retained by each instance. */
    static final int PAIR_CACHE_SIZE = 16;

    private final ProtobufSerializerSettings settings;
    private final int maxSize;
    private final int recursionLimit;
    private final Layout root;

    /** Pairing state for DynamicMessage values, or null for generated messages. */
    private final Pairs pairs;

    private Frame[] frames = new Frame[0];

    /**
     * Largest generated size accepted without traversal in the current check; negative if the root
     * layout's height is unbounded or exceeds the recursion limit.
     */
    private int generatedLimit;

    /**
     * Wire-size lower bound of the repeated strings visited by the current generated-size check.
     */
    private long stringBytes;

    /**
     * Captures immutable limits and the root layout of generated messages; records are referenced
     * only during a check.
     */
    ProtobufMessageSize(ProtobufSerializerSettings settings, Layout root) {
        this(settings, root, false);
    }

    /**
     * Captures immutable limits and the root layout; records are referenced only during a check.
     *
     * @param dynamic whether values are DynamicMessage values that require descriptor pairing
     */
    ProtobufMessageSize(ProtobufSerializerSettings settings, Layout root, boolean dynamic) {
        this.settings = settings;
        maxSize = settings.maxMessageSize();
        recursionLimit = settings.recursionLimit();
        this.root = root;
        pairs = dynamic ? new Pairs(root) : null;
    }

    /**
     * Precomputes field layouts for every message type reachable from a payload descriptor, each
     * paired with itself.
     *
     * @param descriptor payload root without extensions
     * @return the immutable root layout, linked to the layouts of its message fields
     */
    static Layout layout(Descriptor descriptor) {
        return layout(descriptor, descriptor, false);
    }

    /**
     * Precomputes field layouts for DynamicMessage values of a root descriptor, each paired with
     * itself.
     *
     * @param descriptor payload root without extensions
     * @return the immutable root layout, linked to the layouts of its message fields
     */
    static Layout dynamicLayout(Descriptor descriptor) {
        return layout(descriptor, descriptor, true);
    }

    /**
     * Precomputes field layouts for a value descriptor paired with the declaration that the reader
     * parses it with. A message field's layout pairs its type with the type that the paired
     * declaration gives for the same field number, if any.
     *
     * @param descriptor descriptor of the values that the layout describes
     * @param pair reader declaration, the descriptor itself, or null if the reader keeps such
     *     values as unknown fields
     * @param dynamic whether the values are DynamicMessage values, whose map entries can hold
     *     unknown fields and other descriptor instances
     * @return the immutable layout, linked to the layouts of its message fields
     */
    private static Layout layout(Descriptor descriptor, Descriptor pair, boolean dynamic) {
        Map<LayoutKey, Layout> layouts = new HashMap<>();
        List<Layout> pending = new ArrayList<>();
        Layout result = new Layout(descriptor, pair);
        layouts.put(new LayoutKey(descriptor, pair), result);
        pending.add(result);
        for (int i = 0; i < pending.size(); i++) {
            Layout parent = pending.get(i);
            for (Field field : parent.fields) {
                if (field.type == FieldDescriptor.Type.MESSAGE) {
                    Descriptor type = field.descriptor.getMessageType();
                    LayoutKey key = new LayoutKey(type, childPair(parent, field));
                    Layout child = layouts.get(key);
                    if (child == null) {
                        child = new Layout(type, key.pair());
                        layouts.put(key, child);
                        pending.add(child);
                    }
                    field.message = child;
                }
            }
        }
        for (Layout layout : pending) {
            layout.nested = nested(layout, dynamic);
        }
        measure(result);
        return result;
    }

    /** Returns the reader's type for a message field of a layout, or null if it has none. */
    private static Descriptor childPair(Layout parent, Field field) {
        if (parent.pair == parent.descriptor) {
            return field.descriptor.getMessageType();
        }
        if (parent.pair == null) {
            return null;
        }
        FieldDescriptor declared = parent.pair.findFieldByNumber(field.descriptor.getNumber());
        return declared != null && declared.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                ? declared.getMessageType()
                : null;
    }

    /**
     * Selects the message fields whose values can hold unknown fields or need another layout.
     * Generated map entries have neither, so a generated map field is selected only if its values
     * are messages; a DynamicMessage map entry is any message that the application adds.
     */
    private static Field[] nested(Layout layout, boolean dynamic) {
        List<Field> result = new ArrayList<>();
        for (Field field : layout.fields) {
            if (field.message != null
                    && (dynamic
                            || !field.descriptor.isMapField()
                            || hasMessageFields(field.message))) {
                result.add(field);
            }
        }
        return result.toArray(new Field[0]);
    }

    private static boolean hasMessageFields(Layout layout) {
        for (Field field : layout.fields) {
            if (field.message != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Sets the static height of every layout reachable from a root, or {@link #UNBOUNDED} if it
     * reaches a cycle, through an iterative post-order walk. A layout is still on the walk's path
     * when one of its descendants refers to it.
     */
    private static void measure(Layout root) {
        Layout[] path = {root};
        int[] next = new int[1];
        int[] height = new int[1];
        int top = 0;
        root.height = MEASURING;
        while (true) {
            Layout layout = path[top];
            if (next[top] < layout.fields.length) {
                Layout child = layout.fields[next[top]++].message;
                if (child == null) {
                    continue;
                }
                if (child.height == UNMEASURED) {
                    if (++top == path.length) {
                        path = Arrays.copyOf(path, 2 * top);
                        next = Arrays.copyOf(next, 2 * top);
                        height = Arrays.copyOf(height, 2 * top);
                    }
                    path[top] = child;
                    next[top] = 0;
                    height[top] = 0;
                    child.height = MEASURING;
                } else {
                    height[top] = parentHeight(height[top], child.height);
                }
                continue;
            }
            layout.height = height[top];
            if (top-- == 0) {
                return;
            }
            height[top] = parentHeight(height[top], layout.height);
        }
    }

    /** Combines a parent's height so far with a child's height or in-progress state. */
    private static int parentHeight(int parent, int child) {
        return parent < 0 || child < 0 ? UNBOUNDED : Math.max(parent, child + 1);
    }

    /**
     * Checks parser depth and wire size, using the generated size when the caller can confirm it.
     *
     * <p>A result of at most {@link ProtobufTypeSerializer#SCRATCH_SIZE} may be the runtime's
     * generated size. The caller must encode the value into a scratch array of that size and call
     * {@link #traverse(Message)} if encoding fails or the output length differs.
     *
     * @param message message to validate
     * @return payload length, excluding the Flink frame prefix
     * @throws IOException if size/depth limits are exceeded or the traversal rejects the value
     */
    int check(Message message) throws IOException {
        Layout layout = rootLayout(message);
        // A generated message reporting another descriptor is traversed, as nested ones are.
        generatedLimit =
                (pairs != null || layout == root)
                                && layout.height >= 0
                                && layout.height <= recursionLimit
                        ? Math.min(maxSize, ProtobufTypeSerializer.SCRATCH_SIZE)
                        : -1;
        if (generatedLimit >= 0) {
            int size = generatedSize(message, layout);
            if (size >= 0) {
                return size;
            }
        }
        return traverse(message, layout);
    }

    /** Returns the number of descriptor pairs evaluated because they were not cached, for tests. */
    int pairEvaluations() {
        return pairs == null ? 0 : pairs.evaluations;
    }

    /**
     * Returns the root value's layout, accepting a DynamicMessage descriptor other than the root's.
     */
    private Layout rootLayout(Message message) throws IOException {
        Descriptor descriptor = message.getDescriptorForType();
        if (descriptor == root.descriptor) {
            return root;
        }
        if (pairs == null) {
            return layout(descriptor);
        }
        try {
            return pairs.accept(descriptor, root.descriptor);
        } catch (IOException e) {
            throw rejected(descriptor.getFullName(), e);
        }
    }

    /**
     * Returns the generated size of a value whose depth is statically bounded, or -1 if the
     * traversal must decide.
     */
    private int generatedSize(Message message, Layout layout) {
        stringBytes = 0;
        if (visit(message, layout, GENERATED_SIZE_BUDGET) < 0) {
            return -1;
        }
        int size;
        try {
            size = message.getSerializedSize();
        } catch (RuntimeException e) {
            // The runtime rejects a string whose UTF-8 length does not fit in an int.
            return -1;
        }
        return size >= 0 && size <= generatedLimit ? size : -1;
    }

    /**
     * Visits the message values of a statically bounded graph and returns the remaining budget, or
     * -1 if the value must be traversed instead.
     *
     * <p>Unknown groups can nest without a static bound, the traversal rejects unknown fields that
     * the parser would read into a declared field or whose number exceeds the specification's
     * maximum, and a value reporting another descriptor may need another layout. The budget counts
     * repeated elements and singular values other than the default instance below the root; an
     * absent singular value reads as that instance, which holds no fields.
     */
    private int visit(Object candidate, Layout layout, int budget) {
        // A DynamicMessage field can hold a lite message, which the traversal rejects.
        if (budget < 0
                || !(candidate instanceof Message message)
                || message.getDescriptorForType() != layout.descriptor
                || requiresTraversal(message.getUnknownFields(), layout)) {
            return -1;
        }
        for (Field field : layout.strings) {
            if (!repeatedStrings(message, field)) {
                return -1;
            }
        }
        for (Field field : layout.nested) {
            FieldDescriptor descriptor = field.descriptor;
            if (field.copied) {
                // Map reflection copies its entries into a new list; an indexed entry read does
                // not.
                int count = message.getRepeatedFieldCount(descriptor);
                budget -= count;
                for (int i = 0; budget >= 0 && i < count; i++) {
                    budget = visit(message.getRepeatedField(descriptor, i), field.message, budget);
                }
            } else if (field.repeated) {
                List<?> values = (List<?>) message.getField(descriptor);
                budget -= values.size();
                for (int i = 0; budget >= 0 && i < values.size(); i++) {
                    budget = visit(values.get(i), field.message, budget);
                }
            } else if (pairs != null) {
                // DynamicMessage creates a new default instance on each read, and a value set to a
                // default instance of another type still needs its descriptor accepted.
                if (message.hasField(descriptor)) {
                    budget = visit(message.getField(descriptor), field.message, budget - 1);
                }
            } else {
                Message value = (Message) message.getField(descriptor);
                if (value != value.getDefaultInstanceForType()) {
                    budget = visit(value, field.message, budget - 1);
                }
            }
            if (budget < 0) {
                return -1;
            }
        }
        return budget;
    }

    /**
     * Adds a wire-size lower bound for a repeated string or string-valued map field and returns
     * whether the visited strings still fit the generated limit. The runtime measures the UTF-8
     * length at every occurrence, even when the occurrences share one String, so the bound stops
     * that repeated work before it exceeds the scratch size.
     */
    private boolean repeatedStrings(Message message, Field field) {
        FieldDescriptor descriptor = field.descriptor;
        if (field.mapValue == null) {
            List<?> values = (List<?>) message.getField(descriptor);
            // A raw element avoids decoding retained bytes into a String.
            LazyStringList raw = values instanceof LazyStringList lazy ? lazy : null;
            for (int i = 0; i < values.size(); i++) {
                if (!addString(raw == null ? values.get(i) : raw.getRaw(i))) {
                    return false;
                }
            }
            return true;
        }
        int count = message.getRepeatedFieldCount(descriptor);
        for (int i = 0; i < count; i++) {
            // A DynamicMessage entry can be a lite message or use another descriptor instance,
            // which the traversal rejects or accepts before reading its fields.
            if (!(message.getRepeatedField(descriptor, i) instanceof Message entry)
                    || entry.getDescriptorForType() != field.mapValue.getContainingType()
                    || !addString(entry.getField(field.mapValue))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Adds a tag, a length prefix, and at least one byte for each UTF-16 unit or retained byte, or
     * returns false for an element representation that the bound does not know.
     */
    private boolean addString(Object value) {
        int length;
        if (value instanceof String string) {
            length = string.length();
        } else if (value instanceof ByteString bytes) {
            length = bytes.size();
        } else {
            // A lazy string list can also hold byte arrays, which the traversal decodes instead.
            return false;
        }
        stringBytes += 2L + length;
        return stringBytes <= generatedLimit;
    }

    /**
     * Returns whether a value's unknown fields hold a group, whose nesting is unbounded, a number
     * above {@link #MAX_FIELD_NUMBER}, or a value that the parser would read into a declared field,
     * all of which the traversal rejects or measures.
     */
    private static boolean requiresTraversal(UnknownFieldSet unknown, Layout layout) {
        // Protobuf 3 copies even an empty set's map, so skip the shared empty instance.
        if (unknown == UnknownFieldSet.getDefaultInstance()) {
            return false;
        }
        for (Map.Entry<Integer, UnknownFieldSet.Field> entry : unknown.asMap().entrySet()) {
            UnknownFieldSet.Field field = entry.getValue();
            if (entry.getKey() > MAX_FIELD_NUMBER
                    || !field.getGroupList().isEmpty()
                    || collision(layout, entry.getKey(), field) != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Describes an unknown field that the parser would read into a declared field of the containing
     * type, or returns null. A set shared by values of different types can collide in only some of
     * them, so the caller passes the containing message's layout.
     */
    private static String collision(Layout layout, int number, UnknownFieldSet.Field values) {
        if (layout.reader == null) {
            return null;
        }
        FieldDescriptor descriptor = layout.reader.findFieldByNumber(number);
        if (descriptor == null) {
            return null;
        }
        Field field = layout.readerFields[descriptor.getIndex()];
        String wireType =
                switch (field.wireType) {
                    case WireFormat.WIRETYPE_VARINT ->
                            values.getVarintList().isEmpty() ? null : "varint";
                    case WireFormat.WIRETYPE_FIXED32 ->
                            values.getFixed32List().isEmpty() ? null : "fixed32";
                    case WireFormat.WIRETYPE_FIXED64 ->
                            values.getFixed64List().isEmpty() ? null : "fixed64";
                    // Proto3 declares no groups, so every other field is length-delimited.
                    default ->
                            values.getLengthDelimitedList().isEmpty() ? null : "length-delimited";
                };
        if (wireType == null && field.packable && !values.getLengthDelimitedList().isEmpty()) {
            wireType = "length-delimited";
        }
        return wireType == null
                ? null
                : "Unknown "
                        + wireType
                        + " field "
                        + number
                        + " of "
                        + layout.reader.getFullName()
                        + " would be parsed as declared field "
                        + descriptor.getFullName();
    }

    /**
     * Rejects a collision in a summarized unknown-field set reached under another containing type.
     * It looks up each of the type's declared numbers at most once and neither copies nor scans the
     * set.
     */
    private static void checkCollisions(Layout layout, UnknownFieldSet unknown) throws IOException {
        for (Field field : layout.readerFields) {
            int number = field.descriptor.getNumber();
            UnknownFieldSet.Field values = unknown.getField(number);
            String collision =
                    values == UnknownFieldSet.Field.getDefaultInstance()
                            ? null
                            : collision(layout, number, values);
            if (collision != null) {
                throw new IOException(collision);
            }
        }
    }

    /**
     * Checks parser depth and exact wire size by traversing the value.
     *
     * <p>Cached subtree depth is checked again at each occurrence, so sharing a message at a deeper
     * path cannot bypass the recursion limit.
     *
     * @param message message to validate
     * @return exact payload length, excluding the Flink frame prefix
     * @throws IOException if size/depth limits are exceeded or the value is rejected
     */
    int traverse(Message message) throws IOException {
        return traverse(message, rootLayout(message));
    }

    private int traverse(Message message, Layout rootLayout) throws IOException {
        Map<Object, Summary> completed = null;
        int top = 0;
        int used = 1;
        try {
            Frame current = frame(0).start(message, rootLayout, 0);
            while (true) {
                Object child = next(current);
                if (child != null) {
                    if (!(child instanceof Message) && !(child instanceof UnknownFieldSet)) {
                        // DynamicMessage accepts a lite message as a field value.
                        throw rejected(
                                path(top),
                                new IOException(
                                        "Unsupported message value of "
                                                + child.getClass().getName()
                                                + "; expected a full-runtime message"));
                    }
                    // An unknown-field set takes its container's layout, a group none.
                    Layout layout =
                            child instanceof Message value
                                    ? layoutOf(current.edgeLayout, value, top)
                                    : current.edgeLayout;
                    Summary summary = completed == null ? null : completed.get(child);
                    if (summary != null && summary.layout() != layout) {
                        if (child instanceof UnknownFieldSet set) {
                            if (layout != null) {
                                checkCollisions(layout, set);
                            }
                        } else {
                            // A shared DynamicMessage value under another pairing is checked again.
                            summary = null;
                        }
                    }
                    if (summary == null) {
                        int depth = current.depth + current.edgeDepth;
                        checkDepth(depth);
                        // Count the frame as used before start can fail with its reference.
                        used = Math.max(used, ++top + 1);
                        current = frame(top).start(child, layout, depth);
                    } else {
                        checkDepth((long) current.depth + current.edgeDepth + summary.depth);
                        accept(current, summary.size, summary.depth, 1);
                    }
                    continue;
                }
                if (top == 0) {
                    break;
                }
                if (current.work > CACHE_THRESHOLD) {
                    if (completed == null) {
                        completed = new IdentityHashMap<>();
                    }
                    completed.put(
                            current.value,
                            new Summary(current.size, current.nesting, current.layout));
                }
                Frame parent = frames[--top];
                accept(parent, current.size, current.nesting, current.work);
                current = parent;
            }
            return (int) current.size;
        } finally {
            release(used);
        }
    }

    /**
     * Sizes fields up to the next child edge and returns that child, or null once the value is
     * complete. The returned child is described by the frame's edge fields.
     */
    private Object next(Frame frame) throws IOException {
        if (frame.elements != null) {
            if (frame.element < frame.elements.size()) {
                return frame.elements.get(frame.element++);
            }
            frame.elements = null;
        }
        Message message = frame.message;
        if (message == null) {
            return null;
        }
        Field[] fields = frame.layout.fields;
        while (frame.field < fields.length) {
            Field field = fields[frame.field++];
            FieldDescriptor descriptor = field.descriptor;
            if (field.repeated) {
                // Map and enum reflection copy their elements into a new list, so ask for the
                // count first; other repeated fields return their immutable list without copying.
                if (field.copied && message.getRepeatedFieldCount(descriptor) == 0) {
                    continue;
                }
                List<?> values = (List<?>) message.getField(descriptor);
                int count = values.size();
                if (count == 0) {
                    continue;
                }
                frame.work += count;
                if (field.message == null) {
                    repeatedField(frame, field, values);
                    continue;
                }
                frame.size = add(frame.size, (long) count * edge(frame, field));
                frame.elements = values;
                frame.element = 1;
                return values.get(0);
            }
            Object value;
            if (field.implicit) {
                // Reflection decides presence of these fields by reading the value and comparing
                // it with the default, so one read replaces hasField followed by getField.
                value = message.getField(descriptor);
                if (value.equals(field.defaultValue)) {
                    continue;
                }
            } else if (message.hasField(descriptor)) {
                value = message.getField(descriptor);
            } else {
                continue;
            }
            if (field.message == null) {
                field(frame, field, value);
                continue;
            }
            frame.size = add(frame.size, edge(frame, field));
            return value;
        }
        if (frame.field++ == fields.length) {
            UnknownFieldSet unknown = message.getUnknownFields();
            // Protobuf 3 copies even an empty set's map, so skip the shared empty instance.
            if (unknown != UnknownFieldSet.getDefaultInstance()) {
                frame.edgeDepth = 0;
                frame.edgeDelimited = false;
                frame.edgeLayout = frame.layout;
                return unknown;
            }
        }
        return null;
    }

    /** Describes a message-field edge and returns the tag bytes for one occurrence. */
    private static long edge(Frame frame, Field field) {
        frame.edgeDepth = 1;
        frame.edgeDelimited = true;
        frame.edgeLayout = field.message;
        return field.tag;
    }

    /** Adds a completed child to its parent's size, nesting, and subtree work. */
    private void accept(Frame frame, long size, int nesting, long work) throws IOException {
        frame.size = add(frame.size, frame.edgeDelimited ? delimited(size) : size);
        frame.nesting = Math.max(frame.nesting, frame.edgeDepth + nesting);
        frame.work += work;
    }

    /** Counts a packed scalar block or adds individual unpacked occurrences. */
    private void repeatedField(Frame frame, Field field, List<?> values) throws IOException {
        if (!field.packed) {
            for (int i = 0; i < values.size(); i++) {
                field(frame, field, values.get(i));
            }
            return;
        }
        long packed;
        int width = fixedWidth(field.type);
        if (width > 0) {
            packed = (long) width * values.size();
        } else if (values instanceof LongList longs) {
            packed = 0;
            for (int i = 0; i < longs.size(); i++) {
                packed = add(packed, varint(field.type, longs.getLong(i)));
            }
        } else if (values instanceof IntList ints) {
            packed = 0;
            for (int i = 0; i < ints.size(); i++) {
                packed = add(packed, varint(field.type, ints.getInt(i)));
            }
        } else {
            packed = 0;
            for (int i = 0; i < values.size(); i++) {
                packed = add(packed, scalar(field.type, values.get(i), field.descriptor));
            }
        }
        frame.size = add(frame.size, add(field.tag, delimited(packed)));
    }

    /** Counts opaque unknown wire fields and queues groups as child edges. */
    private void unknownFields(Frame frame, UnknownFieldSet unknown) throws IOException {
        List<UnknownFieldSet> groups = null;
        for (Map.Entry<Integer, UnknownFieldSet.Field> entry : unknown.asMap().entrySet()) {
            if (entry.getKey() > MAX_FIELD_NUMBER) {
                throw new IOException(
                        "Unknown field number "
                                + entry.getKey()
                                + " exceeds the maximum field number "
                                + MAX_FIELD_NUMBER);
            }
            UnknownFieldSet.Field field = entry.getValue();
            // A set frame holds its container's layout; a group's frame holds none.
            String collision =
                    frame.layout == null ? null : collision(frame.layout, entry.getKey(), field);
            if (collision != null) {
                throw new IOException(collision);
            }
            int tag = CodedOutputStream.computeTagSize(entry.getKey());
            for (long number : field.getVarintList()) {
                frame.size =
                        add(frame.size, tag + CodedOutputStream.computeUInt64SizeNoTag(number));
            }
            frame.size = add(frame.size, (long) (tag + 4) * field.getFixed32List().size());
            frame.size = add(frame.size, (long) (tag + 8) * field.getFixed64List().size());
            for (ByteString bytes : field.getLengthDelimitedList()) {
                frame.size = add(frame.size, add(tag, delimited(bytes.size())));
            }
            frame.work +=
                    1L
                            + field.getVarintList().size()
                            + field.getFixed32List().size()
                            + field.getFixed64List().size()
                            + field.getLengthDelimitedList().size();
            for (UnknownFieldSet group : field.getGroupList()) {
                frame.size = add(frame.size, 2L * tag);
                if (groups == null) {
                    groups = new ArrayList<>();
                }
                groups.add(group);
            }
        }
        if (groups != null) {
            frame.work += groups.size();
            frame.edgeDepth = 1;
            frame.edgeDelimited = false;
            frame.edgeLayout = null;
            frame.elements = groups;
            frame.element = 0;
        }
    }

    /** Adds one unpacked scalar occurrence. */
    private void field(Frame frame, Field field, Object value) throws IOException {
        frame.size = add(frame.size, field.tag);
        if (field.type == FieldDescriptor.Type.STRING) {
            String string = (String) value;
            frame.work += string.length();
            frame.size = add(frame.size, delimited(utf8Length(string)));
            return;
        }
        frame.size = add(frame.size, scalar(field.type, value, field.descriptor));
    }

    /** Calculates a non-string scalar size without its tag, retaining long arithmetic for bytes. */
    private long scalar(FieldDescriptor.Type type, Object value, FieldDescriptor field)
            throws IOException {
        int width = fixedWidth(type);
        if (width > 0) {
            return width;
        }
        return switch (type) {
            case INT64, UINT64, SINT64 -> varint(type, (Long) value);
            case INT32, UINT32, SINT32 -> varint(type, (Integer) value);
            case ENUM -> varint(type, ((EnumValueDescriptor) value).getNumber());
            // DynamicMessage retains a byte[] that an application sets for a bytes field.
            case BYTES ->
                    delimited(
                            value instanceof byte[] bytes
                                    ? bytes.length
                                    : ((ByteString) value).size());
            default -> throw new IOException("Unsupported scalar field: " + field.getFullName());
        };
    }

    /** Returns the encoded size of a fixed-width scalar, or zero for variable-length types. */
    private static int fixedWidth(FieldDescriptor.Type type) {
        return switch (type) {
            case DOUBLE, FIXED64, SFIXED64 -> 8;
            case FLOAT, FIXED32, SFIXED32 -> 4;
            case BOOL -> 1;
            default -> 0;
        };
    }

    /** Sizes a 64-bit varint scalar without its tag. */
    private static int varint(FieldDescriptor.Type type, long value) {
        return type == FieldDescriptor.Type.SINT64
                ? CodedOutputStream.computeSInt64SizeNoTag(value)
                : CodedOutputStream.computeUInt64SizeNoTag(value);
    }

    /** Sizes a 32-bit varint or enum scalar without its tag. */
    private static int varint(FieldDescriptor.Type type, int value) {
        return switch (type) {
            case UINT32 -> CodedOutputStream.computeUInt32SizeNoTag(value);
            case SINT32 -> CodedOutputStream.computeSInt32SizeNoTag(value);
            case ENUM -> CodedOutputStream.computeEnumSizeNoTag(value);
            default -> CodedOutputStream.computeInt32SizeNoTag(value);
        };
    }

    /** Counts UTF-8 bytes using the runtime replacement behavior for unpaired Java surrogates. */
    private long utf8Length(String value) throws IOException {
        long size = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x80) {
                size++;
            } else if (c < 0x800) {
                size += 2;
            } else if (Character.isHighSurrogate(c)
                    && i + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                size += 4;
                i++;
            } else if (Character.isSurrogate(c)) {
                // CodedOutputStream falls back to String.getBytes(UTF_8), which writes '?'.
                size++;
            } else {
                size += 3;
            }
            checkSize(size);
        }
        return size;
    }

    /** Adds a length-delimited prefix after validating the payload fits the positive int limit. */
    private long delimited(long payload) throws IOException {
        checkSize(payload);
        return add(CodedOutputStream.computeUInt32SizeNoTag((int) payload), payload);
    }

    /** Adds nonnegative sizes in long arithmetic and enforces the configured limit. */
    private long add(long first, long second) throws IOException {
        long result = first + second;
        checkSize(result);
        return result;
    }

    /** Rejects a size above the configured positive-int ceiling before narrowing it to int. */
    private void checkSize(long size) throws IOException {
        if (size > maxSize) {
            throw new IOException("Message size exceeds maxMessageSize " + maxSize + ": " + size);
        }
    }

    /** Checks parser-relative nesting, with the top-level message at depth zero. */
    private void checkDepth(long depth) throws IOException {
        if (depth > recursionLimit) {
            throw new IOException(
                    "Message depth exceeds recursionLimit " + recursionLimit + ": " + depth);
        }
    }

    /** Returns the frame at a stack index, allocating it on first use. */
    private Frame frame(int index) {
        if (index == frames.length) {
            frames = Arrays.copyOf(frames, Math.max(8, 2 * frames.length));
        }
        Frame frame = frames[index];
        if (frame == null) {
            frame = new Frame();
            frames[index] = frame;
        }
        return frame;
    }

    /** Clears record references from used frames and releases frames beyond the retained depth. */
    private void release(int used) {
        for (int i = 0; i < Math.min(used, frames.length); i++) {
            if (frames[i] != null) {
                frames[i].clear();
            }
        }
        if (frames.length > RETAINED_FRAMES) {
            frames = Arrays.copyOf(frames, RETAINED_FRAMES);
        }
    }

    /**
     * Returns the precomputed layout of a message, or one for a descriptor other than the one its
     * field declares: an accepted pair for a DynamicMessage, or an uncached layout for a generated
     * message.
     */
    private Layout layoutOf(Layout expected, Message message, int top) throws IOException {
        Descriptor descriptor = message.getDescriptorForType();
        if (expected.descriptor == descriptor) {
            return expected;
        }
        if (pairs == null) {
            return layout(descriptor);
        }
        try {
            return pairs.accept(descriptor, expected.pair);
        } catch (IOException e) {
            throw rejected(path(top), e);
        }
    }

    /** Names the field path of the child that the frame at {@code top} is visiting. */
    private String path(int top) {
        StringBuilder path = new StringBuilder(frames[0].layout.descriptor.getFullName());
        for (int i = 0; i <= top; i++) {
            Frame frame = frames[i];
            // Unknown-field sets hold no message values, so the path ends at a message frame.
            if (frame.message == null || frame.field > frame.layout.fields.length) {
                break;
            }
            Field field = frame.layout.fields[frame.field - 1];
            path.append('.').append(field.descriptor.getName());
            if (field.repeated) {
                path.append('[').append(frame.element - 1).append(']');
            }
        }
        return path.toString();
    }

    private static IOException rejected(String path, IOException cause) {
        return new IOException(
                "Cannot write DynamicMessage value at " + path + ": " + cause.getMessage(), cause);
    }

    private static int tag(FieldDescriptor field) {
        return CodedOutputStream.computeTagSize(field.getNumber());
    }

    /**
     * Immutable field metadata for one message type, shared by serializers of one payload class.
     */
    static final class Layout {
        private final Descriptor descriptor;

        /**
         * Declaration that the reader parses these values with, the descriptor itself for a
         * generated or identical type, or null if the reader keeps them as unknown fields.
         */
        private final Descriptor pair;

        private final Field[] fields;

        /**
         * Declaration whose fields unknown fields must not collide with, or null if the reader
         * keeps these values as unknown fields, in which no collision can occur.
         */
        private final Descriptor reader;

        /** Fields of {@link #reader}, indexed by declaration order; empty without a reader. */
        private final Field[] readerFields;

        /**
         * Repeated string fields and maps with string values, whose elements can share a String.
         */
        private final Field[] strings;

        /** Message fields whose values can hold unknown fields; set once after linking. */
        private Field[] nested;

        /**
         * Greatest parser depth of a value relative to this type without unknown groups or
         * colliding unknown fields, or {@link #UNBOUNDED} if the type graph from here reaches a
         * cycle; set once after linking.
         */
        private int height = UNMEASURED;

        private Layout(Descriptor descriptor, Descriptor pair) {
            this.descriptor = descriptor;
            this.pair = pair;
            fields = fields(descriptor);
            reader = pair;
            readerFields = pair == null ? new Field[0] : pair == descriptor ? fields : fields(pair);
            List<Field> stringFields = new ArrayList<>();
            for (Field field : fields) {
                FieldDescriptor element =
                        field.mapValue == null ? field.descriptor : field.mapValue;
                if (field.repeated && element.getType() == FieldDescriptor.Type.STRING) {
                    stringFields.add(field);
                }
            }
            strings = stringFields.toArray(new Field[0]);
        }

        private static Field[] fields(Descriptor descriptor) {
            List<FieldDescriptor> declared = descriptor.getFields();
            Field[] result = new Field[declared.size()];
            for (int i = 0; i < result.length; i++) {
                result[i] = new Field(declared.get(i));
            }
            return result;
        }
    }

    /** Identifies a layout by its value descriptor and pair instances. */
    private record LayoutKey(Descriptor descriptor, Descriptor pair) {}

    /**
     * DynamicMessage descriptor pairs accepted by one serializer instance.
     *
     * <p>A value whose descriptor is its pair's instance uses the serializer's own layout. Any
     * other pair is accepted if the value's descriptor satisfies the payload rules and, when the
     * reader has a declaration for it, the schema evaluator accepts the descriptor as saved
     * metadata against that declaration with this serializer's settings on both sides. The least
     * recently used of at most {@link #PAIR_CACHE_SIZE} accepted pairs is evicted.
     */
    private final class Pairs {
        /** Layouts of the serializer's own types, each paired with itself. */
        private final Map<Descriptor, Layout> own = new IdentityHashMap<>();

        private final Map<LayoutKey, Layout> accepted =
                new LinkedHashMap<>(PAIR_CACHE_SIZE, 0.75f, true) {
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean removeEldestEntry(Map.Entry<LayoutKey, Layout> eldest) {
                        return size() > PAIR_CACHE_SIZE;
                    }
                };

        private int evaluations;

        /** Most recently used accepted pair, which bypasses the access-ordered map. */
        private LayoutKey last;

        private Layout lastLayout;

        private Pairs(Layout root) {
            List<Layout> pending = new ArrayList<>(List.of(root));
            own.put(root.descriptor, root);
            for (int i = 0; i < pending.size(); i++) {
                for (Field field : pending.get(i).fields) {
                    if (field.message != null
                            && own.putIfAbsent(field.message.descriptor, field.message) == null) {
                        pending.add(field.message);
                    }
                }
            }
        }

        /**
         * Returns the layout of a value descriptor under a reader declaration, or fails with the
         * reason that the pair is not accepted.
         */
        private Layout accept(Descriptor descriptor, Descriptor pair) throws IOException {
            if (descriptor == pair) {
                Layout layout = own.get(descriptor);
                if (layout != null) {
                    return layout;
                }
            }
            // Values decoded by another serializer instance repeat one pair on every record.
            if (last != null && last.descriptor() == descriptor && last.pair() == pair) {
                return lastLayout;
            }
            LayoutKey key = new LayoutKey(descriptor, pair);
            Layout layout = accepted.get(key);
            if (layout != null) {
                remember(key, layout);
                return layout;
            }
            evaluations++;
            try {
                ProtobufMessageType.validatePayloadGraph(descriptor);
                if (pair != null) {
                    ProtobufSchemaCompatibility.Result result =
                            ProtobufSchemaCompatibility.evaluate(
                                    metadata(descriptor), metadata(pair));
                    if (!result.compatibleAsIs()) {
                        throw new IOException(result.reason());
                    }
                }
            } catch (IllegalArgumentException e) {
                throw new IOException(e.getMessage(), e);
            }
            layout = layout(descriptor, pair, true);
            accepted.put(key, layout);
            remember(key, layout);
            return layout;
        }

        private void remember(LayoutKey key, Layout layout) {
            last = key;
            lastLayout = layout;
        }

        private ProtobufSchemaCompatibility.Metadata metadata(Descriptor descriptor) {
            return ProtobufSchemaCompatibility.Metadata.dynamic(
                    descriptor.getFullName(),
                    settings,
                    ProtobufSchema.normalize(descriptor.getFile()));
        }
    }

    /** Wire properties of one declared field, resolved once from its descriptor. */
    private static final class Field {
        private final FieldDescriptor descriptor;
        private final FieldDescriptor.Type type;
        private final int tag;
        private final boolean repeated;
        private final boolean copied;
        private final boolean packed;

        /** {@link WireFormat} wire type of this field's own encoding. */
        private final int wireType;

        /** Whether the parser also reads the packed encoding into this repeated field. */
        private final boolean packable;

        /** Whether presence is the value differing from its default, as in proto3 scalars. */
        private final boolean implicit;

        private final Object defaultValue;

        /** Value field of a map entry, or null for a field that is not a map. */
        private final FieldDescriptor mapValue;

        /** Layout of a message field; set once while linking the layout graph. */
        private Layout message;

        private Field(FieldDescriptor descriptor) {
            this.descriptor = descriptor;
            type = descriptor.getType();
            tag = tag(descriptor);
            repeated = descriptor.isRepeated();
            copied = descriptor.isMapField() || type == FieldDescriptor.Type.ENUM;
            packed = descriptor.isPacked();
            wireType = descriptor.getLiteType().getWireType();
            // The parser accepts both encodings of a repeated numeric or enum field.
            packable = descriptor.isPackable();
            // Map entries report both fields as present, and the runtime writes both.
            implicit =
                    !repeated
                            && !descriptor.hasPresence()
                            && !descriptor.getContainingType().getOptions().getMapEntry();
            defaultValue = implicit ? descriptor.getDefaultValue() : null;
            mapValue =
                    descriptor.isMapField()
                            ? descriptor.getMessageType().findFieldByNumber(2)
                            : null;
        }
    }

    /**
     * Stores the exact byte size and additional parser depth of a completed value, and the layout
     * that it was checked with: for an unknown-field set, the container layout whose reader fields
     * it was checked against.
     */
    private record Summary(long size, int depth, Layout layout) {}

    /** Tracks one value's iterative traversal. */
    private final class Frame {
        private Object value;
        private Message message;
        private Layout layout;
        private int depth;
        private int field;
        private List<?> elements;
        private int element;
        private int edgeDepth;

        /** Whether the edge's child is a length-delimited message rather than unknown fields. */
        private boolean edgeDelimited;

        private Layout edgeLayout;
        private int nesting;
        private long size;
        private long work;

        /** Resets this frame for a message or unknown field set at an absolute parser depth. */
        private Frame start(Object value, Layout layout, int depth) throws IOException {
            this.value = value;
            this.layout = layout;
            this.depth = depth;
            field = 0;
            elements = null;
            element = 0;
            nesting = 0;
            size = 0;
            if (value instanceof Message candidate) {
                message = candidate;
                work = layout.fields.length;
            } else {
                message = null;
                work = 0;
                unknownFields(this, (UnknownFieldSet) value);
            }
            return this;
        }

        /** Drops record references so that a retained frame does not keep values alive. */
        private void clear() {
            value = null;
            message = null;
            elements = null;
            layout = null;
            edgeLayout = null;
        }
    }
}
