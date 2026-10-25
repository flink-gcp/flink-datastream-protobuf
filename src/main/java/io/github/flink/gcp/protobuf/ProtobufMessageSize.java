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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Overflow-safe wire sizing and parser-depth validation before the caller writes a frame.
 *
 * <p>The parser reads an unknown length-delimited field whose number belongs to a declared message
 * or map field as that field's message, so its bytes can nest beyond the recursion limit; every
 * check rejects such a value, and one with an unknown-field number above the specification's
 * maximum, whose tag the writer would wrap to another number or to field zero. Other unknown fields
 * are sized as opaque bytes; the parser keeps them unknown unless their number and wire type match
 * a declared scalar, string, bytes, or packed field, which it reads them into.
 *
 * <p>When the payload type graph is acyclic and its static height is within the recursion limit, a
 * value without unknown groups or fields colliding with a message or map field cannot exceed that
 * limit. Such a value uses the runtime's memoized generated size if it is at most the scratch size.
 * Until one of the runtime's {@code int} sums first exceeds {@link Integer#MAX_VALUE}, each addend
 * is the exact length that the writer emits, so a size that wrapped implies at least 2 GiB of
 * output; encoding into the scratch array then fails before the caller writes the frame.
 *
 * <p>Otherwise, each check traverses the value graph iteratively over field layouts precomputed for
 * the message class. Values whose subtree sizing examined more than a small constant amount of
 * content are summarized by identity for that invocation only, so shared children are counted at
 * every occurrence without re-traversing large subgraphs. Smaller values are sized again at each
 * occurrence. Fields are sized without encoding them.
 *
 * <p>Traversal frames are reused by later checks on the same instance, which is confined to one
 * serializer and thread. Every check clears their record references before returning.
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
    private static final int MAX_FIELD_NUMBER = (1 << 29) - 1;

    private final int maxSize;
    private final int recursionLimit;
    private final Layout root;

    /**
     * Largest generated size accepted without traversal; negative if the root type's height is
     * unbounded or exceeds the recursion limit.
     */
    private final int generatedLimit;

    private Frame[] frames = new Frame[0];

    /**
     * Wire-size lower bound of the repeated strings visited by the current generated-size check.
     */
    private long stringBytes;

    /**
     * Captures immutable limits and the root layout; records are referenced only during a check.
     */
    ProtobufMessageSize(ProtobufSerializerSettings settings, Layout root) {
        maxSize = settings.maxMessageSize();
        recursionLimit = settings.recursionLimit();
        this.root = root;
        generatedLimit =
                root.height >= 0 && root.height <= recursionLimit
                        ? Math.min(maxSize, ProtobufTypeSerializer.SCRATCH_SIZE)
                        : -1;
    }

    /**
     * Precomputes field layouts for every message type reachable from a payload descriptor.
     *
     * @param descriptor payload root without extensions
     * @return the immutable root layout, linked to the layouts of its message fields
     */
    static Layout layout(Descriptor descriptor) {
        Map<Descriptor, Layout> layouts = new IdentityHashMap<>();
        List<Layout> pending = new ArrayList<>();
        Layout result = new Layout(descriptor);
        layouts.put(descriptor, result);
        pending.add(result);
        for (int i = 0; i < pending.size(); i++) {
            for (Field field : pending.get(i).fields) {
                if (field.type == FieldDescriptor.Type.MESSAGE) {
                    Descriptor type = field.descriptor.getMessageType();
                    Layout child = layouts.get(type);
                    if (child == null) {
                        child = new Layout(type);
                        layouts.put(type, child);
                        pending.add(child);
                    }
                    field.message = child;
                }
            }
        }
        for (Layout layout : pending) {
            layout.nested = nested(layout);
        }
        measure(result);
        return result;
    }

    /**
     * Selects the message fields whose values can hold unknown fields. Map entries have none, so a
     * map field is selected only if its values are messages.
     */
    private static Field[] nested(Layout layout) {
        List<Field> result = new ArrayList<>();
        for (Field field : layout.fields) {
            if (field.message != null
                    && (!field.descriptor.isMapField() || hasMessageFields(field.message))) {
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
     * @param message generated message to validate
     * @return payload length, excluding the Flink frame prefix
     * @throws IOException if size/depth limits are exceeded or the traversal rejects the value
     */
    int check(Message message) throws IOException {
        if (generatedLimit >= 0) {
            int size = generatedSize(message);
            if (size >= 0) {
                return size;
            }
        }
        return traverse(message);
    }

    /**
     * Returns the generated size of a value whose depth is statically bounded, or -1 if the
     * traversal must decide.
     */
    private int generatedSize(Message message) {
        stringBytes = 0;
        if (visit(message, root, GENERATED_SIZE_BUDGET) < 0) {
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
     * the parser would read as a declared message or whose number exceeds the specification's
     * maximum, and a value reporting another descriptor may need another layout. The budget counts
     * repeated elements and singular values other than the default instance below the root; an
     * absent singular value reads as that instance, which holds no fields.
     */
    private int visit(Message message, Layout layout, int budget) {
        if (budget < 0
                || message.getDescriptorForType() != layout.descriptor
                || hasGroupsOrInvalidNumbers(message.getUnknownFields())
                || collision(layout, message.getUnknownFields()) != null) {
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
                    Message entry = (Message) message.getRepeatedField(descriptor, i);
                    budget = visit(entry, field.message, budget);
                }
            } else if (field.repeated) {
                List<?> values = (List<?>) message.getField(descriptor);
                budget -= values.size();
                for (int i = 0; budget >= 0 && i < values.size(); i++) {
                    budget = visit((Message) values.get(i), field.message, budget);
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
            Message entry = (Message) message.getRepeatedField(descriptor, i);
            if (!addString(entry.getField(field.mapValue))) {
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
     * Returns whether a value's unknown fields hold a group, whose nesting is unbounded, or a
     * number above {@link #MAX_FIELD_NUMBER}, which the traversal rejects.
     */
    private static boolean hasGroupsOrInvalidNumbers(UnknownFieldSet unknown) {
        // Protobuf 3 copies even an empty set's map, so skip the shared empty instance.
        if (unknown == UnknownFieldSet.getDefaultInstance()) {
            return false;
        }
        for (Map.Entry<Integer, UnknownFieldSet.Field> entry : unknown.asMap().entrySet()) {
            if (entry.getKey() > MAX_FIELD_NUMBER || !entry.getValue().getGroupList().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the message or map field whose number an unknown length-delimited value reuses, or
     * null. The parser would read those bytes as that field's message, including any unknown groups
     * nested in them, so their depth is not the opaque size that sizing counts. The lookup uses the
     * containing message's layout, because a set shared by values of different types can collide in
     * only some of them, and probes each declared number without copying the set.
     */
    private static Field collision(Layout layout, UnknownFieldSet unknown) {
        if (unknown == UnknownFieldSet.getDefaultInstance()) {
            return null;
        }
        for (Field field : layout.messageFields) {
            if (!unknown.getField(field.descriptor.getNumber())
                    .getLengthDelimitedList()
                    .isEmpty()) {
                return field;
            }
        }
        return null;
    }

    /**
     * Checks parser depth and exact wire size by traversing the value.
     *
     * <p>Cached subtree depth is checked again at each occurrence, so sharing a message at a deeper
     * path cannot bypass the recursion limit.
     *
     * @param message generated message to validate
     * @return exact payload length, excluding the Flink frame prefix
     * @throws IOException if size/depth limits are exceeded
     */
    int traverse(Message message) throws IOException {
        Map<Object, Summary> completed = null;
        int top = 0;
        int used = 1;
        try {
            Frame current = frame(0).start(message, layoutOf(root, message), 0);
            while (true) {
                Object child = next(current);
                if (child != null) {
                    Summary summary = completed == null ? null : completed.get(child);
                    if (summary == null) {
                        int depth = current.depth + current.edgeDepth;
                        checkDepth(depth);
                        Layout layout =
                                child instanceof Message value
                                        ? layoutOf(current.edgeLayout, value)
                                        : null;
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
                    completed.put(current.value, new Summary(current.size, current.nesting));
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
                Field collision = collision(frame.layout, unknown);
                if (collision != null) {
                    throw new IOException(
                            "Unknown length-delimited field "
                                    + collision.descriptor.getNumber()
                                    + " of "
                                    + frame.layout.descriptor.getFullName()
                                    + " would be parsed as message field "
                                    + collision.descriptor.getFullName());
                }
                frame.edgeDepth = 0;
                frame.edgeLayout = null;
                return unknown;
            }
        }
        return null;
    }

    /** Describes a message-field edge and returns the tag bytes for one occurrence. */
    private static long edge(Frame frame, Field field) {
        frame.edgeDepth = 1;
        frame.edgeLayout = field.message;
        return field.tag;
    }

    /** Adds a completed child to its parent's size, nesting, and subtree work. */
    private void accept(Frame frame, long size, int nesting, long work) throws IOException {
        // Message edges are length-delimited; unknown sets and groups have no layout.
        frame.size = add(frame.size, frame.edgeLayout != null ? delimited(size) : size);
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
            int tag = CodedOutputStream.computeTagSize(entry.getKey());
            UnknownFieldSet.Field field = entry.getValue();
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
            case BYTES -> delimited(((ByteString) value).size());
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
     * Returns the precomputed layout of a message, or derives an uncached one if a generated
     * message reports a descriptor other than the one its field declares.
     */
    private static Layout layoutOf(Layout expected, Message message) {
        Descriptor descriptor = message.getDescriptorForType();
        return expected.descriptor == descriptor ? expected : layout(descriptor);
    }

    private static int tag(FieldDescriptor field) {
        return CodedOutputStream.computeTagSize(field.getNumber());
    }

    /**
     * Immutable field metadata for one message type, shared by serializers of one payload class.
     */
    static final class Layout {
        private final Descriptor descriptor;
        private final Field[] fields;

        /**
         * Repeated string fields and maps with string values, whose elements can share a String.
         */
        private final Field[] strings;

        /** Message and map fields, whose numbers unknown length-delimited values must not reuse. */
        private final Field[] messageFields;

        /** Message fields whose values can hold unknown fields; set once after linking. */
        private Field[] nested;

        /**
         * Greatest parser depth of a value relative to this type without unknown groups or
         * colliding unknown fields, or {@link #UNBOUNDED} if the type graph from here reaches a
         * cycle; set once after linking.
         */
        private int height = UNMEASURED;

        private Layout(Descriptor descriptor) {
            this.descriptor = descriptor;
            List<FieldDescriptor> declared = descriptor.getFields();
            fields = new Field[declared.size()];
            List<Field> stringFields = new ArrayList<>();
            List<Field> messages = new ArrayList<>();
            for (int i = 0; i < fields.length; i++) {
                FieldDescriptor field = declared.get(i);
                fields[i] = new Field(field);
                FieldDescriptor element = fields[i].mapValue == null ? field : fields[i].mapValue;
                if (field.isRepeated() && element.getType() == FieldDescriptor.Type.STRING) {
                    stringFields.add(fields[i]);
                }
                if (field.getType() == FieldDescriptor.Type.MESSAGE) {
                    messages.add(fields[i]);
                }
            }
            strings = stringFields.toArray(new Field[0]);
            messageFields = messages.toArray(new Field[0]);
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

    /** Stores the exact byte size and additional parser depth of a completed value. */
    private record Summary(long size, int depth) {}

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
