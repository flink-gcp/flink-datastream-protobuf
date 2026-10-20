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
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Overflow-safe wire sizing and parser-depth validation before the caller writes a frame.
 *
 * <p>Each check traverses the value graph iteratively over field layouts precomputed for the
 * message class. Values whose subtree sizing examined more than a small constant amount of content
 * are summarized by identity for that invocation only, so shared children are counted at every
 * occurrence without re-traversing large subgraphs. Smaller values are sized again at each
 * occurrence. Ordinary fields are sized without encoding; ambiguous proto2 strings require counting
 * generated wire output because decoded values can hide retained invalid UTF-8.
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

    /** Frames retained across checks; a deeper traversal releases the additional frames. */
    private static final int RETAINED_FRAMES = 64;

    private final int maxSize;
    private final int recursionLimit;
    private final Layout root;
    private Frame[] frames = new Frame[0];
    private boolean requiresWireSize;

    /**
     * Captures immutable limits and the root layout; records are referenced only during a check.
     */
    ProtobufMessageSize(ProtobufSerializerSettings settings, Layout root) {
        maxSize = settings.maxMessageSize();
        recursionLimit = settings.recursionLimit();
        this.root = root;
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
                if (field.type == FieldDescriptor.Type.MESSAGE
                        || field.type == FieldDescriptor.Type.GROUP) {
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
        return result;
    }

    /**
     * Checks required fields, parser depth, and exact wire size.
     *
     * <p>Cached subtree depth is checked again at each occurrence, so sharing a message at a deeper
     * path cannot bypass the recursion limit. If any proto2 string is ambiguous, the traversal
     * computes a lower bound and validates the entire graph before one counting pass establishes
     * its exact wire size.
     *
     * @param message generated message to validate
     * @return exact payload length, excluding the Flink frame prefix
     * @throws IOException if required fields are absent or size/depth limits are exceeded
     */
    int check(Message message) throws IOException {
        Map<Object, Summary> completed = null;
        requiresWireSize = false;
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
            return requiresWireSize ? wireSize(message) : (int) current.size;
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
                frame.edgeLayout = null;
                return unknown;
            }
        }
        return null;
    }

    /** Describes a message-field edge and returns the tag bytes for one occurrence. */
    private static long edge(Frame frame, Field field) {
        boolean group = field.type == FieldDescriptor.Type.GROUP;
        frame.edgeDepth = 1;
        frame.edgeDelimited = !group;
        frame.edgeLayout = field.message;
        return group ? 2L * field.tag : field.tag;
    }

    /** Adds a completed child to its parent's size, nesting, and subtree work. */
    private void accept(Frame frame, long size, int nesting, long work) throws IOException {
        frame.size = add(frame.size, frame.edgeDelimited ? delimited(size) : size);
        frame.nesting = Math.max(frame.nesting, frame.edgeDepth + nesting);
        frame.work += work;
    }

    /** Validates required fields when a message frame starts. */
    private static void requiredFields(Message message, Layout layout) throws IOException {
        for (FieldDescriptor field : layout.required) {
            if (!message.hasField(field)) {
                throw new IOException("Uninitialized message: missing " + field.getFullName());
            }
        }
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
            frame.edgeDelimited = false;
            frame.edgeLayout = null;
            frame.elements = groups;
            frame.element = 0;
        }
    }

    /** Adds one unpacked scalar occurrence, deferring ambiguous proto2 strings. */
    private void field(Frame frame, Field field, Object value) throws IOException {
        frame.size = add(frame.size, field.tag);
        if (field.type == FieldDescriptor.Type.STRING) {
            String string = (String) value;
            frame.work += string.length();
            if (field.rawString && string.indexOf('\ufffd') >= 0) {
                // Reflection exposes a decoded String, which may hide retained invalid UTF-8.
                // Keep a lower bound, then count the root once after validating the whole graph.
                requiresWireSize = true;
            } else {
                frame.size = add(frame.size, delimited(utf8Length(string)));
            }
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

    /**
     * Counts generated wire output when decoded string values cannot establish an exact size.
     *
     * <p>The caller has already checked this subtree for required fields and excessive depth. The
     * output retains no payload bytes and stops as soon as the configured size is exceeded.
     */
    private int wireSize(Message message) throws IOException {
        CountingOutputStream counter = new CountingOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(counter, 256);
        message.writeTo(output);
        output.flush();
        return (int) counter.size;
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
        private final FieldDescriptor[] required;

        private Layout(Descriptor descriptor) {
            this.descriptor = descriptor;
            List<FieldDescriptor> declared = descriptor.getFields();
            fields = new Field[declared.size()];
            List<FieldDescriptor> requiredFields = new ArrayList<>();
            for (int i = 0; i < fields.length; i++) {
                FieldDescriptor field = declared.get(i);
                fields[i] = new Field(field);
                if (field.isRequired()) {
                    requiredFields.add(field);
                }
            }
            required = requiredFields.toArray(new FieldDescriptor[0]);
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
        private final boolean rawString;

        /** Whether presence is the value differing from its default, as in proto3 scalars. */
        private final boolean implicit;

        private final Object defaultValue;

        /** Layout of a message or group field; set once while linking the layout graph. */
        private Layout message;

        private Field(FieldDescriptor descriptor) {
            this.descriptor = descriptor;
            type = descriptor.getType();
            tag = tag(descriptor);
            repeated = descriptor.isRepeated();
            copied = descriptor.isMapField() || type == FieldDescriptor.Type.ENUM;
            packed = descriptor.isPacked();
            rawString = type == FieldDescriptor.Type.STRING && !descriptor.needsUtf8Check();
            // Map entries report both fields as present, and the runtime writes both.
            implicit =
                    !repeated
                            && !descriptor.hasPresence()
                            && !descriptor.getContainingType().getOptions().getMapEntry();
            defaultValue = implicit ? descriptor.getDefaultValue() : null;
        }
    }

    /**
     * Stores a subtree byte lower bound and exact additional parser depth for a completed value.
     */
    private record Summary(long size, int depth) {}

    /**
     * Tracks one value's iterative traversal; size is a lower bound until ambiguous strings are
     * counted.
     */
    private final class Frame {
        private Object value;
        private Message message;
        private Layout layout;
        private int depth;
        private int field;
        private List<?> elements;
        private int element;
        private int edgeDepth;
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
                requiredFields(candidate, layout);
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

    /** Counts bulk writes without allocating a buffer proportional to the payload. */
    private final class CountingOutputStream extends OutputStream {
        private long size;

        @Override
        public void write(int value) throws IOException {
            size = add(size, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            size = add(size, length);
        }
    }
}
