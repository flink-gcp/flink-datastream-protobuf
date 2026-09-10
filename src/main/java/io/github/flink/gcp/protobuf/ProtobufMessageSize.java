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
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Overflow-safe wire sizing and parser-depth validation before the caller writes a frame.
 *
 * <p>Each check traverses the value graph iteratively and caches size/depth summaries by identity
 * only for that invocation. Shared children are counted at every occurrence without retaining
 * records between calls. Ordinary fields are sized without encoding; ambiguous proto2 strings
 * require counting generated wire output because decoded values can hide retained invalid UTF-8.
 */
@Internal
final class ProtobufMessageSize {
    private final int maxSize;
    private final int recursionLimit;

    /** Captures immutable limits; no record references are retained on this object. */
    ProtobufMessageSize(ProtobufSerializerSettings settings) {
        maxSize = settings.maxMessageSize();
        recursionLimit = settings.recursionLimit();
    }

    /**
     * Checks required fields, parser depth, and exact wire size using a per-call identity cache.
     *
     * <p>Completed subtree depth is checked again at each occurrence, so sharing a message at a
     * deeper path cannot bypass the recursion limit. If any proto2 string is ambiguous, the
     * traversal computes a lower bound and validates the entire graph before one counting pass
     * establishes its exact wire size.
     *
     * @param message generated message to validate
     * @return exact payload length, excluding the Flink frame prefix
     * @throws IOException if required fields are absent or size/depth limits are exceeded
     */
    int check(Message message) throws IOException {
        Map<Object, Summary> completed = new IdentityHashMap<>();
        ArrayDeque<Frame> stack = new ArrayDeque<>();
        boolean requiresWireSize = false;
        stack.push(frame(message, 0));
        while (!stack.isEmpty()) {
            Frame current = stack.peek();
            if (current.next == current.children.size()) {
                requiresWireSize |= current.requiresWireSize;
                completed.put(current.value, new Summary(current.size, current.nesting));
                stack.pop();
                continue;
            }
            Child child = current.children.get(current.next);
            Summary summary = completed.get(child.value);
            if (summary == null) {
                int depth = current.depth + child.depth;
                checkDepth(depth);
                stack.push(frame(child.value, depth));
                continue;
            }
            checkDepth((long) current.depth + child.depth + summary.depth);
            long size = child.delimited ? delimited(summary.size) : summary.size;
            current.size = add(current.size, size);
            current.nesting = Math.max(current.nesting, child.depth + summary.depth);
            current.next++;
        }

        return requiresWireSize ? wireSize(message) : (int) completed.get(message).size;
    }

    /**
     * Collects immediate field sizes and child edges without recursively visiting child messages.
     */
    private Frame frame(Object value, int depth) throws IOException {
        Frame frame = new Frame(value, depth);
        if (value instanceof Message message) {
            messageFields(frame, message);
        } else {
            unknownFields(frame, (UnknownFieldSet) value);
        }
        return frame;
    }

    /** Validates immediate required fields and gathers populated fields plus unknown content. */
    private void messageFields(Frame frame, Message message) throws IOException {
        for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
            if (field.isRequired() && !message.hasField(field)) {
                throw new IOException("Uninitialized message: missing " + field.getFullName());
            }
        }
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            FieldDescriptor field = entry.getKey();
            if (field.isRepeated()) {
                repeatedField(frame, field, (List<?>) entry.getValue());
            } else {
                field(frame, field, entry.getValue());
            }
        }
        frame.children.add(new Child(message.getUnknownFields(), 0, false));
    }

    /** Counts a packed scalar block or adds individual unpacked occurrences. */
    private void repeatedField(Frame frame, FieldDescriptor field, List<?> values)
            throws IOException {
        if (!field.isPacked()) {
            for (Object element : values) {
                field(frame, field, element);
            }
            return;
        }
        long packed = 0;
        for (Object element : values) {
            packed = add(packed, scalar(field, element));
        }
        frame.size = add(frame.size, add(tag(field), delimited(packed)));
    }

    /** Counts opaque unknown wire fields, deferring groups to the iterative traversal. */
    private void unknownFields(Frame frame, UnknownFieldSet unknown) throws IOException {
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
            for (UnknownFieldSet group : field.getGroupList()) {
                frame.size = add(frame.size, 2L * tag);
                frame.children.add(new Child(group, 1, false));
            }
        }
    }

    /**
     * Adds one unpacked field occurrence or defers its child payload until the child is validated.
     */
    private void field(Frame frame, FieldDescriptor field, Object value) throws IOException {
        frame.size = add(frame.size, tag(field));
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            boolean group = field.getType() == FieldDescriptor.Type.GROUP;
            if (group) {
                frame.size = add(frame.size, tag(field));
            }
            frame.children.add(new Child(value, 1, !group));
            return;
        }
        if (field.getType() == FieldDescriptor.Type.STRING) {
            String string = (String) value;
            if (!field.needsUtf8Check() && string.indexOf('\ufffd') >= 0) {
                // Reflection exposes a decoded String, which may hide retained invalid UTF-8.
                // Keep a lower bound, then count the root once after validating the whole graph.
                frame.requiresWireSize = true;
            } else {
                frame.size = add(frame.size, delimited(utf8Length(string)));
            }
            return;
        }
        frame.size = add(frame.size, scalar(field, value));
    }

    /** Calculates a non-string scalar size without its tag, retaining long arithmetic for bytes. */
    private long scalar(FieldDescriptor field, Object value) throws IOException {
        return switch (field.getType()) {
            case DOUBLE, FIXED64, SFIXED64 -> 8;
            case FLOAT, FIXED32, SFIXED32 -> 4;
            case BOOL -> 1;
            case INT64, UINT64 -> CodedOutputStream.computeUInt64SizeNoTag((Long) value);
            case INT32 -> CodedOutputStream.computeInt32SizeNoTag((Integer) value);
            case UINT32 -> CodedOutputStream.computeUInt32SizeNoTag((Integer) value);
            case SINT32 -> CodedOutputStream.computeSInt32SizeNoTag((Integer) value);
            case SINT64 -> CodedOutputStream.computeSInt64SizeNoTag((Long) value);
            case ENUM ->
                    CodedOutputStream.computeEnumSizeNoTag(
                            ((EnumValueDescriptor) value).getNumber());
            case BYTES -> delimited(((ByteString) value).size());
            default -> throw new IOException("Unsupported scalar field: " + field.getFullName());
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

    private static int tag(FieldDescriptor field) {
        return CodedOutputStream.computeTagSize(field.getNumber());
    }

    /**
     * Stores a subtree byte lower bound and exact additional parser depth for a completed value.
     */
    private record Summary(long size, int depth) {}

    /**
     * Describes a message/unknown-group edge, its parser-depth increment, and length-prefix need.
     */
    private record Child(Object value, int depth, boolean delimited) {}

    /**
     * Tracks iterative traversal progress; size is a lower bound until ambiguous strings are
     * counted.
     */
    private static final class Frame {
        private final Object value;
        private final int depth;
        private final List<Child> children = new ArrayList<>();
        private int next;
        private int nesting;
        private long size;
        private boolean requiresWireSize;

        private Frame(Object value, int depth) {
            this.value = value;
            this.depth = depth;
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
