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

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Directional compatibility of saved serializer metadata with a new generated-message reader.
 *
 * <p>The evaluator decides whether bytes written under the saved schema and settings can be read as
 * is by the new configuration. It does not read snapshots or load generated classes. Comparison
 * starts at the root message and follows fields present in both schemas, so only declarations
 * through which the new reader interprets saved bytes are compared. Each reached message and enum,
 * its enclosing messages, and its declaring file must match after removing the explicitly supported
 * differences: fresh optional or repeated fields, removed fields whose numbers are reserved, added
 * reservations, and added enum values with fresh numbers that keep the first value and the order of
 * retained values. Every other difference in those declarations, including options and unknown
 * descriptor content, is incompatible.
 */
@Internal
final class ProtobufSchemaCompatibility {
    private static final Result COMPATIBLE_AS_IS = new Result(null);

    // Protobuf 3.25 has no constant for FileDescriptorProto.option_dependency.
    private static final int OPTION_DEPENDENCY_FIELD_NUMBER = 15;

    // Descriptor fields excluded from declaration metadata: they are compared by the rules below,
    // through reachability, or not at all.
    private static final int[] FILE_CONTENT = {
        FileDescriptorProto.DEPENDENCY_FIELD_NUMBER,
        FileDescriptorProto.MESSAGE_TYPE_FIELD_NUMBER,
        FileDescriptorProto.ENUM_TYPE_FIELD_NUMBER,
        FileDescriptorProto.SERVICE_FIELD_NUMBER,
        FileDescriptorProto.EXTENSION_FIELD_NUMBER,
        FileDescriptorProto.SOURCE_CODE_INFO_FIELD_NUMBER,
        FileDescriptorProto.PUBLIC_DEPENDENCY_FIELD_NUMBER,
        FileDescriptorProto.WEAK_DEPENDENCY_FIELD_NUMBER,
        OPTION_DEPENDENCY_FIELD_NUMBER
    };
    private static final int[] MESSAGE_CONTENT = {
        DescriptorProto.FIELD_FIELD_NUMBER,
        DescriptorProto.NESTED_TYPE_FIELD_NUMBER,
        DescriptorProto.ENUM_TYPE_FIELD_NUMBER,
        DescriptorProto.ONEOF_DECL_FIELD_NUMBER,
        DescriptorProto.RESERVED_RANGE_FIELD_NUMBER,
        DescriptorProto.RESERVED_NAME_FIELD_NUMBER
    };
    private static final int[] ENUM_CONTENT = {
        EnumDescriptorProto.VALUE_FIELD_NUMBER,
        EnumDescriptorProto.RESERVED_RANGE_FIELD_NUMBER,
        EnumDescriptorProto.RESERVED_NAME_FIELD_NUMBER
    };

    private ProtobufSchemaCompatibility() {}

    /**
     * Serializer identity and reader configuration recorded by a snapshot.
     *
     * @param className Java binary class name of the generated message
     * @param fullName Protobuf full name of the root message
     * @param settings reader limits and write mode
     * @param schema complete descriptor closure of the root message's file
     */
    record Metadata(
            String className,
            String fullName,
            ProtobufSerializerSettings settings,
            FileDescriptorSet schema) {
        /** Rejects missing components. */
        Metadata {
            Objects.requireNonNull(className, "className");
            Objects.requireNonNull(fullName, "fullName");
            Objects.requireNonNull(settings, "settings");
            Objects.requireNonNull(schema, "schema");
        }
    }

    /**
     * Compatibility decision with a diagnostic for incompatible metadata.
     *
     * @param reason the first detected incompatibility, or {@code null} when compatible as is
     */
    record Result(String reason) {
        /**
         * @return whether the new reader can read saved bytes without migration
         */
        boolean compatibleAsIs() {
            return reason == null;
        }
    }

    /**
     * Evaluates whether the current configuration can read bytes permitted by saved metadata.
     *
     * @param saved metadata of the serializer that wrote the state
     * @param current metadata of the serializer that will read it
     * @return compatible as is, or incompatible with the first detected reason
     * @throws IllegalArgumentException if either descriptor closure is invalid or lacks its root
     */
    static Result evaluate(Metadata saved, Metadata current) {
        if (!saved.className().equals(current.className())) {
            return incompatible(
                    "Java class changes from " + saved.className() + " to " + current.className());
        }
        if (!saved.fullName().equals(current.fullName())) {
            return incompatible(
                    "Protobuf message changes from "
                            + saved.fullName()
                            + " to "
                            + current.fullName());
        }
        ProtobufSerializerSettings old = saved.settings();
        ProtobufSerializerSettings next = current.settings();
        if (next.maxMessageSize() < old.maxMessageSize()) {
            return incompatible(
                    "maxMessageSize decreases from "
                            + old.maxMessageSize()
                            + " to "
                            + next.maxMessageSize());
        }
        if (next.recursionLimit() < old.recursionLimit()) {
            return incompatible(
                    "recursionLimit decreases from "
                            + old.recursionLimit()
                            + " to "
                            + next.recursionLimit());
        }
        String fullName = saved.fullName();
        FileDescriptorSet oldSchema = ProtobufSchema.normalize(saved.schema(), fullName);
        FileDescriptorSet newSchema = ProtobufSchema.normalize(current.schema(), fullName);
        if (oldSchema.equals(newSchema)) {
            return COMPATIBLE_AS_IS;
        }
        if (!oldSchema.getUnknownFields().equals(newSchema.getUnknownFields())) {
            return incompatible("Descriptor set unknown fields change");
        }
        try {
            new Comparison()
                    .run(
                            ProtobufSchema.resolveRootMessage(oldSchema, fullName),
                            ProtobufSchema.resolveRootMessage(newSchema, fullName));
        } catch (Incompatible e) {
            return incompatible(e.getMessage());
        }
        return COMPATIBLE_AS_IS;
    }

    private static Result incompatible(String reason) {
        return new Result(reason);
    }

    /** Pairwise traversal state for one evaluation; types are paired by their equal full names. */
    private static final class Comparison {
        private final ArrayDeque<Descriptor[]> pending = new ArrayDeque<>();
        private final Set<String> comparedMessages = new HashSet<>();
        private final Set<String> comparedEnums = new HashSet<>();
        private final Set<String> comparedScopes = new HashSet<>();

        void run(Descriptor savedRoot, Descriptor currentRoot) {
            pending.add(new Descriptor[] {savedRoot, currentRoot});
            while (!pending.isEmpty()) {
                Descriptor[] pair = pending.removeFirst();
                if (comparedMessages.add(pair[0].getFullName())) {
                    compareMessage(pair[0], pair[1]);
                }
            }
        }

        private void compareMessage(Descriptor saved, Descriptor current) {
            String name = saved.getFullName();
            compareMessageScope(saved, current);
            DescriptorProto savedProto = saved.toProto();
            DescriptorProto currentProto = current.toProto();
            requireRangeMetadata(
                    "Message " + name,
                    savedProto.getReservedRangeList(),
                    currentProto.getReservedRangeList());
            requireReserved(
                    "Message " + name,
                    "field numbers",
                    fieldRanges(savedProto),
                    fieldRanges(currentProto),
                    savedProto.getReservedNameList(),
                    currentProto.getReservedNameList());
            for (FieldDescriptor field : saved.getFields()) {
                FieldDescriptor next = current.findFieldByNumber(field.getNumber());
                if (next != null) {
                    compareField(field, next);
                } else if (!current.isReservedNumber(field.getNumber())) {
                    throw new Incompatible(
                            describe(field) + " is removed without reserving its number");
                }
            }
            for (FieldDescriptor next : current.getFields()) {
                if (saved.findFieldByNumber(next.getNumber()) != null) {
                    continue;
                }
                if (saved.isReservedNumber(next.getNumber())) {
                    throw new Incompatible(describe(next) + " reuses a reserved field number");
                }
                if (saved.isReservedName(next.getName())) {
                    throw new Incompatible(describe(next) + " reuses a reserved field name");
                }
                // Saved bytes may contain extensions, which an empty registry keeps as unknown.
                if (saved.isExtensionNumber(next.getNumber())) {
                    throw new Incompatible(describe(next) + " uses a saved extension number");
                }
                FieldDescriptor renumbered = saved.findFieldByName(next.getName());
                if (renumbered != null) {
                    throw new Incompatible(
                            describe(renumbered) + " is renumbered to " + next.getNumber());
                }
                if (next.isRequired()) {
                    throw new Incompatible(describe(next) + " adds a required field");
                }
            }
            requireRetainedOrder(
                    "Message " + name + " reorders retained fields",
                    saved.getFields(),
                    current.getFields(),
                    FieldDescriptor::getNumber);
            for (OneofDescriptor oneof : saved.getOneofs()) {
                OneofDescriptor next = findOneof(current, oneof.getName());
                if (next != null && !oneof.toProto().equals(next.toProto())) {
                    throw new Incompatible(
                            "Oneof " + oneof.getName() + " of " + name + " changes its options");
                }
            }
            requireRetainedOrder(
                    "Message " + name + " reorders retained oneofs",
                    saved.getOneofs(),
                    current.getOneofs(),
                    OneofDescriptor::getName);
        }

        private void compareField(FieldDescriptor saved, FieldDescriptor current) {
            String field = describe(saved);
            FieldDescriptorProto savedProto = saved.toProto();
            FieldDescriptorProto currentProto = current.toProto();
            if (!saved.getName().equals(current.getName())) {
                throw new Incompatible(field + " is renamed to " + current.getName());
            }
            if (saved.isRepeated() != current.isRepeated()
                    || saved.isRequired() != current.isRequired()
                    || savedProto.getLabel() != currentProto.getLabel()) {
                throw new Incompatible(
                        field
                                + " changes cardinality from "
                                + cardinality(saved)
                                + " to "
                                + cardinality(current));
            }
            if (saved.getType() != current.getType()) {
                throw new Incompatible(
                        field
                                + " changes type from "
                                + saved.getType()
                                + " to "
                                + current.getType());
            }
            // Compare resolved names: an unchanged relative type name can resolve differently.
            String savedType = referencedType(saved);
            String currentType = referencedType(current);
            if (!Objects.equals(savedType, currentType)) {
                throw new Incompatible(
                        field
                                + " changes referenced type from "
                                + savedType
                                + " to "
                                + currentType);
            }
            // Proto3 optional fields use synthetic oneofs, which the presence check reports.
            OneofDescriptor savedOneof = saved.getRealContainingOneof();
            OneofDescriptor currentOneof = current.getRealContainingOneof();
            if (savedOneof == null && currentOneof != null) {
                throw new Incompatible(field + " moves into oneof " + currentOneof.getName());
            }
            if (savedOneof != null && currentOneof == null) {
                throw new Incompatible(field + " moves out of oneof " + savedOneof.getName());
            }
            if (savedOneof != null && !savedOneof.getName().equals(currentOneof.getName())) {
                throw new Incompatible(
                        field
                                + " moves from oneof "
                                + savedOneof.getName()
                                + " to "
                                + currentOneof.getName());
            }
            if (saved.hasPresence() != current.hasPresence()) {
                throw new Incompatible(field + " changes field presence");
            }
            if (!Objects.equals(oneofName(saved), oneofName(current))) {
                throw new Incompatible(field + " changes its synthetic oneof");
            }
            if (!savedProto.getDefaultValue().equals(currentProto.getDefaultValue())) {
                throw new Incompatible(field + " changes its default value");
            }
            // The oneof index depends on unrelated oneof declarations; membership is checked above.
            if (!savedProto.toBuilder()
                    .clearOneofIndex()
                    .build()
                    .equals(currentProto.toBuilder().clearOneofIndex().build())) {
                throw new Incompatible(field + " changes its options or descriptor metadata");
            }
            if (saved.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                pending.add(new Descriptor[] {saved.getMessageType(), current.getMessageType()});
            } else if (saved.getJavaType() == FieldDescriptor.JavaType.ENUM
                    && comparedEnums.add(saved.getEnumType().getFullName())) {
                compareEnum(saved.getEnumType(), current.getEnumType());
            }
        }

        private void compareEnum(EnumDescriptor saved, EnumDescriptor current) {
            String name = "Enum " + saved.getFullName();
            EnumDescriptorProto savedProto = saved.toProto();
            EnumDescriptorProto currentProto = current.toProto();
            requireSameDeclaration(
                    name,
                    saved.getContainingType(),
                    current.getContainingType(),
                    savedProto,
                    currentProto,
                    ENUM_CONTENT);
            compareFile(saved.getFile(), current.getFile(), saved.getFullName());
            requireRangeMetadata(
                    name, savedProto.getReservedRangeList(), currentProto.getReservedRangeList());
            requireReserved(
                    name,
                    "numbers",
                    enumRanges(savedProto),
                    enumRanges(currentProto),
                    savedProto.getReservedNameList(),
                    currentProto.getReservedNameList());
            for (EnumValueDescriptor value : saved.getValues()) {
                EnumValueDescriptor next = current.findValueByName(value.getName());
                if (next == null) {
                    throw new Incompatible(name + " removes or renames value " + value.getName());
                }
                if (value.getNumber() != next.getNumber()) {
                    throw new Incompatible(
                            name
                                    + " changes the number of "
                                    + value.getName()
                                    + " from "
                                    + value.getNumber()
                                    + " to "
                                    + next.getNumber());
                }
                if (!value.toProto().equals(next.toProto())) {
                    throw new Incompatible(
                            name + " changes the options or metadata of " + value.getName());
                }
            }
            for (EnumValueDescriptor next : current.getValues()) {
                if (saved.findValueByName(next.getName()) != null) {
                    continue;
                }
                if (saved.findValueByNumber(next.getNumber()) != null) {
                    throw new Incompatible(
                            name
                                    + " adds alias "
                                    + next.getName()
                                    + " for existing number "
                                    + next.getNumber());
                }
                if (saved.isReservedNumber(next.getNumber())) {
                    throw new Incompatible(
                            name
                                    + " value "
                                    + next.getName()
                                    + " reuses reserved number "
                                    + next.getNumber());
                }
                if (saved.isReservedName(next.getName())) {
                    throw new Incompatible(
                            name + " value " + next.getName() + " reuses a reserved name");
                }
            }
            // The first declared value is the proto2 default for fields without an explicit one.
            if (!saved.getValues().get(0).getName().equals(current.getValues().get(0).getName())) {
                throw new Incompatible(name + " changes its first declared value");
            }
            requireRetainedOrder(
                    name + " reorders retained values",
                    saved.getValues(),
                    current.getValues(),
                    EnumValueDescriptor::getName);
        }

        private void compareMessageScope(Descriptor saved, Descriptor current) {
            if (comparedScopes.add("message:" + saved.getFullName())) {
                requireSameDeclaration(
                        "Message " + saved.getFullName(),
                        saved.getContainingType(),
                        current.getContainingType(),
                        saved.toProto(),
                        current.toProto(),
                        MESSAGE_CONTENT);
            }
            compareFile(saved.getFile(), current.getFile(), saved.getFullName());
        }

        /** Enclosing messages contribute inherited options even when no field reaches them. */
        private void compareEnclosingScope(Descriptor saved, Descriptor current) {
            if (saved != null && comparedScopes.add("message:" + saved.getFullName())) {
                requireSameDeclaration(
                        "Enclosing message " + saved.getFullName(),
                        saved.getContainingType(),
                        current.getContainingType(),
                        saved.toProto(),
                        current.toProto(),
                        MESSAGE_CONTENT);
            }
        }

        /** Compares a declaration's own metadata and then its enclosing messages. */
        private void requireSameDeclaration(
                String declaration,
                Descriptor savedContainer,
                Descriptor currentContainer,
                Message savedProto,
                Message currentProto,
                int[] content) {
            if (!sameContainer(savedContainer, currentContainer)) {
                throw new Incompatible(declaration + " changes its enclosing scope");
            }
            if (!strip(savedProto, content).equals(strip(currentProto, content))) {
                throw new Incompatible(declaration + " changes its options or descriptor metadata");
            }
            compareEnclosingScope(savedContainer, currentContainer);
        }

        private void compareFile(FileDescriptor saved, FileDescriptor current, String type) {
            if (!saved.getName().equals(current.getName())) {
                throw new Incompatible(
                        type + " moves from file " + saved.getName() + " to " + current.getName());
            }
            if (comparedScopes.add("file:" + saved.getName())
                    && !strip(saved.toProto(), FILE_CONTENT)
                            .equals(strip(current.toProto(), FILE_CONTENT))) {
                throw new Incompatible(
                        "File "
                                + saved.getName()
                                + " changes its package, syntax, edition, options, or metadata");
            }
        }
    }

    /** Signals the first incompatibility without retaining a stack trace. */
    private static final class Incompatible extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Incompatible(String reason) {
            super(reason, null, false, false);
        }
    }

    private static boolean sameContainer(Descriptor saved, Descriptor current) {
        return saved == null
                ? current == null
                : current != null && saved.getFullName().equals(current.getFullName());
    }

    private static OneofDescriptor findOneof(Descriptor message, String name) {
        for (OneofDescriptor oneof : message.getOneofs()) {
            if (oneof.getName().equals(name)) {
                return oneof;
            }
        }
        return null;
    }

    private static String referencedType(FieldDescriptor field) {
        return switch (field.getJavaType()) {
            case MESSAGE -> field.getMessageType().getFullName();
            case ENUM -> field.getEnumType().getFullName();
            default -> null;
        };
    }

    private static String oneofName(FieldDescriptor field) {
        OneofDescriptor oneof = field.getContainingOneof();
        return oneof == null ? null : oneof.getName();
    }

    private static String describe(FieldDescriptor field) {
        return "Field "
                + field.getNumber()
                + " ("
                + field.getName()
                + ") of "
                + field.getContainingType().getFullName();
    }

    private static String cardinality(FieldDescriptor field) {
        return field.isRepeated() ? "repeated" : field.isRequired() ? "required" : "singular";
    }

    /**
     * Removes the selected fields, including unknown fields with those numbers.
     *
     * <p>Clearing unknown numbers keeps the comparison independent of whether the descriptor reader
     * knows a recently added descriptor field.
     */
    private static Message strip(Message message, int[] numbers) {
        Message.Builder builder = message.toBuilder();
        UnknownFieldSet.Builder unknown = message.getUnknownFields().toBuilder();
        for (int number : numbers) {
            FieldDescriptor field = message.getDescriptorForType().findFieldByNumber(number);
            if (field != null) {
                builder.clearField(field);
            }
            unknown.clearField(number);
        }
        return builder.setUnknownFields(unknown.build()).build();
    }

    private static <T> void requireRetainedOrder(
            String reason, List<T> saved, List<T> current, Function<T, Object> key) {
        Set<Object> savedKeys = new HashSet<>();
        Set<Object> currentKeys = new HashSet<>();
        saved.forEach(item -> savedKeys.add(key.apply(item)));
        current.forEach(item -> currentKeys.add(key.apply(item)));
        List<Object> savedOrder = new ArrayList<>();
        List<Object> currentOrder = new ArrayList<>();
        saved.stream().map(key).filter(currentKeys::contains).forEach(savedOrder::add);
        current.stream().map(key).filter(savedKeys::contains).forEach(currentOrder::add);
        if (!savedOrder.equals(currentOrder)) {
            throw new Incompatible(reason);
        }
    }

    /** Field reserved ranges are end-exclusive. */
    private static List<long[]> fieldRanges(DescriptorProto message) {
        List<long[]> ranges = new ArrayList<>();
        for (DescriptorProto.ReservedRange range : message.getReservedRangeList()) {
            ranges.add(new long[] {range.getStart(), range.getEnd()});
        }
        return ranges;
    }

    /** Enum reserved ranges are end-inclusive; convert them to the end-exclusive form. */
    private static List<long[]> enumRanges(EnumDescriptorProto value) {
        List<long[]> ranges = new ArrayList<>();
        for (EnumDescriptorProto.EnumReservedRange range : value.getReservedRangeList()) {
            ranges.add(new long[] {range.getStart(), (long) range.getEnd() + 1});
        }
        return ranges;
    }

    /**
     * Requires each reserved range that carries metadata, such as unknown fields, to remain exact.
     *
     * <p>Coverage alone does not see that metadata. Ranges without it may still be split, merged,
     * or added under the coverage rule.
     */
    private static void requireRangeMetadata(
            String scope, List<? extends Message> saved, List<? extends Message> current) {
        if (!annotatedRanges(saved).equals(annotatedRanges(current))) {
            throw new Incompatible(scope + " changes reserved range metadata");
        }
    }

    private static Set<Message> annotatedRanges(List<? extends Message> ranges) {
        Set<Message> annotated = new HashSet<>();
        for (Message range : ranges) {
            if (!range.getUnknownFields().asMap().isEmpty()) {
                annotated.add(range);
            }
        }
        return annotated;
    }

    /** Requires every saved reservation to remain covered, independently of range splitting. */
    private static void requireReserved(
            String scope,
            String kind,
            List<long[]> savedRanges,
            List<long[]> currentRanges,
            List<String> savedNames,
            List<String> currentNames) {
        List<long[]> merged = new ArrayList<>();
        currentRanges.stream()
                .sorted(Comparator.comparingLong((long[] range) -> range[0]))
                .forEach(
                        range -> {
                            long[] last = merged.isEmpty() ? null : merged.get(merged.size() - 1);
                            if (last != null && range[0] <= last[1]) {
                                last[1] = Math.max(last[1], range[1]);
                            } else {
                                merged.add(range.clone());
                            }
                        });
        for (long[] range : savedRanges) {
            boolean covered = false;
            for (long[] candidate : merged) {
                if (range[0] >= candidate[0] && range[1] <= candidate[1]) {
                    covered = true;
                    break;
                }
            }
            if (!covered && range[0] < range[1]) {
                throw new Incompatible(
                        scope
                                + " no longer reserves "
                                + kind
                                + " "
                                + range[0]
                                + " to "
                                + (range[1] - 1));
            }
        }
        for (String name : savedNames) {
            if (!currentNames.contains(name)) {
                throw new Incompatible(scope + " no longer reserves name " + name);
            }
        }
    }
}
