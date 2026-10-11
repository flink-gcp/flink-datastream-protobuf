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

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.SourceCodeInfo;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.TextFormat;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.protobuf.ProtobufSchemaCompatibility.Metadata;
import io.github.flink.gcp.protobuf.ProtobufSchemaCompatibility.Result;
import io.github.flink.gcp.protobuf.generated.CommonTypesEnvelope;
import io.github.flink.gcp.protobuf.generated.OptionMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufSchemaCompatibilityTest {
    private static final String ROOT = "t.Root";
    private static final String REPEATED = "label: LABEL_REPEATED";
    private static final String REQUIRED = "label: LABEL_REQUIRED";
    private static final String OPTIONAL3 = "proto3_optional: true";
    private static final String ID = field("id", 1, "INT32");
    private static final String ID2 = field("n", 2, "INT32");

    /**
     * One saved/current descriptor pair. A null rejection means the forward direction is
     * compatible; a reverse rejection additionally names the rule that rejects the opposite
     * direction.
     */
    record Case(
            String name, String saved, String current, String rejection, String reverseRejection) {
        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<Case> transitions() {
        String child = message("Child", field("x", 1, "INT32"));
        String enumRoot = message("Root", ID, enumField("kind", 2, ".t.Kind"));
        return Stream.of(
                // Accepted transitions; reversing a supported change is a removal or unreservation.
                both("unchanged schema", proto3(message("Root", ID)), proto3(message("Root", ID))),
                oneWay(
                        "adds a scalar field",
                        "Field 2 (name) of t.Root is removed without reserving its number",
                        proto3(message("Root", ID)),
                        proto3(message("Root", ID, field("name", 2, "STRING")))),
                oneWay(
                        "adds a repeated field",
                        "Field 2 (tags) of t.Root is removed without reserving its number",
                        proto3(message("Root", ID)),
                        proto3(message("Root", ID, field("tags", 2, "STRING", REPEATED)))),
                oneWay(
                        "adds a map field",
                        "Field 2 (counts) of t.Root is removed without reserving its number",
                        proto3(message("Root", ID)),
                        proto3(
                                message(
                                        "Root",
                                        ID,
                                        field(
                                                "counts",
                                                2,
                                                "MESSAGE",
                                                REPEATED,
                                                "type_name: '.t.Root.CountsEntry'"),
                                        mapEntry("CountsEntry", "STRING", "INT32")))),
                oneWay(
                        "adds a field of a new message type",
                        "Field 2 (child) of t.Root is removed without reserving its number",
                        proto3(message("Root", ID)),
                        proto3(message("Root", ID, messageField("child", 2, ".t.Child")), child)),
                oneWay(
                        "adds a field to an existing oneof",
                        "Field 3 (b) of t.Root is removed without reserving its number",
                        proto3(message("Root", ID, oneofField("a", 2, 0), oneof("choice"))),
                        proto3(
                                message(
                                        "Root",
                                        ID,
                                        oneofField("a", 2, 0),
                                        oneofField("b", 3, 0),
                                        oneof("choice")))),
                oneWay(
                        "adds a new oneof of new fields",
                        "Field 2 (a) of t.Root is removed without reserving its number",
                        proto3(message("Root", ID)),
                        proto3(message("Root", ID, oneofField("a", 2, 0), oneof("choice")))),
                oneWay(
                        "adds a proto2 optional field with a default",
                        "Field 2 (n) of t.Root is removed without reserving its number",
                        proto2(message("Root", ID)),
                        proto2(message("Root", ID, field("n", 2, "INT32", "default_value: '7'")))),
                oneWay(
                        "inserts a field before retained fields",
                        "Field 2 (first) of t.Root is removed without reserving its number",
                        proto3(message("Root", ID)),
                        proto3(message("Root", field("first", 2, "INT32"), ID))),
                oneWay(
                        "removes a field and reserves its number",
                        "Message t.Root no longer reserves field numbers 2 to 2",
                        proto3(message("Root", ID, field("name", 2, "STRING"))),
                        proto3(message("Root", ID, reserved(2, 3)))),
                oneWay(
                        "removes a field and reserves its number and name",
                        "Message t.Root no longer reserves field numbers 2 to 2",
                        proto3(message("Root", ID, field("name", 2, "STRING"))),
                        proto3(message("Root", ID, reserved(2, 3), "reserved_name: 'name'"))),
                oneWay(
                        "removes a oneof together with its fields",
                        "Message t.Root no longer reserves field numbers 2 to 2",
                        proto3(message("Root", ID, oneofField("a", 2, 0), oneof("choice"))),
                        proto3(message("Root", ID, reserved(2, 3)))),
                oneWay(
                        "removes a proto2 required field and reserves its number",
                        "Message t.Root no longer reserves field numbers 2 to 2",
                        proto2(message("Root", ID, field("r", 2, "INT32", REQUIRED))),
                        proto2(message("Root", ID, reserved(2, 3)))),
                oneWay(
                        "adds a reservation",
                        "Message t.Root no longer reserves field numbers 5 to 9",
                        proto3(message("Root", ID)),
                        proto3(message("Root", ID, reserved(5, 10), "reserved_name: 'gone'"))),
                both(
                        "splits a reserved range",
                        proto3(message("Root", ID, reserved(2, 4))),
                        proto3(message("Root", ID, reserved(2, 3), reserved(3, 4)))),
                oneWay(
                        "appends an enum value",
                        "Enum t.Kind removes or renames value B",
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0), value("A", 1))),
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind",
                                        value("UNKNOWN", 0),
                                        value("A", 1),
                                        value("B", 2)))),
                oneWay(
                        "inserts an enum value with a fresh number",
                        "Enum t.Kind removes or renames value A",
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0), value("B", 2))),
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind",
                                        value("UNKNOWN", 0),
                                        value("A", 1),
                                        value("B", 2)))),
                oneWay(
                        "adds an enum value to a closed proto2 enum",
                        "Enum t.Kind removes or renames value SECOND",
                        proto2(
                                message("Root", ID, enumField("kind", 2, ".t.Kind")),
                                enumType("Kind", value("FIRST", 1))),
                        proto2(
                                message("Root", ID, enumField("kind", 2, ".t.Kind")),
                                enumType("Kind", value("FIRST", 1), value("SECOND", 2)))),
                oneWay(
                        "adds a field to a nested message",
                        "Field 2 (y) of t.Child is removed without reserving its number",
                        proto3(message("Root", ID, messageField("child", 2, ".t.Child")), child),
                        proto3(
                                message("Root", ID, messageField("child", 2, ".t.Child")),
                                message("Child", field("x", 1, "INT32"), field("y", 2, "INT32")))),
                oneWay(
                        "adds a field to a recursive message",
                        "Field 3 (label) of t.Root is removed without reserving its number",
                        proto3(message("Root", ID, messageField("next", 2, ".t.Root"))),
                        proto3(
                                message(
                                        "Root",
                                        ID,
                                        messageField("next", 2, ".t.Root"),
                                        field("label", 3, "STRING")))),
                oneWay(
                        "adds a field inside a message cycle",
                        "Field 2 (n) of t.B is removed without reserving its number",
                        proto3(
                                message("Root", messageField("b", 1, ".t.B")),
                                message("B", messageField("root", 1, ".t.Root"))),
                        proto3(
                                message("Root", messageField("b", 1, ".t.B")),
                                message(
                                        "B",
                                        messageField("root", 1, ".t.Root"),
                                        field("n", 2, "INT32")))),
                both(
                        "changes an unrelated message",
                        proto3(message("Root", ID), message("Other", field("x", 1, "INT32"))),
                        proto3(message("Root", ID), message("Other", field("x", 1, "STRING")))),
                both(
                        "changes an unrelated enum",
                        proto3(message("Root", ID), enumType("Other", value("ZERO", 0))),
                        proto3(message("Root", ID), enumType("Other", value("NONE", 0)))),
                oneWay(
                        "changes a message reachable only through a removed field",
                        "Message t.Root no longer reserves field numbers 2 to 2",
                        proto3(message("Root", ID, messageField("child", 2, ".t.Child")), child),
                        proto3(
                                message("Root", ID, reserved(2, 3)),
                                message("Child", field("x", 1, "STRING")))),
                // Rejected transitions.
                rejected(
                        "removes a field without reserving its number",
                        proto3(message("Root", ID, field("name", 2, "STRING"))),
                        proto3(message("Root", ID)),
                        "Field 2 (name) of t.Root is removed without reserving its number"),
                rejected(
                        "reuses a reserved field number",
                        proto3(message("Root", ID, reserved(2, 3))),
                        proto3(message("Root", ID, field("name", 2, "STRING"))),
                        "Message t.Root no longer reserves field numbers 2 to 2"),
                rejected(
                        "reuses a number inside a reserved range",
                        proto3(message("Root", ID, reserved(2, 10))),
                        proto3(
                                message(
                                        "Root",
                                        ID,
                                        reserved(2, 5),
                                        reserved(6, 10),
                                        field("n", 5, "INT32"))),
                        "no longer reserves field numbers 2 to 9"),
                rejected(
                        "drops a reserved name",
                        proto3(message("Root", ID, "reserved_name: 'gone'")),
                        proto3(message("Root", ID)),
                        "Message t.Root no longer reserves name gone"),
                rejected(
                        "renames a field",
                        proto3(message("Root", ID)),
                        proto3(message("Root", field("key", 1, "INT32"))),
                        "Field 1 (id) of t.Root is renamed to key"),
                rejected(
                        "renumbers a field",
                        proto3(message("Root", ID)),
                        proto3(message("Root", field("id", 2, "INT32"), reserved(1, 2))),
                        "Field 1 (id) of t.Root is renumbered to 2"),
                rejected(
                        "widens int32 to int64",
                        proto3(message("Root", ID)),
                        proto3(message("Root", field("id", 1, "INT64"))),
                        "changes type from INT32 to INT64"),
                rejected(
                        "changes int32 to uint32",
                        proto3(message("Root", ID)),
                        proto3(message("Root", field("id", 1, "UINT32"))),
                        "changes type from INT32 to UINT32"),
                rejected(
                        "changes int32 to sint32",
                        proto3(message("Root", ID)),
                        proto3(message("Root", field("id", 1, "SINT32"))),
                        "changes type from INT32 to SINT32"),
                rejected(
                        "changes fixed32 to sfixed32",
                        proto3(message("Root", field("id", 1, "FIXED32"))),
                        proto3(message("Root", field("id", 1, "SFIXED32"))),
                        "changes type from FIXED32 to SFIXED32"),
                rejected(
                        "changes string to bytes",
                        proto3(message("Root", field("id", 1, "STRING"))),
                        proto3(message("Root", field("id", 1, "BYTES"))),
                        "changes type from STRING to BYTES"),
                rejected(
                        "changes an enum to int32",
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0))),
                        proto3(
                                message("Root", ID, field("kind", 2, "INT32")),
                                enumType("Kind", value("UNKNOWN", 0))),
                        "changes type from ENUM to INT32"),
                rejected(
                        "changes a message field to bytes",
                        proto3(message("Root", ID, messageField("child", 2, ".t.Child")), child),
                        proto3(message("Root", ID, field("child", 2, "BYTES")), child),
                        "changes type from MESSAGE to BYTES"),
                rejected(
                        "changes a group to a message",
                        proto2(
                                message(
                                        "Root",
                                        field("g", 1, "GROUP", "type_name: '.t.Root.G'"),
                                        nested("G", field("x", 2, "INT32")))),
                        proto2(
                                message(
                                        "Root",
                                        messageField("g", 1, ".t.Root.G"),
                                        nested("G", field("x", 2, "INT32")))),
                        "changes type from GROUP to MESSAGE"),
                rejected(
                        "changes the referenced message",
                        proto3(
                                message("Root", ID, messageField("child", 2, ".t.Child")),
                                child,
                                message("Other", field("x", 1, "INT32"))),
                        proto3(
                                message("Root", ID, messageField("child", 2, ".t.Other")),
                                child,
                                message("Other", field("x", 1, "INT32"))),
                        "changes referenced type from t.Child to t.Other"),
                rejected(
                        "changes a map value type",
                        proto3(
                                message(
                                        "Root",
                                        field(
                                                "counts",
                                                1,
                                                "MESSAGE",
                                                REPEATED,
                                                "type_name: '.t.Root.CountsEntry'"),
                                        mapEntry("CountsEntry", "STRING", "INT32"))),
                        proto3(
                                message(
                                        "Root",
                                        field(
                                                "counts",
                                                1,
                                                "MESSAGE",
                                                REPEATED,
                                                "type_name: '.t.Root.CountsEntry'"),
                                        mapEntry("CountsEntry", "STRING", "INT64"))),
                        "Field 2 (value) of t.Root.CountsEntry changes type from INT32 to INT64"),
                rejected(
                        "changes singular to repeated",
                        proto3(message("Root", ID)),
                        proto3(message("Root", field("id", 1, "INT32", REPEATED))),
                        "changes cardinality from singular to repeated"),
                rejected(
                        "changes optional to required",
                        proto2(message("Root", ID)),
                        proto2(message("Root", field("id", 1, "INT32", REQUIRED))),
                        "changes cardinality from singular to required"),
                rejected(
                        "adds a required field",
                        proto2(message("Root", ID)),
                        proto2(message("Root", ID, field("r", 2, "INT32", REQUIRED))),
                        "Field 2 (r) of t.Root adds a required field"),
                rejected(
                        "adds proto3 explicit presence",
                        proto3(message("Root", ID)),
                        proto3(
                                message(
                                        "Root",
                                        field("id", 1, "INT32", "oneof_index: 0", OPTIONAL3),
                                        oneof("_id"))),
                        "Field 1 (id) of t.Root changes field presence"),
                rejected(
                        "removes proto3 explicit presence",
                        proto3(
                                message(
                                        "Root",
                                        field("id", 1, "INT32", "oneof_index: 0", OPTIONAL3),
                                        oneof("_id"))),
                        proto3(message("Root", ID)),
                        "changes field presence"),
                rejected(
                        "moves a field into a oneof",
                        proto3(message("Root", ID, field("a", 2, "INT32"))),
                        proto3(message("Root", ID, oneofField("a", 2, 0), oneof("choice"))),
                        "Field 2 (a) of t.Root moves into oneof choice"),
                rejected(
                        "moves a field out of a oneof",
                        proto3(message("Root", ID, oneofField("a", 2, 0), oneof("choice"))),
                        proto3(message("Root", ID, field("a", 2, "INT32"))),
                        "Field 2 (a) of t.Root moves out of oneof choice"),
                rejected(
                        "moves a field between oneofs",
                        proto3(
                                message(
                                        "Root",
                                        oneofField("a", 2, 0),
                                        oneof("left"),
                                        oneof("right"),
                                        oneofField("b", 3, 1))),
                        proto3(
                                message(
                                        "Root",
                                        oneofField("a", 2, 1),
                                        oneof("left"),
                                        oneof("right"),
                                        oneofField("b", 3, 1),
                                        oneofField("c", 4, 0))),
                        "moves from oneof left to right"),
                rejected(
                        "changes a proto2 default value",
                        proto2(message("Root", field("n", 1, "INT32", "default_value: '1'"))),
                        proto2(message("Root", field("n", 1, "INT32", "default_value: '2'"))),
                        "Field 1 (n) of t.Root changes its default value"),
                rejected(
                        "changes a field option",
                        proto3(message("Root", ID)),
                        proto3(
                                message(
                                        "Root",
                                        field("id", 1, "INT32", "options { deprecated: true }"))),
                        "Field 1 (id) of t.Root changes its options or descriptor metadata"),
                rejected(
                        "changes packed encoding",
                        proto3(message("Root", field("v", 1, "INT32", REPEATED))),
                        proto3(
                                message(
                                        "Root",
                                        field(
                                                "v",
                                                1,
                                                "INT32",
                                                REPEATED,
                                                "options { packed: false }"))),
                        "changes its options or descriptor metadata"),
                rejected(
                        "changes a JSON name",
                        proto3(message("Root", ID)),
                        proto3(message("Root", field("id", 1, "INT32", "json_name: 'ident'"))),
                        "changes its options or descriptor metadata"),
                rejected(
                        "reorders retained fields",
                        proto3(message("Root", ID, field("name", 2, "STRING"))),
                        proto3(message("Root", field("name", 2, "STRING"), ID)),
                        "Message t.Root reorders retained fields"),
                rejected(
                        "changes a message option",
                        proto3(message("Root", ID)),
                        proto3(message("Root", ID, "options { deprecated: true }")),
                        "Message t.Root changes its options or descriptor metadata"),
                rejected(
                        "changes a file option",
                        proto3(message("Root", ID)),
                        "name: 'case.proto' package: 't' syntax: 'proto3' "
                                + "options { java_multiple_files: true } "
                                + message("Root", ID),
                        "File case.proto changes its package, syntax, edition, options, or metadata"),
                rejected(
                        "changes proto2 to proto3",
                        proto2(message("Root", ID)),
                        proto3(message("Root", ID)),
                        "File case.proto changes"),
                rejected(
                        "changes a nested type deep in the graph",
                        proto3(message("Root", ID, messageField("child", 2, ".t.Child")), child),
                        proto3(
                                message("Root", ID, messageField("child", 2, ".t.Child")),
                                message("Child", field("x", 1, "INT64"))),
                        "Field 1 (x) of t.Child changes type from INT32 to INT64"),
                rejected(
                        "changes a message inside a cycle",
                        proto3(
                                message("Root", messageField("b", 1, ".t.B")),
                                message("B", messageField("root", 1, ".t.Root"), ID2)),
                        proto3(
                                message("Root", messageField("b", 1, ".t.B")),
                                message(
                                        "B",
                                        messageField("root", 1, ".t.Root"),
                                        field("n", 2, "INT64"))),
                        "Field 2 (n) of t.B changes type from INT32 to INT64"),
                rejected(
                        "changes an enclosing message option",
                        proto3(
                                message("Root", messageField("inner", 1, ".t.Outer.Inner")),
                                message("Outer", nested("Inner", ID))),
                        proto3(
                                message("Root", messageField("inner", 1, ".t.Outer.Inner")),
                                message(
                                        "Outer",
                                        nested("Inner", ID),
                                        "options { deprecated: true }")),
                        "Enclosing message t.Outer changes its options"),
                rejected(
                        "removes an enum value",
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0), value("A", 1))),
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0))),
                        "Enum t.Kind removes or renames value A"),
                rejected(
                        "renames an enum value",
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0), value("A", 1))),
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0), value("B", 1))),
                        "Enum t.Kind removes or renames value A"),
                rejected(
                        "renumbers an enum value",
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0), value("A", 1))),
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0), value("A", 2))),
                        "Enum t.Kind changes the number of A from 1 to 2"),
                rejected(
                        "adds an enum alias",
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind",
                                        "options { allow_alias: true }",
                                        value("UNKNOWN", 0),
                                        value("A", 1))),
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind",
                                        "options { allow_alias: true }",
                                        value("UNKNOWN", 0),
                                        value("A", 1),
                                        value("ALSO_A", 1))),
                        "Enum t.Kind adds alias ALSO_A for existing number 1"),
                rejected(
                        "changes the first proto2 enum value",
                        proto2(
                                message("Root", ID, enumField("kind", 2, ".t.Kind")),
                                enumType("Kind", value("FIRST", 1))),
                        proto2(
                                message("Root", ID, enumField("kind", 2, ".t.Kind")),
                                enumType("Kind", value("ZERO", 0), value("FIRST", 1))),
                        "Enum t.Kind changes its first declared value"),
                rejected(
                        "reorders retained enum values",
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind", value("UNKNOWN", 0), value("A", 1), value("B", 2))),
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind", value("UNKNOWN", 0), value("B", 2), value("A", 1))),
                        "Enum t.Kind reorders retained values"),
                rejected(
                        "reuses a reserved enum number",
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind",
                                        value("UNKNOWN", 0),
                                        "reserved_range { start: 1 end: 3 }")),
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind",
                                        value("UNKNOWN", 0),
                                        value("A", 2),
                                        "reserved_range { start: 1 end: 1 }")),
                        "Enum t.Kind no longer reserves numbers 1 to 3"),
                rejected(
                        "changes an enum option",
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0))),
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind",
                                        value("UNKNOWN", 0),
                                        "options { deprecated: true }")),
                        "Enum t.Kind changes its options or descriptor metadata"),
                oneWay(
                        "adds a real oneof before a synthetic oneof",
                        "Field 2 (a) of t.Root is removed without reserving its number",
                        proto3(
                                message(
                                        "Root",
                                        field("x", 1, "INT32", "oneof_index: 0", OPTIONAL3),
                                        oneof("_x"))),
                        proto3(
                                message(
                                        "Root",
                                        field("x", 1, "INT32", "oneof_index: 1", OPTIONAL3),
                                        oneofField("a", 2, 0),
                                        oneof("choice"),
                                        oneof("_x")))),
                rejected(
                        "uses a number that the current message still reserves",
                        proto3(message("Root", ID, reserved(2, 3))),
                        proto3(message("Root", ID, reserved(2, 3), ID2)),
                        "Field 2 (n) of t.Root reuses a reserved field number"),
                rejected(
                        "uses a name that the current message still reserves",
                        proto3(message("Root", ID, "reserved_name: 'n'")),
                        proto3(message("Root", ID, "reserved_name: 'n'", ID2)),
                        "Field 2 (n) of t.Root reuses a reserved field name"),
                rejected(
                        "adds a field inside a saved extension range",
                        proto2(message("Root", ID, "extension_range { start: 100 end: 200 }")),
                        proto2(
                                message(
                                        "Root",
                                        ID,
                                        "extension_range { start: 100 end: 200 }",
                                        field("n", 150, "STRING"))),
                        "Field 150 (n) of t.Root uses a saved extension number"),
                rejected(
                        "resolves an unchanged relative type name to a new type",
                        proto3(
                                message(
                                        "Root",
                                        messageField("c", 1, ".t.C"),
                                        messageField("n", 2, ".t.Root.N"),
                                        nested("N", messageField("g", 1, "C"))),
                                message("C", ID)),
                        proto3(
                                message(
                                        "Root",
                                        messageField("c", 1, ".t.C"),
                                        messageField("n", 2, ".t.Root.N"),
                                        nested(
                                                "N",
                                                messageField("g", 1, "C"),
                                                nested("C", field("y", 1, "STRING")))),
                                message("C", ID)),
                        "Field 1 (g) of t.Root.N changes referenced type from t.C to t.Root.N.C"),
                rejected(
                        "changes oneof options",
                        proto3(message("Root", ID, oneofField("a", 2, 0), oneof("choice"))),
                        proto3(
                                message(
                                        "Root",
                                        ID,
                                        oneofField("a", 2, 0),
                                        "oneof_decl { name: 'choice' options {"
                                                + " uninterpreted_option { identifier_value: 'x' }"
                                                + " } }")),
                        "Oneof choice of t.Root changes its options"),
                rejected(
                        "reorders retained oneofs",
                        proto3(
                                message(
                                        "Root",
                                        oneofField("a", 1, 0),
                                        oneofField("b", 2, 1),
                                        oneof("left"),
                                        oneof("right"))),
                        proto3(
                                message(
                                        "Root",
                                        oneofField("a", 1, 1),
                                        oneofField("b", 2, 0),
                                        oneof("right"),
                                        oneof("left"))),
                        "Message t.Root reorders retained oneofs"),
                rejected(
                        "renames a synthetic oneof",
                        proto3(
                                message(
                                        "Root",
                                        field("id", 1, "INT32", "oneof_index: 0", OPTIONAL3),
                                        oneof("_id"))),
                        proto3(
                                message(
                                        "Root",
                                        field("id", 1, "INT32", "oneof_index: 0", OPTIONAL3),
                                        oneof("_other"))),
                        "Field 1 (id) of t.Root changes its synthetic oneof"),
                rejected(
                        "uses a number that the current enum still reserves",
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind",
                                        value("UNKNOWN", 0),
                                        "reserved_range { start: 1 end: 3 }")),
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind",
                                        value("UNKNOWN", 0),
                                        value("A", 2),
                                        "reserved_range { start: 1 end: 3 }")),
                        "Enum t.Kind value A reuses reserved number 2"),
                rejected(
                        "uses a name that the current enum still reserves",
                        proto3(
                                enumRoot,
                                enumType("Kind", value("UNKNOWN", 0), "reserved_name: 'OLD'")),
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind",
                                        value("UNKNOWN", 0),
                                        value("OLD", 5),
                                        "reserved_name: 'OLD'")),
                        "Enum t.Kind value OLD reuses a reserved name"),
                rejected(
                        "drops a reserved enum name",
                        proto3(
                                enumRoot,
                                enumType("Kind", value("UNKNOWN", 0), "reserved_name: 'OLD'")),
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0))),
                        "Enum t.Kind no longer reserves name OLD"),
                rejected(
                        "changes a message that encloses a reached enum",
                        proto3(
                                message("Root", ID, enumField("kind", 2, ".t.Outer.Kind")),
                                message("Outer", enumType("Kind", value("UNKNOWN", 0)))),
                        proto3(
                                message("Root", ID, enumField("kind", 2, ".t.Outer.Kind")),
                                message(
                                        "Outer",
                                        enumType("Kind", value("UNKNOWN", 0)),
                                        "options { deprecated: true }")),
                        "Enclosing message t.Outer changes its options"),
                rejected(
                        "changes an enum value option",
                        proto3(enumRoot, enumType("Kind", value("UNKNOWN", 0))),
                        proto3(
                                enumRoot,
                                enumType(
                                        "Kind",
                                        "value { name: 'UNKNOWN' number: 0"
                                                + " options { deprecated: true } }")),
                        "Enum t.Kind changes the options or metadata of UNKNOWN"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("transitions")
    void evaluatesDescriptorPairsInTheRestoreDirection(Case transition) throws Exception {
        Result forward = evaluate(schema(transition.saved()), schema(transition.current()));
        if (transition.rejection() != null) {
            assertThat(forward.reason()).contains(transition.rejection());
            return;
        }
        assertThat(forward.reason()).isNull();
        Result reverse = evaluate(schema(transition.current()), schema(transition.saved()));
        if (transition.reverseRejection() == null) {
            assertThat(reverse.reason()).isNull();
        } else {
            assertThat(reverse.reason()).contains(transition.reverseRejection());
        }
    }

    @Test
    void namesAndSettingsAreCheckedBeforeDescriptorContent() throws Exception {
        FileDescriptorSet schema = schema(proto3(message("Root", ID)));
        ProtobufSerializerSettings settings = ProtobufSerializerSettings.DEFAULT;
        Metadata saved = new Metadata("example.Root", ROOT, settings, schema);
        assertThat(
                        ProtobufSchemaCompatibility.evaluate(
                                saved, new Metadata("example.Renamed", ROOT, settings, schema)))
                .isEqualTo(new Result("Java class changes from example.Root to example.Renamed"));
        FileDescriptorSet renamed = schema(proto3(message("Renamed", ID)));
        assertThat(
                        ProtobufSchemaCompatibility.evaluate(
                                saved,
                                new Metadata("example.Root", "t.Renamed", settings, renamed)))
                .isEqualTo(new Result("Protobuf message changes from t.Root to t.Renamed"));
    }

    @Test
    void readerLimitsMustNotDecreaseWhileDeterminismMayChange() throws Exception {
        FileDescriptorSet schema = schema(proto3(message("Root", ID)));
        FileDescriptorSet added = schema(proto3(message("Root", ID, field("n", 2, "INT32"))));
        ProtobufSerializerSettings base = new ProtobufSerializerSettings(false, 1024, 10);
        for (ProtobufSerializerSettings accepted :
                List.of(
                        base,
                        new ProtobufSerializerSettings(true, 1024, 10),
                        new ProtobufSerializerSettings(false, 2048, 10),
                        new ProtobufSerializerSettings(true, 2048, 20))) {
            assertThat(evaluate(schema, base, added, accepted).compatibleAsIs()).isTrue();
        }
        assertThat(evaluate(schema, base, added, new ProtobufSerializerSettings(false, 512, 20)))
                .isEqualTo(new Result("maxMessageSize decreases from 1024 to 512"));
        assertThat(evaluate(schema, base, schema, new ProtobufSerializerSettings(false, 2048, 9)))
                .isEqualTo(new Result("recursionLimit decreases from 10 to 9"));
    }

    @Test
    void fingerprintsAreNeitherNecessaryNorSufficientForCompatibility() throws Exception {
        FileDescriptorSet schema = schema(proto3(message("Root", ID)));
        ProtobufSerializerSettings settings = ProtobufSerializerSettings.DEFAULT;
        // Equal fingerprints still require equal class names and nondecreasing limits.
        FileDescriptorSet copy = schema(proto3(message("Root", ID)));
        assertThat(fingerprint(copy)).isEqualTo(fingerprint(schema));
        assertThat(
                        ProtobufSchemaCompatibility.evaluate(
                                        new Metadata("example.Root", ROOT, settings, schema),
                                        new Metadata("example.Other", ROOT, settings, copy))
                                .compatibleAsIs())
                .isFalse();
        assertThat(
                        evaluate(
                                        schema,
                                        settings,
                                        copy,
                                        new ProtobufSerializerSettings(false, 1024, 100))
                                .compatibleAsIs())
                .isFalse();
        // Different fingerprints are compatible when only unreachable declarations differ.
        FileDescriptorSet unrelated =
                schema(proto3(message("Root", ID), message("Unrelated", field("x", 1, "INT32"))));
        assertThat(fingerprint(unrelated)).isNotEqualTo(fingerprint(schema));
        assertThat(evaluate(schema, unrelated).compatibleAsIs()).isTrue();
        // Different fingerprints from reachable metadata are incompatible.
        FileDescriptorSet option =
                schema(proto3(message("Root", ID, "options { deprecated: true }")));
        assertThat(fingerprint(option)).isNotEqualTo(fingerprint(schema));
        assertThat(evaluate(schema, option).compatibleAsIs()).isFalse();
    }

    @Test
    void inputsAreNormalizedBeforeComparison() throws Exception {
        FileDescriptorProto dependency =
                parse("name: 'dep.proto' package: 't' syntax: 'proto3' " + message("Dep", ID));
        FileDescriptorProto root =
                parse(
                        "name: 'case.proto' package: 't' syntax: 'proto3' dependency: 'dep.proto' "
                                + message("Root", messageField("dep", 1, ".t.Dep")));
        FileDescriptorSet ordered =
                FileDescriptorSet.newBuilder().addFile(dependency).addFile(root).build();
        FileDescriptorSet reordered =
                FileDescriptorSet.newBuilder()
                        .addFile(
                                root.toBuilder()
                                        .setSourceCodeInfo(
                                                SourceCodeInfo.newBuilder()
                                                        .addLocation(
                                                                SourceCodeInfo.Location.newBuilder()
                                                                        .setLeadingComments("x"))))
                        .addFile(dependency)
                        .build();
        assertThat(evaluate(ordered, reordered).compatibleAsIs()).isTrue();
        // Custom option content compares equally whether or not its extension was registered.
        FileDescriptor options = OptionMessage.getDescriptor().getFile();
        FileDescriptorSet.Builder known = FileDescriptorSet.newBuilder();
        FileDescriptorSet.Builder decoded = FileDescriptorSet.newBuilder();
        for (FileDescriptor file : List.of(options.getDependencies().get(0), options)) {
            known.addFile(file.toProto());
            decoded.addFile(FileDescriptorProto.parseFrom(file.toProto().toByteArray()));
        }
        // The generated descriptor holds the option as a known extension; decoding keeps bytes.
        assertThat(known.build()).isNotEqualTo(decoded.build());
        assertThat(
                        ProtobufSchemaCompatibility.evaluate(
                                        metadata(
                                                OptionMessage.getDescriptor().getFullName(),
                                                known.build()),
                                        metadata(
                                                OptionMessage.getDescriptor().getFullName(),
                                                decoded.build()))
                                .compatibleAsIs())
                .isTrue();
    }

    @Test
    void unknownDescriptorContentIsComparedWhereSavedBytesCanReachIt() throws Exception {
        FileDescriptorSet base =
                schema(proto3(message("Root", ID), message("Unrelated", field("x", 1, "INT32"))));
        UnknownFieldSet unknown =
                UnknownFieldSet.newBuilder()
                        .addField(1000, UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                        .build();
        FileDescriptorSet field =
                mutateFile(
                        base,
                        file ->
                                file.toBuilder()
                                        .setMessageType(
                                                0,
                                                file.getMessageType(0).toBuilder()
                                                        .setField(
                                                                0,
                                                                file
                                                                        .getMessageType(0)
                                                                        .getField(0)
                                                                        .toBuilder()
                                                                        .setUnknownFields(unknown)))
                                        .build());
        assertThat(evaluate(base, field).reason())
                .isEqualTo("Field 1 (id) of t.Root changes its options or descriptor metadata");
        FileDescriptorSet message =
                mutateFile(
                        base,
                        file ->
                                file.toBuilder()
                                        .setMessageType(
                                                0,
                                                file.getMessageType(0).toBuilder()
                                                        .setUnknownFields(unknown))
                                        .build());
        assertThat(evaluate(base, message).reason())
                .isEqualTo("Message t.Root changes its options or descriptor metadata");
        FileDescriptorSet file =
                mutateFile(base, proto -> proto.toBuilder().setUnknownFields(unknown).build());
        assertThat(evaluate(base, file).reason()).startsWith("File case.proto changes");
        FileDescriptorSet set = base.toBuilder().setUnknownFields(unknown).build();
        assertThat(evaluate(base, set).reason()).isEqualTo("Descriptor set unknown fields change");
        FileDescriptorSet unrelated =
                mutateFile(
                        base,
                        proto ->
                                proto.toBuilder()
                                        .setMessageType(
                                                1,
                                                proto.getMessageType(1).toBuilder()
                                                        .setUnknownFields(unknown))
                                        .build());
        assertThat(evaluate(base, unrelated).compatibleAsIs()).isTrue();
    }

    @Test
    void unknownReservedRangeContentIsCompared() throws Exception {
        FileDescriptorSet base =
                schema(
                        proto3(
                                message(
                                        "Root",
                                        ID,
                                        enumField("kind", 2, ".t.Kind"),
                                        reserved(10, 11)),
                                enumType(
                                        "Kind",
                                        value("UNKNOWN", 0),
                                        "reserved_range { start: 5 end: 5 }")));
        UnknownFieldSet unknown =
                UnknownFieldSet.newBuilder()
                        .addField(1000, UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                        .build();
        FileDescriptorSet message =
                mutateFile(
                        base,
                        file -> {
                            FileDescriptorProto.Builder builder = file.toBuilder();
                            builder.getMessageTypeBuilder(0)
                                    .getReservedRangeBuilder(0)
                                    .setUnknownFields(unknown);
                            return builder.build();
                        });
        assertThat(evaluate(base, message).reason())
                .isEqualTo("Message t.Root changes reserved range metadata");
        // Identical annotated ranges remain compatible when another supported change differs.
        FileDescriptorSet added =
                mutateFile(
                        message,
                        file -> {
                            FileDescriptorProto.Builder builder = file.toBuilder();
                            builder.getMessageTypeBuilder(0)
                                    .addField(
                                            FieldDescriptorProto.newBuilder()
                                                    .setName("added")
                                                    .setNumber(3)
                                                    .setLabel(
                                                            FieldDescriptorProto.Label
                                                                    .LABEL_OPTIONAL)
                                                    .setType(
                                                            FieldDescriptorProto.Type.TYPE_STRING));
                            return builder.build();
                        });
        assertThat(evaluate(message, added).reason()).isNull();
        assertThat(evaluate(base, added).reason())
                .isEqualTo("Message t.Root changes reserved range metadata");
        // An unannotated reservation for a removed field does not disturb the annotated range.
        FileDescriptorSet removed =
                mutateFile(
                        message,
                        file -> {
                            FileDescriptorProto.Builder builder = file.toBuilder();
                            builder.getMessageTypeBuilder(0)
                                    .removeField(1)
                                    .addReservedRange(
                                            DescriptorProto.ReservedRange.newBuilder()
                                                    .setStart(2)
                                                    .setEnd(3));
                            return builder.build();
                        });
        assertThat(evaluate(message, removed).reason()).isNull();
        FileDescriptorSet enumRange =
                mutateFile(
                        base,
                        file -> {
                            FileDescriptorProto.Builder builder = file.toBuilder();
                            builder.getEnumTypeBuilder(0)
                                    .getReservedRangeBuilder(0)
                                    .setUnknownFields(unknown);
                            return builder.build();
                        });
        assertThat(evaluate(base, enumRange).reason())
                .isEqualTo("Enum t.Kind changes reserved range metadata");
    }

    @Test
    void importsMayChangeWhileReachableImportedTypesAreCompared() throws Exception {
        FileDescriptorProto timestamp = Timestamp.getDescriptor().getFile().toProto();
        FileDescriptorSet saved = schema(proto3(message("Root", ID)));
        FileDescriptorSet added =
                FileDescriptorSet.newBuilder()
                        .addFile(timestamp)
                        .addFile(
                                parse(
                                        "name: 'case.proto' package: 't' syntax: 'proto3' "
                                                + "dependency: 'google/protobuf/timestamp.proto' "
                                                + message(
                                                        "Root",
                                                        ID,
                                                        messageField(
                                                                "at",
                                                                2,
                                                                ".google.protobuf.Timestamp"))))
                        .build();
        assertThat(evaluate(saved, added).compatibleAsIs()).isTrue();
        // Removing the field and the import that it needed is a reserved removal.
        FileDescriptorSet removed = schema(proto3(message("Root", ID, reserved(2, 3))));
        assertThat(evaluate(added, removed).compatibleAsIs()).isTrue();
        FileDescriptorSet unrelatedImport =
                mutateFile(
                        added,
                        "google/protobuf/timestamp.proto",
                        file ->
                                file.toBuilder()
                                        .addMessageType(
                                                DescriptorProto.newBuilder().setName("Unused"))
                                        .build());
        assertThat(evaluate(added, unrelatedImport).compatibleAsIs()).isTrue();
        FileDescriptorSet changedImport =
                mutateFile(
                        added,
                        "google/protobuf/timestamp.proto",
                        file ->
                                file.toBuilder()
                                        .setMessageType(
                                                0,
                                                file.getMessageType(0).toBuilder()
                                                        .setField(
                                                                1,
                                                                file
                                                                        .getMessageType(0)
                                                                        .getField(1)
                                                                        .toBuilder()
                                                                        .setType(
                                                                                FieldDescriptorProto
                                                                                        .Type
                                                                                        .TYPE_INT64)))
                                        .build());
        assertThat(evaluate(added, changedImport).reason())
                .isEqualTo(
                        "Field 2 (nanos) of google.protobuf.Timestamp changes type from INT32 to"
                                + " INT64");
        FileDescriptorSet builtInOption =
                mutateFile(
                        added,
                        "google/protobuf/timestamp.proto",
                        file ->
                                file.toBuilder()
                                        .setOptions(
                                                file.getOptions().toBuilder().setDeprecated(true))
                                        .build());
        assertThat(evaluate(added, builtInOption).reason())
                .startsWith("File google/protobuf/timestamp.proto changes");
    }

    @Test
    void typesMustKeepTheirDeclaringFileAndScope() throws Exception {
        String child = message("Child", ID);
        FileDescriptorSet sameFile =
                schema(proto3(message("Root", messageField("child", 1, ".t.Child")), child));
        FileDescriptorSet movedFile =
                FileDescriptorSet.newBuilder()
                        .addFile(
                                parse("name: 'child.proto' package: 't' syntax: 'proto3' " + child))
                        .addFile(
                                parse(
                                        "name: 'case.proto' package: 't' syntax: 'proto3' "
                                                + "dependency: 'child.proto' "
                                                + message(
                                                        "Root",
                                                        messageField("child", 1, ".t.Child"))))
                        .build();
        assertThat(evaluate(sameFile, movedFile).reason())
                .isEqualTo("t.Child moves from file case.proto to child.proto");
        FileDescriptorSet nested =
                schema(
                        proto3(
                                message("Root", messageField("child", 1, ".t.Outer.Child")),
                                message("Outer", nested("Child", ID))));
        FileDescriptorSet packaged =
                FileDescriptorSet.newBuilder()
                        .addFile(
                                parse(
                                        "name: 'outer.proto' package: 't.Outer' syntax: 'proto3' "
                                                + child))
                        .addFile(
                                parse(
                                        "name: 'case.proto' package: 't' syntax: 'proto3' "
                                                + "dependency: 'outer.proto' "
                                                + message(
                                                        "Root",
                                                        messageField(
                                                                "child", 1, ".t.Outer.Child"))))
                        .build();
        assertThat(evaluate(nested, packaged).reason())
                .isEqualTo("Message t.Outer.Child changes its enclosing scope");
    }

    @Test
    void reachedEnumsCompareTheirDeclaringFile() throws Exception {
        String kind = enumType("Kind", value("FIRST", 1));
        String root =
                "name: 'case.proto' package: 't' syntax: 'proto2' dependency: 'kind.proto' "
                        + message("Root", ID, enumField("kind", 2, ".t.Kind"));
        FileDescriptorSet saved =
                FileDescriptorSet.newBuilder()
                        .addFile(parse("name: 'kind.proto' package: 't' syntax: 'proto2' " + kind))
                        .addFile(parse(root))
                        .build();
        FileDescriptorSet current =
                FileDescriptorSet.newBuilder()
                        .addFile(
                                parse(
                                        "name: 'kind.proto' package: 't' syntax: 'proto2' "
                                                + "options { java_package: 'changed' } "
                                                + kind))
                        .addFile(parse(root))
                        .build();
        // No reached message is declared in kind.proto, so only the enum reaches its file.
        assertThat(evaluate(saved, current).reason()).startsWith("File kind.proto changes");
    }

    @Test
    void anyPayloadSchemasAreOpaqueToTheWrapperComparison() throws Exception {
        FileDescriptorProto any = com.google.protobuf.Any.getDescriptor().getFile().toProto();
        String header =
                "name: 'case.proto' package: 't' syntax: 'proto3' "
                        + "dependency: 'google/protobuf/any.proto' ";
        String root = message("Root", messageField("payload", 1, ".google.protobuf.Any"));
        FileDescriptorSet saved =
                FileDescriptorSet.newBuilder()
                        .addFile(any)
                        .addFile(parse(header + root + message("Payload", ID)))
                        .build();
        // The packed message type is selected by data, so changing its declaration is unchecked.
        FileDescriptorSet current =
                FileDescriptorSet.newBuilder()
                        .addFile(any)
                        .addFile(
                                parse(header + root + message("Payload", field("id", 1, "STRING"))))
                        .build();
        assertThat(evaluate(saved, current).compatibleAsIs()).isTrue();
    }

    @Test
    void wellKnownAndOpenTelemetryDeclarationsAreComparedButTheirDataIsNot() throws Exception {
        String root = CommonTypesEnvelope.getDescriptor().getFullName();
        FileDescriptorSet saved =
                ProtobufSchema.normalize(CommonTypesEnvelope.getDescriptor().getFile());
        assertThat(evaluate(root, saved, saved).compatibleAsIs()).isTrue();
        // Struct keys and AnyValue variants are message data; their declared schemas are compared.
        FileDescriptorSet struct =
                mutateFile(
                        saved,
                        "google/protobuf/struct.proto",
                        file ->
                                withField(
                                        file,
                                        "Value",
                                        "number_value",
                                        FieldDescriptorProto.Type.TYPE_FLOAT));
        assertThat(evaluate(root, saved, struct).reason())
                .isEqualTo(
                        "Field 2 (number_value) of google.protobuf.Value changes type from DOUBLE"
                                + " to FLOAT");
        String common = "opentelemetry/proto/common/v1/common.proto";
        FileDescriptorSet variant =
                mutateFile(
                        saved,
                        common,
                        file -> {
                            int index = messageIndex(file, "AnyValue");
                            DescriptorProto anyValue = file.getMessageType(index);
                            int number =
                                    anyValue.getFieldList().stream()
                                                    .mapToInt(FieldDescriptorProto::getNumber)
                                                    .max()
                                                    .orElseThrow()
                                            + 1;
                            return file.toBuilder()
                                    .setMessageType(
                                            index,
                                            anyValue.toBuilder()
                                                    .addField(
                                                            FieldDescriptorProto.newBuilder()
                                                                    .setName("added_value")
                                                                    .setNumber(number)
                                                                    .setLabel(
                                                                            FieldDescriptorProto
                                                                                    .Label
                                                                                    .LABEL_OPTIONAL)
                                                                    .setType(
                                                                            FieldDescriptorProto
                                                                                    .Type
                                                                                    .TYPE_STRING)
                                                                    .setOneofIndex(0)))
                                    .build();
                        });
        assertThat(evaluate(root, saved, variant).compatibleAsIs()).isTrue();
        assertThat(evaluate(root, variant, saved).compatibleAsIs()).isFalse();
        FileDescriptorSet changedVariant =
                mutateFile(
                        saved,
                        common,
                        file ->
                                withField(
                                        file,
                                        "AnyValue",
                                        "int_value",
                                        FieldDescriptorProto.Type.TYPE_SINT64));
        assertThat(evaluate(root, saved, changedVariant).reason())
                .contains("int_value) of opentelemetry.proto.common.v1.AnyValue changes type");
    }

    @Test
    void invalidDescriptorClosuresFailInsteadOfBecomingIncompatible() throws Exception {
        FileDescriptorSet schema = schema(proto3(message("Root", ID)));
        assertThatThrownBy(
                        () ->
                                ProtobufSchemaCompatibility.evaluate(
                                        metadata("t.Missing", schema),
                                        metadata("t.Missing", schema)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing root message: t.Missing");
    }

    private static Case both(String name, String saved, String current) {
        return new Case(name, saved, current, null, null);
    }

    private static Case oneWay(String name, String reverse, String saved, String current) {
        return new Case(name, saved, current, null, reverse);
    }

    private static Case rejected(String name, String saved, String current, String rejection) {
        return new Case(name, saved, current, rejection, null);
    }

    private static String proto3(String... declarations) {
        return "name: 'case.proto' package: 't' syntax: 'proto3' " + String.join(" ", declarations);
    }

    private static String proto2(String... declarations) {
        return "name: 'case.proto' package: 't' syntax: 'proto2' " + String.join(" ", declarations);
    }

    private static String message(String name, String... parts) {
        return "message_type { name: '" + name + "' " + String.join(" ", parts) + " }";
    }

    private static String nested(String name, String... parts) {
        return "nested_type { name: '" + name + "' " + String.join(" ", parts) + " }";
    }

    private static String mapEntry(String name, String key, String value) {
        return nested(
                name,
                field("key", 1, key),
                field("value", 2, value),
                "options { map_entry: true }");
    }

    private static String field(String name, int number, String type, String... extra) {
        String parts = String.join(" ", extra);
        String label = parts.contains("label:") ? "" : "label: LABEL_OPTIONAL ";
        return "field { name: '"
                + name
                + "' number: "
                + number
                + " "
                + label
                + "type: TYPE_"
                + type
                + " "
                + parts
                + " }";
    }

    private static String messageField(String name, int number, String type) {
        return field(name, number, "MESSAGE", "type_name: '" + type + "'");
    }

    private static String enumField(String name, int number, String type) {
        return field(name, number, "ENUM", "type_name: '" + type + "'");
    }

    private static String oneofField(String name, int number, int oneof) {
        return field(name, number, "INT32", "oneof_index: " + oneof);
    }

    private static String oneof(String name) {
        return "oneof_decl { name: '" + name + "' }";
    }

    private static String reserved(int start, int end) {
        return "reserved_range { start: " + start + " end: " + end + " }";
    }

    private static String enumType(String name, String... parts) {
        return "enum_type { name: '" + name + "' " + String.join(" ", parts) + " }";
    }

    private static String value(String name, int number) {
        return "value { name: '" + name + "' number: " + number + " }";
    }

    private static FileDescriptorProto parse(String text) throws Exception {
        return TextFormat.parse(text, FileDescriptorProto.class);
    }

    private static FileDescriptorSet schema(String root) throws Exception {
        return FileDescriptorSet.newBuilder().addFile(parse(root)).build();
    }

    private static Result evaluate(FileDescriptorSet saved, FileDescriptorSet current) {
        return evaluate(ROOT, saved, current);
    }

    private static Result evaluate(
            String root, FileDescriptorSet saved, FileDescriptorSet current) {
        return ProtobufSchemaCompatibility.evaluate(metadata(root, saved), metadata(root, current));
    }

    private static Result evaluate(
            FileDescriptorSet saved,
            ProtobufSerializerSettings savedSettings,
            FileDescriptorSet current,
            ProtobufSerializerSettings currentSettings) {
        return ProtobufSchemaCompatibility.evaluate(
                new Metadata("example.Root", ROOT, savedSettings, saved),
                new Metadata("example.Root", ROOT, currentSettings, current));
    }

    private static Metadata metadata(String root, FileDescriptorSet schema) {
        return new Metadata("example.Root", root, ProtobufSerializerSettings.DEFAULT, schema);
    }

    private static FileDescriptorSet mutateFile(
            FileDescriptorSet set, UnaryOperator<FileDescriptorProto> mutation) {
        return mutateFile(set, "case.proto", mutation);
    }

    private static FileDescriptorSet mutateFile(
            FileDescriptorSet set, String name, UnaryOperator<FileDescriptorProto> mutation) {
        List<FileDescriptorProto> files = new ArrayList<>(set.getFileList());
        boolean found = false;
        for (int i = 0; i < files.size(); i++) {
            if (files.get(i).getName().equals(name)) {
                files.set(i, mutation.apply(files.get(i)));
                found = true;
            }
        }
        assertThat(found).as(name).isTrue();
        return set.toBuilder().clearFile().addAllFile(files).build();
    }

    private static int messageIndex(FileDescriptorProto file, String name) {
        for (int i = 0; i < file.getMessageTypeCount(); i++) {
            if (file.getMessageType(i).getName().equals(name)) {
                return i;
            }
        }
        throw new AssertionError("Missing message " + name);
    }

    private static FileDescriptorProto withField(
            FileDescriptorProto file,
            String message,
            String field,
            FieldDescriptorProto.Type type) {
        int index = messageIndex(file, message);
        DescriptorProto.Builder builder = file.getMessageType(index).toBuilder();
        for (FieldDescriptorProto.Builder candidate : builder.getFieldBuilderList()) {
            if (candidate.getName().equals(field)) {
                candidate.setType(type);
            }
        }
        return file.toBuilder().setMessageType(index, builder).build();
    }

    private static String fingerprint(FileDescriptorSet set) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization();
        set.writeTo(output);
        output.flush();
        return HexFormat.of()
                .formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    }
}
