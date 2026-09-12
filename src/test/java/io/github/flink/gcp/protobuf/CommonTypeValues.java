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

import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import com.google.protobuf.FieldMask;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.ListValue;
import com.google.protobuf.Message;
import com.google.protobuf.NullValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.Value;
import io.github.flink.gcp.protobuf.generated.CommonTypesEnvelope;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;

import java.util.ArrayList;
import java.util.List;

/** Shared test values; also used by the attached common-types recovery job. */
public final class CommonTypeValues {
    private CommonTypeValues() {}

    public static UnknownFieldSet unknown() {
        return UnknownFieldSet.newBuilder()
                .addField(
                        100,
                        UnknownFieldSet.Field.newBuilder()
                                .addVarint(987)
                                .addLengthDelimited(ByteString.copyFromUtf8("future"))
                                .build())
                .build();
    }

    public static Struct struct(int ordinal) {
        return Struct.newBuilder()
                .putFields(
                        "key-" + ordinal,
                        Value.newBuilder().setStringValue("value-" + ordinal).build())
                .putFields(
                        "nested",
                        Value.newBuilder()
                                .setListValue(
                                        ListValue.newBuilder()
                                                .addValues(
                                                        Value.newBuilder()
                                                                .setNullValue(NullValue.NULL_VALUE))
                                                .addValues(Value.newBuilder().setBoolValue(false))
                                                .addValues(
                                                        Value.newBuilder()
                                                                .setNumberValue(ordinal + 0.5))
                                                .addValues(
                                                        Value.newBuilder()
                                                                .setStructValue(
                                                                        Struct
                                                                                .getDefaultInstance()))
                                                .addValues(
                                                        Value.newBuilder()
                                                                .setListValue(
                                                                        ListValue
                                                                                .getDefaultInstance()))
                                                .addValues(Value.getDefaultInstance()))
                                .build())
                .setUnknownFields(unknown())
                .build();
    }

    public static AnyValue otel(int ordinal) {
        AnyValue.Builder value = AnyValue.newBuilder().setUnknownFields(unknown());
        switch (ordinal % 9) {
            case 0 -> value.setStringValue("value-" + ordinal);
            case 1 -> value.setBoolValue(ordinal % 2 == 0);
            case 2 -> value.setIntValue(-ordinal);
            case 3 -> value.setDoubleValue(ordinal + 0.5);
            case 4 -> value.setArrayValue(array());
            case 5 -> value.setKvlistValue(keyValues());
            case 6 ->
                    value.setBytesValue(
                            ByteString.copyFrom(new byte[] {0, (byte) ordinal, (byte) 255}));
            case 7 -> value.setStringValueStrindex(ordinal);
            default -> {
                /* The unset oneof is distinct from every present default. */
            }
        }
        return value.build();
    }

    private static ArrayValue array() {
        return ArrayValue.newBuilder()
                .addValues(AnyValue.newBuilder().setStringValue("text"))
                .addValues(AnyValue.newBuilder().setIntValue(Long.MIN_VALUE))
                .addValues(AnyValue.newBuilder().setBoolValue(true))
                .addValues(AnyValue.newBuilder().setDoubleValue(0.25))
                .addValues(AnyValue.newBuilder().setBytesValue(ByteString.copyFromUtf8("bytes")))
                .addValues(AnyValue.newBuilder().setKvlistValue(keyValues()))
                .addValues(AnyValue.newBuilder().setArrayValue(ArrayValue.getDefaultInstance()))
                .addValues(AnyValue.getDefaultInstance())
                .build();
    }

    private static KeyValueList keyValues() {
        return KeyValueList.newBuilder()
                .addValues(
                        KeyValue.newBuilder()
                                .setKey("nested")
                                .setValue(
                                        AnyValue.newBuilder()
                                                .setArrayValue(
                                                        ArrayValue.newBuilder()
                                                                .addValues(
                                                                        AnyValue.newBuilder()
                                                                                .setBoolValue(
                                                                                        false)))))
                .addValues(
                        KeyValue.newBuilder()
                                .setKey("empty")
                                .setValue(
                                        AnyValue.newBuilder()
                                                .setKvlistValue(KeyValueList.getDefaultInstance())))
                .build();
    }

    public static CommonTypesEnvelope envelope(int ordinal) {
        return CommonTypesEnvelope.newBuilder()
                .setOrdinal(ordinal)
                .setStructValue(struct(ordinal))
                .setValue(Value.newBuilder().setStructValue(struct(ordinal)))
                .setList(
                        ListValue.newBuilder()
                                .addValues(Value.newBuilder().setStructValue(struct(ordinal))))
                .setAny(
                        Any.newBuilder()
                                .setTypeUrl("https://unresolvable.invalid/app.Value")
                                .setValue(
                                        ByteString.copyFrom(
                                                new byte[] {(byte) 255, 0, (byte) ordinal})))
                .setTimestamp(Timestamp.newBuilder().setSeconds(123).setNanos(456))
                .setDuration(Duration.newBuilder().setSeconds(-123).setNanos(-456))
                .setEmpty(Empty.newBuilder().setUnknownFields(unknown()))
                .setMask(FieldMask.newBuilder().addPaths("nested.name").addPaths("count"))
                .setDoubleWrapper(DoubleValue.of(0.25))
                .setFloatWrapper(FloatValue.of(-0.5f))
                .setInt64Wrapper(Int64Value.of(Long.MIN_VALUE))
                .setUint64Wrapper(UInt64Value.of(-1L))
                .setInt32Wrapper(Int32Value.of(Integer.MIN_VALUE))
                .setUint32Wrapper(UInt32Value.of(-1))
                .setBoolWrapper(BoolValue.of(false))
                .setStringWrapper(StringValue.of("日本語"))
                .setBytesWrapper(BytesValue.of(ByteString.copyFrom(new byte[] {0, (byte) 255})))
                .setOtelValue(otel(ordinal))
                .setOtelArray(array())
                .setOtelList(keyValues())
                .setOtelEntry(KeyValue.newBuilder().setKeyStrindex(17).setValue(otel(ordinal)))
                .setUnknownFields(unknown())
                .build();
    }

    static List<Message> samples() {
        List<Message> samples = new ArrayList<>();
        samples.add(envelope(0));
        for (Object field : envelope(0).getAllFields().values()) {
            if (field instanceof Message message) {
                samples.add(message);
                samples.add(message.getDefaultInstanceForType());
            }
        }
        samples.add(CommonTypesEnvelope.getDefaultInstance());
        samples.add(BoolValue.of(true));
        samples.add(KeyValue.newBuilder().setValue(AnyValue.getDefaultInstance()).build());
        var defaults = CommonTypesEnvelope.newBuilder();
        for (var field : defaults.getDescriptorForType().getFields()) {
            if (field.getJavaType()
                    == com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                defaults.setField(field, defaults.newBuilderForField(field).build());
            }
        }
        samples.add(defaults.build());
        for (int i = 0; i < 9; i++) {
            samples.add(otel(i));
        }
        // Enumerate every present default, including empty strings/bytes and empty containers.
        for (var root : List.of(Value.getDefaultInstance(), AnyValue.getDefaultInstance())) {
            for (var field : root.getDescriptorForType().getOneofs().get(0).getFields()) {
                var builder = root.newBuilderForType();
                builder.setField(
                        field,
                        field.getJavaType()
                                        == com.google.protobuf.Descriptors.FieldDescriptor.JavaType
                                                .MESSAGE
                                ? builder.newBuilderForField(field).build()
                                : field.getDefaultValue());
                samples.add(builder.build());
            }
        }
        samples.add(Value.newBuilder().setNumberValue(Double.NaN).build());
        samples.add(Value.newBuilder().setNumberValue(Double.POSITIVE_INFINITY).build());
        samples.add(Value.newBuilder().setNumberValue(-0.0).build());
        return samples;
    }
}
