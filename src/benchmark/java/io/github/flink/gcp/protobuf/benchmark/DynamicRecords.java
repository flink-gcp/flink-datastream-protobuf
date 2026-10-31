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

package io.github.flink.gcp.protobuf.benchmark;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.protobuf.benchmark.generated.BenchmarkProtos;

import java.util.Map;

/**
 * Builds DynamicMessage records of the benchmark schema for one descriptor instance.
 *
 * <p>Field descriptors are resolved once, as an application holding a descriptor would do.
 */
final class DynamicRecords {
    private final Descriptor record;
    private final FieldDescriptor id;
    private final FieldDescriptor timestamp;
    private final FieldDescriptor active;
    private final FieldDescriptor name;
    private final FieldDescriptor note;
    private final FieldDescriptor payload;
    private final FieldDescriptor child;
    private final FieldDescriptor values;
    private final FieldDescriptor attributes;
    private final Descriptor childType;
    private final FieldDescriptor childCode;
    private final FieldDescriptor childName;
    private final Descriptor entryType;
    private final FieldDescriptor entryKey;
    private final FieldDescriptor entryValue;

    DynamicRecords(Descriptor record) {
        this.record = record;
        id = record.findFieldByName("id");
        timestamp = record.findFieldByName("timestamp");
        active = record.findFieldByName("active");
        name = record.findFieldByName("name");
        note = record.findFieldByName("note");
        payload = record.findFieldByName("payload");
        child = record.findFieldByName("child");
        values = record.findFieldByName("values");
        attributes = record.findFieldByName("attributes");
        childType = child.getMessageType();
        childCode = childType.findFieldByName("code");
        childName = childType.findFieldByName("name");
        entryType = attributes.getMessageType();
        entryKey = entryType.findFieldByName("key");
        entryValue = entryType.findFieldByName("value");
    }

    /**
     * Rebuilds the record descriptor from the generated file's content, as an application loading a
     * descriptor set does; each call returns a distinct instance with equal content.
     */
    static Descriptor rebuild() {
        try {
            return FileDescriptor.buildFrom(
                            BenchmarkProtos.getDescriptor().toProto(), new FileDescriptor[0])
                    .findMessageTypeByName("Record");
        } catch (DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    DynamicMessage build(Corpus.Value v) {
        DynamicMessage.Builder b =
                DynamicMessage.newBuilder(record)
                        .setField(id, v.id())
                        .setField(timestamp, v.timestamp())
                        .setField(active, v.active())
                        .setField(name, v.name())
                        .setField(payload, ByteString.copyFrom(v.payload()))
                        .setField(
                                child,
                                DynamicMessage.newBuilder(childType)
                                        .setField(childCode, v.child().code())
                                        .setField(childName, v.child().name())
                                        .build());
        for (Long value : v.values()) {
            b.addRepeatedField(values, value);
        }
        for (Map.Entry<String, Long> entry : v.attributes().entrySet()) {
            b.addRepeatedField(
                    attributes,
                    DynamicMessage.newBuilder(entryType)
                            .setField(entryKey, entry.getKey())
                            .setField(entryValue, entry.getValue())
                            .build());
        }
        if (v.note() != null) {
            b.setField(note, v.note());
        }
        return b.build();
    }
}
