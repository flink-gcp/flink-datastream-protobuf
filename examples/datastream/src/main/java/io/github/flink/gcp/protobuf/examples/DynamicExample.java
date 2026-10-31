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

package io.github.flink.gcp.protobuf.examples;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.protobuf.ProtobufTypeInformation;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A bounded example of DynamicMessage values and state, with a descriptor loaded from a descriptor
 * set instead of a generated class.
 */
public final class DynamicExample {
    private DynamicExample() {}

    /**
     * Builds the event descriptor from the descriptor set that the build writes into the jar.
     *
     * @throws IOException if the resource or an import that a file needs is missing
     */
    public static Descriptor eventDescriptor() throws IOException, DescriptorValidationException {
        // docs:start dynamic-descriptor
        FileDescriptorSet set;
        try (InputStream input = DynamicExample.class.getResourceAsStream("/event.pb")) {
            if (input == null) {
                throw new IOException("Missing descriptor set resource /event.pb");
            }
            set = FileDescriptorSet.parseFrom(input);
        }
        // The set lists each file after the files that it imports.
        Map<String, FileDescriptor> files = new HashMap<>();
        for (FileDescriptorProto file : set.getFileList()) {
            FileDescriptor[] dependencies = new FileDescriptor[file.getDependencyCount()];
            for (int i = 0; i < dependencies.length; i++) {
                dependencies[i] = files.get(file.getDependency(i));
                if (dependencies[i] == null) {
                    throw new IOException("Descriptor set lacks import " + file.getDependency(i));
                }
            }
            files.put(file.getName(), FileDescriptor.buildFrom(file, dependencies));
        }
        // The file name is relative to the proto source root; the message name is not qualified.
        Descriptor event = files.get("event.proto").findMessageTypeByName("Event");
        // docs:end dynamic-descriptor
        return event;
    }

    public static DataStream<String> totals(StreamExecutionEnvironment env) throws Exception {
        // One parallel source and parser keep each account's input order.
        DataStream<String> lines =
                env.fromData("alice,2", "bob,7", "alice,3", "alice,4")
                        .setParallelism(1)
                        .uid("lines");
        // docs:start dynamic-returns
        ProtobufTypeInformation<DynamicMessage> eventType =
                ProtobufTypeInformation.of(eventDescriptor());
        SingleOutputStreamOperator<DynamicMessage> events =
                lines.map(new ParseEvent()).returns(eventType);
        // docs:end dynamic-returns
        events.setParallelism(1).uid("events");
        // docs:start dynamic-keys
        return events.keyBy(
                        event ->
                                (String)
                                        event.getField(
                                                event.getDescriptorForType()
                                                        .findFieldByName("account")),
                        Types.STRING)
                .process(new RunningTotal(eventType))
                .returns(Types.STRING)
                .uid("account-totals");
        // docs:end dynamic-keys
    }

    public static void main(String[] args) throws Exception {
        try (var env =
                StreamExecutionEnvironment.getExecutionEnvironment(
                        ExplicitExample.configuration())) {
            env.setParallelism(2);
            totals(env).print().uid("output");
            env.execute("Protobuf DynamicMessage values");
        }
    }

    // docs:start dynamic-values
    /** Parses {@code account,amount} lines with a descriptor that each task loads itself. */
    public static final class ParseEvent extends RichMapFunction<String, DynamicMessage> {
        private transient Descriptor event;

        @Override
        public void open(OpenContext context) throws Exception {
            event = eventDescriptor();
        }

        @Override
        public DynamicMessage map(String line) {
            String[] fields = line.split(",");
            return DynamicMessage.newBuilder(event)
                    .setField(event.findFieldByName("account"), fields[0])
                    .setField(event.findFieldByName("amount"), Long.parseLong(fields[1]))
                    .build();
        }
    }

    // docs:end dynamic-values

    /** Keeps each account's running total as a DynamicMessage in ValueState. */
    public static final class RunningTotal
            extends KeyedProcessFunction<String, DynamicMessage, String> {
        // docs:start dynamic-state
        private final ProtobufTypeInformation<DynamicMessage> eventType;
        private transient ValueState<DynamicMessage> total;

        public RunningTotal(ProtobufTypeInformation<DynamicMessage> eventType) {
            this.eventType = eventType;
        }

        @Override
        public void open(OpenContext context) {
            total = getRuntimeContext().getState(new ValueStateDescriptor<>("total", eventType));
        }

        // docs:end dynamic-state

        @Override
        public void processElement(DynamicMessage event, Context context, Collector<String> out)
                throws Exception {
            // docs:start dynamic-fields
            FieldDescriptor amount = event.getDescriptorForType().findFieldByName("amount");
            long sum = (Long) event.getField(amount);
            DynamicMessage previous = total.value();
            if (previous != null) {
                FieldDescriptor saved = previous.getDescriptorForType().findFieldByName("amount");
                sum += (Long) previous.getField(saved);
            }
            total.update(event.toBuilder().setField(amount, sum).build());
            // docs:end dynamic-fields
            out.collect(context.getCurrentKey() + ":" + sum);
        }
    }
}
