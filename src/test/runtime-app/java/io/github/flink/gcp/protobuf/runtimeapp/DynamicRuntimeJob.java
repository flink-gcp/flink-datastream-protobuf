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

package io.github.flink.gcp.protobuf.runtimeapp;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.protobuf.ProtobufTypeInformation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * The stateful job of {@link RuntimeJob} with DynamicMessage values whose descriptor is loaded from
 * a descriptor set file, so its jar needs no generated message class.
 *
 * <p>It keeps the operator UIDs and state names of {@link RuntimeJob}. Each observation also
 * reports the root descriptor that the observed value carries, encoded as its {@code
 * DescriptorProto}.
 */
public final class DynamicRuntimeJob {
    private static final String ENTRY = "flink.protobuf.runtime.Entry";

    private DynamicRuntimeJob() {}

    public static JobGraph create(
            String directory,
            String backend,
            String stateKind,
            boolean deterministic,
            int size,
            int depth,
            boolean restart,
            String schema)
            throws Exception {
        Configuration config = new Configuration();
        config.set(PipelineOptions.GENERIC_TYPES, false);
        config.set(StateBackendOptions.STATE_BACKEND, backend);
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, false);
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, new MemorySize(0));
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                Path.of(directory, "checkpoints").toUri().toString());
        config.setString("state.backend.rocksdb.memory.managed", "false");
        config.setString("state.backend.rocksdb.memory.fixed-per-slot", "32mb");
        config.setString("restart-strategy.type", restart ? "fixed-delay" : "none");
        config.setString("restart-strategy.fixed-delay.attempts", "1");
        config.setString("restart-strategy.fixed-delay.delay", "0 ms");
        StreamExecutionEnvironment env = new StreamExecutionEnvironment(config);
        env.setParallelism(2);
        env.setMaxParallelism(16);
        env.disableOperatorChaining();
        env.enableCheckpointing(3_600_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        ProtobufTypeInformation<DynamicMessage> info =
                ProtobufTypeInformation.newBuilder(load(schema))
                        .deterministicSerialization(deterministic)
                        .maxMessageSize(size)
                        .recursionLimit(depth)
                        .build();
        env.fromSource(
                        new ControlledSource(directory),
                        WatermarkStrategy.noWatermarks(),
                        "input",
                        Types.INT)
                .setParallelism(1)
                .uid("input")
                .map(new Values(directory, schema))
                .returns(info)
                .uid("messages")
                .rebalance()
                .keyBy(entry -> (Integer) entry.getField(field(entry, "ordinal")) % 4, Types.INT)
                .process(new StatefulValues(directory, backend, stateKind, info))
                .returns(Types.STRING)
                .uid("values")
                .print()
                .uid("output");
        return env.getStreamGraph()
                .getJobGraph(DynamicRuntimeJob.class.getClassLoader(), new JobID());
    }

    /** Builds the runtime message descriptor from a variant's single-file descriptor set. */
    private static Descriptor load(String schema) throws Exception {
        FileDescriptorSet set = FileDescriptorSet.parseFrom(Files.readAllBytes(Path.of(schema)));
        return FileDescriptor.buildFrom(set.getFile(0), new FileDescriptor[0])
                .findMessageTypeByName("Entry");
    }

    /** Resolves a field through the value, whose descriptor may differ from the application's. */
    private static FieldDescriptor field(DynamicMessage value, String name) {
        return value.getDescriptorForType().findFieldByName(name);
    }

    private static void checkValue(DynamicMessage entry) {
        if (entry == null) {
            return;
        }
        Descriptor type = entry.getDescriptorForType();
        String payload = payload(entry);
        int length = payload.length();
        FieldDescriptor child = field(entry, "child");
        if (!type.getFullName().equals(ENTRY)
                || !entry.getUnknownFields().getField(100).getVarintList().equals(List.of(99L))
                || !entry.hasField(child)
                || (length != 8
                        && length != 1500
                        && (field(entry, "payload") != null || length != 0))
                || !payload.equals("v".repeat(length))) {
            throw new IllegalStateException("Recovered application value differs");
        }
        // Values restored with a schema that lacks the field have no field to read.
        FieldDescriptor added = field(entry, "added");
        int ordinal = (Integer) entry.getField(field(entry, "ordinal"));
        if (added != null
                && !entry.getField(added).equals("")
                && !entry.getField(added).equals("added-" + ordinal)) {
            throw new IllegalStateException("Changed added field");
        }
        FieldDescriptor detail = field(entry, "detail");
        if (detail != null) {
            DynamicMessage nested = (DynamicMessage) entry.getField(detail);
            FieldDescriptor kind = nested.getDescriptorForType().findFieldByName("kind");
            int number = ((EnumValueDescriptor) nested.getField(kind)).getNumber();
            if (!entry.hasField(detail)
                    || number == 0
                    || kind.getEnumType().findValueByNumber(number) == null) {
                throw new IllegalStateException("Unexpected nested enum value: " + number);
            }
        }
        DynamicMessage nested = (DynamicMessage) entry.getField(child);
        int nesting = 1;
        while (nested.hasField(field(nested, "child"))) {
            if ((Integer) nested.getField(field(nested, "count")) != 0) {
                throw new IllegalStateException("Changed intermediate nested value");
            }
            nested = (DynamicMessage) nested.getField(field(nested, "child"));
            nesting++;
        }
        if ((Integer) nested.getField(field(nested, "count")) != 7
                || (nesting != 1 && nesting != 6)) {
            throw new IllegalStateException("Changed nested leaf or depth");
        }
    }

    /** Returns the payload, which a schema reserving its number retains as an unknown field. */
    private static String payload(DynamicMessage entry) {
        FieldDescriptor payload = field(entry, "payload");
        if (payload != null) {
            return (String) entry.getField(payload);
        }
        List<ByteString> unknown = entry.getUnknownFields().getField(3).getLengthDelimitedList();
        if (unknown.size() > 1) {
            throw new IllegalStateException("Repeated unknown payload");
        }
        return unknown.isEmpty() ? "" : unknown.get(0).toStringUtf8();
    }

    private static String encode(DynamicMessage entry) {
        return entry == null ? "" : Base64.getEncoder().encodeToString(entry.toByteArray());
    }

    private static String schema(DynamicMessage entry) {
        return entry == null
                ? ""
                : Base64.getEncoder()
                        .encodeToString(entry.getDescriptorForType().toProto().toByteArray());
    }

    private static void write(Path target, String text) throws Exception {
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        Files.writeString(temporary, text);
        Files.move(
                temporary,
                target,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    /** Creates input values with a descriptor that this function loads itself. */
    public static final class Values extends RichMapFunction<Integer, DynamicMessage> {
        private final String directory;
        private final String schema;
        private transient Descriptor entry;

        Values(String directory, String schema) {
            this.directory = directory;
            this.schema = schema;
        }

        @Override
        public void open(OpenContext context) throws Exception {
            entry = load(schema);
        }

        @Override
        public DynamicMessage map(Integer ordinal) {
            int payloadSize = Files.exists(Path.of(directory, "large")) ? 1500 : 8;
            int nesting = Files.exists(Path.of(directory, "deep")) ? 6 : 1;
            DynamicMessage.Builder value =
                    DynamicMessage.newBuilder(entry)
                            .setField(entry.findFieldByName("ordinal"), ordinal)
                            .setField(entry.findFieldByName("count"), 1)
                            .setUnknownFields(
                                    UnknownFieldSet.newBuilder()
                                            .addField(
                                                    100,
                                                    UnknownFieldSet.Field.newBuilder()
                                                            .addVarint(99)
                                                            .build())
                                            .build());
            FieldDescriptor payload = entry.findFieldByName("payload");
            if (payload != null) {
                value.setField(payload, "v".repeat(payloadSize));
            }
            FieldDescriptor added = entry.findFieldByName("added");
            if (added != null) {
                value.setField(added, "added-" + ordinal);
            }
            FieldDescriptor detail = entry.findFieldByName("detail");
            if (detail != null) {
                // Each enum variant writes its last declared value.
                FieldDescriptor kind = detail.getMessageType().findFieldByName("kind");
                List<EnumValueDescriptor> kinds = kind.getEnumType().getValues();
                value.setField(
                        detail,
                        DynamicMessage.newBuilder(detail.getMessageType())
                                .setField(kind, kinds.get(kinds.size() - 1))
                                .build());
            }
            FieldDescriptor child = entry.findFieldByName("child");
            DynamicMessage nested =
                    DynamicMessage.newBuilder(entry)
                            .setField(entry.findFieldByName("count"), 7)
                            .build();
            for (int i = 1; i < nesting; i++) {
                nested = DynamicMessage.newBuilder(entry).setField(child, nested).build();
            }
            return value.setField(child, nested).build();
        }
    }

    public static final class StatefulValues
            extends KeyedProcessFunction<Integer, DynamicMessage, String>
            implements CheckpointedFunction, CheckpointListener {
        private final String directory;
        private final String kind;
        private final String backend;
        private final ProtobufTypeInformation<DynamicMessage> info;
        private transient ValueState<DynamicMessage> keyed;
        private transient MapState<String, DynamicMessage> map;
        private transient ListState<DynamicMessage> operator;
        private transient List<DynamicMessage> entries;

        StatefulValues(
                String directory,
                String backend,
                String kind,
                ProtobufTypeInformation<DynamicMessage> info) {
            this.directory = directory;
            this.kind = kind;
            this.backend = backend;
            this.info = info;
        }

        private boolean enabled(String stateKind) {
            return kind.equals("all") || kind.equals(stateKind);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            var serializer = info.createSerializer(new ExecutionConfig().getSerializerConfig());
            if (!serializer
                            .getClass()
                            .getName()
                            .equals("io.github.flink.gcp.protobuf.ProtobufTypeSerializer")
                    || !serializer
                            .snapshotConfiguration()
                            .getClass()
                            .getName()
                            .equals(
                                    "io.github.flink.gcp.protobuf.ProtobufDynamicSerializerSnapshot")) {
                throw new IllegalStateException(
                        "Production serializer and dynamic snapshot were not selected");
            }
            entries = new ArrayList<>();
            if (enabled("operator")) {
                operator =
                        context.getOperatorStateStore()
                                .getListState(new ListStateDescriptor<>("operator-values", info));
                for (DynamicMessage entry : operator.get()) {
                    checkValue(entry);
                    entries.add(entry);
                }
            }
            StringBuilder restored =
                    new StringBuilder(Boolean.toString(context.isRestored())).append('\n');
            for (DynamicMessage entry : entries) {
                restored.append(encode(entry)).append(',').append(schema(entry)).append('\n');
            }
            write(Path.of(directory, "restore-" + identity()), restored.toString());
        }

        @Override
        public void open(OpenContext context) throws Exception {
            if (enabled("value")) {
                keyed =
                        getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("keyed-values", info));
                String expected =
                        backend.equals("rocksdb") ? "RocksDBValueState" : "HeapValueState";
                if (!keyed.getClass().getSimpleName().equals(expected)) {
                    throw new IllegalStateException(
                            "Unexpected state backend: " + keyed.getClass().getName());
                }
            }
            if (enabled("map")) {
                map =
                        getRuntimeContext()
                                .getMapState(
                                        new MapStateDescriptor<>("map-values", Types.STRING, info));
            }
        }

        private String identity() {
            var task = getRuntimeContext().getTaskInfo();
            return task.getAttemptNumber() + "-" + task.getIndexOfThisSubtask();
        }

        @Override
        public void processElement(DynamicMessage entry, Context context, Collector<String> output)
                throws Exception {
            checkValue(entry);
            DynamicMessage previous = keyed == null ? null : keyed.value();
            int ordinal = (Integer) entry.getField(field(entry, "ordinal"));
            String userKey = "bucket-" + ((ordinal / 4) % 2);
            DynamicMessage previousMap = map == null ? null : map.get(userKey);
            checkValue(previous);
            checkValue(previousMap);
            int count =
                    previous == null ? 0 : (Integer) previous.getField(field(previous, "count"));
            int mapCount =
                    previousMap == null
                            ? 0
                            : (Integer) previousMap.getField(field(previousMap, "count"));
            FieldDescriptor counter = field(entry, "count");
            if (keyed != null) {
                keyed.update(entry.toBuilder().setField(counter, count + 1).build());
            }
            if (map != null) {
                map.put(userKey, entry.toBuilder().setField(counter, mapCount + 1).build());
            }
            if (operator != null) {
                entries.add(entry);
            }
            String result =
                    count
                            + ","
                            + mapCount
                            + ","
                            + entries.size()
                            + ","
                            + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            write(
                    Path.of(
                            directory,
                            "value-"
                                    + getRuntimeContext().getTaskInfo().getAttemptNumber()
                                    + "-"
                                    + ordinal),
                    String.join(
                            ",",
                            result,
                            encode(previous),
                            encode(previousMap),
                            schema(previous),
                            schema(previousMap)));
            output.collect(result);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (operator != null) {
                operator.update(entries);
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            write(Path.of(directory, "checkpoint-" + checkpointId + "-" + identity()), "complete");
        }
    }
}
