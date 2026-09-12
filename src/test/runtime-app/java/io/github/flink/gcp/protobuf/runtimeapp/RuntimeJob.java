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

import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.protobuf.ProtobufTypeInformation;
import io.github.flink.gcp.protobuf.runtimeapp.generated.RuntimeMessages.Entry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** Application loaded only from an attached user jar, including all generated message classes. */
public final class RuntimeJob {
    private RuntimeJob() {}

    public static JobGraph create(
            String directory,
            String backend,
            String stateKind,
            boolean deterministic,
            int size,
            int depth,
            boolean restart)
            throws Exception {
        Configuration config = new Configuration();
        config.set(PipelineOptions.GENERIC_TYPES, false);
        config.set(StateBackendOptions.STATE_BACKEND, backend);
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
        ProtobufTypeInformation<Entry> info = information(deterministic, size, depth);
        env.fromSource(
                        new ControlledSource(directory),
                        WatermarkStrategy.noWatermarks(),
                        "input",
                        Types.INT)
                .setParallelism(1)
                .uid("input")
                .map(ordinal -> value(ordinal, 1, directory))
                .returns(info)
                .uid("messages")
                .rebalance()
                .keyBy(entry -> entry.getOrdinal() % 4, Types.INT)
                .process(
                        new StatefulValues(
                                directory, backend, stateKind, deterministic, size, depth))
                .returns(Types.STRING)
                .uid("values")
                .print()
                .uid("output");
        return env.getStreamGraph().getJobGraph(RuntimeJob.class.getClassLoader(), new JobID());
    }

    private static ProtobufTypeInformation<Entry> information(
            boolean deterministic, int size, int depth) {
        return ProtobufTypeInformation.newBuilder(Entry.class)
                .deterministicSerialization(deterministic)
                .maxMessageSize(size)
                .recursionLimit(depth)
                .build();
    }

    private static Entry value(int ordinal, int count, String directory) throws Exception {
        int payloadSize = Files.exists(Path.of(directory, "large")) ? 1500 : 8;
        int nesting = Files.exists(Path.of(directory, "deep")) ? 6 : 1;
        Entry.Builder entry =
                Entry.newBuilder()
                        .setOrdinal(ordinal)
                        .setCount(count)
                        .setPayload("v".repeat(payloadSize))
                        .setUnknownFields(
                                UnknownFieldSet.newBuilder()
                                        .addField(
                                                100,
                                                UnknownFieldSet.Field.newBuilder()
                                                        .addVarint(99)
                                                        .build())
                                        .build());
        Entry child = Entry.newBuilder().setCount(7).build();
        for (int i = 1; i < nesting; i++) {
            child = Entry.newBuilder().setChild(child).build();
        }
        return entry.setChild(child).build();
    }

    private static void checkValue(Entry entry) {
        if (entry == null) {
            return;
        }
        int length = entry.getPayload().length();
        if (entry.getClass().getClassLoader() != RuntimeJob.class.getClassLoader()
                || !entry.getUnknownFields().getField(100).getVarintList().equals(List.of(99L))
                || !entry.hasChild()
                || (length != 8 && length != 1500)
                || !entry.getPayload().equals("v".repeat(length))) {
            throw new IllegalStateException(
                    "Recovered application value or user-code classloader differs");
        }
        Entry child = entry.getChild();
        int nesting = 1;
        while (child.hasChild()) {
            if (child.getCount() != 0) {
                throw new IllegalStateException("Changed intermediate nested value");
            }
            child = child.getChild();
            nesting++;
        }
        if (child.getCount() != 7 || (nesting != 1 && nesting != 6)) {
            throw new IllegalStateException("Changed nested leaf or depth");
        }
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

    public static final class StatefulValues extends KeyedProcessFunction<Integer, Entry, String>
            implements CheckpointedFunction, CheckpointListener {
        private final String directory;
        private final String kind;
        private final String backend;
        private final boolean deterministic;
        private final int size;
        private final int depth;
        private transient ValueState<Entry> keyed;
        private transient MapState<String, Entry> map;
        private transient ListState<Entry> operator;
        private transient List<Entry> entries;

        StatefulValues(
                String directory,
                String backend,
                String kind,
                boolean deterministic,
                int size,
                int depth) {
            this.directory = directory;
            this.kind = kind;
            this.backend = backend;
            this.deterministic = deterministic;
            this.size = size;
            this.depth = depth;
        }

        private boolean enabled(String stateKind) {
            return kind.equals("all") || kind.equals(stateKind);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            var info = information(deterministic, size, depth);
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
                                    "io.github.flink.gcp.protobuf.ProtobufTypeSerializerSnapshot")) {
                throw new IllegalStateException(
                        "Production serializer and snapshot were not selected");
            }
            entries = new ArrayList<>();
            if (enabled("operator")) {
                operator =
                        context.getOperatorStateStore()
                                .getListState(new ListStateDescriptor<>("operator-values", info));
                for (Entry entry : operator.get()) {
                    checkValue(entry);
                    entries.add(entry);
                }
            }
            StringBuilder restored =
                    new StringBuilder(Boolean.toString(context.isRestored())).append('\n');
            for (Entry entry : entries) {
                restored.append(Base64.getEncoder().encodeToString(entry.toByteArray()))
                        .append('\n');
            }
            write(Path.of(directory, "restore-" + identity()), restored.toString());
        }

        @Override
        public void open(OpenContext context) throws Exception {
            var info = information(deterministic, size, depth);
            if (enabled("value")) {
                keyed =
                        getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("keyed-values", info));
                checkBackend(keyed);
            }
            if (enabled("map")) {
                map =
                        getRuntimeContext()
                                .getMapState(
                                        new MapStateDescriptor<>("map-values", Types.STRING, info));
            }
        }

        private void checkBackend(Object state) {
            String expected = backend.equals("rocksdb") ? "RocksDBValueState" : "HeapValueState";
            if (!state.getClass().getSimpleName().equals(expected)) {
                throw new IllegalStateException(
                        "Unexpected state backend: " + state.getClass().getName());
            }
        }

        private String identity() {
            var info = getRuntimeContext().getTaskInfo();
            return info.getAttemptNumber() + "-" + info.getIndexOfThisSubtask();
        }

        @Override
        public void processElement(Entry entry, Context context, Collector<String> output)
                throws Exception {
            checkValue(entry);
            Entry previous = keyed == null ? null : keyed.value();
            String userKey = "bucket-" + ((entry.getOrdinal() / 4) % 2);
            Entry previousMap = map == null ? null : map.get(userKey);
            checkValue(previous);
            checkValue(previousMap);
            int count = previous == null ? 0 : previous.getCount();
            int mapCount = previousMap == null ? 0 : previousMap.getCount();
            if (keyed != null) {
                keyed.update(entry.toBuilder().setCount(count + 1).build());
            }
            if (map != null) {
                map.put(userKey, entry.toBuilder().setCount(mapCount + 1).build());
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
                                    + entry.getOrdinal()),
                    result);
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
