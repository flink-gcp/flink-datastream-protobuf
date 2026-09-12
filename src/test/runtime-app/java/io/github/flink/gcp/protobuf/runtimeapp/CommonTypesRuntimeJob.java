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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
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

import com.google.protobuf.Struct;
import io.github.flink.gcp.protobuf.CommonTypeValues;
import io.github.flink.gcp.protobuf.ProtobufTypeInformation;
import io.github.flink.gcp.protobuf.generated.CommonTypesEnvelope;
import io.opentelemetry.proto.common.v1.AnyValue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** Recovery job using WKT runtime classes and common types on the test classpath. */
public final class CommonTypesRuntimeJob {
    private CommonTypesRuntimeJob() {}

    public static JobGraph create(String directory, String backend, boolean restart)
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
        env.fromSource(
                        new ControlledSource(directory),
                        WatermarkStrategy.noWatermarks(),
                        "input",
                        Types.INT)
                .setParallelism(1)
                .uid("input")
                .map(CommonTypeValues::envelope)
                .returns(ProtobufTypeInformation.of(CommonTypesEnvelope.class))
                .uid("messages")
                .rebalance()
                .keyBy(v -> v.getOrdinal() % 4, Types.INT)
                .process(new StatefulValues(directory, backend))
                .returns(Types.STRING)
                .uid("common-values")
                .print()
                .uid("output");
        return env.getStreamGraph()
                .getJobGraph(CommonTypesRuntimeJob.class.getClassLoader(), new JobID());
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

    public static final class StatefulValues
            extends KeyedProcessFunction<Integer, CommonTypesEnvelope, String>
            implements CheckpointedFunction {
        private final String directory;
        private final String backend;
        private transient ValueState<Struct> keyed;
        private transient MapState<String, AnyValue> map;
        private transient ListState<CommonTypesEnvelope> operator;
        private transient List<CommonTypesEnvelope> entries;

        StatefulValues(String directory, String backend) {
            this.directory = directory;
            this.backend = backend;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            operator =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "common-operator",
                                            ProtobufTypeInformation.of(CommonTypesEnvelope.class)));
            entries = new ArrayList<>();
            StringBuilder restored =
                    new StringBuilder(Boolean.toString(context.isRestored())).append('\n');
            for (CommonTypesEnvelope entry : operator.get()) {
                check(entry, CommonTypeValues.envelope(entry.getOrdinal()), "operator restore");
                entries.add(entry);
                restored.append(Base64.getEncoder().encodeToString(entry.toByteArray()))
                        .append('\n');
            }
            var task = getRuntimeContext().getTaskInfo();
            write(
                    Path.of(
                            directory,
                            "restore-"
                                    + task.getAttemptNumber()
                                    + "-"
                                    + task.getIndexOfThisSubtask()),
                    restored.toString());
        }

        @Override
        public void open(OpenContext context) throws Exception {
            keyed =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "common-struct",
                                            ProtobufTypeInformation.of(Struct.class)));
            map =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "common-otel",
                                            Types.STRING,
                                            ProtobufTypeInformation.of(AnyValue.class)));
            String expected = backend.equals("rocksdb") ? "RocksDBValueState" : "HeapValueState";
            if (!keyed.getClass().getSimpleName().equals(expected)) {
                throw new IllegalStateException(
                        "Unexpected backend: " + keyed.getClass().getName());
            }
        }

        @Override
        public void processElement(
                CommonTypesEnvelope entry, Context context, Collector<String> output)
                throws Exception {
            int ordinal = entry.getOrdinal();
            check(entry, CommonTypeValues.envelope(ordinal), "transport");
            check(
                    keyed.value(),
                    ordinal < 4 ? null : CommonTypeValues.struct(ordinal - 4),
                    "ValueState");
            String userKey = "bucket-" + ((ordinal / 4) % 2);
            check(
                    map.get(userKey),
                    ordinal < 8 ? null : CommonTypeValues.otel(ordinal - 8),
                    "MapState");
            keyed.update(entry.getStructValue());
            map.put(userKey, entry.getOtelValue());
            entries.add(entry);
            var task = getRuntimeContext().getTaskInfo();
            String result = "verified," + entries.size() + "," + task.getIndexOfThisSubtask();
            write(Path.of(directory, "value-" + task.getAttemptNumber() + "-" + ordinal), result);
            output.collect(result);
        }

        private static void check(Object actual, Object expected, String state) {
            if (!java.util.Objects.equals(actual, expected)) {
                throw new IllegalStateException(
                        "Changed " + state + ": expected " + expected + " but got " + actual);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            operator.update(entries);
        }
    }
}
