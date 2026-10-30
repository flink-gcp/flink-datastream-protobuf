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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.protobuf.generated.CommonTypesEnvelope;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real job execution for isolated-schema and shared-classpath common-type recovery tests. */
final class RuntimeRecoveryHarness implements AutoCloseable {
    static final String APPLICATION = "io.github.flink.gcp.protobuf.runtimeapp.RuntimeJob";
    private static final String DYNAMIC_APPLICATION =
            "io.github.flink.gcp.protobuf.runtimeapp.DynamicRuntimeJob";
    static final String MESSAGE =
            "io.github.flink.gcp.protobuf.runtimeapp.generated.RuntimeMessages$Entry";
    private static final List<String> VARIANTS =
            List.of(
                    "original",
                    "changed",
                    "reserved",
                    "incompatible",
                    "enum-original",
                    "enum-added");
    private static final Map<DescriptorProto, String> ROOTS = new HashMap<>();
    static final Settings BASELINE = new Settings(false, 1024, 4);
    static final Settings RAISED = new Settings(true, 2048, 8);
    private final MiniCluster cluster;

    RuntimeRecoveryHarness() throws Exception {
        Configuration config = new Configuration();
        config.set(RestOptions.BIND_PORT, "0");
        config.set(RestOptions.PORT, 0);
        cluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumTaskManagers(2)
                                .setNumSlotsPerTaskManager(2)
                                .build());
        try {
            cluster.start();
        } catch (Exception failure) {
            try {
                close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    Job start(
            Path directory,
            String backend,
            String kind,
            Settings settings,
            String savepoint,
            String variant,
            boolean restart)
            throws Exception {
        return submit(
                directory, variant, APPLICATION, backend, kind, settings, savepoint, restart, null);
    }

    /**
     * Starts the DynamicMessage application with the descriptor of a schema variant.
     *
     * @param variant schema variant whose descriptor set the application loads
     * @param jar application jar variant: {@code dynamic} has no generated class, while a schema
     *     variant's jar also contains that variant's generated class
     */
    Job startDynamic(
            Path directory,
            String backend,
            String kind,
            Settings settings,
            String savepoint,
            String variant,
            String jar,
            boolean restart)
            throws Exception {
        return submit(
                directory,
                jar,
                DYNAMIC_APPLICATION,
                backend,
                kind,
                settings,
                savepoint,
                restart,
                apps().resolve(variant + "/schema.pb").toString());
    }

    /**
     * @param schema descriptor set for the DynamicMessage application, or {@code null} for the
     *     generated one
     */
    private Job submit(
            Path directory,
            String jarVariant,
            String applicationName,
            String backend,
            String kind,
            Settings settings,
            String savepoint,
            boolean restart,
            String schema)
            throws Exception {
        Files.createDirectories(directory);
        ClassLoader parent = getClass().getClassLoader();
        for (String name : List.of(APPLICATION, DYNAMIC_APPLICATION, MESSAGE)) {
            assertThatThrownBy(() -> Class.forName(name, false, parent))
                    .isInstanceOf(ClassNotFoundException.class);
        }
        Path jar = apps().resolve("application-" + jarVariant + ".jar");
        assertThat(jar).isRegularFile();
        try (URLClassLoader loader = new URLClassLoader(new URL[] {jar.toUri().toURL()}, parent)) {
            Class<?> application = loader.loadClass(applicationName);
            assertThat(application.getClassLoader()).isSameAs(loader);
            if (jarVariant.equals("dynamic")) {
                assertThatThrownBy(() -> loader.loadClass(MESSAGE))
                        .isInstanceOf(ClassNotFoundException.class);
            } else {
                assertThat(loader.loadClass(MESSAGE).getClassLoader()).isSameAs(loader);
            }
            List<Class<?>> parameters =
                    new ArrayList<>(
                            List.of(
                                    String.class,
                                    String.class,
                                    String.class,
                                    boolean.class,
                                    int.class,
                                    int.class,
                                    boolean.class));
            List<Object> arguments =
                    new ArrayList<>(
                            List.of(
                                    directory.toString(),
                                    backend,
                                    kind,
                                    settings.deterministic,
                                    settings.size,
                                    settings.depth,
                                    restart));
            if (schema != null) {
                parameters.add(String.class);
                arguments.add(schema);
            }
            JobGraph graph =
                    (JobGraph)
                            application
                                    .getMethod("create", parameters.toArray(new Class<?>[0]))
                                    .invoke(null, arguments.toArray());
            graph.addJar(new org.apache.flink.core.fs.Path(jar.toUri()));
            if (savepoint != null) {
                graph.setSavepointRestoreSettings(
                        SavepointRestoreSettings.forPath(savepoint, false));
            }
            cluster.submitJob(graph).get(60, TimeUnit.SECONDS);
            return new Job(directory, graph, kind);
        }
    }

    /** Builds the runtime message descriptor of a variant without loading its generated class. */
    static Descriptors.Descriptor descriptor(String variant) throws Exception {
        FileDescriptorSet schema =
                FileDescriptorSet.parseFrom(
                        Files.readAllBytes(apps().resolve(variant + "/schema.pb")));
        return Descriptors.FileDescriptor.buildFrom(
                        schema.getFile(0), new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("Entry");
    }

    static Path apps() {
        String directory = System.getProperty("protobuf.test.runtime.apps");
        assertThat(directory)
                .as(
                        "Run Maven process-test-classes to build and locate the isolated application jars")
                .isNotBlank();
        return Path.of(directory).toAbsolutePath();
    }

    Job startCommonTypes(Path directory, String backend, String savepoint, boolean restart)
            throws Exception {
        Files.createDirectories(directory);
        Path jar = apps().resolve("application-original.jar");
        assertThat(jar).isRegularFile();
        ClassLoader parent = getClass().getClassLoader();
        String name = "io.github.flink.gcp.protobuf.runtimeapp.CommonTypesRuntimeJob";
        assertThatThrownBy(() -> Class.forName(name, false, parent))
                .isInstanceOf(ClassNotFoundException.class);
        try (URLClassLoader loader = new URLClassLoader(new URL[] {jar.toUri().toURL()}, parent)) {
            Class<?> application = loader.loadClass(name);
            assertThat(application.getClassLoader()).isSameAs(loader);
            JobGraph graph =
                    (JobGraph)
                            application
                                    .getMethod("create", String.class, String.class, boolean.class)
                                    .invoke(null, directory.toString(), backend, restart);
            graph.addJar(new org.apache.flink.core.fs.Path(jar.toUri()));
            if (savepoint != null) {
                graph.setSavepointRestoreSettings(
                        SavepointRestoreSettings.forPath(savepoint, false));
            }
            cluster.submitJob(graph).get(60, TimeUnit.SECONDS);
            return new Job(directory, graph, "all");
        }
    }

    static void command(Path directory, String name, String value) throws Exception {
        Path temporary = directory.resolve(name + ".pending");
        Files.writeString(temporary, value);
        Files.move(
                temporary,
                directory.resolve(name),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    final class Job implements AutoCloseable {
        final Path directory;
        final JobGraph graph;
        final String kind;

        Job(Path directory, JobGraph graph, String kind) {
            this.directory = directory;
            this.graph = graph;
            this.kind = kind;
        }

        void process(int first, int end, int attempt) throws Exception {
            command(directory, "limit", Integer.toString(end));
            await(() -> Files.exists(directory.resolve("value-" + attempt + "-" + (end - 1))));
            for (int ordinal = first; ordinal < end; ordinal++) {
                Path result = directory.resolve("value-" + attempt + "-" + ordinal);
                await(() -> Files.exists(result));
                String[] fields = Files.readString(result).split(",");
                assertThat(Integer.parseInt(fields[0]))
                        .as("ValueState before input %s", ordinal)
                        .isEqualTo(enabled("value") ? ordinal / 4 : 0);
                assertThat(Integer.parseInt(fields[1]))
                        .as("MapState before input %s", ordinal)
                        .isEqualTo(enabled("map") ? ordinal / 8 : 0);
                int subtask =
                        KeyGroupRangeAssignment.assignKeyToParallelOperator(ordinal % 4, 16, 2);
                assertThat(Integer.parseInt(fields[3])).isEqualTo(subtask);
                int expectedOperatorCount = 0;
                if (enabled("operator")) {
                    for (int input = 0; input <= ordinal; input++) {
                        if (KeyGroupRangeAssignment.assignKeyToParallelOperator(input % 4, 16, 2)
                                == subtask) {
                            expectedOperatorCount++;
                        }
                    }
                }
                assertThat(Integer.parseInt(fields[2]))
                        .as("Operator state after input %s", ordinal)
                        .isEqualTo(expectedOperatorCount);
            }
        }

        private boolean enabled(String stateKind) {
            return kind.equals("all") || kind.equals(stateKind);
        }

        void processCommonTypes(int first, int end, int attempt) throws Exception {
            command(directory, "limit", Integer.toString(end));
            for (int ordinal = first; ordinal < end; ordinal++) {
                Path result = directory.resolve("value-" + attempt + "-" + ordinal);
                await(() -> Files.exists(result));
                int subtask =
                        KeyGroupRangeAssignment.assignKeyToParallelOperator(ordinal % 4, 16, 2);
                int expectedCount = 0;
                for (int input = 0; input <= ordinal; input++) {
                    if (KeyGroupRangeAssignment.assignKeyToParallelOperator(input % 4, 16, 2)
                            == subtask) {
                        expectedCount++;
                    }
                }
                assertThat(Files.readString(result))
                        .as("Verified keyed values and operator count at input %s", ordinal)
                        .isEqualTo("verified," + expectedCount + "," + subtask);
            }
        }

        void assertRestored(int attempt, int expectedCount) throws Exception {
            List<Integer> restored = new ArrayList<>();
            Descriptors.Descriptor descriptor = descriptor("original");
            for (DynamicMessage message : restored(attempt, descriptor)) {
                restored.add((Integer) message.getField(descriptor.findFieldByName("ordinal")));
                assertThat(message.getUnknownFields().getField(100).getVarintList())
                        .containsExactly(99L);
                assertThat(message.hasField(descriptor.findFieldByName("child"))).isTrue();
                assertThat(message.getField(descriptor.findFieldByName("payload")))
                        .isInstanceOf(String.class);
            }
            assertThat(restored)
                    .containsExactlyInAnyOrderElementsOf(
                            enabled("operator")
                                    ? java.util.stream.IntStream.range(0, expectedCount)
                                            .boxed()
                                            .toList()
                                    : List.of());
        }

        /**
         * Decodes the operator-state values that each subtask re-serialized after restoring.
         *
         * @param descriptor runtime message descriptor that interprets the bytes
         */
        List<DynamicMessage> restored(int attempt, Descriptors.Descriptor descriptor)
                throws Exception {
            List<DynamicMessage> messages = new ArrayList<>();
            for (int subtask = 0; subtask < 2; subtask++) {
                Path observation = directory.resolve("restore-" + attempt + "-" + subtask);
                await(() -> Files.exists(observation));
                List<String> lines = Files.readAllLines(observation);
                assertThat(lines.get(0)).isEqualTo("true");
                for (String line : lines.subList(1, lines.size())) {
                    // The DynamicMessage application appends the descriptor of the value.
                    String value = line.split(",", -1)[0];
                    messages.add(
                            DynamicMessage.parseFrom(
                                    descriptor, Base64.getDecoder().decode(value)));
                }
            }
            return messages;
        }

        /**
         * Decodes the ValueState and MapState values that an input replaced, ValueState first.
         *
         * @param descriptor runtime message descriptor that interprets the bytes
         * @return the replaced values that existed
         */
        List<DynamicMessage> replaced(int attempt, int ordinal, Descriptors.Descriptor descriptor)
                throws Exception {
            String[] fields =
                    Files.readString(directory.resolve("value-" + attempt + "-" + ordinal))
                            .split(",", -1);
            List<DynamicMessage> values = new ArrayList<>();
            for (String value : List.of(fields[4], fields[5])) {
                if (!value.isEmpty()) {
                    values.add(
                            DynamicMessage.parseFrom(
                                    descriptor, Base64.getDecoder().decode(value)));
                }
            }
            return values;
        }

        /**
         * Names the schema variants whose root descriptors the restored operator-state values
         * carried in the DynamicMessage application.
         */
        List<String> restoredSchemas(int attempt) throws Exception {
            List<String> schemas = new ArrayList<>();
            for (int subtask = 0; subtask < 2; subtask++) {
                Path observation = directory.resolve("restore-" + attempt + "-" + subtask);
                await(() -> Files.exists(observation));
                List<String> lines = Files.readAllLines(observation);
                for (String line : lines.subList(1, lines.size())) {
                    schemas.add(variant(line.split(",", -1)[1]));
                }
            }
            return schemas;
        }

        /**
         * Names the schema variants whose root descriptors the ValueState and MapState values that
         * an input replaced carried in the DynamicMessage application, ValueState first.
         */
        List<String> replacedSchemas(int attempt, int ordinal) throws Exception {
            String[] fields =
                    Files.readString(directory.resolve("value-" + attempt + "-" + ordinal))
                            .split(",", -1);
            List<String> schemas = new ArrayList<>();
            for (String schema : List.of(fields[6], fields[7])) {
                if (!schema.isEmpty()) {
                    schemas.add(variant(schema));
                }
            }
            return schemas;
        }

        String checkpoint() throws Exception {
            String checkpoint =
                    cluster.triggerCheckpoint(graph.getJobID()).get(60, TimeUnit.SECONDS);
            assertThat(checkpoint).isNotBlank();
            return checkpoint;
        }

        void assertCommonTypesRestored(int attempt, int expectedCount) throws Exception {
            List<Integer> restored = new ArrayList<>();
            for (int subtask = 0; subtask < 2; subtask++) {
                Path observation = directory.resolve("restore-" + attempt + "-" + subtask);
                await(() -> Files.exists(observation));
                List<String> lines = Files.readAllLines(observation);
                assertThat(lines.get(0)).isEqualTo("true");
                for (String line : lines.subList(1, lines.size())) {
                    CommonTypesEnvelope value =
                            CommonTypesEnvelope.parseFrom(Base64.getDecoder().decode(line));
                    assertThat(value).isEqualTo(CommonTypeValues.envelope(value.getOrdinal()));
                    restored.add(value.getOrdinal());
                }
            }
            assertThat(restored)
                    .containsExactlyInAnyOrderElementsOf(
                            java.util.stream.IntStream.range(0, expectedCount).boxed().toList());
        }

        String savepoint(Path target) throws Exception {
            return cluster.triggerSavepoint(
                            graph.getJobID(),
                            target.toUri().toString(),
                            false,
                            SavepointFormatType.CANONICAL)
                    .get(60, TimeUnit.SECONDS);
        }

        Throwable failure() throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (!cluster.getJobStatus(graph.getJobID())
                    .get(10, TimeUnit.SECONDS)
                    .isGloballyTerminalState()) {
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError(
                            "Job is still running; its restore unexpectedly succeeded");
                }
                Thread.sleep(20);
            }
            var result = cluster.requestJobResult(graph.getJobID()).get(60, TimeUnit.SECONDS);
            assertThat(result.isSuccess()).isFalse();
            return result.getSerializedThrowable()
                    .orElseThrow()
                    .deserializeError(getClass().getClassLoader());
        }

        private void await(Condition condition) throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (!condition.ready()) {
                if (cluster.getJobStatus(graph.getJobID())
                        .get(10, TimeUnit.SECONDS)
                        .isGloballyTerminalState()) {
                    throw new AssertionError(
                            "Job ended before the expected observation", failure());
                }
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("Timed out waiting for " + directory);
                }
                Thread.sleep(20);
            }
        }

        @Override
        public void close() throws Exception {
            if (!cluster.getJobStatus(graph.getJobID())
                    .get(10, TimeUnit.SECONDS)
                    .isGloballyTerminalState()) {
                cluster.cancelJob(graph.getJobID()).get(30, TimeUnit.SECONDS);
                cluster.requestJobResult(graph.getJobID()).get(30, TimeUnit.SECONDS);
            }
        }
    }

    /** Finds the schema variant whose root descriptor has the encoded content. */
    private static synchronized String variant(String encoded) throws Exception {
        if (ROOTS.isEmpty()) {
            for (String variant : VARIANTS) {
                ROOTS.put(descriptor(variant).toProto(), variant);
            }
        }
        DescriptorProto observed = DescriptorProto.parseFrom(Base64.getDecoder().decode(encoded));
        String variant = ROOTS.get(observed);
        if (variant == null) {
            throw new AssertionError("Unknown root descriptor: " + observed);
        }
        return variant;
    }

    private interface Condition {
        boolean ready() throws Exception;
    }

    record Settings(boolean deterministic, int size, int depth) {}

    @Override
    public void close() throws Exception {
        cluster.closeAsync().get(60, TimeUnit.SECONDS);
    }
}
