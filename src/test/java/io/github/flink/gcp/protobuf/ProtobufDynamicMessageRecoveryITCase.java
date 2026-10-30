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

import org.apache.flink.util.ExceptionUtils;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

import static io.github.flink.gcp.protobuf.RuntimeRecoveryHarness.BASELINE;
import static io.github.flink.gcp.protobuf.RuntimeRecoveryHarness.command;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checkpoint recovery and separate-job savepoint restores of DynamicMessage state, with an
 * application whose jar contains no generated message class and loads each schema from its
 * descriptor set.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(900)
class ProtobufDynamicMessageRecoveryITCase {
    @TempDir Path root;
    private RuntimeRecoveryHarness harness;
    private SchemaEvolutionChecks checks;
    private int sequence;

    @BeforeAll
    void startCluster() throws Exception {
        harness = new RuntimeRecoveryHarness();
        checks = new SchemaEvolutionChecks(this::dynamic, this::directory);
    }

    @AfterAll
    void stopCluster() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    private Path directory() {
        return root.resolve("job-" + sequence++);
    }

    private RuntimeRecoveryHarness.Job dynamic(
            Path directory,
            String backend,
            String kind,
            RuntimeRecoveryHarness.Settings settings,
            String saved,
            String variant)
            throws Exception {
        return harness.startDynamic(
                directory, backend, kind, settings, saved, variant, "dynamic", false);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void completedCheckpointRestoresAllStateAndReplaysUnsavedUpdates(String backend)
            throws Exception {
        try (var job =
                harness.startDynamic(
                        directory(), backend, "all", BASELINE, null, "original", "dynamic", true)) {
            job.process(0, 8, 0);
            job.checkpoint();
            job.process(8, 12, 0);
            command(job.directory, "fail", "once");
            job.assertRestored(1, 8);
            job.process(8, 16, 1);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void unchangedSchemaRestoresSavedValuesAndContinuesProcessing(String backend) throws Exception {
        Descriptor original = RuntimeRecoveryHarness.descriptor("original");
        evolve(
                backend,
                "original",
                "original",
                original,
                value -> value.getField(original.findFieldByName("payload")),
                ordinal -> "v".repeat(8));
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void fieldAdditionRestoresSavedValuesAndContinuesProcessing(String backend) throws Exception {
        Descriptor changed = RuntimeRecoveryHarness.descriptor("changed");
        evolve(
                backend,
                "original",
                "changed",
                changed,
                value -> value.getField(changed.findFieldByName("added")),
                ordinal -> ordinal < 8 ? "" : "added-" + ordinal);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void reservedFieldRemovalPreservesTheRemovedValueAsUnknownField(String backend)
            throws Exception {
        // The original descriptor reads back the payload that the reserved schema kept as unknown.
        Descriptor original = RuntimeRecoveryHarness.descriptor("original");
        evolve(
                backend,
                "original",
                "reserved",
                original,
                value -> value.getField(original.findFieldByName("payload")),
                ordinal -> ordinal < 8 ? "v".repeat(8) : "");
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void enumAdditionInANestedTypeRestoresSavedValuesAndContinuesProcessing(String backend)
            throws Exception {
        Descriptor added = RuntimeRecoveryHarness.descriptor("enum-added");
        FieldDescriptor detail = added.findFieldByName("detail");
        FieldDescriptor kind = detail.getMessageType().findFieldByName("kind");
        evolve(
                backend,
                "enum-original",
                "enum-added",
                added,
                value -> {
                    Message nested = (Message) value.getField(detail);
                    return ((EnumValueDescriptor) nested.getField(kind)).getName();
                },
                ordinal -> ordinal < 8 ? "KIND_FIRST" : "KIND_SECOND");
    }

    /**
     * Each unsupported change fails the restore, while the writer's own schema restores the same
     * state.
     */
    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void unsafeChangesFailRestoreWhileTheWriterSchemaRestoresTheSameState(String backend)
            throws Exception {
        for (String kind : List.of("value", "map", "operator")) {
            String saved = saved(backend, kind, "original");
            rejected(backend, kind, saved, "incompatible");
            accepted(backend, kind, saved, "original");
        }
        String changed = saved(backend, "all", "changed");
        rejected(backend, "all", changed, "original");
        accepted(backend, "all", changed, "changed");
        String enumAdded = saved(backend, "all", "enum-added");
        rejected(backend, "all", enumAdded, "enum-original");
        accepted(backend, "all", enumAdded, "enum-added");
    }

    /**
     * Saved state of one kind does not restore into the other kind, with or without the generated
     * class in the DynamicMessage application.
     */
    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void savedStateOfTheOtherKindFailsRestore(String backend) throws Exception {
        String generated;
        try (var job =
                harness.start(directory(), backend, "all", BASELINE, null, "original", false)) {
            job.process(0, 8, 0);
            generated = job.savepoint(directory());
        }
        try (var job =
                harness.startDynamic(
                        directory(),
                        backend,
                        "all",
                        BASELINE,
                        generated,
                        "original",
                        "dynamic",
                        false)) {
            assertThat(job.failure())
                    .as("Generated state restored without the generated class")
                    .hasStackTraceContaining(
                            "Cannot restore Protobuf serializer for "
                                    + RuntimeRecoveryHarness.MESSAGE)
                    .hasStackTraceContaining("ClassNotFoundException");
        }
        try (var job =
                harness.startDynamic(
                        directory(),
                        backend,
                        "all",
                        BASELINE,
                        generated,
                        "original",
                        "original",
                        false)) {
            Throwable failure = job.failure();
            assertIncompatible(failure, "Generated state restored with the generated class");
            assertThat(ExceptionUtils.stringifyException(failure))
                    .as("The generated class loads from the application jar")
                    .doesNotContain("ClassNotFoundException");
        }
        String dynamic = saved(backend, "all", "original");
        try (var job =
                harness.start(directory(), backend, "all", BASELINE, dynamic, "original", false)) {
            assertIncompatible(job.failure(), "DynamicMessage state restored as generated");
        }
    }

    /**
     * Writes state with one schema, restores it twice with another, and checks the descriptors that
     * restored values carry.
     */
    private void evolve(
            String backend,
            String writer,
            String reader,
            Descriptor decoder,
            Function<DynamicMessage, Object> observed,
            Function<Integer, Object> expected)
            throws Exception {
        checks.restoreTwice(
                backend,
                BASELINE,
                saved(backend, "all", writer),
                8,
                reader,
                decoder,
                observed,
                expected,
                (job, restore, first, end) -> {
                    // Operator state and heap keyed state yield values that the restored
                    // serializer read; RocksDB reads values with the application's serializer.
                    // Values that the first restore leaves untouched, such as half of the MapState
                    // values, carry the new descriptor after the second restore.
                    String saved = restore == 1 ? writer : reader;
                    String keyed = backend.equals("hashmap") ? saved : reader;
                    assertThat(job.restoredSchemas(0))
                            .as("Descriptors of restored operator values")
                            .hasSize(first)
                            .containsOnly(saved);
                    for (int input = first; input < end; input++) {
                        assertThat(job.replacedSchemas(0, input))
                                .as("Descriptors of the values that input %s replaced", input)
                                .containsExactly(keyed, keyed);
                    }
                });
    }

    private String saved(String backend, String kind, String variant) throws Exception {
        try (var job = dynamic(directory(), backend, kind, BASELINE, null, variant)) {
            job.process(0, 8, 0);
            return job.savepoint(directory());
        }
    }

    private void accepted(String backend, String kind, String savepoint, String variant)
            throws Exception {
        try (var job = dynamic(directory(), backend, kind, BASELINE, savepoint, variant)) {
            job.assertRestored(0, 8);
            job.process(8, 12, 0);
        }
    }

    private void rejected(String backend, String kind, String savepoint, String variant)
            throws Exception {
        try (var job = dynamic(directory(), backend, kind, BASELINE, savepoint, variant)) {
            assertIncompatible(job.failure(), kind + " state restored by " + variant);
        }
    }

    /** Flink rejects the new serializer at resolution, without a reason from the library. */
    private static void assertIncompatible(Throwable failure, String description) {
        assertThat(failure)
                .as(description)
                .hasStackTraceContaining("StateMigrationException")
                .hasStackTraceContaining("must not be incompatible");
        assertThat(ExceptionUtils.stringifyException(failure))
                .as(description)
                .doesNotContain("cannot read the saved Protobuf schema");
    }
}
