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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static io.github.flink.gcp.protobuf.RuntimeRecoveryHarness.BASELINE;
import static io.github.flink.gcp.protobuf.RuntimeRecoveryHarness.RAISED;
import static io.github.flink.gcp.protobuf.RuntimeRecoveryHarness.command;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(600)
class ProtobufRecoveryITCase {
    @TempDir Path root;
    private RuntimeRecoveryHarness harness;
    private int sequence;

    @BeforeAll
    void startCluster() throws Exception {
        harness = new RuntimeRecoveryHarness();
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

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void completedCheckpointRestoresAllStateAndReplaysUnsavedUpdates(String backend)
            throws Exception {
        try (var job = harness.start(directory(), backend, "all", BASELINE, null, false, true)) {
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
    void separateJobsRestoreUnchangedAndIncreasedSettingsInBothDeterministicDirections(
            String backend) throws Exception {
        String original = saved(backend, "all", BASELINE);
        var transitions =
                List.of(
                        BASELINE,
                        new RuntimeRecoveryHarness.Settings(false, 2048, 4),
                        new RuntimeRecoveryHarness.Settings(false, 1024, 8),
                        new RuntimeRecoveryHarness.Settings(false, 2048, 8),
                        RAISED);
        for (var settings : transitions) {
            String next;
            try (var job =
                    harness.start(directory(), backend, "all", settings, original, false, false)) {
                job.assertRestored(0, 8);
                if (settings.size() > BASELINE.size()) {
                    command(job.directory, "large", "true");
                }
                if (settings.depth() > BASELINE.depth()) {
                    command(job.directory, "deep", "true");
                }
                job.process(8, 12, 0);
                next = job.savepoint(directory());
            }
            try (var restored =
                    harness.start(directory(), backend, "all", settings, next, false, false)) {
                restored.assertRestored(0, 12);
                restored.process(12, 20, 0);
            }
        }
        String deterministic =
                saved(backend, "all", new RuntimeRecoveryHarness.Settings(true, 1024, 4));
        try (var job =
                harness.start(directory(), backend, "all", BASELINE, deterministic, false, false)) {
            job.assertRestored(0, 8);
            job.process(8, 12, 0);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void eachStateKindRejectsDecreasedLimitsAndChangedSchema(String backend) throws Exception {
        for (String kind : List.of("value", "map", "operator")) {
            String original = saved(backend, kind, BASELINE);
            for (var reduced :
                    List.of(
                            new RuntimeRecoveryHarness.Settings(false, 512, 4),
                            new RuntimeRecoveryHarness.Settings(false, 1024, 3),
                            new RuntimeRecoveryHarness.Settings(true, 512, 8),
                            new RuntimeRecoveryHarness.Settings(true, 2048, 3))) {
                rejected(backend, kind, reduced, original, false);
            }
            rejected(backend, kind, BASELINE, original, true);
            String raised;
            try (var job =
                    harness.start(directory(), backend, kind, RAISED, original, false, false)) {
                job.assertRestored(0, 8);
                job.process(8, 12, 0);
                raised = job.savepoint(directory());
            }
            rejected(
                    backend,
                    kind,
                    new RuntimeRecoveryHarness.Settings(true, 1024, 8),
                    raised,
                    false);
            rejected(
                    backend,
                    kind,
                    new RuntimeRecoveryHarness.Settings(true, 2048, 4),
                    raised,
                    false);
        }
    }

    private String saved(String backend, String kind, RuntimeRecoveryHarness.Settings settings)
            throws Exception {
        try (var job = harness.start(directory(), backend, kind, settings, null, false, false)) {
            job.process(0, 8, 0);
            return job.savepoint(directory());
        }
    }

    private void rejected(
            String backend,
            String kind,
            RuntimeRecoveryHarness.Settings settings,
            String savepoint,
            boolean changed)
            throws Exception {
        try (var job =
                harness.start(directory(), backend, kind, settings, savepoint, changed, false)) {
            Throwable failure = job.failure();
            List<String> causes = new ArrayList<>();
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                causes.add(cause.getClass().getName() + ": " + cause.getMessage());
            }
            assertThat(String.join("\n", causes))
                    .containsAnyOf("StateMigrationException", "different Protobuf schema");
            assertThat(String.join("\n", causes))
                    .containsAnyOf("incompatible", "different Protobuf schema");
            if (changed) {
                assertThat(String.join("\n", causes)).contains(RuntimeRecoveryHarness.MESSAGE);
            }
        }
    }
}
