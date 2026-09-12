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

import static io.github.flink.gcp.protobuf.RuntimeRecoveryHarness.command;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(180)
class CommonTypesRecoveryITCase {
    @TempDir Path root;
    private RuntimeRecoveryHarness harness;

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

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void checkpointRestoresValuesAndReplaysDataVariantChanges(String backend) throws Exception {
        try (var job =
                harness.startCommonTypes(
                        root.resolve(backend + "-checkpoint"), backend, null, true)) {
            job.processCommonTypes(0, 12, 0);
            job.checkpoint();
            job.processCommonTypes(12, 20, 0);
            command(job.directory, "fail", "once");
            job.assertCommonTypesRestored(1, 12);
            job.processCommonTypes(12, 28, 1);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void separateJobsRestoreSavepointsAndContinueWithNewKeysAndVariants(String backend)
            throws Exception {
        String saved;
        try (var writer =
                harness.startCommonTypes(root.resolve(backend + "-writer"), backend, null, false)) {
            writer.processCommonTypes(0, 12, 0);
            saved = writer.savepoint(root.resolve(backend + "-savepoint"));
        }
        try (var reader =
                harness.startCommonTypes(
                        root.resolve(backend + "-reader"), backend, saved, false)) {
            reader.assertCommonTypesRestored(0, 12);
            reader.processCommonTypes(12, 28, 0);
        }
    }
}
