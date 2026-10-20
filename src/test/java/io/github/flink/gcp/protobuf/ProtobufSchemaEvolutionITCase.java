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

/**
 * Separate-job restores of saved state into applications that contain only the new generated
 * classes.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(600)
class ProtobufSchemaEvolutionITCase {
    private static final String ENTRY = "flink.protobuf.runtime.Entry";

    @TempDir Path root;
    private RuntimeRecoveryHarness harness;
    private SchemaEvolutionChecks checks;
    private int sequence;

    @BeforeAll
    void startCluster() throws Exception {
        harness = new RuntimeRecoveryHarness();
        checks = new SchemaEvolutionChecks(harness, this::directory);
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

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void unsafeChangesFailRestoreWithTheFirstIncompatibility(String backend) throws Exception {
        for (String kind : List.of("value", "map", "operator")) {
            rejected(
                    backend,
                    kind,
                    saved(backend, kind, "original"),
                    "incompatible",
                    "Field 2 (count) of " + ENTRY + " changes type from INT32 to SINT32");
        }
        rejected(
                backend,
                "all",
                saved(backend, "all", "changed"),
                "original",
                "Field 5 (added) of " + ENTRY + " is removed without reserving its number");
        rejected(
                backend,
                "all",
                saved(backend, "all", "enum-added"),
                "enum-original",
                "Enum " + ENTRY + ".Kind removes or renames value KIND_SECOND");
    }

    /** Writes state with one schema and restores it twice with another. */
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
                reader,
                decoder,
                observed,
                expected);
    }

    private String saved(String backend, String kind, String variant) throws Exception {
        try (var job = harness.start(directory(), backend, kind, BASELINE, null, variant, false)) {
            job.process(0, 8, 0);
            return job.savepoint(directory());
        }
    }

    private void rejected(
            String backend, String kind, String savepoint, String variant, String reason)
            throws Exception {
        checks.rejected(backend, kind, BASELINE, savepoint, variant, reason);
    }
}
