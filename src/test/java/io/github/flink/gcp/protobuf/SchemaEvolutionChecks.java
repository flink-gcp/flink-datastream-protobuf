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
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Restore checks shared by the same-build and retained-baseline schema-evolution tests.
 *
 * <p>Saved state holds the eight inputs {@code 0} to {@code 7}, as both test families write it.
 */
final class SchemaEvolutionChecks {
    private final RuntimeRecoveryHarness harness;
    private final Supplier<Path> directories;

    SchemaEvolutionChecks(RuntimeRecoveryHarness harness, Supplier<Path> directories) {
        this.harness = harness;
        this.directories = directories;
    }

    /**
     * Restores saved state with another schema, continues processing, and restores the evolved
     * application's own savepoint.
     *
     * <p>Operator-state values are re-serialized by the evolved application after each restore, so
     * {@code observed} sees the bytes that the evolved schema retained.
     *
     * @param saved savepoint or retained checkpoint path written with the old schema
     * @param decoder runtime message descriptor that interprets the restored bytes
     * @param observed value of the checked field in a decoded message
     * @param expected expected value by input ordinal
     */
    void restoreTwice(
            String backend,
            RuntimeRecoveryHarness.Settings settings,
            String saved,
            String reader,
            Descriptor decoder,
            Function<DynamicMessage, Object> observed,
            Function<Integer, Object> expected)
            throws Exception {
        String next;
        try (var job =
                harness.start(directories.get(), backend, "all", settings, saved, reader, false)) {
            job.assertRestored(0, 8);
            assertValues(job.restored(0, decoder), decoder, observed, expected);
            job.process(8, 12, 0);
            assertReplaced(job, 8, 12, decoder, observed, expected);
            next = job.savepoint(directories.get());
        }
        try (var job =
                harness.start(directories.get(), backend, "all", settings, next, reader, false)) {
            job.assertRestored(0, 12);
            assertValues(job.restored(0, decoder), decoder, observed, expected);
            // Inputs 16 to 19 replace the MapState values that the evolved application wrote.
            job.process(12, 20, 0);
            assertReplaced(job, 12, 20, decoder, observed, expected);
        }
    }

    /**
     * Requires a restore to fail and name the generated class and the first incompatibility.
     *
     * @param reason expected evaluator diagnostic
     */
    void rejected(
            String backend,
            String kind,
            RuntimeRecoveryHarness.Settings settings,
            String saved,
            String variant,
            String reason)
            throws Exception {
        try (var job =
                harness.start(directories.get(), backend, kind, settings, saved, variant, false)) {
            // The job must end unsuccessfully rather than run without the saved state.
            assertThat(job.failure())
                    .as("%s state restored by %s", kind, variant)
                    .hasStackTraceContaining(RuntimeRecoveryHarness.MESSAGE)
                    .hasStackTraceContaining("cannot read the saved Protobuf schema: " + reason);
        }
    }

    /**
     * Checks the restored or newly written ValueState and MapState values that later inputs
     * replaced.
     *
     * <p>Stream keys repeat every four inputs, and each key alternates between two MapState user
     * keys, so input {@code n} replaces the ValueState value of input {@code n - 4} and the
     * MapState value of input {@code n - 8}.
     */
    private static void assertReplaced(
            RuntimeRecoveryHarness.Job job,
            int first,
            int end,
            Descriptor decoder,
            Function<DynamicMessage, Object> observed,
            Function<Integer, Object> expected)
            throws Exception {
        FieldDescriptor ordinal = decoder.findFieldByName("ordinal");
        for (int input = first; input < end; input++) {
            List<DynamicMessage> replaced = job.replaced(0, input, decoder);
            assertThat(replaced)
                    .extracting(value -> value.getField(ordinal))
                    .as("Ordinals of the values that input %s replaced", input)
                    .containsExactly(input - 4, input - 8);
            assertValues(replaced, decoder, observed, expected);
        }
    }

    private static void assertValues(
            List<DynamicMessage> values,
            Descriptor decoder,
            Function<DynamicMessage, Object> observed,
            Function<Integer, Object> expected) {
        var ordinal = decoder.findFieldByName("ordinal");
        for (DynamicMessage value : values) {
            int number = (Integer) value.getField(ordinal);
            assertThat(observed.apply(value))
                    .as("Restored value %s", number)
                    .isEqualTo(expected.apply(number));
        }
    }
}
