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

/** Restore checks shared by the schema-evolution tests and the development bundle readers. */
final class SchemaEvolutionChecks {
    private final Launcher launcher;
    private final Supplier<Path> directories;

    SchemaEvolutionChecks(RuntimeRecoveryHarness harness, Supplier<Path> directories) {
        this(
                (directory, backend, kind, settings, saved, variant) ->
                        harness.start(directory, backend, kind, settings, saved, variant, false),
                directories);
    }

    SchemaEvolutionChecks(Launcher launcher, Supplier<Path> directories) {
        this.launcher = launcher;
        this.directories = directories;
    }

    /** Starts the application of a schema variant without restarts. */
    @FunctionalInterface
    interface Launcher {
        RuntimeRecoveryHarness.Job start(
                Path directory,
                String backend,
                String kind,
                RuntimeRecoveryHarness.Settings settings,
                String saved,
                String variant)
                throws Exception;
    }

    /** Checks a job after a restore and its processing, in addition to the value checks. */
    @FunctionalInterface
    interface Inspection {
        /**
         * @param restore {@code 1} for the restore of the saved state, {@code 2} for the restore of
         *     the evolved application's savepoint
         * @param first first input that the job processed
         * @param end input after the last one that the job processed
         */
        void check(RuntimeRecoveryHarness.Job job, int restore, int first, int end)
                throws Exception;
    }

    /**
     * Restores saved state of eight inputs with another schema, continues processing, and restores
     * the evolved application's own savepoint.
     *
     * @see #restoreTwice(String, RuntimeRecoveryHarness.Settings, String, int, String, Descriptor,
     *     Function, Function)
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
        restoreTwice(backend, settings, saved, 8, reader, decoder, observed, expected);
    }

    /**
     * Restores saved state with another schema, continues processing, and restores the evolved
     * application's own savepoint.
     *
     * <p>Operator-state values are re-serialized by the evolved application after each restore, so
     * {@code observed} sees the bytes that the evolved schema retained.
     *
     * @param saved savepoint or retained checkpoint path written with the old schema
     * @param inputs number of inputs that the saved state holds, a multiple of four from eight
     * @param decoder runtime message descriptor that interprets the restored bytes
     * @param observed value of the checked field in a decoded message
     * @param expected expected value by input ordinal
     */
    void restoreTwice(
            String backend,
            RuntimeRecoveryHarness.Settings settings,
            String saved,
            int inputs,
            String reader,
            Descriptor decoder,
            Function<DynamicMessage, Object> observed,
            Function<Integer, Object> expected)
            throws Exception {
        restoreTwice(
                backend,
                settings,
                saved,
                inputs,
                reader,
                decoder,
                observed,
                expected,
                (job, restore, first, end) -> {});
    }

    /**
     * Restores saved state with another schema, as {@link #restoreTwice(String,
     * RuntimeRecoveryHarness.Settings, String, int, String, Descriptor, Function, Function)} does,
     * and inspects each job after its processing.
     */
    void restoreTwice(
            String backend,
            RuntimeRecoveryHarness.Settings settings,
            String saved,
            int inputs,
            String reader,
            Descriptor decoder,
            Function<DynamicMessage, Object> observed,
            Function<Integer, Object> expected,
            Inspection inspection)
            throws Exception {
        String next;
        try (var job = launcher.start(directories.get(), backend, "all", settings, saved, reader)) {
            job.assertRestored(0, inputs);
            assertValues(job.restored(0, decoder), decoder, observed, expected);
            job.process(inputs, inputs + 4, 0);
            assertReplaced(job, inputs, inputs + 4, decoder, observed, expected);
            inspection.check(job, 1, inputs, inputs + 4);
            next = job.savepoint(directories.get());
        }
        try (var job = launcher.start(directories.get(), backend, "all", settings, next, reader)) {
            job.assertRestored(0, inputs + 4);
            assertValues(job.restored(0, decoder), decoder, observed, expected);
            // The last four inputs replace the MapState values that the evolved application wrote.
            job.process(inputs + 4, inputs + 12, 0);
            assertReplaced(job, inputs + 4, inputs + 12, decoder, observed, expected);
            inspection.check(job, 2, inputs + 4, inputs + 12);
        }
    }

    /**
     * Requires a restore to fail and name the generated class and the first incompatibility.
     *
     * <p>Only generated restores carry this diagnostic; a DynamicMessage restore fails with Flink's
     * incompatible-serializer exception alone.
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
        try (var job = launcher.start(directories.get(), backend, kind, settings, saved, variant)) {
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
