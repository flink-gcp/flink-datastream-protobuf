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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Restores state retained in a baseline bundle with the current library and applications that
 * contain only the current tree's generated classes.
 *
 * <p>The {@code retained-baseline} group is excluded by default; {@code just baseline-upgrade}
 * includes it and names a verified bundle in {@code protobuf.test.retained.bundle}. CI runs it on
 * the bundle that the same build captured; a bundle retained from a published release exercises the
 * forward-restore contract from that release. The bundle's dependency jars and retained library are
 * not used: Maven resolves this build's dependencies, so the test executes no retained code and
 * reads only retained data.
 */
@Tag("retained-baseline")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(600)
class RetainedBaselineUpgradeITCase {
    private static final String ENTRY = "flink.protobuf.runtime.Entry";

    @TempDir Path root;
    private Path bundle;
    private Path fixtures;
    private String format;
    private RuntimeRecoveryHarness harness;
    private SchemaEvolutionChecks checks;
    private SchemaEvolutionChecks dynamicChecks;
    private int sequence;

    @BeforeAll
    void openBundle() throws Exception {
        String selected = System.getProperty("protobuf.test.retained.bundle");
        assertThat(selected).as("Run through just baseline-upgrade with a bundle").isNotBlank();
        bundle = Path.of(selected).toRealPath();
        Properties manifest = BaselineTool.load(bundle.resolve("manifest.properties"));
        assertThat(manifest.getProperty("status"))
                .isIn(BaselineTool.DEVELOPMENT, BaselineTool.PUBLISHED);
        // Library-only controls keep the gencode/runtime pair and the Flink version fixed.
        assertThat(manifest.getProperty("protobuf.version"))
                .as("Select the Protobuf profile that wrote the bundle")
                .isEqualTo(System.getProperty("protobuf.test.protobuf.version"));
        String version = System.getProperty("protobuf.test.flink.version");
        assertThat(BaselineTool.runtimes(manifest.getProperty("line")))
                .as("Select a Flink runtime that the bundle was read with")
                .contains(version);
        format = manifest.getProperty("format.version", "");
        fixtures = bundle.resolve("fixtures/" + version);
        assertThat(fixtures).isDirectory();
        harness = new RuntimeRecoveryHarness();
        checks = new SchemaEvolutionChecks(harness, () -> root.resolve("job-" + sequence++));
        dynamicChecks =
                BaselineRuntime.dynamicChecks(harness, () -> root.resolve("job-" + sequence++));
    }

    @AfterAll
    void stopCluster() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void currentApplicationsKeepTheRetainedWriterAndChangedSchemas() throws Exception {
        // Each variant is the unchanged-schema control only if it is the writer's schema.
        List<String> variants = new ArrayList<>(List.of("original", "changed"));
        if (BaselineTool.stateFixtures(format).contains("enum")) {
            variants.addAll(List.of("enum-original", "enum-added"));
        }
        for (String variant : variants) {
            assertThat(RuntimeRecoveryHarness.apps().resolve(variant + "/schema.pb"))
                    .as("Current %s schema", variant)
                    .hasSameBinaryContentAs(bundle.resolve("apps/" + variant + "/schema.pb"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"default", "raised", "dynamic"})
    void retainedSnapshotFixturesRestoreTheirValues(String name) throws Exception {
        assumeTrue(
                BaselineTool.snapshotFixtures(format).contains(name),
                "Bundle format " + format + " has no " + name + " snapshot fixture");
        Properties fixture =
                BaselineTool.load(fixtures.resolve("snapshot-" + name + ".properties"));
        if (name.equals("dynamic")) {
            BaselineRuntime.checkDynamicSnapshot(fixture);
        } else {
            BaselineRuntime.checkSnapshot(fixture);
        }
    }

    @ParameterizedTest
    @CsvSource({
        "hashmap, baseline, savepoint",
        "hashmap, baseline, checkpoint",
        "hashmap, raised, savepoint",
        "hashmap, raised, checkpoint",
        "rocksdb, baseline, savepoint",
        "rocksdb, baseline, checkpoint",
        "rocksdb, raised, savepoint",
        "rocksdb, raised, checkpoint"
    })
    void retainedStateRestoresAcrossSupportedSchemaChanges(
            String backend, String limits, String kind) throws Exception {
        Path fixture = fixtures.resolve(backend + "/" + limits);
        RuntimeRecoveryHarness.Settings saved = settings(fixture, backend);
        String state = extract(fixture.resolve(kind + ".zip"));
        Descriptor original = RuntimeRecoveryHarness.descriptor("original");
        Descriptor changed = RuntimeRecoveryHarness.descriptor("changed");
        String payload = "v".repeat(8);
        checks.restoreTwice(
                backend,
                saved,
                state,
                "original",
                original,
                value -> value.getField(original.findFieldByName("payload")),
                ordinal -> payload);
        checks.restoreTwice(
                backend,
                saved,
                state,
                "changed",
                changed,
                value -> value.getField(changed.findFieldByName("added")),
                ordinal -> ordinal < 8 ? "" : "added-" + ordinal);
        // The original descriptor reads back the payload that the reserved schema kept as unknown.
        checks.restoreTwice(
                backend,
                saved,
                state,
                "reserved",
                original,
                value -> value.getField(original.findFieldByName("payload")),
                ordinal -> ordinal < 8 ? payload : "");
        // The archive holds all three state kinds, so whichever state the backend restores first
        // fails; the same-build suite rejects each kind from its own savepoint.
        checks.rejected(
                backend,
                "all",
                saved,
                state,
                "incompatible",
                "Field 2 (count) of " + ENTRY + " changes type from INT32 to SINT32");
    }

    /**
     * Restores state that the retained library wrote after a supported schema evolution, and state
     * of an enum-bearing schema, from bundle format 2, and DynamicMessage state from format 3.
     */
    @ParameterizedTest
    @CsvSource({
        "hashmap, evolved, savepoint",
        "hashmap, evolved, checkpoint",
        "hashmap, enum, savepoint",
        "hashmap, enum, checkpoint",
        "rocksdb, evolved, savepoint",
        "rocksdb, evolved, checkpoint",
        "rocksdb, enum, savepoint",
        "rocksdb, enum, checkpoint",
        "hashmap, dynamic, savepoint",
        "hashmap, dynamic, checkpoint",
        "rocksdb, dynamic, savepoint",
        "rocksdb, dynamic, checkpoint"
    })
    void retainedLaterFormatStateRestores(String backend, String name, String kind)
            throws Exception {
        assumeTrue(
                BaselineTool.stateFixtures(format).contains(name),
                "Bundle format " + format + " has no " + name + " state");
        Path fixture = fixtures.resolve(backend + "/" + name);
        // The readers assume the writers, inputs, and baseline limits that capture records.
        assertThat(BaselineTool.load(fixture.resolve("state.properties")))
                .isEqualTo(
                        switch (name) {
                            case "evolved" -> BaselineRuntime.evolvedIdentity(backend);
                            case "enum" -> BaselineRuntime.enumIdentity(backend);
                            default -> BaselineRuntime.dynamicIdentity(backend);
                        });
        String state = extract(fixture.resolve(kind + ".zip"));
        switch (name) {
            case "evolved" -> BaselineRuntime.readEvolved(checks, backend, state, kind);
            case "enum" -> BaselineRuntime.readEnum(checks, backend, state);
            default -> BaselineRuntime.readDynamic(dynamicChecks, backend, state, kind);
        }
    }

    /** Reads the writer's settings and requires the state identity that the checks assume. */
    private static RuntimeRecoveryHarness.Settings settings(Path fixture, String backend)
            throws Exception {
        Properties state = BaselineTool.load(fixture.resolve("state.properties"));
        var recorded =
                new RuntimeRecoveryHarness.Settings(
                        Boolean.parseBoolean(state.getProperty("settings.deterministic")),
                        Integer.parseInt(state.getProperty("settings.maxMessageSize")),
                        Integer.parseInt(state.getProperty("settings.recursionLimit")));
        assertThat(state).isEqualTo(BaselineRuntime.stateIdentity(backend, recorded));
        return recorded;
    }

    private String extract(Path archive) throws Exception {
        Path state = root.resolve("state-" + sequence++);
        BaselineTool.extract(archive, state);
        assertThat(state.resolve("_metadata")).isRegularFile();
        return state.toUri().toString();
    }
}
