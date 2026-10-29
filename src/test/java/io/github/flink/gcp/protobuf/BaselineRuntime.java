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

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotMessages.Envelope;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

import static io.github.flink.gcp.protobuf.RuntimeRecoveryHarness.BASELINE;
import static io.github.flink.gcp.protobuf.RuntimeRecoveryHarness.RAISED;
import static org.assertj.core.api.Assertions.assertThat;

/** Runs retained compiled consumers and recovery inputs in a fresh runtime process. */
public final class BaselineRuntime {
    private BaselineRuntime() {}

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        boolean capture = mode.equals("capture");
        Path bundle = Path.of(args[1]);
        String version = args[2];
        Path results = Path.of(args[3]);
        Properties manifest = BaselineTool.load(bundle.resolve("manifest.properties"));
        System.setProperty("protobuf.test.flink.version", version);
        System.setProperty(
                "protobuf.test.protobuf.version", manifest.getProperty("protobuf.version"));
        System.setProperty("protobuf.test.runtime.apps", bundle.resolve("apps").toString());
        System.setProperty(
                "protobuf.test.production.classes", bundle.resolve("library.jar").toString());
        new RuntimeCompatibilityTest().loadsTheRequestedFlinkRuntime();
        new RuntimeCompatibilityTest().loadsProductionCodeFromTheConfiguredDirectoryOrJar();
        new RuntimeCompatibilityTest().loadsTheRequestedProtobufRuntime();
        if (mode.equals("transport") || mode.equals("legacy")) {
            if (mode.equals("transport")) {
                new ProtobufRegistrationITCase()
                        .registersGeneratedMessagesAndTransportsAllFiveShapes();
            } else {
                assertThat(version).startsWith("1.20.");
                // Invoke the named retained method so a missing legacy check cannot pass silently.
                Class<?> legacy =
                        Class.forName("io.github.flink.gcp.protobuf.ProtobufLegacySerializerTest");
                legacy.getDeclaredMethod(
                                "legacyAndCurrentSerializerEntryPointsUseTheSameNativeSettings")
                        .invoke(legacy.getDeclaredConstructor().newInstance());
            }
            Properties result = new Properties();
            result.setProperty("status", "passed");
            result.setProperty("library.sha256", BaselineTool.hash(bundle.resolve("library.jar")));
            BaselineTool.save(result, results.resolve(mode + ".properties"));
            return;
        }
        BaselineTool.require(capture || mode.equals("read"), "Unsupported runtime operation");
        List<String> cases = new ArrayList<>();
        for (String instrument :
                version.startsWith("1.20.")
                        ? List.of("consumer", "transport", "legacy")
                        : List.of("consumer", "transport")) {
            Properties result = BaselineTool.load(results.resolve(instrument + ".properties"));
            assertThat(result.getProperty("status")).isEqualTo("passed");
            assertThat(result.getProperty("library.sha256"))
                    .isEqualTo(BaselineTool.hash(bundle.resolve("library.jar")));
        }
        cases.add("compiled-consumer");
        cases.add("registered-transport");
        if (version.startsWith("1.20.")) {
            cases.add("legacy-entry-point");
        }
        Path fixtures = bundle.resolve("fixtures/" + version);
        if (capture) {
            Files.createDirectories(fixtures);
        }
        for (boolean raised : List.of(false, true)) {
            String name = raised ? "raised" : "default";
            snapshot(fixtures.resolve("snapshot-" + name + ".properties"), capture, raised);
            cases.add("snapshot-" + name);
        }
        Path work = Files.createDirectory(results.resolve("work"));
        try (var harness = new RuntimeRecoveryHarness()) {
            int[] sequence = {0};
            var checks =
                    new SchemaEvolutionChecks(
                            harness, () -> work.resolve("evolution-" + sequence[0]++));
            for (String backend : List.of("hashmap", "rocksdb")) {
                for (String name : BaselineTool.stateFixtures(BaselineTool.FORMAT)) {
                    String identity = backend + "/" + name;
                    Path fixture = fixtures.resolve(identity);
                    String prefix = identity.replace('/', '-');
                    var settings = name.equals("raised") ? RAISED : BASELINE;
                    if (capture) {
                        Files.createDirectories(fixture);
                        switch (name) {
                            case "evolved" ->
                                    captureEvolved(harness, work, prefix, backend, fixture);
                            case "enum" -> captureEnum(harness, work, prefix, backend, fixture);
                            default ->
                                    captureOriginal(
                                            harness, work, prefix, backend, settings, fixture);
                        }
                    }
                    for (String kind : List.of("checkpoint", "savepoint")) {
                        Path relocated = work.resolve(prefix + "-" + kind);
                        BaselineTool.extract(fixture.resolve(kind + ".zip"), relocated);
                        assertThat(relocated.resolve("_metadata")).isRegularFile();
                        String saved = relocated.toUri().toString();
                        switch (name) {
                            case "evolved" -> readEvolved(checks, backend, saved, kind);
                            case "enum" -> readEnum(checks, backend, saved);
                            default ->
                                    readOriginal(
                                            harness, checks, work, prefix, backend, settings, saved,
                                            kind);
                        }
                        cases.add(identity + "/" + kind);
                    }
                }
            }
        }
        BaselineTool.deleteTree(work);
        assertThat(cases)
                .containsExactlyElementsOf(
                        BaselineTool.expectedCases(version, BaselineTool.FORMAT));
        Properties result = new Properties();
        result.setProperty("status", "passed");
        result.setProperty("flink.version", version);
        result.setProperty("protobuf.version", manifest.getProperty("protobuf.version"));
        result.setProperty("java.version", System.getProperty("java.runtime.version"));
        result.setProperty("library.sha256", BaselineTool.hash(bundle.resolve("library.jar")));
        result.setProperty("consumer.sha256", BaselineTool.hash(bundle.resolve("tools.jar")));
        result.setProperty("case.count", Integer.toString(cases.size()));
        result.setProperty("cases", String.join(",", cases));
        BaselineTool.save(result, results.resolve("result.properties"));
    }

    /** Writes original-schema state, then checks checkpoint failure and replay. */
    private static void captureOriginal(
            RuntimeRecoveryHarness harness,
            Path work,
            String prefix,
            String backend,
            RuntimeRecoveryHarness.Settings settings,
            Path fixture)
            throws Exception {
        Path writerDirectory = work.resolve(prefix + "-writer");
        Path saved;
        try (var writer =
                harness.start(writerDirectory, backend, "all", settings, null, "original", true)) {
            writer.process(0, 8, 0);
            saved = Path.of(URI.create(writer.savepoint(work.resolve(prefix + "-saved"))));
            BaselineTool.zip(saved, fixture.resolve("savepoint.zip"));
            Path checkpoint = Path.of(URI.create(writer.checkpoint()));
            BaselineTool.zip(checkpoint, fixture.resolve("checkpoint.zip"));
            writer.process(8, 12, 0);
            RuntimeRecoveryHarness.command(writer.directory, "fail", "once");
            writer.assertRestored(1, 8);
            writer.process(8, 16, 1);
        }
        deleteWriterState(work, saved, writerDirectory);
        BaselineTool.save(stateIdentity(backend, settings), fixture.resolve("state.properties"));
    }

    /**
     * Restores original-schema state of inputs 0 to 7 with the changed schema and archives it after
     * inputs 8 to 11, at the baseline limits.
     */
    private static void captureEvolved(
            RuntimeRecoveryHarness harness, Path work, String prefix, String backend, Path fixture)
            throws Exception {
        Path writerDirectory = work.resolve(prefix + "-writer");
        Path saved;
        try (var writer =
                harness.start(writerDirectory, backend, "all", BASELINE, null, "original", false)) {
            writer.process(0, 8, 0);
            saved = Path.of(URI.create(writer.savepoint(work.resolve(prefix + "-saved"))));
        }
        Path evolverDirectory = work.resolve(prefix + "-evolver");
        Path written;
        try (var evolver =
                harness.start(
                        evolverDirectory,
                        backend,
                        "all",
                        BASELINE,
                        saved.toUri().toString(),
                        "changed",
                        false)) {
            evolver.assertRestored(0, 8);
            evolver.process(8, 12, 0);
            written = Path.of(URI.create(evolver.savepoint(work.resolve(prefix + "-written"))));
            BaselineTool.zip(written, fixture.resolve("savepoint.zip"));
            BaselineTool.zip(
                    Path.of(URI.create(evolver.checkpoint())), fixture.resolve("checkpoint.zip"));
        }
        deleteWriterState(work, written, evolverDirectory);
        // The archives must not depend on the restored savepoint either.
        deleteWriterState(work, saved, writerDirectory);
        Properties state = stateIdentity(backend, BASELINE);
        state.setProperty("writer.schemas", "original,changed");
        state.setProperty("expected.input.count", "12");
        state.setProperty("expected.ordinals", "0,1,2,3,4,5,6,7,8,9,10,11");
        state.setProperty("expected.keyed.count.per.key", "3");
        // Inputs 0 to 3 and 8 to 11 use bucket-0, and inputs 4 to 7 use bucket-1.
        state.remove("expected.map.count.per.user.key");
        state.setProperty("expected.map.count.bucket-0", "2");
        state.setProperty("expected.map.count.bucket-1", "1");
        state.setProperty("expected.added", "empty for 0 to 7, added-<ordinal> for 8 to 11");
        BaselineTool.save(state, fixture.resolve("state.properties"));
    }

    /** Archives inputs 0 to 7 of the enum-original schema at the baseline limits. */
    private static void captureEnum(
            RuntimeRecoveryHarness harness, Path work, String prefix, String backend, Path fixture)
            throws Exception {
        Path writerDirectory = work.resolve(prefix + "-writer");
        Path saved;
        try (var writer =
                harness.start(
                        writerDirectory, backend, "all", BASELINE, null, "enum-original", false)) {
            writer.process(0, 8, 0);
            saved = Path.of(URI.create(writer.savepoint(work.resolve(prefix + "-saved"))));
            BaselineTool.zip(saved, fixture.resolve("savepoint.zip"));
            BaselineTool.zip(
                    Path.of(URI.create(writer.checkpoint())), fixture.resolve("checkpoint.zip"));
        }
        deleteWriterState(work, saved, writerDirectory);
        Properties state = stateIdentity(backend, BASELINE);
        state.setProperty("writer.schemas", "enum-original");
        state.setProperty("expected.detail.kind", "KIND_FIRST");
        BaselineTool.save(state, fixture.resolve("state.properties"));
    }

    /** Removes a writer's state so that missing archive contents cannot pass unnoticed. */
    private static void deleteWriterState(Path work, Path saved, Path writerDirectory)
            throws Exception {
        assertThat(saved.toAbsolutePath().normalize())
                .startsWith(work.toAbsolutePath().normalize());
        BaselineTool.deleteTree(saved);
        BaselineTool.deleteTree(writerDirectory);
        assertThat(saved).doesNotExist();
        assertThat(writerDirectory).doesNotExist();
    }

    /** Restores original-schema state unchanged and checks the schema and limit rejections. */
    private static void readOriginal(
            RuntimeRecoveryHarness harness,
            SchemaEvolutionChecks checks,
            Path work,
            String prefix,
            String backend,
            RuntimeRecoveryHarness.Settings settings,
            String saved,
            String kind)
            throws Exception {
        try (var reader =
                harness.start(
                        work.resolve(prefix + "-" + kind + "-reader"),
                        backend,
                        "all",
                        settings,
                        saved,
                        "original",
                        false)) {
            reader.assertRestored(0, 8);
            reader.process(8, 16, 0);
        }
        if (!kind.equals("savepoint")) {
            return;
        }
        checks.rejected(
                backend,
                "all",
                settings,
                saved,
                "incompatible",
                "Field 2 (count) of flink.protobuf.runtime.Entry changes type from INT32 to SINT32");
        var decreased =
                new RuntimeRecoveryHarness.Settings(
                        settings.deterministic(), settings.size() - 1, settings.depth());
        try (var rejected =
                harness.start(
                        work.resolve(prefix + "-reject-limit"),
                        backend,
                        "all",
                        decreased,
                        saved,
                        "original",
                        false)) {
            assertThat(rejected.failure())
                    .hasStackTraceContaining("StateMigrationException")
                    .hasStackTraceContaining("incompatible");
        }
    }

    /**
     * Restores evolved state with the changed schema that wrote it, and requires the original
     * schema, which removes the added field without reserving it, to be rejected.
     */
    private static void readEvolved(
            SchemaEvolutionChecks checks, String backend, String saved, String kind)
            throws Exception {
        Descriptor changed = RuntimeRecoveryHarness.descriptor("changed");
        checks.restoreTwice(
                backend,
                BASELINE,
                saved,
                12,
                "changed",
                changed,
                value -> value.getField(changed.findFieldByName("added")),
                ordinal -> ordinal < 8 ? "" : "added-" + ordinal);
        if (kind.equals("savepoint")) {
            checks.rejected(
                    backend,
                    "all",
                    BASELINE,
                    saved,
                    "original",
                    "Field 5 (added) of flink.protobuf.runtime.Entry is removed without reserving"
                            + " its number");
        }
    }

    /** Restores enum-original state unchanged and with the schema that adds an enum value. */
    private static void readEnum(SchemaEvolutionChecks checks, String backend, String saved)
            throws Exception {
        for (String reader : List.of("enum-original", "enum-added")) {
            Descriptor entry = RuntimeRecoveryHarness.descriptor(reader);
            FieldDescriptor detail = entry.findFieldByName("detail");
            FieldDescriptor kind = detail.getMessageType().findFieldByName("kind");
            checks.restoreTwice(
                    backend,
                    BASELINE,
                    saved,
                    reader,
                    entry,
                    value ->
                            ((EnumValueDescriptor)
                                            ((Message) value.getField(detail)).getField(kind))
                                    .getName(),
                    // Each variant writes its last declared value.
                    ordinal ->
                            ordinal < 8 || reader.equals("enum-original")
                                    ? "KIND_FIRST"
                                    : "KIND_SECOND");
        }
    }

    private static void snapshot(Path file, boolean capture, boolean raised) throws Exception {
        int size = raised ? 134217728 : 67108864;
        int depth = raised ? 200 : 100;
        var info =
                ProtobufTypeInformation.newBuilder(Envelope.class)
                        .deterministicSerialization(raised)
                        .maxMessageSize(size)
                        .recursionLimit(depth)
                        .build();
        var serializer = info.createSerializer(new SerializerConfigImpl());
        if (capture) {
            DataOutputSerializer snapshot = new DataOutputSerializer(32);
            TypeSerializerSnapshot.writeVersionedSnapshot(
                    snapshot, serializer.snapshotConfiguration());
            DataOutputSerializer message = new DataOutputSerializer(32);
            serializer.serialize(ProtobufTypeSerializerSnapshotTest.value(), message);
            Properties properties = new Properties();
            properties.setProperty(
                    "snapshot.base64",
                    Base64.getEncoder().encodeToString(snapshot.getCopyOfBuffer()));
            properties.setProperty(
                    "message.base64",
                    Base64.getEncoder().encodeToString(message.getCopyOfBuffer()));
            properties.setProperty(
                    "snapshot.class", serializer.snapshotConfiguration().getClass().getName());
            properties.setProperty(
                    "snapshot.version",
                    Integer.toString(serializer.snapshotConfiguration().getCurrentVersion()));
            properties.setProperty("settings.deterministic", Boolean.toString(raised));
            properties.setProperty("settings.maxMessageSize", Integer.toString(size));
            properties.setProperty("settings.recursionLimit", Integer.toString(depth));
            properties.setProperty(
                    "schema.sources",
                    "src/test/proto/snapshot.proto,src/test/proto/snapshot_dependency.proto");
            properties.setProperty(
                    "expected.value",
                    "id=42,kind=READY,detail=saved,child.id=7,nested.text=nested,unknown.100=99");
            BaselineTool.save(properties, file);
        }
        checkSnapshot(BaselineTool.load(file));
    }

    /**
     * Returns the state identity that capture records and readers require.
     *
     * @param settings writer settings of the retained state
     */
    static Properties stateIdentity(String backend, RuntimeRecoveryHarness.Settings settings) {
        Properties state = new Properties();
        state.setProperty("backend", backend);
        state.setProperty("checkpoint.incremental", "false");
        state.setProperty("savepoint.format", "CANONICAL");
        state.setProperty("settings.deterministic", Boolean.toString(settings.deterministic()));
        state.setProperty("settings.maxMessageSize", Integer.toString(settings.size()));
        state.setProperty("settings.recursionLimit", Integer.toString(settings.depth()));
        state.setProperty("job.parallelism", "2");
        state.setProperty("job.maxParallelism", "16");
        state.setProperty("job.uids", "input,messages,values,output");
        state.setProperty("state.names", "keyed-values,map-values,operator-values");
        state.setProperty("expected.input.count", "8");
        state.setProperty("expected.ordinals", "0,1,2,3,4,5,6,7");
        state.setProperty("expected.keyed.count.per.key", "2");
        state.setProperty("expected.map.count.per.user.key", "1");
        state.setProperty("expected.unknown.100", "99");
        return state;
    }

    /**
     * Restores a snapshot fixture's framed value with the loaded library at the fixture's settings.
     *
     * <p>A loaded library with the fixture's snapshot version must also write byte-identical
     * snapshot metadata, which shows that the snapshot format, settings, and test schema are
     * unchanged rather than merely compatible.
     */
    static void checkSnapshot(Properties fixture) throws Exception {
        var serializer =
                ProtobufTypeInformation.newBuilder(Envelope.class)
                        .deterministicSerialization(
                                Boolean.parseBoolean(fixture.getProperty("settings.deterministic")))
                        .maxMessageSize(
                                Integer.parseInt(fixture.getProperty("settings.maxMessageSize")))
                        .recursionLimit(
                                Integer.parseInt(fixture.getProperty("settings.recursionLimit")))
                        .build()
                        .createSerializer(new SerializerConfigImpl());
        byte[] bytes = Base64.getDecoder().decode(fixture.getProperty("snapshot.base64"));
        if (serializer.snapshotConfiguration().getCurrentVersion()
                == Integer.parseInt(fixture.getProperty("snapshot.version"))) {
            DataOutputSerializer current = new DataOutputSerializer(32);
            TypeSerializerSnapshot.writeVersionedSnapshot(
                    current, serializer.snapshotConfiguration());
            assertThat(current.getCopyOfBuffer()).as("Current snapshot bytes").isEqualTo(bytes);
        }
        TypeSerializerSnapshot<Envelope> saved =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        new DataInputDeserializer(bytes), BaselineRuntime.class.getClassLoader());
        assertThat(
                        serializer
                                .snapshotConfiguration()
                                .resolveSchemaCompatibility(saved)
                                .isCompatibleAsIs())
                .isTrue();
        assertThat(
                        saved.restoreSerializer()
                                .deserialize(
                                        new DataInputDeserializer(
                                                Base64.getDecoder()
                                                        .decode(
                                                                fixture.getProperty(
                                                                        "message.base64")))))
                .isEqualTo(ProtobufTypeSerializerSnapshotTest.value());
    }
}
