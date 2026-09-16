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
        assertThat(
                        Path.of(
                                        Message.class
                                                .getProtectionDomain()
                                                .getCodeSource()
                                                .getLocation()
                                                .toURI())
                                .getFileName()
                                .toString())
                .isEqualTo("protobuf-java-" + manifest.getProperty("protobuf.version") + ".jar");
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
            for (String backend : List.of("hashmap", "rocksdb")) {
                for (boolean raised : List.of(false, true)) {
                    var settings = raised ? RAISED : BASELINE;
                    String identity = backend + "/" + (raised ? "raised" : "baseline");
                    Path fixture = fixtures.resolve(identity);
                    String prefix = identity.replace('/', '-');
                    if (capture) {
                        Files.createDirectories(fixture);
                        Path writerDirectory = work.resolve(prefix + "-writer");
                        Path saved;
                        try (var writer =
                                harness.start(
                                        writerDirectory,
                                        backend,
                                        "all",
                                        settings,
                                        null,
                                        false,
                                        true)) {
                            writer.process(0, 8, 0);
                            saved =
                                    Path.of(
                                            URI.create(
                                                    writer.savepoint(
                                                            work.resolve(prefix + "-saved"))));
                            BaselineTool.zip(saved, fixture.resolve("savepoint.zip"));
                            Path checkpoint = Path.of(URI.create(writer.checkpoint()));
                            BaselineTool.zip(checkpoint, fixture.resolve("checkpoint.zip"));
                            writer.process(8, 12, 0);
                            RuntimeRecoveryHarness.command(writer.directory, "fail", "once");
                            writer.assertRestored(1, 8);
                            writer.process(8, 16, 1);
                        }
                        assertThat(saved.toAbsolutePath().normalize())
                                .startsWith(work.toAbsolutePath().normalize());
                        BaselineTool.deleteTree(saved);
                        BaselineTool.deleteTree(writerDirectory);
                        assertThat(saved).doesNotExist();
                        assertThat(writerDirectory).doesNotExist();
                        Properties state = new Properties();
                        state.setProperty("backend", backend);
                        state.setProperty("checkpoint.incremental", "false");
                        state.setProperty("savepoint.format", "CANONICAL");
                        state.setProperty(
                                "settings.deterministic",
                                Boolean.toString(settings.deterministic()));
                        state.setProperty(
                                "settings.maxMessageSize", Integer.toString(settings.size()));
                        state.setProperty(
                                "settings.recursionLimit", Integer.toString(settings.depth()));
                        state.setProperty("job.parallelism", "2");
                        state.setProperty("job.maxParallelism", "16");
                        state.setProperty("job.uids", "input,messages,values,output");
                        state.setProperty("state.names", "keyed-values,map-values,operator-values");
                        state.setProperty("expected.input.count", "8");
                        state.setProperty("expected.ordinals", "0,1,2,3,4,5,6,7");
                        state.setProperty("expected.keyed.count.per.key", "2");
                        state.setProperty("expected.map.count.per.user.key", "1");
                        state.setProperty("expected.unknown.100", "99");
                        BaselineTool.save(state, fixture.resolve("state.properties"));
                    }
                    for (String kind : List.of("checkpoint", "savepoint")) {
                        Path relocated = work.resolve(prefix + "-" + kind);
                        BaselineTool.extract(fixture.resolve(kind + ".zip"), relocated);
                        assertThat(relocated.resolve("_metadata")).isRegularFile();
                        try (var reader =
                                harness.start(
                                        work.resolve(prefix + "-" + kind + "-reader"),
                                        backend,
                                        "all",
                                        settings,
                                        relocated.toUri().toString(),
                                        false,
                                        false)) {
                            reader.assertRestored(0, 8);
                            reader.process(8, 16, 0);
                        }
                        if (kind.equals("savepoint")) {
                            for (boolean changed : List.of(false, true)) {
                                var rejectedSettings =
                                        changed
                                                ? settings
                                                : new RuntimeRecoveryHarness.Settings(
                                                        settings.deterministic(),
                                                        settings.size() - 1,
                                                        settings.depth());
                                try (var rejected =
                                        harness.start(
                                                work.resolve(prefix + "-reject-" + changed),
                                                backend,
                                                "all",
                                                rejectedSettings,
                                                relocated.toUri().toString(),
                                                changed,
                                                false)) {
                                    assertThat(rejected.failure())
                                            .hasStackTraceContaining(
                                                    changed
                                                            ? "different Protobuf schema"
                                                            : "StateMigrationException");
                                }
                            }
                        }
                        cases.add(identity + "/" + kind);
                    }
                }
            }
        }
        BaselineTool.deleteTree(work);
        assertThat(cases).containsExactlyElementsOf(BaselineTool.expectedCases(version));
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
        Properties fixture = BaselineTool.load(file);
        TypeSerializerSnapshot<Envelope> saved =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        new DataInputDeserializer(
                                Base64.getDecoder().decode(fixture.getProperty("snapshot.base64"))),
                        BaselineRuntime.class.getClassLoader());
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
