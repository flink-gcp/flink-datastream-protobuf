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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static io.github.flink.gcp.protobuf.RuntimeRecoveryHarness.BASELINE;
import static io.github.flink.gcp.protobuf.RuntimeRecoveryHarness.RAISED;
import static org.assertj.core.api.Assertions.assertThat;

/** Fixed development savepoints, with an explicit opt-in capture path outside tracked resources. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(300)
class ProtobufSavepointFixtureITCase {
    @TempDir Path temporary;
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
    void restoresPortableFixturesWithRecordedValuesAndContinues(String backend) throws Exception {
        String runtime = System.getProperty("protobuf.test.flink.version");
        String protobuf = System.getProperty("protobuf.test.protobuf.version");
        assertThat(runtime)
                .as("Run fixture tests through the Maven verification recipes")
                .isNotBlank();
        assertThat(protobuf)
                .as("Run fixture tests with a paired Maven Protobuf profile")
                .isNotBlank();
        String profile = "flink-" + runtime + "/protobuf-" + protobuf + "/" + backend;
        for (boolean raised : new boolean[] {false, true}) {
            String name = raised ? "raised" : "baseline";
            var settings = raised ? RAISED : BASELINE;
            String capture = System.getProperty("protobuf.fixture.capture");
            Path fixture;
            if (capture == null) {
                fixture = Path.of("src/test/resources/savepoints/v1", profile, name);
            } else {
                fixture = Path.of(capture).toAbsolutePath().resolve(profile).resolve(name);
                assertThat(fixture).doesNotExist();
                capture(fixture, backend, settings);
            }
            Path manifestPath = fixture.resolve("manifest.properties");
            assertThat(manifestPath)
                    .as(
                            "Missing fixture for %s/%s; capture the new pinned profile using src/test/resources/savepoints/v1/README.md",
                            profile, name)
                    .isRegularFile();
            Properties manifest = new Properties();
            try (InputStream input = Files.newInputStream(manifestPath)) {
                manifest.load(input);
            }
            assertThat(manifest.getProperty("status")).isEqualTo("development-savepoint-fixture");
            assertThat(manifest.getProperty("flink.version")).isEqualTo(runtime);
            assertThat(manifest.getProperty("protobuf.version")).isEqualTo(protobuf);
            assertThat(manifest.getProperty("backend")).isEqualTo(backend);
            assertThat(manifest.getProperty("settings.deterministic"))
                    .isEqualTo(Boolean.toString(settings.deterministic()));
            assertThat(manifest.getProperty("settings.maxMessageSize"))
                    .isEqualTo(Integer.toString(settings.size()));
            assertThat(manifest.getProperty("settings.recursionLimit"))
                    .isEqualTo(Integer.toString(settings.depth()));
            Path archive = fixture.resolve("savepoint.zip");
            assertThat(hash(archive)).isEqualTo(manifest.getProperty("savepoint.sha256"));
            assertThat(hash(fixture.resolve("runtime.proto")))
                    .isEqualTo(manifest.getProperty("schema.source.sha256"));
            assertThat(hash(fixture.resolve("schema.pb")))
                    .isEqualTo(manifest.getProperty("schema.descriptor.sha256"));
            Path relocated = temporary.resolve(backend + "-" + name + "-relocated");
            extract(archive, relocated);
            try (var reader =
                    harness.start(
                            temporary.resolve(backend + "-" + name + "-reader"),
                            backend,
                            "all",
                            settings,
                            relocated.toUri().toString(),
                            false,
                            false)) {
                reader.assertRestored(0, 8);
                reader.process(8, 16, 0);
            }
        }
    }

    private void capture(Path destination, String backend, RuntimeRecoveryHarness.Settings settings)
            throws Exception {
        assertThat(System.getProperty("protobuf.fixture.revision")).isNotBlank();
        String protoc = System.getProperty("protobuf.test.protoc.version");
        assertThat(protoc).as("Capture requires Maven's configured protoc.version").isNotBlank();
        Path artifact =
                Path.of(
                        ProtobufTypeInformation.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI());
        assertThat(artifact).as("Capture must execute the packaged writer jar").isRegularFile();
        Path saved;
        String identity = backend + "-" + settings.deterministic();
        Path writerDirectory = temporary.resolve(identity + "-writer");
        try (var writer =
                harness.start(writerDirectory, backend, "all", settings, null, false, false)) {
            writer.process(0, 8, 0);
            saved = Path.of(URI.create(writer.savepoint(temporary.resolve(identity + "-saved"))));
        }
        Files.createDirectories(destination);
        Path archive = destination.resolve("savepoint.zip");
        try (ZipOutputStream output = new ZipOutputStream(Files.newOutputStream(archive));
                var files = Files.walk(saved)) {
            for (Path file : files.filter(Files::isRegularFile).sorted().toList()) {
                ZipEntry entry = new ZipEntry(saved.relativize(file).toString().replace('\\', '/'));
                entry.setTime(0);
                output.putNextEntry(entry);
                Files.copy(file, output);
                output.closeEntry();
            }
        }
        // Remove this writer's savepoint and checkpoint storage before restoring the archive.
        for (Path original : List.of(saved, writerDirectory)) {
            assertThat(original.toAbsolutePath().normalize())
                    .startsWith(temporary.toAbsolutePath().normalize());
            try (var files = Files.walk(original)) {
                for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(file);
                }
            }
        }
        Files.copy(
                Path.of("src/test/runtime-app/proto/original/runtime.proto"),
                destination.resolve("runtime.proto"));
        Files.copy(
                RuntimeRecoveryHarness.apps().resolve("original/schema.pb"),
                destination.resolve("schema.pb"));
        Properties manifest = new Properties();
        manifest.setProperty("status", "development-savepoint-fixture");
        manifest.setProperty(
                "writer.artifact",
                "io.github.flink-gcp:flink-datastream-protobuf:"
                        + System.getProperty("protobuf.test.library.version"));
        manifest.setProperty("writer.artifact.sha256", hash(artifact));
        manifest.setProperty(
                "writer.base.revision", System.getProperty("protobuf.fixture.revision"));
        manifest.setProperty(
                "application.jar.sha256",
                hash(RuntimeRecoveryHarness.apps().resolve("application-original.jar")));
        manifest.setProperty("flink.version", System.getProperty("protobuf.test.flink.version"));
        manifest.setProperty(
                "protobuf.version", System.getProperty("protobuf.test.protobuf.version"));
        manifest.setProperty("protoc.version", protoc);
        manifest.setProperty("java.version", System.getProperty("java.runtime.version"));
        manifest.setProperty("backend", backend);
        manifest.setProperty("savepoint.format", "CANONICAL");
        manifest.setProperty("savepoint.sha256", hash(archive));
        manifest.setProperty("schema.source.sha256", hash(destination.resolve("runtime.proto")));
        manifest.setProperty("schema.descriptor.sha256", hash(destination.resolve("schema.pb")));
        manifest.setProperty("settings.deterministic", Boolean.toString(settings.deterministic()));
        manifest.setProperty("settings.maxMessageSize", Integer.toString(settings.size()));
        manifest.setProperty("settings.recursionLimit", Integer.toString(settings.depth()));
        manifest.setProperty("snapshot.class", ProtobufTypeSerializerSnapshot.class.getName());
        manifest.setProperty("snapshot.version", "1");
        manifest.setProperty("job.parallelism", "2");
        manifest.setProperty("job.maxParallelism", "16");
        manifest.setProperty("job.uids", "input,messages,values,output");
        manifest.setProperty("state.names", "keyed-values,map-values,operator-values");
        manifest.setProperty("expected.input.count", "8");
        manifest.setProperty("expected.ordinals", "0,1,2,3,4,5,6,7");
        manifest.setProperty("expected.keyed.count.per.key", "2");
        manifest.setProperty("expected.map.count.per.user.key", "1");
        for (String sourceRoot : new String[] {"src/main", "src/test/runtime-app"}) {
            try (var sources = Files.walk(Path.of(sourceRoot))) {
                for (Path source : sources.filter(Files::isRegularFile).sorted().toList()) {
                    manifest.setProperty(
                            "source.sha256." + source.toString().replace('\\', '/'), hash(source));
                }
            }
        }
        try (OutputStream output =
                Files.newOutputStream(destination.resolve("manifest.properties"))) {
            manifest.store(
                    output,
                    "Copyright 2026 The flink-gcp authors\nLicensed under the Apache License, Version 2.0 (the \"License\");\nSee the accompanying README for capture commands and development-only provenance.");
        }
    }

    private static void extract(Path archive, Path directory) throws Exception {
        Files.createDirectories(directory);
        try (ZipInputStream input = new ZipInputStream(Files.newInputStream(archive))) {
            for (ZipEntry entry; (entry = input.getNextEntry()) != null; ) {
                Path target = directory.resolve(entry.getName()).normalize();
                assertThat(target.startsWith(directory)).isTrue();
                Files.createDirectories(target.getParent());
                Files.copy(input, target);
            }
        }
        assertThat(directory.resolve("_metadata")).isRegularFile();
    }

    private static String hash(Path file) throws Exception {
        return HexFormat.of()
                .formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
    }
}
