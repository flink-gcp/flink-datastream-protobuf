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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BaselineToolTest {
    @TempDir Path temporary;

    @ParameterizedTest
    @ValueSource(
            strings = {
                "../outside",
                "/absolute",
                "a/../outside",
                "C:/windows",
                "a\\outside",
                "a//b",
                "a/./b",
                "line\nbreak"
            })
    void rejectsUnsafeArchivePathsBeforeExtraction(String name) throws Exception {
        Path zip = temporary.resolve("unsafe.zip");
        try (ZipOutputStream output = new ZipOutputStream(Files.newOutputStream(zip))) {
            output.putNextEntry(new ZipEntry(name));
            output.write(1);
            output.closeEntry();
        }
        Path destination = temporary.resolve("extract");
        assertThatThrownBy(() -> BaselineTool.extract(zip, destination))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsafe");
        assertThat(destination).doesNotExist();
    }

    @Test
    void rejectsDisagreementBetweenLocalAndCentralArchiveNames() throws Exception {
        Path archive = temporary.resolve("mismatched.zip");
        try (ZipOutputStream output = new ZipOutputStream(Files.newOutputStream(archive))) {
            output.putNextEntry(new ZipEntry("good"));
            output.write(1);
            output.closeEntry();
        }
        byte[] bytes = Files.readAllBytes(archive);
        System.arraycopy("../x".getBytes(java.nio.charset.StandardCharsets.UTF_8), 0, bytes, 30, 4);
        Files.write(archive, bytes);
        assertThatThrownBy(() -> BaselineTool.extract(archive, temporary.resolve("extracted")))
                .hasMessageContaining("Archive headers disagree");
        assertThat(temporary.resolve("x")).doesNotExist();
    }

    @Test
    void preservesFilesAcrossArchiveRelocation() throws Exception {
        Path source = Files.createDirectory(temporary.resolve("source"));
        Files.writeString(source.resolve("_metadata"), "metadata");
        Files.createDirectory(source.resolve("state"));
        Files.write(source.resolve("state/part"), new byte[] {0, 1, 2, -1});
        var before = BaselineTool.inventory(source);
        Path archive = temporary.resolve("state.zip");
        BaselineTool.zip(source, archive);
        BaselineTool.deleteTree(source);
        Path restored = temporary.resolve("restored");
        BaselineTool.extract(archive, restored);
        assertThat(BaselineTool.inventory(restored)).isEqualTo(before);
        assertThatThrownBy(() -> BaselineTool.extract(archive, restored))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
    }

    @Test
    void rejectsMissingModifiedAndUnexpectedFilesBeforeLoadingMetadata() throws Exception {
        Path bundle = Files.createDirectory(temporary.resolve("bundle"));
        Path file = bundle.resolve("library.jar");
        Files.writeString(file, "original");
        BaselineTool.seal(bundle);
        Files.writeString(file, "changed");
        assertInventoryFailure(bundle);
        Files.delete(file);
        assertInventoryFailure(bundle);
        Files.writeString(file, "original");
        Files.writeString(bundle.resolve("unexpected"), "extra");
        assertInventoryFailure(bundle);
    }

    @Test
    void rejectsRewritingBothFilesAndTheirInventoryDuringRead() throws Exception {
        Path bundle = Files.createDirectory(temporary.resolve("bundle"));
        Path file = bundle.resolve("library.jar");
        Files.writeString(file, "original");
        BaselineTool.seal(bundle);
        String before = BaselineTool.hash(bundle.resolve("SHA256SUMS"));
        Files.writeString(file, "replacement");
        Files.delete(bundle.resolve("SHA256SUMS"));
        BaselineTool.seal(bundle);
        assertThatThrownBy(() -> BaselineTool.checkUnchanged(bundle, before))
                .hasMessageContaining("inventory changed during read");
    }

    @Test
    void rejectsDuplicateInventoryAndSymlinkInputs() throws Exception {
        Path bundle = Files.createDirectory(temporary.resolve("bundle"));
        Files.writeString(bundle.resolve("library.jar"), "bytes");
        BaselineTool.seal(bundle);
        Path sums = bundle.resolve("SHA256SUMS");
        Files.writeString(sums, Files.readString(sums).repeat(2));
        assertThatThrownBy(() -> BaselineTool.check(bundle)).hasMessageContaining("Duplicate");
        Files.createSymbolicLink(bundle.resolve("link"), temporary);
        assertThatThrownBy(() -> BaselineTool.inventory(bundle))
                .hasMessageContaining("Symbolic links");
    }

    @Test
    void refusesOverwriteAndOutputsThroughSymlinksIntoInputs() throws Exception {
        Path input = Files.createDirectory(temporary.resolve("input"));
        Path link = temporary.resolve("link");
        Files.createSymbolicLink(link, input);
        assertThatThrownBy(() -> BaselineTool.newDirectory(link.resolve("output"), input))
                .hasMessageContaining("outside");
        Path repository = Files.createDirectory(temporary.resolve("repository"));
        assertThatThrownBy(
                        () ->
                                BaselineTool.newDirectory(
                                        repository.resolve("output"), input, repository))
                .hasMessageContaining("outside");
        Path output = BaselineTool.newDirectory(temporary.resolve("output"), input);
        assertThatThrownBy(() -> BaselineTool.newDirectory(output, input))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        assertThatThrownBy(() -> BaselineTool.check(output)).hasMessageContaining("Incomplete");
    }

    @Test
    void rejectsUnsupportedRuntimeCombinations() {
        assertThat(BaselineTool.runtimes("flink2")).containsExactly("2.2.1", "2.3.0");
        assertThat(BaselineTool.runtimes("flink1")).containsExactly("1.20.5");
        assertThatThrownBy(() -> BaselineTool.runtimes("flink3"))
                .hasMessageContaining("Unsupported");
        BaselineTool.validateJava("flink2", 21);
        assertThatThrownBy(() -> BaselineTool.validateJava("flink2", 11))
                .hasMessageContaining("Unsupported");
        assertThat(BaselineTool.expectedCases("1.20.5", "1"))
                .hasSize(13)
                .contains("legacy-entry-point");
        assertThat(BaselineTool.expectedCases("2.2.1", "1"))
                .hasSize(12)
                .doesNotContain("legacy-entry-point")
                .noneMatch(name -> name.contains("/evolved/") || name.contains("/enum/"));
        assertThat(BaselineTool.expectedCases("1.20.5", "2"))
                .hasSize(21)
                .contains("legacy-entry-point")
                .noneMatch(name -> name.contains("dynamic"));
        // Format 3 adds a DynamicMessage snapshot fixture and two archives per backend.
        assertThat(BaselineTool.expectedCases("1.20.5", "3"))
                .hasSize(26)
                .contains(
                        "snapshot-dynamic",
                        "hashmap/dynamic/checkpoint",
                        "rocksdb/dynamic/savepoint");
        assertThat(BaselineTool.expectedCases("2.2.1", "3")).hasSize(25);
        assertThat(BaselineTool.expectedCases("2.2.1", "2"))
                .hasSize(20)
                .doesNotContain("legacy-entry-point")
                .contains(
                        "hashmap/evolved/checkpoint",
                        "rocksdb/evolved/savepoint",
                        "hashmap/enum/savepoint",
                        "rocksdb/enum/checkpoint");
        for (int version : List.of(11, 21, 25)) {
            assertThatThrownBy(() -> BaselineTool.validateJava("flink1", version))
                    .hasMessageContaining("Unsupported");
        }
    }

    @ParameterizedTest
    @CsvSource({
        // The v0.1.0 layout has no enum inputs; it fails first on its missing capture results.
        "1, enum, capture-2.2.1/result.properties",
        "2, apps/application-enum-original.jar, apps/application-enum-original.jar",
        "2, apps/enum-original/schema.pb, apps/enum-original/schema.pb",
        "2, apps/application-enum-added.jar, apps/application-enum-added.jar",
        "2, apps/enum-added/schema.pb, apps/enum-added/schema.pb",
        // The v0.2.0 layout has no DynamicMessage application.
        "2, dynamic, capture-2.2.1/result.properties",
        "3, apps/application-dynamic.jar, apps/application-dynamic.jar",
        "4, '', Unsupported bundle format"
    })
    void requiresEachFormatsApplicationsOnlyFromThatFormat(
            String format, String absent, String failure) throws Exception {
        Path bundle = bundle(format, "3", "3.25.9", absent);
        assertThatThrownBy(() -> BaselineTool.check(bundle)).hasMessageContaining(failure);
    }

    @ParameterizedTest
    @CsvSource({
        // An accepted runtime fails later, on the capture results that this bundle omits.
        "3, 3.25.9, capture-2.2.1/result.properties",
        "4, 4.33.6, capture-2.2.1/result.properties",
        "4, 4.36.2, capture-2.2.1/result.properties",
        "3, 3.24.4, Unsupported Protobuf runtime",
        "3, 4.36.2, Unsupported Protobuf runtime",
        "4, 3.25.9, Unsupported Protobuf runtime",
        "4, 4x36.2, Unsupported Protobuf runtime"
    })
    void acceptsOnlySupportedRuntimesOfTheBundleMajor(String major, String protobuf, String failure)
            throws Exception {
        Path bundle = bundle("2", major, protobuf, "");
        assertThatThrownBy(() -> BaselineTool.check(bundle)).hasMessageContaining(failure);
    }

    @ParameterizedTest
    @CsvSource({
        // Complete bundles of each format pass; format 2 needs no DynamicMessage fixture.
        "2, '', ''",
        "3, '', ''",
        "3, fixtures/2.2.1/snapshot-dynamic.properties, Missing snapshot fixture",
        "3, fixtures/2.3.0/rocksdb/dynamic/checkpoint.zip, Missing state fixture",
        "3, fixtures/2.2.1/hashmap/dynamic/savepoint.zip, Missing state fixture"
    })
    void requiresEachFormatsFixtures(String format, String absent, String failure)
            throws Exception {
        Path bundle = bundle(format, "4", "4.36.2", "");
        Files.delete(bundle.resolve("SHA256SUMS"));
        Properties manifest = BaselineTool.load(bundle.resolve("manifest.properties"));
        for (String version : BaselineTool.runtimes("flink2")) {
            Properties result = new Properties();
            List<String> cases = BaselineTool.expectedCases(version, format);
            result.setProperty("status", "passed");
            result.setProperty("flink.version", version);
            result.setProperty("case.count", Integer.toString(cases.size()));
            result.setProperty("cases", String.join(",", cases));
            result.setProperty("library.sha256", BaselineTool.hash(bundle.resolve("library.jar")));
            result.setProperty("consumer.sha256", BaselineTool.hash(bundle.resolve("tools.jar")));
            Files.createDirectories(bundle.resolve("capture-" + version));
            BaselineTool.save(result, bundle.resolve("capture-" + version + "/result.properties"));
            Path fixtures = bundle.resolve("fixtures/" + version);
            for (String name : BaselineTool.snapshotFixtures(format)) {
                write(fixtures.resolve("snapshot-" + name + ".properties"), absent);
            }
            for (String backend : List.of("hashmap", "rocksdb")) {
                for (String name : BaselineTool.stateFixtures(format)) {
                    for (String kind : List.of("savepoint", "checkpoint")) {
                        Path archive = fixtures.resolve(backend + "/" + name + "/" + kind + ".zip");
                        if (absent.isEmpty() || !archive.toString().endsWith(absent)) {
                            Files.createDirectories(archive.getParent());
                            zip(archive);
                        }
                    }
                }
            }
        }
        zip(bundle.resolve("source.zip"));
        assertThat(manifest.getProperty("format.version")).isEqualTo(format);
        BaselineTool.seal(bundle);
        if (failure.isEmpty()) {
            assertThat(BaselineTool.check(bundle)).isEqualTo(manifest);
        } else {
            assertThatThrownBy(() -> BaselineTool.check(bundle)).hasMessageContaining(failure);
        }
    }

    private static void zip(Path archive) throws Exception {
        try (ZipOutputStream output = new ZipOutputStream(Files.newOutputStream(archive))) {
            output.putNextEntry(new ZipEntry("_metadata"));
            output.closeEntry();
        }
    }

    /** Writes a placeholder file unless its path ends with the absent name. */
    private static void write(Path file, String absent) throws Exception {
        if (absent.isEmpty() || !file.toString().endsWith(absent)) {
            Files.createDirectories(file.getParent());
            Files.writeString(file, file.getFileName().toString());
        }
    }

    private Path bundle(String format, String major, String protobuf, String absent)
            throws Exception {
        Path bundle = Files.createDirectory(temporary.resolve("bundle"));
        Files.writeString(
                bundle.resolve("manifest.properties"),
                String.join(
                        "\n",
                        "format.version=" + format,
                        "status=development-tooling-baseline",
                        "source.revision=" + "a".repeat(40),
                        "line=flink2",
                        "protobuf.major=" + major,
                        "protobuf.version=" + protobuf));
        for (String name :
                List.of(
                        "source.zip",
                        "library.jar",
                        "tools.jar",
                        "effective-pom.xml",
                        "apps/application-original.jar",
                        "apps/application-changed.jar",
                        "apps/original/schema.pb",
                        "apps/changed/schema.pb",
                        "apps/application-enum-original.jar",
                        "apps/enum-original/schema.pb",
                        "apps/application-enum-added.jar",
                        "apps/enum-added/schema.pb",
                        "apps/application-dynamic.jar")) {
            if (absent.isEmpty() || !name.contains(absent)) {
                Files.createDirectories(bundle.resolve(name).getParent());
                Files.writeString(bundle.resolve(name), name);
            }
        }
        BaselineTool.seal(bundle);
        return bundle;
    }

    private static void assertInventoryFailure(Path bundle) {
        assertThatThrownBy(() -> BaselineTool.check(bundle))
                .hasMessageContaining("inventory mismatch");
    }
}
