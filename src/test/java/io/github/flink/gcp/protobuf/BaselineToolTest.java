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
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
        Path output = BaselineTool.newDirectory(temporary.resolve("output"), input);
        assertThatThrownBy(() -> BaselineTool.newDirectory(output, input))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        assertThatThrownBy(() -> BaselineTool.check(output)).hasMessageContaining("Incomplete");
    }

    @Test
    void rejectsUnsupportedRuntimeCombinations() {
        assertThat(BaselineTool.runtimes("flink2")).containsExactly("2.2.1", "2.3.0");
        assertThat(BaselineTool.runtimes("flink1")).containsExactly("1.20.4");
        assertThatThrownBy(() -> BaselineTool.runtimes("flink3"))
                .hasMessageContaining("Unsupported");
        BaselineTool.validateJava("flink2", 21);
        assertThatThrownBy(() -> BaselineTool.validateJava("flink2", 11))
                .hasMessageContaining("Unsupported");
        assertThat(BaselineTool.expectedCases("1.20.4")).hasSize(13).contains("legacy-entry-point");
        assertThat(BaselineTool.expectedCases("2.2.1"))
                .hasSize(12)
                .doesNotContain("legacy-entry-point");
        for (int version : List.of(11, 21, 25)) {
            assertThatThrownBy(() -> BaselineTool.validateJava("flink1", version))
                    .hasMessageContaining("Unsupported");
        }
    }

    private static void assertInventoryFailure(Path bundle) {
        assertThatThrownBy(() -> BaselineTool.check(bundle))
                .hasMessageContaining("inventory mismatch");
    }
}
