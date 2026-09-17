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

package io.github.flink.gcp.protobuf.examples;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Launches only packaged application classes, with Flink supplied by the consumer runtime. */
class PackagedExamplesIT {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(strings = {"ExplicitExample", "RegisteredExample", "StatefulExample"})
    void runsThePackagedEntrypoint(String mainClass) throws Exception {
        Path jar = Path.of("target/example-job.jar").toAbsolutePath();
        try (var archive = new JarFile(jar.toFile())) {
            var entries = archive.stream().map(entry -> entry.getName()).toList();
            assertThat(entries)
                    .contains(
                            "io/github/flink/gcp/protobuf/ProtobufTypeInformation.class",
                            "io/github/flink/gcp/protobuf/examples/generated/Event.class",
                            "com/google/protobuf/Message.class",
                            "META-INF/LICENSE",
                            "META-INF/NOTICE");
            assertThat(entries)
                    .noneMatch(
                            name ->
                                    name.startsWith("org/apache/flink/")
                                            || name.startsWith("org/junit/")
                                            || name.contains("/spike/"));
        }
        String dependencies =
                Arrays.stream(
                                System.getProperty("surefire.test.class.path")
                                        .split(File.pathSeparator))
                        .filter(path -> path.endsWith(".jar"))
                        .filter(
                                path ->
                                        !Path.of(path)
                                                .getFileName()
                                                .toString()
                                                .startsWith("flink-datastream-protobuf-"))
                        .filter(
                                path ->
                                        !Path.of(path)
                                                .getFileName()
                                                .toString()
                                                .startsWith("protobuf-java-"))
                        .collect(Collectors.joining(File.pathSeparator));
        var command =
                new ArrayList<>(
                        List.of(
                                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                "-Dorg.slf4j.simpleLogger.defaultLogLevel=error",
                                "-cp",
                                jar + File.pathSeparator + dependencies,
                                "io.github.flink.gcp.protobuf.examples." + mainClass));
        if (mainClass.equals("RegisteredExample")) {
            command.add(Path.of("config").toAbsolutePath().toString());
        }
        Path output = directory.resolve("output.txt");
        Process process =
                new ProcessBuilder(command)
                        .redirectErrorStream(true)
                        .redirectOutput(output.toFile())
                        .start();
        try {
            assertThat(process.waitFor(90, TimeUnit.SECONDS))
                    .as("Entrypoint completed: %s", mainClass)
                    .isTrue();
            String text = Files.readString(output);
            assertThat(process.exitValue()).as("%s output: %s", mainClass, text).isZero();
            switch (mainClass) {
                case "ExplicitExample" -> assertThat(text).contains("alice:3", "bob:8");
                case "RegisteredExample" -> assertThat(text).contains("alice:2", "bob:7");
                case "StatefulExample" ->
                        assertThat(text).contains("alice:2:0", "bob:7:0", "alice:5:2", "alice:9:3");
                default -> throw new AssertionError(mainClass);
            }
        } finally {
            process.destroyForcibly();
        }
    }
}
