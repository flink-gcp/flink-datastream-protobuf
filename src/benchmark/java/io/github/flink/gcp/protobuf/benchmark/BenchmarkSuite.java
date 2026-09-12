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

package io.github.flink.gcp.protobuf.benchmark;

import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Pattern;

/** Opt-in entry point: correctness gates, bounded smoke, or reproducible independent forks. */
public final class BenchmarkSuite {
    static final List<String> OPERATIONS =
            List.of(
                    "serialize",
                    "deserialize",
                    "objectCopy",
                    "streamCopy",
                    "deserializeReuse",
                    "objectCopyReuse",
                    "serializeSameInstance",
                    "buildAndSerialize");

    private BenchmarkSuite() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !List.of("check", "smoke", "run").contains(args[0])) {
            throw new IllegalArgumentException(
                    "Usage: BenchmarkSuite <check|smoke|run> <new-output-directory>");
        }
        Path directory = Path.of(args[1]).toAbsolutePath();
        Lane.require(!Files.exists(directory), "Output already exists: " + directory);
        Files.createDirectories(directory);
        verifyLibraryArtifact();
        usePackagedLibrary();
        provenance(directory);
        List<Lane> supported = new ArrayList<>();
        var openedLanes = EnumSet.noneOf(Lane.class);
        boolean lts = System.getProperty("benchmark.flink.version", "").startsWith("1.");
        for (Lane lane : Lane.values()) {
            Properties plain = validate(lane, directory, false);
            if ("supported".equals(plain.getProperty("status"))) {
                supported.add(lane);
            } else if (lts && lane.generic()) {
                Properties opened = validate(lane, directory, true);
                if ("supported".equals(opened.getProperty("status"))) {
                    supported.add(lane);
                    openedLanes.add(lane);
                }
            }
        }
        Lane.require(
                supported.containsAll(
                        List.of(
                                Lane.POJO,
                                Lane.TUPLE,
                                Lane.AVRO_SPECIFIC,
                                Lane.AVRO_GENERIC,
                                Lane.PROTOBUF_NATIVE)),
                "Missing required native lane");
        if (args[0].equals("check")) {
            return;
        }
        boolean smoke = args[0].equals("smoke");
        List<String> workloads = smoke ? List.of("collections") : Corpus.WORKLOADS;
        int forks = smoke ? 1 : 5;
        List<String> expected = new ArrayList<>();
        for (int fork = 0; fork < forks; fork++) {
            for (String workload : workloads) {
                for (String operation : OPERATIONS) {
                    // Alternate all representations by block; keep the Protobuf pair adjacent.
                    List<List<Lane>> groups = new ArrayList<>();
                    for (Lane lane : supported) {
                        if (lane != Lane.PROTOBUF_NATIVE && lane != Lane.PROTOBUF_CHILL) {
                            groups.add(List.of(lane));
                        }
                    }
                    List<Lane> pair = new ArrayList<>(List.of(Lane.PROTOBUF_NATIVE));
                    if (supported.contains(Lane.PROTOBUF_CHILL)) {
                        pair.add(Lane.PROTOBUF_CHILL);
                    }
                    if (fork % 2 == 1) {
                        Collections.reverse(pair);
                    }
                    groups.add(pair);
                    Collections.shuffle(
                            groups,
                            new Random(
                                    Corpus.SEED
                                            + fork
                                            + workload.hashCode()
                                            + operation.hashCode()));
                    for (List<Lane> group : groups) {
                        for (Lane lane : group) {
                            String file =
                                    lane + "-" + workload + "-" + operation + "-" + fork + ".json";
                            expected.add(file);
                            run(
                                    directory.resolve(file),
                                    lane,
                                    workload,
                                    operation,
                                    false,
                                    smoke,
                                    openedLanes.contains(lane),
                                    fork);
                        }
                    }
                }
            }
            if (!smoke) {
                String file =
                        "PROTOBUF_NATIVE-collections-serialize-deterministic-" + fork + ".json";
                expected.add(file);
                run(
                        directory.resolve(file),
                        Lane.PROTOBUF_NATIVE,
                        "collections",
                        "serialize",
                        true,
                        false,
                        false,
                        fork);
            }
        }
        for (String file : expected) {
            Lane.require(Files.size(directory.resolve(file)) > 0, "Missing result " + file);
        }
        Files.write(directory.resolve("completed-results.txt"), expected);
        if (!smoke) {
            BenchmarkReport.main(new String[] {directory.toString()});
        }
    }

    private static void usePackagedLibrary() throws Exception {
        Path classes =
                Path.of(
                                io.github.flink.gcp.protobuf.ProtobufTypeInformation.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI())
                        .toRealPath();
        Path jar = Path.of(System.getProperty("benchmark.library.jar")).toRealPath();
        List<String> entries = new ArrayList<>();
        int replacements = 0;
        for (String entry :
                System.getProperty("java.class.path")
                        .split(Pattern.quote(System.getProperty("path.separator")))) {
            if (Path.of(entry).toRealPath().equals(classes)) {
                entries.add(jar.toString());
                replacements++;
            } else {
                entries.add(entry);
            }
        }
        Lane.require(replacements == 1, "Expected one production classpath entry");
        // Both correctness subprocesses and JMH forks load the packaged production classes.
        System.setProperty(
                "java.class.path", String.join(System.getProperty("path.separator"), entries));
    }

    private static void verifyLibraryArtifact() throws Exception {
        Path jar = Path.of(System.getProperty("benchmark.library.jar"));
        Lane.require(
                Files.isRegularFile(jar), "Package the production library before benchmarking");
        try (var archive = new java.util.jar.JarFile(jar.toFile())) {
            for (var entry : java.util.Collections.list(archive.entries())) {
                String name = entry.getName();
                Lane.require(
                        !name.contains("benchmark")
                                && !name.contains("BenchmarkList")
                                && !name.startsWith("org/openjdk/jmh/")
                                && !name.startsWith("com/twitter/")
                                && !name.startsWith("org/apache/avro/")
                                && !name.startsWith("org/apache/thrift/"),
                        "Benchmark content in production artifact: " + name);
            }
        }
    }

    static void requirePackagedLibrary() throws Exception {
        Path actual =
                Path.of(
                                io.github.flink.gcp.protobuf.ProtobufTypeInformation.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI())
                        .toRealPath();
        Path expected = Path.of(System.getProperty("benchmark.library.jar")).toRealPath();
        Lane.require(
                actual.equals(expected), "JMH fork did not load the packaged library: " + actual);
    }

    private static Properties validate(Lane lane, Path directory, boolean opened) throws Exception {
        String suffix = opened ? "-opened" : "-plain";
        Path output = directory.resolve(lane + suffix + ".properties");
        List<String> command = new ArrayList<>();
        command.add(java());
        command.addAll(jvm(opened));
        command.addAll(
                List.of(
                        "-cp",
                        System.getProperty("java.class.path"),
                        BenchmarkCheck.class.getName(),
                        lane.name(),
                        output.toString()));
        Process p =
                new ProcessBuilder(command)
                        .redirectErrorStream(true)
                        .redirectOutput(directory.resolve(lane + suffix + ".log").toFile())
                        .start();
        Lane.require(
                p.waitFor() == 0,
                "Correctness failed: " + directory.resolve(lane + suffix + ".log"));
        Properties report = new Properties();
        try (InputStream in = Files.newInputStream(output)) {
            report.load(in);
        }
        return report;
    }

    private static void run(
            Path file,
            Lane lane,
            String workload,
            String operation,
            boolean deterministic,
            boolean smoke,
            boolean opened,
            int fork)
            throws Exception {
        var options =
                new OptionsBuilder()
                        .include(
                                "^"
                                        + Pattern.quote(
                                                SerializerBenchmark.class.getName()
                                                        + "."
                                                        + operation)
                                        + "$")
                        .param("lane", lane.name())
                        .param("workload", workload)
                        .param("deterministic", Boolean.toString(deterministic))
                        .threads(1)
                        .forks(1)
                        .warmupIterations(smoke ? 1 : 5)
                        .measurementIterations(smoke ? 1 : 5)
                        .warmupTime(smoke ? TimeValue.milliseconds(100) : TimeValue.seconds(1))
                        .measurementTime(smoke ? TimeValue.milliseconds(100) : TimeValue.seconds(1))
                        .jvm(java())
                        .jvmArgs(jvm(opened).toArray(String[]::new))
                        .jvmArgsAppend(
                                "-Dbenchmark.library.jar="
                                        + System.getProperty("benchmark.library.jar"))
                        .shouldFailOnError(true)
                        .addProfiler("gc")
                        .resultFormat(ResultFormatType.JSON)
                        .result(file.toString())
                        .output(file.toString().replace(".json", ".log"))
                        .build();
        System.out.println("Measuring " + file.getFileName() + " (independent block " + fork + ")");
        var results = new Runner(options).run();
        Lane.require(results.size() == 1, "Unexpected JMH result count");
        RunResult result = results.iterator().next();
        Lane.require(Double.isFinite(result.getPrimaryResult().getScore()), "Non-finite JMH score");
        Lane.require(
                result.getSecondaryResults().containsKey("gc.alloc.rate.norm"),
                "Missing allocation profile");
    }

    private static List<String> jvm(boolean opened) {
        List<String> flags = new ArrayList<>(List.of("-Xms1g", "-Xmx1g", "-XX:+UseG1GC"));
        if (opened) {
            flags.add("--add-opens=java.base/java.util=ALL-UNNAMED");
        }
        return flags;
    }

    private static String java() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }

    private static void provenance(Path directory) throws Exception {
        Properties p = new Properties();
        for (String key :
                List.of(
                        "java.version",
                        "java.vendor",
                        "java.vm.name",
                        "java.vm.version",
                        "os.name",
                        "os.version",
                        "os.arch",
                        "benchmark.flink.version",
                        "benchmark.protobuf.version",
                        "benchmark.protoc.version",
                        "benchmark.thrift.compiler",
                        "benchmark.library.jar")) {
            p.setProperty(key, System.getProperty(key, "unknown"));
        }
        p.setProperty("timestamp.utc", java.time.Instant.now().toString());
        p.setProperty("corpus.seed", Long.toString(Corpus.SEED));
        p.setProperty("corpus.records", Integer.toString(Corpus.RECORDS));
        p.setProperty(
                "thrift.compiler.version",
                capture(System.getProperty("benchmark.thrift.compiler"), "--version"));
        p.setProperty("jmh.version", "1.37");
        p.setProperty(
                "jvm.arguments",
                ManagementFactory.getRuntimeMXBean().getInputArguments().toString());
        p.setProperty("source.revision", capture("git", "rev-parse", "HEAD"));
        p.setProperty("source.status", capture("git", "status", "--porcelain"));
        p.setProperty("machine", capture("uname", "-srm"));
        if (System.getProperty("os.name").contains("Mac")) {
            p.setProperty("cpu", capture("sysctl", "-n", "machdep.cpu.brand_string"));
            p.setProperty("memory.bytes", capture("sysctl", "-n", "hw.memsize"));
            p.setProperty("cpu.count", capture("sysctl", "-n", "hw.ncpu"));
        } else {
            p.setProperty("cpu", capture("lscpu"));
        }
        try (OutputStream out = Files.newOutputStream(directory.resolve("manifest.properties"))) {
            p.store(out, "Benchmark provenance");
        }
        var hashes = new LinkedHashSet<String>();
        Path library = Path.of(System.getProperty("benchmark.library.jar"));
        hashes.add(hash(library) + "  " + library);
        for (String entry :
                System.getProperty("java.class.path")
                        .split(Pattern.quote(System.getProperty("path.separator")))) {
            Path path = Path.of(entry);
            if (Files.isRegularFile(path)) {
                hashes.add(hash(path) + "  " + path);
            } else if (Files.isDirectory(path)) {
                try (var files = Files.walk(path)) {
                    for (Path file : files.filter(Files::isRegularFile).sorted().toList()) {
                        hashes.add(hash(file) + "  " + file);
                    }
                }
            }
        }
        Files.write(directory.resolve("classpath-sha256.txt"), hashes);
    }

    private static String hash(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(path)) {
            byte[] buffer = new byte[65536];
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String capture(String... command) throws Exception {
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        String result =
                new String(
                                p.getInputStream().readAllBytes(),
                                java.nio.charset.StandardCharsets.UTF_8)
                        .trim();
        return p.waitFor() == 0 ? result : "unavailable: " + result;
    }
}
