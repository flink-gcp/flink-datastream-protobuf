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

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/** JDK-only development bundle launcher, also executable in Java source-file mode for bootstrap. */
public final class BaselineTool {
    private BaselineTool() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 4 && args[0].equals("capture")) {
            capture(args[1], args[2], Path.of(args[3]));
        } else if (args.length == 2 && args[0].equals("check")) {
            check(Path.of(args[1]).toRealPath());
        } else if (args.length == 4 && args[0].equals("read")) {
            Path bundle = Path.of(args[1]).toRealPath();
            Properties manifest = check(bundle);
            String inventoryHash = hash(bundle.resolve("SHA256SUMS"));
            require(
                    runtimes(manifest.getProperty("line")).contains(args[2]),
                    "Unsupported Flink runtime");
            validateJava(manifest.getProperty("line"), Runtime.version().feature());
            Path output = newDirectory(Path.of(args[3]), bundle);
            try {
                runtime(bundle, args[2], "read", output);
            } finally {
                checkUnchanged(bundle, inventoryHash);
            }
        } else {
            throw new IllegalArgumentException(
                    "Expected capture <flink2|flink1> <3|4> <new-output>, check <bundle>, or read <bundle> <flink-version> <new-results>");
        }
    }

    static List<String> runtimes(String line) {
        return switch (line) {
            case "flink2" -> List.of("2.2.1", "2.3.0");
            case "flink1" -> List.of("1.20.4");
            default -> throw new IllegalArgumentException("Unsupported artifact line");
        };
    }

    static void validateJava(String line, int version) {
        require(
                version == 17 || (line.equals("flink2") && version == 21),
                "Unsupported JDK for artifact line");
    }

    private static void capture(String line, String major, Path destination) throws Exception {
        List<String> versions = runtimes(line);
        require(major.equals("3") || major.equals("4"), "Unsupported Protobuf profile");
        require(Runtime.version().feature() == 17, "Capture requires JDK 17");
        Path repository = Path.of(git("rev-parse", "--show-toplevel")).toRealPath();
        require(
                Path.of("").toRealPath().equals(repository),
                "Capture must start at the repository root");
        require(
                git("status", "--porcelain").isEmpty(),
                "Capture requires a clean committed checkout");
        require(
                git("ls-files", "--others", "--ignored", "--exclude-standard", "--", "src")
                        .isEmpty(),
                "Ignored source inputs are not attributable to the committed revision");
        String revision = git("rev-parse", "HEAD");
        Path bundle = newDirectory(destination, repository);
        Properties manifest = new Properties();
        manifest.setProperty("format.version", "1");
        manifest.setProperty("status", "development-tooling-baseline");
        manifest.setProperty("milestone.final", "false");
        manifest.setProperty("source.revision", revision);
        manifest.setProperty("line", line);
        manifest.setProperty("protobuf.major", major);
        manifest.setProperty("writer.java", System.getProperty("java.runtime.version"));
        manifest.setProperty("writer.java.vendor", System.getProperty("java.vendor"));
        manifest.setProperty(
                "writer.os", System.getProperty("os.name") + "/" + System.getProperty("os.arch"));
        command(
                bundle,
                "source",
                List.of(
                        "git",
                        "archive",
                        "--format=zip",
                        "--output=" + bundle.resolve("source.zip"),
                        revision));
        command(bundle, "build", maven(line, major, versions.get(0), "clean", "verify"));
        command(
                bundle,
                "model",
                maven(
                        line,
                        major,
                        versions.get(0),
                        "help:effective-pom",
                        "-Doutput=" + bundle.resolve("effective-pom.xml")));
        Document model = xml(bundle.resolve("effective-pom.xml"));
        String artifactVersion = directChild(model.getDocumentElement(), "version");
        String protobuf = property(model, "protobuf.version");
        require(
                protobuf.equals(major.equals("3") ? "3.25.9" : "4.33.6"),
                "Unexpected Protobuf runtime");
        require(property(model, "protoc.version").equals(protobuf), "Unpaired protoc/runtime");
        require(
                property(model, "flink.version").equals(versions.get(0)),
                "Unexpected compile floor");
        require(property(model, "flink.compat").equals(line), "Unexpected Flink adapter");
        manifest.setProperty(
                "writer.artifact",
                "io.github.flink-gcp:flink-datastream-protobuf:" + artifactVersion);
        manifest.setProperty("protobuf.version", protobuf);
        manifest.setProperty("protoc.version", protobuf);
        manifest.setProperty("compile.flink", versions.get(0));
        copy(
                Path.of("target/flink-datastream-protobuf-" + artifactVersion + ".jar"),
                bundle.resolve("library.jar"));
        copy(Path.of("target/baseline/tools.jar"), bundle.resolve("tools.jar"));
        for (String variant : List.of("original", "changed")) {
            copy(
                    Path.of("target/runtime-app/application-" + variant + ".jar"),
                    bundle.resolve("apps/application-" + variant + ".jar"));
            copy(
                    Path.of("target/runtime-app/" + variant + "/schema.pb"),
                    bundle.resolve("apps/" + variant + "/schema.pb"));
        }
        manifest.setProperty("library.sha256", hash(bundle.resolve("library.jar")));
        manifest.setProperty("consumer.sha256", hash(bundle.resolve("tools.jar")));
        command(bundle, "maven-version", List.of("./mvnw", "-version"));
        List<Path> compilers =
                files(Path.of("target/protobuf-maven-plugin")).stream()
                        .filter(
                                path ->
                                        path.getFileName().toString().startsWith("protoc-")
                                                && path.toString().endsWith(".exe"))
                        .toList();
        require(!compilers.isEmpty(), "Missing executed protoc binary");
        Path protoc = compilers.get(0).toAbsolutePath();
        for (Path compiler : compilers) {
            require(hash(compiler).equals(hash(protoc)), "Different protoc binaries were used");
        }
        manifest.setProperty("protoc.executable.sha256", hash(protoc));
        command(bundle, "protoc-version", List.of(protoc.toString(), "--version"));
        save(manifest, bundle.resolve("manifest.properties"));
        for (String version : versions) {
            command(
                    bundle,
                    "dependencies-" + version,
                    maven(
                            line,
                            major,
                            version,
                            "dependency:copy-dependencies",
                            "-DincludeScope=test",
                            "-Dmdep.useRepositoryLayout=true",
                            "-DoutputDirectory=" + bundle.resolve("dependencies/" + version)));
            command(
                    bundle,
                    "dependency-tree-" + version,
                    maven(
                            line,
                            major,
                            version,
                            "dependency:tree",
                            "-DoutputFile=" + bundle.resolve("dependencies/" + version + ".txt")));
            Path results = Files.createDirectory(bundle.resolve("capture-" + version));
            runtime(bundle, version, "capture", results);
        }
        require(
                git("ls-files", "--others", "--ignored", "--exclude-standard", "--", "src")
                        .isEmpty(),
                "Ignored source inputs appeared during capture");
        require(
                git("rev-parse", "HEAD").equals(revision) && git("status", "--porcelain").isEmpty(),
                "Source changed during capture");
        require(
                hash(bundle.resolve("library.jar")).equals(manifest.getProperty("library.sha256"))
                        && hash(bundle.resolve("tools.jar"))
                                .equals(manifest.getProperty("consumer.sha256")),
                "Writer or consumer changed during capture");
        seal(bundle);
        check(bundle);
        System.out.println("Captured development tooling bundle: " + bundle);
    }

    private static List<String> maven(String line, String major, String version, String... goals) {
        List<String> command =
                new ArrayList<>(
                        List.of(
                                "./mvnw",
                                "-ntp",
                                "-Pprotobuf" + major,
                                "-Dflink.compat=" + line,
                                "-Dflink.version=" + version));
        command.addAll(List.of(goals));
        return command;
    }

    private static void runtime(Path bundle, String version, String mode, Path results)
            throws Exception {
        List<String> classpath =
                new ArrayList<>(
                        List.of(
                                bundle.resolve("tools.jar").toString(),
                                bundle.resolve("library.jar").toString()));
        for (Path jar : files(bundle.resolve("dependencies/" + version))) {
            if (jar.toString().endsWith(".jar")) {
                classpath.add(jar.toString());
            }
        }
        require(classpath.size() > 2, "Missing runtime dependencies");
        command(
                results,
                "consumer",
                List.of(
                        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-Dorg.slf4j.simpleLogger.defaultLogLevel=error",
                        "-cp",
                        String.join(File.pathSeparator, classpath),
                        "io.github.flink.gcp.protobuf.BaselineConsumer",
                        results.resolve("consumer.properties").toString()));
        Properties consumer = load(results.resolve("consumer.properties"));
        require(
                consumer.getProperty("status", "").equals("passed")
                        && consumer.getProperty("library.sha256", "")
                                .equals(hash(bundle.resolve("library.jar"))),
                "Compiled consumer did not verify the retained library");
        for (String instrument :
                version.startsWith("1.20.")
                        ? List.of("transport", "legacy")
                        : List.of("transport")) {
            command(
                    results,
                    instrument,
                    List.of(
                            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                            "-Dorg.slf4j.simpleLogger.defaultLogLevel=error",
                            "-cp",
                            String.join(File.pathSeparator, classpath),
                            "io.github.flink.gcp.protobuf.BaselineRuntime",
                            instrument,
                            bundle.toString(),
                            version,
                            results.toString()));
        }
        command(
                results,
                "runtime",
                List.of(
                        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-Dorg.slf4j.simpleLogger.defaultLogLevel=error",
                        "-cp",
                        String.join(File.pathSeparator, classpath),
                        "io.github.flink.gcp.protobuf.BaselineRuntime",
                        mode,
                        bundle.toString(),
                        version,
                        results.toString()));
        validateResult(bundle, version, results.resolve("result.properties"));
    }

    static List<String> expectedCases(String version) {
        List<String> cases = new ArrayList<>(List.of("compiled-consumer", "registered-transport"));
        if (version.startsWith("1.20.")) {
            cases.add("legacy-entry-point");
        }
        cases.addAll(List.of("snapshot-default", "snapshot-raised"));
        for (String backend : List.of("hashmap", "rocksdb")) {
            for (String settings : List.of("baseline", "raised")) {
                for (String kind : List.of("checkpoint", "savepoint")) {
                    cases.add(backend + "/" + settings + "/" + kind);
                }
            }
        }
        return cases;
    }

    private static void validateResult(Path bundle, String version, Path result) throws Exception {
        Properties success = load(result);
        require(
                success.getProperty("status", "").equals("passed"),
                "Missing successful runtime result");
        require(success.getProperty("flink.version", "").equals(version), "Wrong runtime result");
        require(
                success.getProperty("case.count", "")
                                .equals(Integer.toString(expectedCases(version).size()))
                        && success.getProperty("cases", "")
                                .equals(String.join(",", expectedCases(version))),
                "Incomplete runtime case inventory");
        require(
                success.getProperty("library.sha256", "")
                                .equals(hash(bundle.resolve("library.jar")))
                        && success.getProperty("consumer.sha256", "")
                                .equals(hash(bundle.resolve("tools.jar"))),
                "Result uses different compiled inputs");
    }

    static void checkUnchanged(Path bundle, String inventoryHash) throws Exception {
        require(
                hash(bundle.resolve("SHA256SUMS")).equals(inventoryHash),
                "Bundle inventory changed during read");
        check(bundle);
    }

    static Properties check(Path bundle) throws Exception {
        require(
                Files.isRegularFile(bundle.resolve("SHA256SUMS")),
                "Incomplete bundle: missing SHA256SUMS");
        TreeMap<String, String> expected = new TreeMap<>();
        for (String line : Files.readAllLines(bundle.resolve("SHA256SUMS"))) {
            require(line.matches("[0-9a-f]{64}  .+"), "Malformed checksum inventory");
            String name = line.substring(66);
            safeName(name);
            require(
                    !name.equals("SHA256SUMS") && expected.put(name, line.substring(0, 64)) == null,
                    "Duplicate inventory member");
        }
        TreeMap<String, String> actual = inventory(bundle);
        require(
                !expected.isEmpty() && expected.equals(actual),
                "Bundle checksum or file inventory mismatch");
        Properties manifest = load(bundle.resolve("manifest.properties"));
        require(
                manifest.getProperty("format.version", "").equals("1"),
                "Unsupported bundle format");
        require(
                manifest.getProperty("status", "").equals("development-tooling-baseline"),
                "Unsupported bundle status");
        require(
                manifest.getProperty("source.revision", "").matches("[0-9a-f]{40}"),
                "Missing source revision");
        String major = manifest.getProperty("protobuf.major", "");
        require(major.equals("3") || major.equals("4"), "Unsupported Protobuf profile");
        require(
                manifest.getProperty("protobuf.version", "")
                        .equals(major.equals("3") ? "3.25.9" : "4.33.6"),
                "Unsupported Protobuf runtime");
        for (String required :
                List.of(
                        "source.zip",
                        "library.jar",
                        "tools.jar",
                        "effective-pom.xml",
                        "apps/application-original.jar",
                        "apps/application-changed.jar",
                        "apps/original/schema.pb",
                        "apps/changed/schema.pb")) {
            require(expected.containsKey(required), "Missing required bundle input: " + required);
        }
        for (String version : runtimes(manifest.getProperty("line", ""))) {
            validateResult(
                    bundle, version, bundle.resolve("capture-" + version + "/result.properties"));
            for (String setting : List.of("default", "raised")) {
                require(
                        expected.containsKey(
                                "fixtures/" + version + "/snapshot-" + setting + ".properties"),
                        "Missing snapshot fixture");
            }
            for (String backend : List.of("hashmap", "rocksdb")) {
                for (String settings : List.of("baseline", "raised")) {
                    for (String kind : List.of("savepoint", "checkpoint")) {
                        String fixture =
                                "fixtures/"
                                        + version
                                        + "/"
                                        + backend
                                        + "/"
                                        + settings
                                        + "/"
                                        + kind
                                        + ".zip";
                        require(expected.containsKey(fixture), "Missing state fixture: " + fixture);
                        archiveNames(bundle.resolve(fixture));
                    }
                }
            }
        }
        archiveNames(bundle.resolve("source.zip"));
        System.out.println("Verified " + expected.size() + " retained files");
        return manifest;
    }

    static void seal(Path bundle) throws Exception {
        StringBuilder sums = new StringBuilder();
        inventory(bundle)
                .forEach((name, hash) -> sums.append(hash).append("  ").append(name).append('\n'));
        Files.writeString(bundle.resolve("SHA256SUMS"), sums, StandardOpenOption.CREATE_NEW);
    }

    static TreeMap<String, String> inventory(Path bundle) throws Exception {
        TreeMap<String, String> result = new TreeMap<>();
        for (Path file : files(bundle)) {
            String relative = bundle.relativize(file).toString().replace(File.separatorChar, '/');
            safeName(relative);
            if (!relative.equals("SHA256SUMS")) {
                result.put(relative, hash(file));
            }
        }
        return result;
    }

    static List<Path> files(Path directory) throws IOException {
        require(
                Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS),
                "Missing directory: " + directory);
        try (var walk = Files.walk(directory)) {
            List<Path> paths = walk.sorted().toList();
            for (Path path : paths) {
                require(!Files.isSymbolicLink(path), "Symbolic links are not supported");
                require(
                        Files.isDirectory(path) || Files.isRegularFile(path),
                        "Unsupported file type");
            }
            return paths.stream().filter(Files::isRegularFile).toList();
        }
    }

    static String hash(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(file)) {
            byte[] buffer = new byte[65536];
            for (int count; (count = input.read(buffer)) != -1; ) {
                digest.update(buffer, 0, count);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    static Path newDirectory(Path requested, Path excluded) throws IOException {
        Path absolute = requested.toAbsolutePath().normalize();
        Path real = absolute.getParent().toRealPath().resolve(absolute.getFileName());
        require(!real.startsWith(excluded.toRealPath()), "Output must be outside the input tree");
        return Files.createDirectory(real);
    }

    static void safeName(String name) {
        require(
                !name.isEmpty()
                        && !name.startsWith("/")
                        && !name.contains("\\")
                        && !name.contains(":")
                        && name.chars().noneMatch(Character::isISOControl),
                "Unsafe archive or inventory path");
        for (String part : name.split("/", -1)) {
            require(
                    !part.equals("..") && !part.equals(".") && !part.isEmpty(),
                    "Unsafe archive or inventory path");
        }
    }

    static Set<String> archiveNames(Path archive) throws IOException {
        Set<String> names = new HashSet<>();
        try (ZipFile zip = new ZipFile(archive.toFile())) {
            var entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                String name = entry.getName();
                safeName(entry.isDirectory() ? name.substring(0, name.length() - 1) : name);
                require(names.add(name), "Duplicate archive member");
            }
        }
        require(!names.isEmpty(), "Empty archive");
        return names;
    }

    static void zip(Path source, Path archive) throws Exception {
        try (ZipOutputStream output =
                new ZipOutputStream(
                        Files.newOutputStream(archive, StandardOpenOption.CREATE_NEW))) {
            for (Path file : files(source)) {
                String name = source.relativize(file).toString().replace(File.separatorChar, '/');
                safeName(name);
                ZipEntry entry = new ZipEntry(name);
                entry.setTime(0);
                output.putNextEntry(entry);
                Files.copy(file, output);
                output.closeEntry();
            }
        }
    }

    static void extract(Path archive, Path output) throws Exception {
        Set<String> remaining = archiveNames(archive);
        Files.createDirectory(output);
        try (ZipInputStream input = new ZipInputStream(Files.newInputStream(archive))) {
            for (ZipEntry entry; (entry = input.getNextEntry()) != null; ) {
                require(
                        remaining.remove(entry.getName()),
                        "Archive headers disagree or duplicate a member");
                Path target = output.resolve(entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    Files.copy(input, target);
                }
            }
        }
        require(remaining.isEmpty(), "Archive headers omit members");
    }

    static void deleteTree(Path path) throws IOException {
        // Flink can still delete discarded checkpoint files after job cancellation completes.
        Files.walkFileTree(
                path,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                            throws IOException {
                        Files.deleteIfExists(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException failure)
                            throws IOException {
                        if (!(failure instanceof NoSuchFileException)) {
                            throw failure;
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path directory, IOException failure)
                            throws IOException {
                        if (failure != null && !(failure instanceof NoSuchFileException)) {
                            throw failure;
                        }
                        Files.deleteIfExists(directory);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    static Properties load(Path path) throws IOException {
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            properties.load(input);
        }
        return properties;
    }

    static void save(Properties properties, Path path) throws IOException {
        try (OutputStream output = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)) {
            properties.store(output, "Development tooling evidence; not a published artifact");
        }
    }

    private static void copy(Path source, Path destination) throws IOException {
        Files.createDirectories(destination.getParent());
        Files.copy(source, destination);
    }

    private static Document xml(Path file) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        return factory.newDocumentBuilder().parse(file.toFile());
    }

    private static String property(Document model, String name) {
        return directChild((Element) model.getElementsByTagName("properties").item(0), name);
    }

    private static String directChild(Element parent, String name) {
        for (var node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (node.getNodeName().equals(name)) {
                return node.getTextContent();
            }
        }
        throw new IllegalArgumentException("Missing effective model property: " + name);
    }

    private static String git(String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of("git"));
        command.addAll(List.of(args));
        Process process =
                new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        String result =
                new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
        require(process.waitFor() == 0, "Git command failed");
        return result;
    }

    private static void command(Path directory, String name, List<String> command)
            throws Exception {
        Properties arguments = new Properties();
        for (int i = 0; i < command.size(); i++) {
            arguments.setProperty("argv." + i, command.get(i));
        }
        arguments.setProperty("cwd", Path.of("").toAbsolutePath().normalize().toString());
        arguments.setProperty("java.home", System.getProperty("java.home"));
        save(arguments, directory.resolve(name + "-command.properties"));
        ProcessBuilder builder =
                new ProcessBuilder(command)
                        .redirectErrorStream(true)
                        .redirectOutput(directory.resolve(name + ".log").toFile());
        builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        Process process = builder.start();
        if (!process.waitFor(30, TimeUnit.MINUTES)) {
            process.destroyForcibly();
            throw new IOException("Timed out: " + name);
        }
        require(
                process.exitValue() == 0,
                "Failed " + name + "; see " + directory.resolve(name + ".log"));
    }

    static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
