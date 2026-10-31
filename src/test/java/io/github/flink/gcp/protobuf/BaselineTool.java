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
import java.net.URI;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
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
    /**
     * Bundle format that capture writes. Format 1 is the v0.1.0 layout without evolved state, and
     * format 2 the v0.2.0 layout without DynamicMessage state.
     */
    static final String FORMAT = "3";

    /** Status of a bundle that a development build wrote. */
    static final String DEVELOPMENT = "development-tooling-baseline";

    /** Status of a bundle whose library is a version published on Maven Central. */
    static final String PUBLISHED = "published-release-baseline";

    /** Maven Central, from which a published capture downloads the release jar. */
    static final String CENTRAL = "https://repo1.maven.org/maven2";

    private static final int DOWNLOAD_TIMEOUT_MILLIS = 60_000;

    private static final String COORDINATES = "io.github.flink-gcp:flink-datastream-protobuf:";

    private BaselineTool() {}

    public static void main(String[] args) throws Exception {
        if ((args.length == 4 || args.length == 5) && args[0].equals("capture")) {
            capture(args[1], args[2], Path.of(args[3]), args.length == 5 ? args[4] : null);
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
        } else if (args.length == 5 && args[0].equals("consume")) {
            consume(
                    Path.of(args[1]).toRealPath(),
                    Path.of(args[2]).toRealPath(),
                    args[3],
                    Path.of(args[4]));
        } else {
            throw new IllegalArgumentException(
                    "Expected capture <flink2|flink1> <3|4> <new-output> [<published-version>], check <bundle>, read <bundle> <flink-version> <new-results>, or consume <bundle> <candidate-jar> <flink-version> <new-results>");
        }
    }

    static List<String> runtimes(String line) {
        return switch (line) {
            case "flink2" -> List.of("2.2.1", "2.3.0");
            case "flink1" -> List.of("1.20.5");
            default -> throw new IllegalArgumentException("Unsupported artifact line");
        };
    }

    static void validateJava(String line, int version) {
        require(
                version == 17 || (line.equals("flink2") && version == 21),
                "Unsupported JDK for artifact line");
    }

    /**
     * Returns the release tag of a published version, which must belong to the artifact line:
     * {@code X.Y.Z} to flink2 and {@code X.Y.Z-1.20} to flink1.
     */
    static String releaseTag(String line, String version) {
        require(
                version.matches("(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-1\\.20)?"),
                "Expected a released X.Y.Z or X.Y.Z-1.20 version");
        require(
                version.endsWith("-1.20") == line.equals("flink1"),
                "The published version belongs to the other artifact line");
        return "v" + version.substring(0, version.length() - (line.equals("flink1") ? 5 : 0));
    }

    /** Returns the URL of a published version's jar in a Maven repository. */
    static String publishedUrl(String repository, String version) {
        return repository
                + "/io/github/flink-gcp/flink-datastream-protobuf/"
                + version
                + "/flink-datastream-protobuf-"
                + version
                + ".jar";
    }

    /**
     * Writes the published jar as the bundle's library and its signature beside it, after checking
     * the jar against the SHA-1 checksum that the repository publishes; a failed download or check
     * leaves neither file. Returns the jar's URL.
     */
    static String fetchPublished(String repository, String version, Path bundle) throws Exception {
        String url = publishedUrl(repository, version);
        Path jar = Files.createTempFile(bundle, ".library", ".jar");
        Path signature = Files.createTempFile(bundle, ".library", ".asc");
        Path checksum = Files.createTempFile(bundle, ".library", ".sha1");
        try {
            download(url, jar);
            download(url + ".asc", signature);
            download(url + ".sha1", checksum);
            String[] expected =
                    Files.readString(checksum, StandardCharsets.US_ASCII).strip().split("\\s+");
            require(
                    hash(jar, "SHA-1").equals(expected[0]),
                    "The downloaded library does not match its published SHA-1 checksum");
            Files.move(jar, bundle.resolve("library.jar"));
            Files.move(signature, bundle.resolve("library.jar.asc"));
        } finally {
            Files.deleteIfExists(jar);
            Files.deleteIfExists(signature);
            Files.delete(checksum);
        }
        return url;
    }

    private static void download(String url, Path target) throws Exception {
        URLConnection connection = URI.create(url).toURL().openConnection();
        connection.setConnectTimeout(DOWNLOAD_TIMEOUT_MILLIS);
        connection.setReadTimeout(DOWNLOAD_TIMEOUT_MILLIS);
        try (InputStream input = connection.getInputStream()) {
            Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /** Requires the annotated release tag to name the checked-out commit of the repository. */
    static void requireReleaseCommit(Path repository, String tag) throws Exception {
        require(
                git(repository, "cat-file", "-t", "refs/tags/" + tag).equals("tag"),
                "A published capture needs the annotated release tag " + tag);
        require(
                git(repository, "rev-parse", "refs/tags/" + tag + "^{commit}")
                        .equals(git(repository, "rev-parse", "HEAD")),
                "A published capture must run on the commit of " + tag);
    }

    private static void capture(String line, String major, Path destination, String published)
            throws Exception {
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
        String tag = published == null ? null : releaseTag(line, published);
        if (tag != null) {
            requireReleaseCommit(repository, tag);
        }
        Path bundle = newDirectory(destination, repository);
        Properties manifest = new Properties();
        manifest.setProperty("format.version", FORMAT);
        manifest.setProperty("status", tag == null ? DEVELOPMENT : PUBLISHED);
        if (tag == null) {
            manifest.setProperty("milestone.final", "false");
        } else {
            // Before the build, so that a version that Central does not serve yet fails first.
            manifest.setProperty("release.tag", tag);
            manifest.setProperty("library.url", fetchPublished(CENTRAL, published, bundle));
        }
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
                protobuf.equals(major.equals("3") ? "3.25.9" : "4.36.2"),
                "Unexpected Protobuf runtime");
        require(property(model, "protoc.version").equals(protobuf), "Unpaired protoc/runtime");
        require(
                property(model, "flink.version").equals(versions.get(0)),
                "Unexpected compile floor");
        require(property(model, "flink.compat").equals(line), "Unexpected Flink adapter");
        manifest.setProperty(
                "writer.artifact", COORDINATES + (tag == null ? artifactVersion : published));
        manifest.setProperty("protobuf.version", protobuf);
        manifest.setProperty("protoc.version", protobuf);
        manifest.setProperty("compile.flink", versions.get(0));
        if (tag == null) {
            copy(
                    Path.of("target/flink-datastream-protobuf-" + artifactVersion + ".jar"),
                    bundle.resolve("library.jar"));
        }
        // A published jar writes the fixtures; the tagged build supplies the applications and
        // tools.
        copy(Path.of("target/baseline/tools.jar"), bundle.resolve("tools.jar"));
        copy(
                Path.of("target/runtime-app/application-dynamic.jar"),
                bundle.resolve("apps/application-dynamic.jar"));
        // Name every variant that pom.xml packages so that a missing jar fails the capture.
        for (String variant :
                List.of(
                        "original",
                        "changed",
                        "reserved",
                        "incompatible",
                        "enum-original",
                        "enum-added")) {
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
        System.out.println(
                (tag == null
                                ? "Captured development tooling bundle: "
                                : "Captured published release bundle: ")
                        + bundle);
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

    /**
     * Runs the retained compiled consumers against a candidate library without recompiling them.
     *
     * <p>The bundle's dependency jars were resolved on the capture host, so the current tree
     * resolves the runtime dependencies for the bundle's adapter line, Protobuf profile, and Flink
     * version.
     */
    private static void consume(Path bundle, Path candidate, String version, Path destination)
            throws Exception {
        Properties manifest = check(bundle);
        String inventoryHash = hash(bundle.resolve("SHA256SUMS"));
        String line = manifest.getProperty("line");
        require(runtimes(line).contains(version), "Unsupported Flink runtime");
        validateJava(line, Runtime.version().feature());
        require(
                Files.isRegularFile(candidate) && candidate.toString().endsWith(".jar"),
                "Missing candidate library jar");
        Path repository = Path.of(git("rev-parse", "--show-toplevel")).toRealPath();
        require(
                Path.of("").toRealPath().equals(repository),
                "Consume must start at the repository root");
        Path output = newDirectory(destination, bundle, repository);
        try {
            command(
                    output,
                    "dependencies",
                    maven(
                            line,
                            manifest.getProperty("protobuf.major"),
                            version,
                            "dependency:copy-dependencies",
                            // The retained instruments require their own Protobuf runtime.
                            "-Dprotobuf.version=" + manifest.getProperty("protobuf.version"),
                            "-DincludeScope=test",
                            "-Dmdep.useRepositoryLayout=true",
                            "-DoutputDirectory=" + output.resolve("dependencies")));
            // Retained instruments find the library and manifest through their bundle argument.
            Path view = Files.createDirectory(output.resolve("candidate"));
            copy(bundle.resolve("manifest.properties"), view.resolve("manifest.properties"));
            copy(candidate, view.resolve("library.jar"));
            Path results = Files.createDirectory(output.resolve("results"));
            instruments(
                    view,
                    classpath(bundle.resolve("tools.jar"), view, output.resolve("dependencies")),
                    version,
                    results);
            List<String> cases = compiledCases(version);
            Properties result = new Properties();
            result.setProperty("status", "passed");
            result.setProperty("flink.version", version);
            result.setProperty("protobuf.version", manifest.getProperty("protobuf.version"));
            result.setProperty("java.version", System.getProperty("java.runtime.version"));
            result.setProperty("baseline.source.revision", manifest.getProperty("source.revision"));
            result.setProperty("baseline.library.sha256", manifest.getProperty("library.sha256"));
            result.setProperty("consumer.sha256", hash(bundle.resolve("tools.jar")));
            result.setProperty("library.sha256", hash(view.resolve("library.jar")));
            // The jar may come from elsewhere; this identifies the checkout that resolved the
            // classpath.
            result.setProperty("checkout.source.revision", git("rev-parse", "HEAD"));
            result.setProperty(
                    "checkout.source.clean",
                    Boolean.toString(git("status", "--porcelain").isEmpty()));
            result.setProperty("case.count", Integer.toString(cases.size()));
            result.setProperty("cases", String.join(",", cases));
            save(result, output.resolve("result.properties"));
            System.out.println("Retained compiled consumers passed on the candidate: " + output);
        } finally {
            checkUnchanged(bundle, inventoryHash);
        }
    }

    private static List<String> classpath(Path tools, Path bundle, Path dependencies)
            throws IOException {
        List<String> classpath =
                new ArrayList<>(
                        List.of(tools.toString(), bundle.resolve("library.jar").toString()));
        for (Path jar : files(dependencies)) {
            if (jar.toString().endsWith(".jar")) {
                classpath.add(jar.toString());
            }
        }
        require(classpath.size() > 2, "Missing runtime dependencies");
        return classpath;
    }

    /** Runs the compiled consumer and transport checks, plus the LTS legacy check, on a library. */
    private static void instruments(
            Path bundle, List<String> classpath, String version, Path results) throws Exception {
        String library = hash(bundle.resolve("library.jar"));
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
        for (String instrument : instruments(version)) {
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
        // The consumer hashes the loaded library; the other instruments assert its location.
        List<String> checked = new ArrayList<>(List.of("consumer"));
        checked.addAll(instruments(version));
        for (String instrument : checked) {
            Properties result = load(results.resolve(instrument + ".properties"));
            require(
                    result.getProperty("status", "").equals("passed")
                            && result.getProperty("library.sha256", "").equals(library),
                    "Compiled " + instrument + " check did not pass on the selected library");
        }
    }

    private static void runtime(Path bundle, String version, String mode, Path results)
            throws Exception {
        List<String> classpath =
                classpath(
                        bundle.resolve("tools.jar"),
                        bundle,
                        bundle.resolve("dependencies/" + version));
        instruments(bundle, classpath, version, results);
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
        validateResult(
                bundle,
                version,
                load(bundle.resolve("manifest.properties")).getProperty("format.version"),
                results.resolve("result.properties"));
    }

    /** Separate-JVM instruments after the compiled consumer; LTS adds the legacy entry point. */
    private static List<String> instruments(String version) {
        return version.startsWith("1.20.") ? List.of("transport", "legacy") : List.of("transport");
    }

    private static List<String> compiledCases(String version) {
        List<String> cases = new ArrayList<>(List.of("compiled-consumer", "registered-transport"));
        if (version.startsWith("1.20.")) {
            cases.add("legacy-entry-point");
        }
        return cases;
    }

    /**
     * Returns the state fixtures under each backend: the original schema at baseline and raised
     * limits, then from format 2 state that the changed schema wrote after restoring original
     * state, and state of the enum-bearing schema, and from format 3 DynamicMessage state of the
     * original schema.
     */
    static List<String> stateFixtures(String format) {
        return switch (format) {
            case "1" -> List.of("baseline", "raised");
            case "2" -> List.of("baseline", "raised", "evolved", "enum");
            default -> List.of("baseline", "raised", "evolved", "enum", "dynamic");
        };
    }

    /**
     * Returns the serializer snapshot fixtures: generated messages at default and raised settings,
     * and from format 3 a DynamicMessage type at default settings.
     */
    static List<String> snapshotFixtures(String format) {
        return switch (format) {
            case "1", "2" -> List.of("default", "raised");
            default -> List.of("default", "raised", "dynamic");
        };
    }

    static List<String> expectedCases(String version, String format) {
        List<String> cases = compiledCases(version);
        for (String name : snapshotFixtures(format)) {
            cases.add("snapshot-" + name);
        }
        for (String backend : List.of("hashmap", "rocksdb")) {
            for (String fixture : stateFixtures(format)) {
                for (String kind : List.of("checkpoint", "savepoint")) {
                    cases.add(backend + "/" + fixture + "/" + kind);
                }
            }
        }
        return cases;
    }

    private static void validateResult(Path bundle, String version, String format, Path result)
            throws Exception {
        Properties success = load(result);
        require(
                success.getProperty("status", "").equals("passed"),
                "Missing successful runtime result");
        require(success.getProperty("flink.version", "").equals(version), "Wrong runtime result");
        require(
                success.getProperty("case.count", "")
                                .equals(Integer.toString(expectedCases(version, format).size()))
                        && success.getProperty("cases", "")
                                .equals(String.join(",", expectedCases(version, format))),
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
        String format = manifest.getProperty("format.version", "");
        require(List.of("1", "2", FORMAT).contains(format), "Unsupported bundle format");
        String status = manifest.getProperty("status", "");
        require(
                status.equals(DEVELOPMENT) || status.equals(PUBLISHED),
                "Unsupported bundle status");
        if (status.equals(PUBLISHED)) {
            String artifact = manifest.getProperty("writer.artifact", "");
            require(artifact.startsWith(COORDINATES), "Published bundle does not name its release");
            String version = artifact.substring(COORDINATES.length());
            require(
                    manifest.getProperty("release.tag", "")
                                    .equals(releaseTag(manifest.getProperty("line", ""), version))
                            && manifest.getProperty("library.url", "")
                                    .equals(publishedUrl(CENTRAL, version)),
                    "Published bundle does not name its release");
        }
        require(
                manifest.getProperty("source.revision", "").matches("[0-9a-f]{40}"),
                "Missing source revision");
        String major = manifest.getProperty("protobuf.major", "");
        require(major.equals("3") || major.equals("4"), "Unsupported Protobuf profile");
        // Retained bundles keep the runtime that wrote them; accept any supported release.
        require(
                manifest.getProperty("protobuf.version", "")
                        .matches(major.equals("3") ? "3\\.25\\.\\d+" : "4\\.\\d+\\.\\d+"),
                "Unsupported Protobuf runtime");
        List<String> required =
                new ArrayList<>(
                        List.of("source.zip", "library.jar", "tools.jar", "effective-pom.xml"));
        // The v0.1.0 layout retains only the two variants that its fixtures use.
        for (String variant :
                format.equals("1")
                        ? List.of("original", "changed")
                        : List.of("original", "changed", "enum-original", "enum-added")) {
            required.add("apps/application-" + variant + ".jar");
            required.add("apps/" + variant + "/schema.pb");
        }
        if (stateFixtures(format).contains("dynamic")) {
            required.add("apps/application-dynamic.jar");
        }
        if (status.equals(PUBLISHED)) {
            required.add("library.jar.asc");
        }
        for (String input : required) {
            require(expected.containsKey(input), "Missing required bundle input: " + input);
        }
        for (String version : runtimes(manifest.getProperty("line", ""))) {
            validateResult(
                    bundle,
                    version,
                    format,
                    bundle.resolve("capture-" + version + "/result.properties"));
            for (String setting : snapshotFixtures(format)) {
                require(
                        expected.containsKey(
                                "fixtures/" + version + "/snapshot-" + setting + ".properties"),
                        "Missing snapshot fixture");
            }
            for (String backend : List.of("hashmap", "rocksdb")) {
                for (String name : stateFixtures(format)) {
                    for (String kind : List.of("savepoint", "checkpoint")) {
                        String fixture =
                                "fixtures/"
                                        + version
                                        + "/"
                                        + backend
                                        + "/"
                                        + name
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
        return hash(file, "SHA-256");
    }

    static String hash(Path file, String algorithm) throws Exception {
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        try (InputStream input = Files.newInputStream(file)) {
            byte[] buffer = new byte[65536];
            for (int count; (count = input.read(buffer)) != -1; ) {
                digest.update(buffer, 0, count);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    static Path newDirectory(Path requested, Path... excluded) throws IOException {
        Path absolute = requested.toAbsolutePath().normalize();
        Path real = absolute.getParent().toRealPath().resolve(absolute.getFileName());
        for (Path tree : excluded) {
            require(!real.startsWith(tree.toRealPath()), "Output must be outside the input tree");
        }
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
            properties.store(output, "Baseline tooling evidence");
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
        return git(Path.of("."), args);
    }

    private static String git(Path directory, String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of("git", "-C", directory.toString()));
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
