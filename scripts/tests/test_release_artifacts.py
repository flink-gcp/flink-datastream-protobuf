# Copyright 2026 The flink-gcp authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Synthetic bundles and a stub `just` only: no Maven build, signing key, or Central access."""

import hashlib
import io
import os
from pathlib import Path
import subprocess
import warnings
import zipfile

import pytest

from conftest import load_script

release = load_script("release-artifacts.py")

VERSION = "1.0.0"
LTS = VERSION + "-1.20"
PACKAGE = "io/github/flink/gcp/protobuf/"
SOURCES = {"src/main/java/" + PACKAGE + "Api.java": b"class Api {}",
           "src/main/java-flink2/" + PACKAGE + "Adapter.java": b"class Adapter {} // 2.x",
           "src/main/java-flink1/" + PACKAGE + "Adapter.java": b"class Adapter {} // 1.20"}
NOTICES = {"LICENSE": b"Apache License", "NOTICE": b"The flink-gcp authors"}
MAVEN_METADATA = {"META-INF/maven/io.github.flink-gcp/flink-datastream-protobuf/pom.xml": b"<project/>",
                  "META-INF/maven/io.github.flink-gcp/flink-datastream-protobuf/pom.properties": b"version=1"}
STUB = Path(__file__).with_name("release-just-stub.sh")
# The checksums central-publishing-maven-plugin 0.12.0 writes beside each staged file.
CHECKSUMS = {".md5": "md5", ".sha1": "sha1", ".sha256": "sha256", ".sha512": "sha512"}
POM = """<project xmlns="http://maven.apache.org/POM/4.0.0">
  <groupId>io.github.flink-gcp</groupId><artifactId>flink-datastream-protobuf</artifactId><version>{version}</version>
  <name>n</name><description>d</description><url>u</url>
  <licenses><license><name>Apache</name></license></licenses>
  <developers><developer><name>The flink-gcp authors</name></developer></developers>
  <scm><url>u</url><connection>c</connection></scm>
  <dependencies>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-core</artifactId><scope>provided</scope></dependency>
    {dependency}
  </dependencies>
  <profiles><profile><id>chill</id><dependencies>
    <dependency><groupId>com.twitter</groupId><artifactId>chill-protobuf</artifactId><scope>test</scope></dependency>
    {profile_dependency}
  </dependencies></profile></profiles>
</project>"""


class ClassFile:
    """Builds a class file whose constant pool holds every constant kind a compiler emits."""

    def __init__(self, name):
        self.pool = []
        self.this = self.class_ref(name)
        self.pool += [b"\x03" + bytes(4), b"\x05" + bytes(8), b""]  # a long occupies two slots
        self.add(15, bytes(3))
        self.add(17, bytes(4))
        self.add(18, bytes(4))

    def add(self, tag, body):
        self.pool.append(bytes([tag]) + body)
        return len(self.pool)

    def utf8(self, text):
        data = text.encode()
        return self.add(1, len(data).to_bytes(2, "big") + data)

    def class_ref(self, name, index=None):
        return self.add(7, (index or self.utf8(name)).to_bytes(2, "big"))

    def literal(self, text):
        index = self.utf8(text)
        self.add(8, index.to_bytes(2, "big"))
        return index

    def member(self, owner, name, descriptor="()V", tag=10):
        owner_index = self.class_ref(owner)
        nat = self.add(12, self.utf8(name).to_bytes(2, "big") + self.utf8(descriptor).to_bytes(2, "big"))
        return self.add(tag, owner_index.to_bytes(2, "big") + nat.to_bytes(2, "big"))

    def bytes(self, major=61):
        return (b"\xca\xfe\xba\xbe" + bytes(2) + major.to_bytes(2, "big")
                + (len(self.pool) + 1).to_bytes(2, "big") + b"".join(self.pool) + bytes(10))


def class_file(name, *types, major=61):
    built = ClassFile(name)
    built.member("org/apache/flink/api/common/typeinfo/TypeInformation", "getArity", "()I")
    built.member("java/lang/String", "CASE_INSENSITIVE_ORDER", "Ljava/util/Comparator;", tag=9)
    for type_name in types:
        built.class_ref(type_name)
    return built.bytes(major)


def archive(entries):
    buffer = io.BytesIO()
    with warnings.catch_warnings(), zipfile.ZipFile(buffer, "w") as output:
        warnings.simplefilter("ignore", UserWarning)  # repeated names are deliberate in one test
        for name, data in entries:
            output.writestr(name, data)
    return buffer.getvalue()


def sources_for(version):
    compat = "flink1" if version.endswith("-1.20") else "flink2"
    return {name.split("/", 3)[3]: data for name, data in SOURCES.items()
            if name.startswith(("src/main/java/", f"src/main/java-{compat}/"))}


def artifacts(version=VERSION, dependency="", profile_dependency=""):
    notices = {f"META-INF/{name}": data for name, data in NOTICES.items()}
    classes = {PACKAGE + "Api.class": class_file(PACKAGE + "Api"),
               PACKAGE + "Api$Inner.class": class_file(PACKAGE + "Api$Inner"),
               PACKAGE + "Adapter.class": class_file(PACKAGE + "Adapter")}
    manifest = {"META-INF/MANIFEST.MF": b"Manifest-Version: 1.0\n"}
    return {"pom": POM.format(version=version, dependency=dependency, profile_dependency=profile_dependency).encode(),
            "jar": {**manifest, **classes, **notices},
            "sources": {**manifest, **sources_for(version), **notices, **MAVEN_METADATA},
            "javadoc": {"index.html": b"<html/>", PACKAGE + "package-summary.html": b"<html/>"}}


def base_name(version=VERSION):
    return f"io/github/flink-gcp/flink-datastream-protobuf/{version}/flink-datastream-protobuf-{version}"


def bundle(parts, version=VERSION, signed=True, extra=None, drop=()):
    base = base_name(version)
    files = {base + ".pom": parts["pom"], base + ".jar": archive(parts["jar"].items()),
             base + "-sources.jar": archive(parts["sources"].items()),
             base + "-javadoc.jar": archive(parts["javadoc"].items())}
    for name, data in list(files.items()):
        if signed:
            files[name + ".asc"] = b"-----BEGIN PGP SIGNATURE-----"
        for suffix, algorithm in CHECKSUMS.items():
            files[name + suffix] = hashlib.new(algorithm, data).hexdigest().encode()
    files.update(extra or {})
    for suffix in drop:
        del files[base + suffix]
    return archive(files.items())


@pytest.fixture
def source(tmp_path):
    root = tmp_path / "source"
    for name, data in {**SOURCES, **NOTICES}.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_bytes(data)
    return root


@pytest.fixture
def inspect(tmp_path, source):
    def run(data, version=VERSION, unsigned=False):
        path = tmp_path / "bundle.zip"
        path.write_bytes(data)
        return release.inspect_bundle(path, version, source, unsigned)
    return run


def with_class(inspect, data, name=PACKAGE + "Api.class"):
    parts = artifacts()
    parts["jar"][name] = data
    return inspect(bundle(parts))


def assert_defect(errors, fragment):
    assert any(fragment in error for error in errors), errors


@pytest.mark.parametrize("version", [VERSION, LTS])
def test_accepts_both_lines_signed_and_unsigned(inspect, version):
    assert inspect(bundle(artifacts(version), version), version) == []
    assert inspect(bundle(artifacts(version), version, signed=False), version, unsigned=True) == []


def test_requires_signatures_and_artifacts(inspect):
    assert_defect(inspect(bundle(artifacts(), signed=False)), "has no flink-datastream-protobuf-1.0.0.jar.asc")
    assert_defect(inspect(bundle(artifacts(), drop=("-javadoc.jar",))), "has no flink-datastream-protobuf-1.0.0-javadoc.jar")


@pytest.mark.parametrize("suffix", [".md5", ".sha1"])
def test_requires_artifact_checksums(inspect, suffix):
    assert_defect(inspect(bundle(artifacts(), drop=(".pom" + suffix,))), f".pom has no {suffix} checksum")


@pytest.mark.parametrize("suffix", list(CHECKSUMS))
def test_rejects_wrong_checksums(inspect, suffix):
    errors = inspect(bundle(artifacts(), extra={base_name() + ".jar" + suffix: b"0" * 32}))
    assert_defect(errors, f".jar{suffix} does not match")


def test_reports_content_defects_alongside_checksum_defects(inspect):
    parts = artifacts()
    parts["jar"][PACKAGE + "ApiTest.class"] = class_file(PACKAGE + "ApiTest")
    errors = inspect(bundle(parts, extra={base_name() + ".pom.md5": b"0" * 32}))
    assert_defect(errors, ".pom.md5 does not match")
    assert_defect(errors, "non-production entry " + PACKAGE + "ApiTest.class")


@pytest.mark.parametrize("name", ["io/github/flink-gcp/flink-datastream-protobuf/1.0.0/_remote.repositories",
                                  "io/github/flink-gcp/flink-datastream-protobuf/maven-metadata-local.xml",
                                  base_name(LTS) + ".pom"])
def test_rejects_local_repository_and_other_line_files(inspect, name):
    assert_defect(inspect(bundle(artifacts(), extra={name: b"x"})), "unexpected file " + name)


def test_rejects_a_bundle_of_the_other_line(inspect):
    assert_defect(inspect(bundle(artifacts(LTS), LTS)), "bundle has no flink-datastream-protobuf-1.0.0.pom")


def test_rejects_unreadable_and_repeated_archives(inspect):
    assert inspect(b"not a zip") == ["bundle is not a readable archive", *(
        f"bundle has no flink-datastream-protobuf-1.0.0{suffix}"
        for suffix in (".pom", ".jar", "-sources.jar", "-javadoc.jar", ".pom.asc", ".jar.asc",
                       "-sources.jar.asc", "-javadoc.jar.asc"))]
    parts = artifacts()
    jar = archive([*parts["jar"].items(), (PACKAGE + "Api.class", class_file(PACKAGE + "Api", "com/twitter/chill/Foo"))])
    base = base_name()
    data = bundle(parts, extra={base + ".jar": jar, **{base + ".jar" + suffix: hashlib.new(algorithm, jar).hexdigest().encode()
                                                       for suffix, algorithm in CHECKSUMS.items()}})
    assert_defect(inspect(data), "jar repeats entry " + PACKAGE + "Api.class")


@pytest.mark.parametrize("name", [PACKAGE + "ApiTest.class", PACKAGE + "probe/ChillProbe.class",
                                  "com/google/protobuf/Message.class", PACKAGE + "Api$shaded/com/google/protobuf/Message.class",
                                  PACKAGE + "runtime.proto", "META-INF/services/org.example.Service"])
def test_rejects_non_production_jar_entries(inspect, name):
    assert_defect(with_class(inspect, class_file(name.removesuffix(".class")), name), "non-production entry " + name)


@pytest.mark.parametrize("name", ["Adapter", "Api"])  # Api keeps its nested Api$Inner
def test_requires_every_production_class(inspect, name):
    parts = artifacts()
    del parts["jar"][PACKAGE + name + ".class"]
    assert_defect(inspect(bundle(parts)), "no class for " + PACKAGE + name + ".java")


@pytest.mark.parametrize("major", [55, 65])
def test_requires_java_17_bytecode(inspect, major):
    assert_defect(with_class(inspect, class_file(PACKAGE + "Api", major=major)), f"major {major}")


def test_reports_an_unreadable_class(inspect):
    assert_defect(with_class(inspect, class_file(PACKAGE + "Api")[:40]), "is unreadable")


@pytest.mark.parametrize("type_name", ["com/esotericsoftware/kryo/Kryo", "com/twitter/chill/protobuf/ProtobufSerializer",
                                       "org/apache/flink/api/java/typeutils/runtime/kryo/KryoSerializer",
                                       "org/apache/flink/api/java/typeutils/GenericTypeInfo"])
def test_rejects_kryo_chill_and_generic_type_references(inspect, type_name):
    assert_defect(with_class(inspect, class_file(PACKAGE + "Api", type_name)), "references " + type_name)


def test_rejects_flink_kryo_members(inspect):
    built = ClassFile(PACKAGE + "Api")
    built.member("org/apache/flink/api/common/serialization/SerializerConfig", "registerKryoType", "(Ljava/lang/Class;)V")
    assert_defect(with_class(inspect, built.bytes()), "SerializerConfig.registerKryoType")


def test_dotted_messages_and_own_names_are_not_references(inspect):
    built = ClassFile(PACKAGE + "Api")
    built.literal("never falls back to Kryo or com.twitter.chill.Foo")
    built.member(PACKAGE + "Api", "rejectsKryoFallback")
    assert with_class(inspect, built.bytes()) == []


def test_declared_descriptors_count_even_when_shared_with_a_literal(inspect):
    # A declared field's descriptor lives outside the constant pool's references and may share a literal's entry.
    built = ClassFile(PACKAGE + "Api")
    built.literal("Lcom/esotericsoftware/kryo/Kryo;")
    assert_defect(with_class(inspect, built.bytes()), "references Lcom/esotericsoftware/kryo/Kryo;")


@pytest.mark.parametrize("part, label", [("jar", "jar"), ("sources", "sources jar")])
def test_requires_tracked_notices(inspect, part, label):
    parts = artifacts()
    parts[part]["META-INF/NOTICE"] = b"other"
    assert f"{label} META-INF/NOTICE differs from the tracked NOTICE" in inspect(bundle(parts))
    del parts[part]["META-INF/LICENSE"]
    assert f"{label} has no META-INF/LICENSE" in inspect(bundle(parts))


def test_sources_must_be_the_lines_production_sources(inspect):
    parts = artifacts()
    parts["sources"][PACKAGE + "Adapter.java"] = SOURCES["src/main/java-flink1/" + PACKAGE + "Adapter.java"]
    assert_defect(inspect(bundle(parts)), "Adapter.java differs")
    parts = artifacts()
    parts["sources"][PACKAGE + "ApiTest.java"] = b"test"
    assert_defect(inspect(bundle(parts)), "non-production entry " + PACKAGE + "ApiTest.java")
    del parts["sources"][PACKAGE + "Api.java"]
    assert_defect(inspect(bundle(parts)), "sources jar has no " + PACKAGE + "Api.java")


@pytest.mark.parametrize("name", ["index.html", PACKAGE + "package-summary.html"])
def test_requires_generated_api_documentation(inspect, name):
    parts = artifacts()
    del parts["javadoc"][name]
    assert_defect(inspect(bundle(parts)), "javadoc jar has no " + name)


COMPILE_SCOPE = "<dependency><groupId>org.example</groupId><artifactId>lib</artifactId></dependency>"
RUNTIME_SCOPE = "<dependency><groupId>org.example</groupId><artifactId>lib</artifactId><scope>runtime</scope></dependency>"


@pytest.mark.parametrize("dependencies, fragment", [({"dependency": COMPILE_SCOPE}, "org.example:lib has scope compile"),
                                                    ({"dependency": RUNTIME_SCOPE}, "org.example:lib has scope runtime"),
                                                    ({"profile_dependency": COMPILE_SCOPE}, "org.example:lib has scope compile")])
def test_pom_dependencies_do_not_reach_consumers(inspect, dependencies, fragment):
    assert_defect(inspect(bundle(artifacts(**dependencies))), fragment)


@pytest.mark.parametrize("old, new, fragment", [
    (b"<version>1.0.0</version>", b"<version>1.0.1</version>", "POM version is '1.0.1'"),
    (b"<groupId>io.github.flink-gcp</groupId>", b"<groupId>org.example</groupId>", "POM groupId"),
    (b"<artifactId>flink-datastream-protobuf</artifactId>", b"<artifactId>x</artifactId>", "POM artifactId"),
    (b"<name>n</name>", b"<name>n</name><packaging>pom</packaging>", "POM packaging is 'pom'"),
    (b"<licenses>", b"<x>", "POM has no licenses/license/name"),
    (b"<developers>", b"<x>", "POM has no developers/developer/name"),
    (b"<connection>c</connection>", b"", "POM has no scm/connection"),
    (b"<scm><url>u</url>", b"<scm>", "POM has no scm/url"),
    (b"<description>d</description>", b"", "POM has no description")])
def test_pom_coordinates_and_metadata(inspect, old, new, fragment):
    parts = artifacts()
    pom = parts["pom"].replace(old, new, 1)
    for element in (b"licenses", b"developers"):
        if old == b"<" + element + b">":
            pom = pom.replace(b"</" + element + b">", b"</x>")
    parts["pom"] = pom
    assert_defect(inspect(bundle(parts)), fragment)


class Rehearsal:
    """Runs rehearse in a temporary Git repository with the stub in place of just."""

    def __init__(self, root, monkeypatch):
        self.root, self.monkeypatch = root, monkeypatch
        repository = root / "repository"
        for name, data in {**SOURCES, **NOTICES}.items():
            (repository / name).parent.mkdir(parents=True, exist_ok=True)
            (repository / name).write_bytes(data)
        git = ["git", "-c", "user.name=test", "-c", "user.email=test@invalid", "-c", "commit.gpgsign=false"]
        subprocess.run(["git", "init", "-q", str(repository)], check=True)
        subprocess.run(git + ["-C", str(repository), "add", "."], check=True)
        subprocess.run(git + ["-C", str(repository), "commit", "-q", "-m", "fixture"], check=True)
        tools, self.bundles = root / "tools", root / "prepared"
        tools.mkdir()
        self.bundles.mkdir()
        (tools / "just").symlink_to(STUB)
        self.calls, self.summary = root / "calls", root / "summary.md"
        for name, value in {"PATH": f"{tools}{os.pathsep}{os.environ['PATH']}", "STUB_CALLS": str(self.calls),
                            "STUB_BUNDLES": str(self.bundles), "STUB_GUARD_STATUS": "1",
                            "STUB_GUARD_OUTPUT": release.GUARD_MESSAGE, "GITHUB_STEP_SUMMARY": str(self.summary),
                            "MAVEN_GPG_KEY": "", "MAVEN_GPG_PASSPHRASE": ""}.items():
            monkeypatch.setenv(name, value)
        monkeypatch.chdir(repository)

    def prepare(self, signed, defect=False):
        for version in (VERSION, LTS):
            extra = {base_name(version) + ".extra": b"x"} if defect and version == LTS else None
            (self.bundles / f"central-bundle-{version}.zip").write_bytes(bundle(artifacts(version), version, signed, extra))

    def run(self, *arguments, output="output", **environment):
        for name, value in environment.items():
            self.monkeypatch.setenv(name, value)
        return release.main([VERSION, str(self.root / output), *arguments])

    def calls_made(self):
        return [line.split() for line in self.calls.read_text().splitlines()]


@pytest.fixture
def rehearsal(tmp_path, monkeypatch):
    return Rehearsal(tmp_path, monkeypatch)


def test_unsigned_rehearsal_skips_signing_and_upload_and_checks_the_guard(rehearsal):
    rehearsal.prepare(signed=False)
    assert rehearsal.run("--unsigned") == 0
    assert rehearsal.calls_made() == [
        ["release-stage", VERSION, "-Dgpg.skip=true", "-DskipUpload=true"],
        ["release-stage", LTS, "-Dgpg.skip=true", "-DskipUpload=true"],
        ["release-stage", LTS, "-Dflink.compat=flink2", "-Dgpg.skip=true", "-DskipUpload=true"]]
    assert sorted(path.name for path in (rehearsal.root / "output/bundles").iterdir()) == [
        f"central-bundle-{LTS}.zip", f"central-bundle-{VERSION}.zip"]
    assert "0 defects" in rehearsal.summary.read_text()


def test_signed_rehearsal_requires_the_key_and_keeps_signing(rehearsal):
    rehearsal.prepare(signed=True)
    with pytest.raises(SystemExit, match="MAVEN_GPG_KEY"):
        rehearsal.run()
    assert rehearsal.run(MAVEN_GPG_KEY="key", MAVEN_GPG_PASSPHRASE="secret") == 0
    assert all(call[-1] == "-DskipUpload=true" and "-Dgpg.skip=true" not in call for call in rehearsal.calls_made())


def test_defects_fail_the_rehearsal_and_reach_the_summary(rehearsal):
    rehearsal.prepare(signed=False, defect=True)
    assert rehearsal.run("--unsigned") == 1
    assert f"- {LTS}: bundle has unexpected file {base_name(LTS)}.extra" in rehearsal.summary.read_text()


@pytest.mark.parametrize("status, output", [("0", release.GUARD_MESSAGE), ("1", "unrelated failure")])
def test_a_guard_that_accepts_or_fails_otherwise_stops_the_rehearsal(rehearsal, status, output):
    rehearsal.prepare(signed=False)
    with pytest.raises(SystemExit, match="guard did not reject"):
        rehearsal.run("--unsigned", STUB_GUARD_STATUS=status, STUB_GUARD_OUTPUT=output)


def test_a_failed_build_names_its_line(rehearsal):
    rehearsal.prepare(signed=False)
    (rehearsal.bundles / f"central-bundle-{LTS}.zip").unlink()
    with pytest.raises(SystemExit, match=f"release-stage {LTS} failed"):
        rehearsal.run("--unsigned")


def test_arguments(tmp_path):
    assert release.main([VERSION]) == 2
    assert release.main([VERSION, "output", "--unsigned", "-X"]) == 2
    with pytest.raises(SystemExit, match="bare X.Y.Z"):
        release.main([LTS, str(tmp_path / "output"), "--unsigned"])
    (tmp_path / "output").mkdir()
    with pytest.raises(SystemExit, match="choose a new output"):
        release.main([VERSION, str(tmp_path / "output"), "--unsigned"])
