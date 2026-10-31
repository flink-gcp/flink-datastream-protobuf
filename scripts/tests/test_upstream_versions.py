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

"""Synthetic pins and metadata only: never contact Maven Central."""

import contextlib
import http.client
import io

import pytest

from conftest import load_script

upstream = load_script("upstream-versions.py")

POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <properties>
        <flink.version>{floor}</flink.version>
        <protobuf.version>{protobuf4}</protobuf.version>
    </properties>
    <profiles>
        <profile><id>flink1</id><properties><flink.version>{lts}</flink.version></properties></profile>
        <profile><id>protobuf3</id><properties><protobuf.version>{protobuf3}</protobuf.version></properties></profile>
        <profile><id>protobuf4</id></profile>
    </profiles>
</project>
"""
VERIFY = "env:\n  FLINK_CEILING: '{ceiling}'\njobs: {{}}\n"
PINS = {"floor": "2.2.1", "ceiling": "2.3.0", "lts": "1.20.4", "protobuf4": "4.36.2", "protobuf3": "3.25.9"}
FLINK = ["1.20.3", "1.20.4", "2.0-preview1", "2.2.0", "2.2.1", "2.3.0", "0.6-hadoop2-incubating"]
PROTOBUF = ["3.25.8", "3.25.9", "4.0.0-rc-1", "4.36.1", "4.36.2", "4.37.0-RC1"]
CEILING_MAJOR = "new Flink major {}; review the supported window under ADR-0003"
PROTOBUF_MAJOR = "new Protobuf major {}; review the supported runtimes under ADR-0001"


def metadata(coordinates, versions):
    group, artifact = coordinates
    listed = "".join("<version>" + version + "</version>" for version in versions)
    return ("<metadata><groupId>" + group + "</groupId><artifactId>" + artifact + "</artifactId>"
            "<versioning><versions>" + listed + "</versions></versioning></metadata>").encode()


def releases(coordinates, versions):
    return upstream.parse_metadata(metadata(coordinates, versions), coordinates)


def fetch(flink=(), protobuf=()):
    versions = {upstream.FLINK: FLINK + list(flink), upstream.PROTOBUF: PROTOBUF + list(protobuf)}
    return lambda coordinates: releases(coordinates, versions[coordinates])


@pytest.fixture
def repository(tmp_path):
    """Write a synthetic pom.xml and verify.yaml with the given pins and return the root."""

    def write(**overrides):
        pins = PINS | overrides
        (tmp_path / ".github/workflows").mkdir(parents=True, exist_ok=True)
        (tmp_path / "pom.xml").write_text(POM.format(**pins), encoding="utf-8")
        (tmp_path / ".github/workflows/verify.yaml").write_text(VERIFY.format(**pins), encoding="utf-8")
        return tmp_path

    return write


@pytest.fixture
def findings(repository):
    """Return (name, version, finding) for the synthetic pins against the default and extra releases."""

    def run(flink=(), protobuf=(), decided=upstream.DECIDED_MAJORS, **overrides):
        available = {upstream.FLINK: releases(upstream.FLINK, FLINK + list(flink)),
                     upstream.PROTOBUF: releases(upstream.PROTOBUF, PROTOBUF + list(protobuf))}
        pins = upstream.read_pins(repository(**overrides))
        return [(pin.name, upstream.label(pin.version), finding)
                for pin, finding in upstream.check(pins, available, decided)]

    return run


def test_reads_every_pin_with_its_source_and_artifact(repository):
    assert upstream.read_pins(repository()) == {
        "floor": ("Flink 2.x floor", "pom.xml flink.version", upstream.FLINK, (2, 2, 1)),
        "ceiling": ("Flink 2.x ceiling", ".github/workflows/verify.yaml FLINK_CEILING", upstream.FLINK, (2, 3, 0)),
        "lts": ("Flink 1.20 LTS", "pom.xml flink1 profile flink.version", upstream.FLINK, (1, 20, 4)),
        "protobuf": ("Protobuf 4", "pom.xml protobuf.version", upstream.PROTOBUF, (4, 36, 2)),
        "protobuf3": ("Protobuf 3", "pom.xml protobuf3 profile protobuf.version", upstream.PROTOBUF, (3, 25, 9)),
    }


def test_reads_the_repository_pins():
    assert list(upstream.read_pins()) == ["floor", "ceiling", "lts", "protobuf", "protobuf3"]


def test_rejects_a_pin_that_is_not_a_final_release(repository):
    with pytest.raises(upstream.Rejected, match="Protobuf 3 pin '3.25.9-RC1'"):
        upstream.read_pins(repository(protobuf3="3.25.9-RC1"))


def test_rejects_a_missing_profile(repository):
    root = repository()
    pom = root / "pom.xml"
    pom.write_text(pom.read_text(encoding="utf-8").replace("<id>flink1</id>", "<id>other</id>"), encoding="utf-8")
    with pytest.raises(upstream.Rejected, match="flink1 profile flink.version was not found"):
        upstream.read_pins(root)


@pytest.mark.parametrize("content", ["env:\n  FLINK_CEILING: 2.3.0\n",
                                     "  FLINK_CEILING: '2.3.0'\n  FLINK_CEILING: '2.3.1'\n"],
                         ids=["unquoted", "duplicated"])
def test_rejects_a_missing_or_duplicated_ceiling(repository, content):
    root = repository()
    (root / ".github/workflows/verify.yaml").write_text(content, encoding="utf-8")
    with pytest.raises(upstream.Rejected, match="Expected one FLINK_CEILING"):
        upstream.read_pins(root)


@pytest.mark.parametrize(("path", "data", "message"), [
    ("pom.xml", b"<project", "Cannot read pom.xml"),
    (".github/workflows/verify.yaml", b"\xff", "Cannot read .github/workflows"),
], ids=["pom", "verify"])
def test_names_the_file_it_cannot_read(repository, path, data, message):
    root = repository()
    (root / path).write_bytes(data)
    with pytest.raises(upstream.Rejected, match=message):
        upstream.read_pins(root)


def test_keeps_only_final_releases():
    assert releases(upstream.PROTOBUF, PROTOBUF) == {(3, 25, 8), (3, 25, 9), (4, 36, 1), (4, 36, 2)}


@pytest.mark.parametrize("coordinates", [("org.apache.flink", "flink-streaming-java"), ("org.example", "flink-core")],
                         ids=["artifact", "group"])
def test_rejects_metadata_for_another_group_or_artifact(coordinates):
    with pytest.raises(upstream.Rejected, match="does not describe org.apache.flink:flink-core"):
        upstream.parse_metadata(metadata(coordinates, FLINK), upstream.FLINK)


def test_rejects_metadata_without_a_final_release():
    with pytest.raises(upstream.Rejected, match="lists no final release"):
        releases(upstream.FLINK, ["2.0-preview1"])


def test_rejects_a_document_that_is_not_xml():
    with pytest.raises(upstream.Rejected, match="is not XML"):
        upstream.parse_metadata(b"<html>", upstream.FLINK)


def test_fetches_each_artifact_from_its_maven_central_path(monkeypatch):
    requested = []

    def respond(request, timeout):
        requested.append(request.full_url)
        coordinates = upstream.FLINK if "flink" in request.full_url else upstream.PROTOBUF
        return contextlib.nullcontext(io.BytesIO(metadata(coordinates, ["1.2.3"])))

    monkeypatch.setattr(upstream.urllib.request, "urlopen", respond)
    for coordinates in (upstream.FLINK, upstream.PROTOBUF):
        assert upstream.fetch_releases(coordinates) == {(1, 2, 3)}
    assert requested == [
        "https://repo1.maven.org/maven2/org/apache/flink/flink-core/maven-metadata.xml",
        "https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/maven-metadata.xml",
    ]


@pytest.mark.parametrize("error", [OSError("connection refused"), http.client.IncompleteRead(b"")],
                         ids=["OSError", "IncompleteRead"])
def test_names_the_url_it_cannot_read(monkeypatch, error):
    def fail(request, timeout):
        raise error

    monkeypatch.setattr(upstream.urllib.request, "urlopen", fail)
    with pytest.raises(upstream.Rejected, match="Cannot read https://repo1.maven.org/.*/flink-core/"):
        upstream.fetch_releases(upstream.FLINK)


def test_current_pins_have_no_findings(findings):
    assert findings() == []


def test_prereleases_do_not_count(findings):
    assert findings(flink=["2.4-preview1", "3.0.0-rc1"], protobuf=["5.0.0-RC1", "4.37.0-RC2"]) == []


def test_reports_newer_flink_patches(findings):
    assert findings(flink=["2.2.2", "2.3.1", "2.3.2", "1.20.5"]) == [
        ("Flink 2.x floor", "2.2.1", "newer release 2.2.2"),
        ("Flink 2.x ceiling", "2.3.0", "newer release 2.3.2"),
        ("Flink 1.20 LTS", "1.20.4", "newer release 1.20.5"),
    ]


def test_reports_a_new_flink_minor_on_the_ceiling_only(findings):
    assert findings(flink=["2.4.0", "2.4.1"]) == [
        ("Flink 2.x ceiling", "2.3.0", "new Flink 2.x minor 2.4.1; review the supported window under ADR-0003"),
    ]


def test_ignores_new_minors_below_the_ceiling_or_on_the_lts_major(findings):
    assert findings(flink=["1.21.0", "2.1.3"], floor="2.1.3", ceiling="2.2.1") == [
        ("Flink 2.x ceiling", "2.2.1", "new Flink 2.x minor 2.3.0; review the supported window under ADR-0003"),
    ]


def test_reports_a_new_flink_major(findings):
    assert findings(flink=["3.0.0"]) == [("Flink 2.x ceiling", "2.3.0", CEILING_MAJOR.format("3.0.0"))]


def test_a_ceiling_on_a_new_major_accepts_that_major(findings):
    assert findings(flink=["2.4.0", "3.0.0"], floor="2.4.0", ceiling="3.0.0") == []


def test_reports_newer_protobuf_releases_within_each_major(findings):
    assert findings(protobuf=["3.25.10", "4.36.3", "4.37.0", "4.38.1"]) == [
        ("Protobuf 4", "4.36.2", "newer release 4.38.1"),
        ("Protobuf 3", "3.25.9", "newer release 3.25.10"),
    ]


def test_reports_a_new_protobuf_major_on_the_newest_protobuf_pin(findings):
    assert findings(protobuf=["5.0.0", "5.1.0"]) == [("Protobuf 4", "4.36.2", PROTOBUF_MAJOR.format("5.1.0"))]
    assert findings(protobuf=["5.0.0", "6.0.0"], protobuf4="5.0.0", protobuf3="4.36.2") == [
        ("Protobuf 4", "5.0.0", PROTOBUF_MAJOR.format("6.0.0")),
    ]


def test_a_recorded_decision_silences_its_own_major_only(findings):
    decided = {upstream.FLINK: 3, upstream.PROTOBUF: 5}
    assert findings(flink=["3.0.0", "3.1.0"], protobuf=["5.0.0", "5.1.0"], decided=decided) == []
    assert findings(flink=["4.0.0"], protobuf=["6.0.0"], decided=decided) == [
        ("Flink 2.x ceiling", "2.3.0", CEILING_MAJOR.format("4.0.0")),
        ("Protobuf 4", "4.36.2", PROTOBUF_MAJOR.format("6.0.0")),
    ]


@pytest.fixture
def run_main(repository, monkeypatch, capsys):
    """Run main outside GitHub Actions unless asked, returning the status, stdout, and stderr."""

    def run(fetch, github_actions=False, root=None):
        if github_actions:
            monkeypatch.setenv("GITHUB_ACTIONS", "true")
        else:
            monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        status = upstream.main(fetch=fetch, root=root or repository())
        captured = capsys.readouterr()
        return status, captured.out, captured.err

    return run


def test_passes_when_every_pin_is_current(run_main):
    status, stdout, stderr = run_main(fetch())
    assert status == 0
    assert "Flink 2.x ceiling 2.3.0 (.github/workflows/verify.yaml FLINK_CEILING)\n" in stdout
    assert "All pins match" in stdout
    assert stderr == ""


def test_fails_with_the_outdated_pins_named(run_main):
    status, stdout, _ = run_main(fetch(flink=["1.20.5"], protobuf=["3.25.10"]))
    assert status == 1
    assert "outdated: Flink 1.20 LTS 1.20.4 (pom.xml flink1 profile flink.version): newer release 1.20.5\n" in stdout
    assert ("outdated: Protobuf 3 3.25.9 (pom.xml protobuf3 profile protobuf.version): "
            "newer release 3.25.10\n") in stdout
    assert "All pins match" not in stdout


def test_annotates_findings_in_github_actions(run_main):
    status, stdout, _ = run_main(fetch(flink=["2.4.0"]), github_actions=True)
    assert status == 1
    assert "::error title=Outdated pin::Flink 2.x ceiling 2.3.0" in stdout


def test_reports_unavailable_metadata_as_an_error_without_findings(run_main):
    def unavailable(coordinates):
        raise upstream.Rejected("Cannot read the metadata")

    assert run_main(unavailable) == (1, "", "error: Cannot read the metadata\n")


def test_reports_an_unreadable_pom_as_an_error_without_findings(run_main, repository):
    root = repository()
    (root / "pom.xml").write_text("<project", encoding="utf-8")
    status, stdout, stderr = run_main(fetch(), root=root)
    assert (status, stdout) == (1, "")
    assert "error: Cannot read pom.xml" in stderr
