#!/usr/bin/env python3
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

"""Name the Flink and Protobuf pins that newer Maven Central releases have overtaken."""

import http.client
import os
from pathlib import Path
import re
import sys
from typing import NamedTuple
import urllib.request
import xml.etree.ElementTree as ElementTree

ROOT = Path(__file__).resolve().parents[1]
CENTRAL = "https://repo1.maven.org/maven2/"
FLINK = ("org.apache.flink", "flink-core")
PROTOBUF = ("com.google.protobuf", "protobuf-java")
POM = {"m": "http://maven.apache.org/POM/4.0.0"}
VERIFY = ".github/workflows/verify.yaml"
# Only final releases count: previews, release candidates, and classifier-like suffixes do not.
RELEASE = re.compile(r"(\d+)\.(\d+)\.(\d+)")
# The same entry format that the Dependency submission workflow parses.
CEILING = re.compile(r"^  FLINK_CEILING: '([^']*)'$", re.MULTILINE)
# A new upstream major fails the check until a recorded decision accepts or defers it.
# Raise an entry in the change that records a decision not to support that major yet; pins still move independently.
DECIDED_MAJORS = {FLINK: 2, PROTOBUF: 4}


class Rejected(Exception):
    """A pin or metadata document that the check cannot read or interpret."""


class Pin(NamedTuple):
    name: str
    source: str
    artifact: tuple
    version: tuple


def release(version):
    match = RELEASE.fullmatch(version or "")
    return tuple(int(part) for part in match.groups()) if match else None


def label(version):
    return ".".join(str(part) for part in version)


def property_value(element, path, source):
    value = element.findtext(path, namespaces=POM) if element is not None else None
    if not value:
        raise Rejected(source + " was not found")
    return value.strip()


def read_pins(root=ROOT):
    """Return the pins by role: floor, ceiling, lts, protobuf, and protobuf3."""
    try:
        pom = ElementTree.parse(root / "pom.xml").getroot()
    except (OSError, ElementTree.ParseError) as error:
        raise Rejected("Cannot read pom.xml: " + str(error)) from error
    try:
        ceiling = CEILING.findall((root / VERIFY).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Rejected("Cannot read " + VERIFY + ": " + str(error)) from error
    if len(ceiling) != 1:
        raise Rejected("Expected one FLINK_CEILING entry in " + VERIFY + ", found " + str(len(ceiling)))
    profiles = {profile.findtext("m:id", namespaces=POM): profile
                for profile in pom.findall("m:profiles/m:profile", POM)}
    pins = {}

    def pin(role, name, artifact, source, version):
        parsed = release(version)
        if parsed is None:
            raise Rejected(name + " pin " + repr(version) + " in " + source + " is not an x.y.z release")
        pins[role] = Pin(name, source, artifact, parsed)

    def pom_pin(role, name, artifact, profile, key):
        source = "pom.xml " + (profile + " profile " if profile else "") + key
        element = profiles.get(profile) if profile else pom
        pin(role, name, artifact, source, property_value(element, "m:properties/m:" + key, source))

    pom_pin("floor", "Flink 2.x floor", FLINK, None, "flink.version")
    pin("ceiling", "Flink 2.x ceiling", FLINK, VERIFY + " FLINK_CEILING", ceiling[0])
    pom_pin("lts", "Flink 1.20 LTS", FLINK, "flink1", "flink.version")
    pom_pin("protobuf", "Protobuf 4", PROTOBUF, None, "protobuf.version")
    pom_pin("protobuf3", "Protobuf 3", PROTOBUF, "protobuf3", "protobuf.version")
    return pins


def parse_metadata(document, coordinates):
    """Return the final releases that a maven-metadata.xml document lists."""
    name = ":".join(coordinates)
    try:
        root = ElementTree.fromstring(document)
    except ElementTree.ParseError as error:
        raise Rejected(name + " metadata is not XML: " + str(error)) from error
    if (root.findtext("groupId"), root.findtext("artifactId")) != coordinates:
        raise Rejected("The metadata does not describe " + name)
    versions = {release(element.text) for element in root.findall("versioning/versions/version")}
    versions.discard(None)
    if not versions:
        raise Rejected(name + " metadata lists no final release")
    return versions


def fetch_releases(coordinates):
    group, artifact = coordinates
    url = CENTRAL + group.replace(".", "/") + "/" + artifact + "/maven-metadata.xml"
    request = urllib.request.Request(url, headers={"User-Agent": "flink-datastream-protobuf upstream-versions"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            document = response.read()
    except (OSError, http.client.HTTPException) as error:
        raise Rejected("Cannot read " + url + ": " + str(error)) from error
    return parse_metadata(document, coordinates)


def newest(versions, predicate):
    matching = [version for version in versions if predicate(version)]
    return max(matching) if matching else None


def check(pins, releases, decided=DECIDED_MAJORS):
    """Return (pin, finding) for every pin that an upstream release overtakes."""
    findings = []
    for pin in pins.values():
        # Flink pins track the newest patch of their minor; a new minor moves the window instead.
        # Protobuf pins track the newest release of their major, minors included.
        track = 2 if pin.artifact == FLINK else 1
        update = newest(releases[pin.artifact], lambda v: v[:track] == pin.version[:track] and v > pin.version)
        if update:
            findings.append((pin, "newer release " + label(update)))

    ceiling = pins["ceiling"]
    minor = newest(releases[FLINK], lambda v: v[0] == ceiling.version[0] and v[1] > ceiling.version[1])
    if minor:
        findings.append((ceiling, "new Flink " + str(ceiling.version[0]) + ".x minor " + label(minor)
                         + "; review the supported window under ADR-0003"))
    # Report a new major on the pin with the newest major: the ceiling, or the default Protobuf pin.
    for artifact, upstream, review in [(FLINK, "Flink", "the supported window under ADR-0003"),
                                       (PROTOBUF, "Protobuf", "the supported runtimes under ADR-0001")]:
        top = max((pin for pin in pins.values() if pin.artifact == artifact), key=lambda pin: pin.version)
        major = newest(releases[artifact], lambda v: v[0] > max(top.version[0], decided[artifact]))
        if major:
            findings.append((top, "new " + upstream + " major " + label(major) + "; review " + review))
    return findings


def main(fetch=fetch_releases, root=ROOT):
    try:
        pins = read_pins(root)
        findings = check(pins, {artifact: fetch(artifact) for artifact in (FLINK, PROTOBUF)})
    except Rejected as error:
        print("error: " + str(error), file=sys.stderr)
        return 1
    for pin in pins.values():
        print(pin.name + " " + label(pin.version) + " (" + pin.source + ")")
    annotate = os.environ.get("GITHUB_ACTIONS") == "true"
    for pin, finding in findings:
        message = pin.name + " " + label(pin.version) + " (" + pin.source + "): " + finding
        print(("::error title=Outdated pin::" if annotate else "outdated: ") + message)
    if not findings:
        print("All pins match the newest Maven Central releases they track.")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
