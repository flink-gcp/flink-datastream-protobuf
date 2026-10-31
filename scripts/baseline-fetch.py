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

"""Download, verify, and extract retained bundles from a milestone pre-release or a release."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tarfile
import tempfile

from lib.baseline_bundles import (BUNDLES, CHECKSUMS, DESIGNATION, INVENTORY, Rejected, archive_name, baseline_check,
                                  download, relative, require, sha256)

ROOT = Path(__file__).resolve().parents[1]
# SHA-256 of each designated release's SHA256SUMS asset, which lists every other asset.
# The pin ties downloads to the inventory reviewed in this repository.
RELEASES = {
    "v0.1.0": "8d328241cd4d75d54eb87b94dcc3abb50e9424bac7a023dc03eada74375ccfac",
    "v0.2.0": "a98baaef5a6ad5e19332c0cbb08512410d02843dee1cdd952f3a499e4a6527c7",
    "v0.3.0": "1799eeccd57033b12598c966d4026dbc557c4ab00db32bc85657fe9a33328fb1",
}


def outside_repository(requested, label):
    path = Path(requested).absolute()
    parent = path.parent.resolve()
    require(parent.is_dir(), label + " parent does not exist: " + str(path.parent))
    resolved = parent / path.name
    # samefile also matches case variants of the checkout path on case-insensitive filesystems.
    require(not any(os.path.exists(c) and os.path.samefile(c, ROOT) for c in [resolved, parent, *parent.parents]),
            label + " must be outside the repository: " + str(resolved))
    return resolved


def checksums(tag, directory):
    path = directory / CHECKSUMS
    require(path.is_file(), "Missing release asset: " + CHECKSUMS)
    require(sha256(path) == RELEASES[tag], CHECKSUMS + " does not match the pinned digest for " + tag)
    entries = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9_.-]+)", line)
        require(match is not None, "Malformed " + CHECKSUMS + " line")
        require(match.group(2) not in entries, "Duplicate " + CHECKSUMS + " entry")
        entries[match.group(2)] = match.group(1)
    return entries


def verified(path, name, entries, size=None):
    require(path.is_file() and not path.is_symlink(), "Missing release asset: " + name)
    require(name in entries, "No " + CHECKSUMS + " entry for " + name)
    require(size is None or path.stat().st_size == size, "Size mismatch: " + name)
    require(sha256(path) == entries[name], "Checksum mismatch: " + name)
    return path


def designation(tag, release, entries, bundles):
    final = json.loads(verified(release / DESIGNATION, DESIGNATION, entries).read_text(encoding="utf-8"))
    inventory = json.loads(verified(release / INVENTORY, INVENTORY, entries).read_text(encoding="utf-8"))
    # Format 1 designates a 0.x development milestone, format 2 a published release.
    require((final.get("format_version") == 1 and final.get("milestone") == tag and final.get("milestone_final") is True)
            or (final.get("format_version") == 2 and final.get("tag") == tag and final.get("final") is True),
            DESIGNATION + " does not designate " + tag)
    recorded = {bundle.get("name"): bundle for bundle in final.get("bundles", [])}
    require(len(recorded) == len(final["bundles"]), "Duplicate bundle in " + DESIGNATION)
    selected = {}
    for name in bundles:
        require(name in recorded, DESIGNATION + " does not record bundle " + name)
        bundle, files = recorded[name], inventory.get(name)
        asset = bundle["asset"]
        require(asset["file"] == archive_name(tag, name), "Unexpected asset name for " + name)
        require(entries.get(asset["file"]) == asset["sha256"],
                DESIGNATION + " and " + CHECKSUMS + " disagree on " + asset["file"])
        require(isinstance(files, list) and files, INVENTORY + " does not record bundle " + name)
        expected = {}
        for file in files:
            parts = relative(file["path"])
            require(len(parts) > 1 and parts[0] == name, "Inventory path outside bundle: " + file["path"])
            require(expected.setdefault(file["path"], (file["bytes"], file["sha256"]))
                    == (file["bytes"], file["sha256"]), "Duplicate inventory path: " + file["path"])
        sums = {path.split("/", 1)[1]: digest for path, (_, digest) in expected.items()}
        manifest = bundle["manifest"]
        require(len(expected) == len(files) == bundle["file_count"]
                and sum(size for size, _ in expected.values()) == bundle["uncompressed_bytes"]
                and sums.get(CHECKSUMS) == bundle["inventory_sha256"]
                and sums.get("library.jar") == manifest["library.sha256"]
                and sums.get("tools.jar") == manifest["consumer.sha256"],
                DESIGNATION + " and " + INVENTORY + " disagree on " + name)
        selected[name] = (asset, expected)
    return selected


def archive(tag, asset, entries, release, cache, restored):
    name = asset["file"]
    if cache is None:
        return verified(release / name, name, entries, asset["bytes"])
    path = cache / name
    if not os.path.lexists(path):
        # A restored cache that lacks its archive is reported rather than bypassed by a new download.
        require(not restored, "Restored archive cache lacks " + name + ": " + str(cache))
        with tempfile.TemporaryDirectory(prefix=".download-", dir=cache) as temporary:
            download(tag, [name], Path(temporary))
            verified(Path(temporary) / name, name, entries, asset["bytes"])
            require(not os.path.lexists(path), "Cached archive appeared during download: " + str(path))
            os.rename(Path(temporary) / name, path)
        return path
    try:
        return verified(path, name, entries, asset["bytes"])
    except Rejected as error:
        raise Rejected(str(error) + "; remove the cached copy " + str(path)) from None


def extract(path, name, expected, stage):
    seen = set()
    with tarfile.open(path, mode="r|gz") as tar:
        for member in tar:
            member_name = member.name[:-1] if member.isdir() and member.name.endswith("/") else member.name
            parts = relative(member_name)
            require(parts[0] == name, "Archive member outside bundle: " + repr(member.name))
            target = stage.joinpath(*parts)
            if member.type == tarfile.DIRTYPE:
                target.mkdir(parents=True, exist_ok=True)
                continue
            require(member.type in (tarfile.REGTYPE, tarfile.AREGTYPE),
                    "Unsupported archive member type: " + member_name)
            require(member_name in expected and member_name not in seen,
                    "Unexpected or duplicate archive member: " + member_name)
            size, digest = expected[member_name]
            require(member.size == size, "Size mismatch: " + member_name)
            target.parent.mkdir(parents=True, exist_ok=True)
            hashed = hashlib.sha256()
            with tar.extractfile(member) as source, open(target, "xb") as sink:
                for block in iter(lambda: source.read(1 << 20), b""):
                    hashed.update(block)
                    sink.write(block)
            require(hashed.hexdigest() == digest, "Checksum mismatch: " + member_name)
            seen.add(member_name)
    missing = sorted(set(expected) - seen)
    require(not missing, "Archive is missing inventory files: " + ", ".join(missing[:3]))



def fetch(tag, output, bundles, cache=None, restored=False):
    require(tag in RELEASES, "No pinned retained baseline for tag " + tag)
    require(cache is not None or not restored, "--restored requires --archives")
    bundles = list(dict.fromkeys(bundles or BUNDLES))
    require(all(bundle in BUNDLES for bundle in bundles), "Unknown bundle; choose from " + ", ".join(BUNDLES))
    destination = outside_repository(output, "Output")
    require(not os.path.lexists(destination), "Output already exists; choose a new directory: " + str(destination))
    if cache is not None:
        cache = outside_repository(cache, "Archive cache")
        cache.mkdir(exist_ok=True)
        require(cache.is_dir() and not cache.is_symlink(), "Archive cache is not a directory: " + str(cache))
    # Stage beside the destination so a failure leaves no partial output to mistake for a verified one.
    stage = Path(tempfile.mkdtemp(prefix="." + destination.name + ".", dir=destination.parent))
    claimed = False
    try:
        release = stage / "release"
        release.mkdir()
        # Check the pinned inventory before downloading any archive.
        download(tag, [CHECKSUMS], release)
        entries = checksums(tag, release)
        archives = [] if cache is not None else [archive_name(tag, bundle) for bundle in bundles]
        download(tag, [DESIGNATION, INVENTORY] + archives, release)
        for name, (asset, expected) in designation(tag, release, entries, bundles).items():
            extract(archive(tag, asset, entries, release, cache, restored), name, expected, stage)
            baseline_check(stage / name)
        # Claim the name first, because rename silently replaces an empty directory created meanwhile.
        try:
            os.mkdir(destination)
        except FileExistsError:
            raise Rejected("Output appeared during fetch: " + str(destination)) from None
        claimed = True
        os.rename(stage, destination)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        if claimed:
            # Release the claimed name; rmdir leaves it alone if anything was written into it.
            try:
                os.rmdir(destination)
            except OSError:
                pass
        raise
    for bundle in bundles:
        print("Verified retained bundle: " + str(destination / bundle))


def asset(tag, bundle):
    require(tag in RELEASES, "No pinned retained baseline for tag " + tag)
    require(bundle in BUNDLES, "Unknown bundle; choose from " + ", ".join(BUNDLES))
    with tempfile.TemporaryDirectory() as temporary:
        download(tag, [CHECKSUMS], Path(temporary))
        name = archive_name(tag, bundle)
        entries = checksums(tag, Path(temporary))
        require(name in entries, "No " + CHECKSUMS + " entry for " + name)
        print(entries[name])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    fetching = commands.add_parser("fetch", help="verify and extract bundles into a new directory")
    fetching.add_argument("tag")
    fetching.add_argument("output")
    fetching.add_argument("bundles", nargs="*", metavar="bundle")
    fetching.add_argument("--archives", help="reuse and keep verified archives in this directory")
    fetching.add_argument("--restored", action="store_true",
                          help="require every archive to be present in --archives, as after a cache hit")
    describing = commands.add_parser("asset", help="print the pinned archive checksum of one bundle")
    describing.add_argument("tag")
    describing.add_argument("bundle")
    args = parser.parse_args(argv)
    try:
        if args.command == "fetch":
            fetch(args.tag, args.output, args.bundles, args.archives, args.restored)
        else:
            asset(args.tag, args.bundle)
    except Rejected as error:
        print("baseline-fetch: " + str(error), file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, TypeError, tarfile.TarError) as error:
        print("baseline-fetch: unreadable or malformed input: " + repr(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
