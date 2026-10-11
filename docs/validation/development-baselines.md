---
title: "Development artifact bundles"
bookHidden: true
---
<!--
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Development artifact bundles

Issue [#40](https://github.com/flink-gcp/flink-datastream-protobuf/issues/40) prepared fixed development inputs for the state and API checks in #16 and #17.
The final v0.1.0 bundles are retained as assets of the [`v0.1.0` pre-release](#retained-v010-baseline).
The tooling always labels its output `development-tooling-baseline` with `milestone.final=false`; a successful capture alone does not designate a milestone baseline.
No 0.x coordinates are published to Maven Central.

## Capture

Run from the repository root of a clean, committed dedicated worktree on JDK 17.
Ignored files under `src` are rejected because Maven could otherwise package inputs absent from the retained source revision.
The destination's parent must exist, and the destination must be new and outside the repository.
Each invocation builds one library and its compiled consumers once, then captures state with those bytes on its supported Flink runtimes.

```sh
mise x -- just baseline-capture flink2 3 /tmp/baseline-flink2-pb3
mise x -- just baseline-capture flink2 4 /tmp/baseline-flink2-pb4
mise x -- just baseline-capture flink1 3 /tmp/baseline-flink1-pb3
mise x -- just baseline-capture flink1 4 /tmp/baseline-flink1-pb4
```

The two artifact lines and two paired Protobuf profiles produce four independent bundles.
The 2.x jar is compiled at 2.2.1 and also used unchanged at 2.3.0; the LTS jar is compiled at 1.20.4.
Both retain the development POM version of the captured source, such as `0.1.0-SNAPSHOT` in the v0.1.0 baseline, with the adapter line identified separately in the manifest.
They are not renamed to imply published release coordinates.
Capture keeps the source archive and revision, effective Maven model, exact command arguments, resolved dependency tree and jars, JDK identity, and protoc version and executable checksum.
The application jars contain the original and changed schemas; neither application is on the parent classpath of the isolated recovery jobs.

The source archive includes the schemas/imports and the reader and consumer sources.
The companion `tools.jar` contains compiled test instruments, test messages, and test resources including the existing fixed fixtures; it is never attached as a Maven publication artifact.
It retains the compiled explicit type construction, builder, factory, registered transport, and LTS legacy-entry-point checks for subsequent API investigations.
The explicit consumer, registration transport check, and LTS legacy check run in separate JVMs from the state writer and reader.
LTS results include an additional `legacy-entry-point` case, giving 13 cases on LTS and 12 on 2.x.
This is a concrete consumer inventory, not an exhaustive public-API compatibility checker; that gate belongs to #17.

## Inspect and restore

```sh
mise x -- just baseline-check /tmp/baseline-flink2-pb3
mise x -- just baseline-read /tmp/baseline-flink2-pb3 2.2.1 /tmp/read-pb3-floor-17
mise x -- just baseline-read /tmp/baseline-flink2-pb3 2.3.0 /tmp/read-pb3-ceiling-17
mise x java@temurin-21 just -- just baseline-read /tmp/baseline-flink2-pb3 2.3.0 /tmp/read-pb3-ceiling-21
```

`baseline-check` reads the full file inventory and SHA-256 checksums without Maven, network access, or compilation.
`baseline-read` also starts the retained compiled code with the bundle's dependency jars, validates the loaded library and runtime, and writes results to a new directory outside the bundle.
The bundle is checked again after execution, including after a failed reader.
The reader does not use `target/classes`, regenerate messages, or replace a missing fixture.
To run without a checkout, use `java -cp <bundle>/tools.jar io.github.flink.gcp.protobuf.BaselineTool check <bundle>` or the corresponding `read <bundle> <flink-version> <new-results>` arguments.
Use trusted bundles: the reader executes their Java code, and the internal checksum inventory establishes integrity against that inventory, not publisher authenticity.

Snapshot fixtures contain the full descriptor closure and a framed representative value, with default and raised serializer settings.
Runtime fixtures contain canonical savepoints and full, nonincremental checkpoints for HashMap and RocksDB, with the existing baseline and raised test budgets.
State manifests record settings, scalar-keyed ValueState/MapState and operator ListState identities, UIDs, parallelism, the input cursor, expected counts, and unknown fields.
The retained application sources define the expected nested runtime values.
Capture archives state before closing the writer, deletes the writer's original state storage, and restores the relocated archives before checking continued processing.
It also checks checkpoint failure/replay and savepoint rejection for a schema change and a decreased size limit.
Incomplete archives cannot pass by finding files in the original writer directories.

CI captures each of the four bundles on JDK 17, moves the build output away, and reads the retained inputs on every claimed combination.
Flink 2.2.1 and 2.3.0 run on JDK 17 and 21; Flink 1.20.4 runs on JDK 17.
Each saved-state read uses its exact Flink version and Protobuf profile.
These fixtures use aligned checkpoints and local filesystem storage without entropy injection.
Cross-Flink state upgrades, cross-Protobuf restore, backend migration, rescaling, unaligned or incremental checkpoint portability, and universal development-version compatibility are outside this evidence.
The current companion jars include platform-specific runtime dependencies; recreate a rehearsal on another operating system or architecture rather than assuming portability of native dependencies.

## Retained v0.1.0 baseline

The [`v0.1.0` pre-release](https://github.com/flink-gcp/flink-datastream-protobuf/releases/tag/v0.1.0) is the durable shared copy of the final v0.1.0 development baseline.
Its source is annotated tag `v0.1.0`, commit `f5063f784bdf6799250c521c39a47701900133ea`.
The release assets are:

| Asset | Content |
| --- | --- |
| `v0.1.0-flink2-pb3.tar.gz`, `v0.1.0-flink2-pb4.tar.gz` | 2.x bundles, compiled at Flink 2.2.1 and read at 2.2.1 and 2.3.0 on JDK 17 and 21 |
| `v0.1.0-flink1-pb3.tar.gz`, `v0.1.0-flink1-pb4.tar.gz` | LTS bundles, compiled and read at Flink 1.20.4 on JDK 17 |
| `FINAL-BASELINE.json` | Final designation: source revision, tag object, bundle asset checksums, embedded manifests, inner inventory hashes, and reader results |
| `INVENTORY.json` | Per-file inventory of every bundle |
| `SHA256SUMS` | SHA-256 checksums of the other release assets |
| `README.md` | Download, inspection, and reader instructions |
| `v0.1.0-verification.tar.gz` | Reader results, logs, publication inventory review, and the final CI job inventory |

The embedded manifests keep `milestone.final=false` and the Maven version `0.1.0-SNAPSHOT`.
`FINAL-BASELINE.json`, the outer `SHA256SUMS`, and the completion record on #40 select these exact bundles as the milestone baseline.
Do not rename, re-tag, or replace these assets.
Obtain them with [`baseline-fetch`](#retrieve-the-retained-baseline); the release `README.md` lists equivalent manual commands.

## Retrieve the retained baseline

`baseline-fetch` downloads the designated release assets with `gh release download`, verifies them, and extracts the selected bundles into a new directory outside the repository.
It needs an authenticated `gh`, Python 3.9 or newer, and JDK 17 or 21 for the final `baseline-check`.
Without bundle names it fetches all four bundles.
Give absolute paths: just runs the recipe from the repository root, so a relative path resolves there and is rejected.

```sh
mise x -- just baseline-fetch v0.1.0 /tmp/baseline-v0.1.0
mise x -- just baseline-fetch v0.1.0 /tmp/baseline-v0.1.0-flink2-pb3 flink2-pb3
```

The output contains `release/`, which holds `SHA256SUMS`, `FINAL-BASELINE.json`, `INVENTORY.json`, and the downloaded archives, and one directory per bundle.
With `--archives <directory>`, archives are kept in that directory instead and reused by later runs; for example, `just baseline-fetch v0.1.0 /tmp/new-output flink1-pb3 --archives /tmp/baseline-archives`.

The checks run in this order:

1. The SHA-256 of the downloaded `SHA256SUMS` must equal the digest pinned for the tag in `scripts/baseline_fetch.py`, before any archive is downloaded.
2. `FINAL-BASELINE.json`, `INVENTORY.json`, and each selected archive must match their `SHA256SUMS` entries, and each archive must also match the name, size, and checksum recorded in `FINAL-BASELINE.json`.
3. `FINAL-BASELINE.json` and `INVENTORY.json` must agree on each bundle's file count, total size, inner `SHA256SUMS` hash, and the `library.jar` and `tools.jar` hashes in the embedded manifest.
4. Extraction accepts only directories and regular files under the bundle's top-level directory, with plain path components.
   Every file must appear in `INVENTORY.json` with the same size and SHA-256, exactly once, and every inventory file must be present.
5. `baseline-check` runs the bundle's own `tools.jar` on the extracted bundle, with GitHub token variables removed from its environment.

Any failure leaves the destination absent.
The command also refuses a tag without a pinned digest, an unknown bundle name, an existing destination, and a destination or archive directory inside the repository.
Work happens in a hidden staging directory beside the destination, which is renamed to the destination only after every selected bundle passes and is removed on failure.
A process killed by a signal can leave that hidden directory, or a hidden download directory inside the archive directory, behind; remove it before retrying.
A reused archive that does not match is reported, not replaced; remove that copy and run again.
The command only reads release assets; it never uploads, edits, or re-tags them.

The pinned digest ties every accepted byte to the inventory reviewed in this repository.
The checksums establish integrity against that retained inventory, not publisher authenticity: no signature accompanies the assets, and a change to both the assets and the pin would be accepted.
`baseline-check` executes code from the bundle, so it runs only after the archive and every extracted file have matched the inventory.
Add a pin for a later designated baseline, such as the 0.2.0 baseline in [#63](https://github.com/flink-gcp/flink-datastream-protobuf/issues/63), only after reviewing its `SHA256SUMS` and final manifest.

The follow-up checks consume only part of each bundle: the state archives and snapshot fixtures under `fixtures/`, the manifests (`manifest.properties` and the state and capture properties), the schemas `apps/original/schema.pb` and `apps/changed/schema.pb`, `library.jar`, and the compiled consumers in `tools.jar`.
The `dependencies/` tree was resolved on macOS/aarch64.
Several of its jars carry native libraries, such as RocksDB, Netty, Snappy, and LZ4 for a set of operating systems and architectures, and that resolution and its native loading are not claimed for another host.
CI consumers must not put that tree on a classpath; they resolve their runtime dependencies for the runner instead.
For the same reason, CI does not run `baseline-read` on the retained v0.1.0 bundles, because the reader starts the retained code with those dependency jars.

### Retained bundles in CI

The composite action `.github/actions/baseline-fetch` fetches one bundle per job:

```yaml
- uses: ./.github/actions/baseline-fetch
  with:
    tag: v0.1.0
    bundle: flink2-pb3
    output: ${{ runner.temp }}/retained-baseline
```

Its `bundle` output is the verified bundle directory.
The action reads the archive checksum from the pinned `SHA256SUMS` and uses the cache key `development-baseline-<tag>-<bundle>-<archive SHA-256>`, so jobs that need the same bundle share one cache entry.
An entry saved by a pull request run is visible only to that pull request; other branches restore it once a `main` run has saved it.
It restores only that exact key and passes the cached archive to `baseline-fetch --archives`, which verifies it again on every run.
After a hit the action also passes `--restored`, so an entry without the expected archive fails instead of triggering a new download.
On a miss it downloads and verifies the archive, then saves it after the bundle passes; a failed run saves nothing.
A restored archive that is missing or fails verification fails the job without replacing the entry; delete it with `gh cache delete <key>`, using the key in the error annotation.
The job does not fall back to a fresh download, so a corrupt or replaced entry stays visible instead of being bypassed.
The four entries total about 780 MB of the repository's Actions cache quota.
The `GITHUB_TOKEN` with `contents: read` is sufficient for the download.

The Verify workflow's retained-baseline job runs this action for each of the four v0.1.0 bundles.
The milestone-upgrade jobs planned in [#16](https://github.com/flink-gcp/flink-datastream-protobuf/issues/16) and the API-compatibility jobs planned in [#17](https://github.com/flink-gcp/flink-datastream-protobuf/issues/17) are to use the same action, which shares these cache entries.

## Retention

A failed capture leaves an incomplete destination without a successful checksum inventory; retry in a new directory.
Never repair historical provenance or replace files inside a completed bundle.
Ordinary verification continues to read the existing checked-in snapshot/savepoint fixtures and does not regenerate them.
The new bundles are separate inputs and do not supersede those earlier format baselines.

Keep unreviewed bundles in an access-controlled external directory, backed up outside temporary build and worktree cleanup locations when retaining them for future work.
Do not use an expiring CI artifact or `/tmp` as the sole long-term copy.
CI rehearsals stay in runner temporary storage and are not uploaded by this workflow.

Command records and build/runtime logs preserve exact local paths and may contain machine-identifying information.
Before copying any bundle to shared storage, inspect the exact inventory, archive members, source content, commands, and logs; keep credentials and raw JVM diagnostics out of capture processes.
The checksum checker is not the benchmark content gate in #41 and does not prove the absence of arbitrary secrets.
Retain original bytes privately if a separately sanitized derivative is needed, and identify that derivative with its own inventory.

For #16, consume the retained schema descriptors, snapshot bytes, state archives, expected-value manifests, and application identities with a separately built new-schema application.
For #17, retain both library jars and already-compiled consumer inputs per profile, and select a candidate library on an explicitly reviewed classpath without recompiling the old consumer.
Neither follow-up should rebuild a newer working tree and label it the old writer.
