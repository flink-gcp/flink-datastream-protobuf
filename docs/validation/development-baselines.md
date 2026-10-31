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
The final v0.1.0, v0.2.0, and v0.3.0 bundles are retained as assets of the [`v0.1.0` pre-release](#retained-v010-baseline), the [`v0.2.0` pre-release](#retained-v020-baseline), and the [`v0.3.0` pre-release](#retained-v030-baseline).
The tooling always labels its output `development-tooling-baseline` with `milestone.final=false`; a successful capture alone does not designate a milestone baseline.
No 0.x coordinates are published to Maven Central.
The 1.0.0 format restart ([#140](https://github.com/flink-gcp/flink-datastream-protobuf/issues/140)) removed the 0.x snapshot classes, so the current library does not restore the state in these bundles; they remain retained development records.
The capture, check, read, restore, API, and consumer commands apply to bundles retained from published releases, starting with 1.0.0; `baseline-release` still assembles only development milestone tags, so retaining the published 1.0.0 bundles with it requires a change under [#13](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13).

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
The 2.x jar is compiled at 2.2.1 and also used unchanged at 2.3.0; the LTS jar is compiled at 1.20.5.
Both retain the development POM version of the captured source, such as `0.1.0-SNAPSHOT` in the v0.1.0 baseline, with the adapter line identified separately in the manifest.
They are not renamed to imply published release coordinates.
Capture keeps the source archive and revision, effective Maven model, exact command arguments, resolved dependency tree and jars, JDK identity, and protoc version and executable checksum.
The v0.1.0 application jars contain the original and changed schemas; capture from the current source also retains the reserved, incompatible, and enum-pair schemas described in [runtime state recovery](runtime-recovery.md#schema-changes).
No application is on the parent classpath of the isolated recovery jobs.

The source archive includes the schemas/imports and the reader and consumer sources.
The companion `tools.jar` contains compiled test instruments and test messages; it is never attached as a Maven publication artifact.
It retains the compiled explicit type construction, builder, factory, registered transport, and LTS legacy-entry-point checks for subsequent API investigations.
The explicit consumer, registration transport check, and LTS legacy check run in separate JVMs from the state writer and reader.
LTS results include an additional `legacy-entry-point` case, giving 26 cases on LTS and 25 on 2.x; the format 2 bundles of v0.2.0, which lack the DynamicMessage fixtures below, have 21 and 20, and the format 1 bundles of v0.1.0, which also lack the evolved and enum fixtures, have 13 and 12.
This is a concrete consumer inventory, not an exhaustive public-API compatibility checker.
`baseline-consume` reruns it against a candidate library, and the [API compatibility gate](../development.md#api-compatibility-gate) compares the annotated API separately.

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

Snapshot fixtures contain the full descriptor closure and a framed representative value, with default and raised serializer settings for the generated message and, from format 3, default settings for a DynamicMessage type of the same schema.
Runtime fixtures contain canonical savepoints and full, nonincremental checkpoints for HashMap and RocksDB, with the existing baseline and raised test budgets.
State manifests record settings, scalar-keyed ValueState/MapState and operator ListState identities, UIDs, parallelism, the input cursor, expected counts, and unknown fields.
The retained application sources define the expected nested runtime values.
Capture archives state before closing the writer, deletes the writer's original state storage, and restores the relocated archives before checking continued processing.
For these original-schema fixtures, it also checks checkpoint failure/replay and savepoint rejection for a schema change and a decreased size limit.
The rejected schema change is the field addition in the v0.1.0 bundles and the `count` type change in capture from the current source, which accepts the field addition.
Incomplete archives cannot pass by finding files in the original writer directories.

Bundle format 2 adds two fixtures per backend at the baseline budget for the next milestone's upgrade checks, and format 3, which capture from the current source writes, adds a third:

| Fixture | Writer | Reader checks |
|---|---|---|
| `evolved` | The `original` schema writes inputs 0 to 7; the `changed` schema restores that savepoint, processes inputs 8 to 11, and writes the archives | Restore with `changed`, with `added` empty for inputs 0 to 7 and set from input 8; the savepoint restore with `original` is rejected because it removes `added` without reserving its number |
| `enum` | The `enum-original` schema writes inputs 0 to 7 | Restore with `enum-original` and with `enum-added`; restored values keep `KIND_FIRST`, and `enum-added` writes `KIND_SECOND` for new inputs |
| `dynamic` (format 3) | The [DynamicMessage application](runtime-recovery.md#dynamicmessage) writes inputs 0 to 7 with the `original` descriptor | Restore with the DynamicMessage application and the `original` descriptor, and with the `changed` one, checking the descriptors that restored values carry; the savepoint restore with `incompatible` fails with Flink's incompatible-serializer exception |

Their manifests also record the writer schemas, and the `dynamic` manifest its writer application; capture deletes the savepoint that the `evolved` writer restored as well as each writer's state.
Each reader check restores the archive, processes four more inputs, and restores the savepoint that it then writes, as the [schema-evolution test](runtime-recovery.md#schema-changes) does, checking the restored and replaced values and continued processing each time.
Format 2 bundles also require the `enum-original` and `enum-added` applications and schemas, and format 3 bundles also `apps/application-dynamic.jar`; `check` still accepts the format 1 layout of the v0.1.0 bundles and the format 2 layout of the v0.2.0 bundles.

CI captures each of the four bundles on JDK 17, moves the build output away, and reads the retained inputs on every claimed combination.
Until a published release is retained, it then runs [`baseline-upgrade`](#restore-retained-state-with-the-current-library), the [API compatibility gate](../development.md#api-compatibility-gate), and `baseline-consume` on that bundle on JDK 17, at each Flink version of its line for the restore and the consumers.
The bundle comes from the same build, so these runs keep the checks working rather than compare releases.
Flink 2.2.1 and 2.3.0 run on JDK 17 and 21; Flink 1.20.5 runs on JDK 17.
Each saved-state read uses its exact Flink version and Protobuf profile.
These fixtures use aligned checkpoints and local filesystem storage without entropy injection.
Cross-Flink state upgrades, cross-Protobuf restore, backend migration, rescaling, unaligned or incremental checkpoint portability, and universal development-version compatibility are outside this evidence.
The current companion jars include platform-specific runtime dependencies; recreate a rehearsal on another operating system or architecture rather than assuming portability of native dependencies.

## Publish a milestone baseline

`baseline-release` turns four captured bundles into the assets of a milestone pre-release, in the same layout as the [v0.1.0 baseline](#retained-v010-baseline), so that [`baseline-fetch`](#retrieve-the-retained-baseline) can verify them.
It works in one external work directory that holds `bundles/<bundle>/` and the maintainer's `README.md`, and it never modifies the bundles.

1. After every code-changing pull request of the milestone is merged and the `main` CI run of the merge commit has passed, create the annotated tag `v0.Y.Z` on that commit locally, and add a detached worktree at the tag.
2. From that worktree on JDK 17, run `baseline-capture` four times into `<work>/bundles/flink2-pb3`, `flink2-pb4`, `flink1-pb3`, and `flink1-pb4`.
3. Move the worktree's `target` directory out of the checkout, then read every bundle on each claimed Flink version and JDK and run the two negative controls:

   ```sh
   mise x -- just baseline-release read <work> --capture-checkout <worktree> \
     --java17 "$(mise where java@temurin-17)/bin/java" --java21 "$(mise where java@temurin-21)/bin/java"
   ```

   The readers run from the work directory with the retained jars and dependencies, and write `<work>/verification/`; the command records the capture checkout's HEAD and the absence of its build output.
4. Run `mise x -- just baseline-release review <work>`, which searches every bundle file and reader output, including the members of nested ZIP, JAR, and tar archives, the bytes other than the members' contents, such as a launcher prefix, local headers, a comment, or trailing data, and the content of gzip, bzip2, and xz streams, with the bounded credential rules of the [benchmark evidence gate](../development.md#automated-content-gate), and lists JVM diagnostic files, credential files, and compressed files it cannot open by name.
   It also reports archives and members that cannot be read and archives nested more deeply than it searches, and rejects symbolic links, including a linked `verification/`.
   The scan of four bundles takes several minutes.
   Like the evidence gate, it supports the reviewer's inspection of bytes that this project produced; it does not detect deliberately hidden content.
   It writes `<work>/PUBLICATION-REVIEW.json` with every flagged file and finding unresolved; each names its path, rule, and the SHA-256 of its bytes, never the matched text.
   Inspect each one.
   Move a flagged file that is harmless, such as a tracked repository file in the source archive whose name matches but that holds no credential, to `reviewed_files`, and a harmless match, such as a key header literal in public dependency bytecode or a synthetic test value, to `reviewed_matches`, each with a `disposition`; only members of a bundle's source archive can be reviewed files, so stop if any other flagged file or unexplained match remains.
5. Write `<work>/README.md` from the previous milestone's release README, naming the tag, its commit, and its tag object, then run `mise x -- just baseline-release assemble v0.Y.Z <work> <ci-run-id>` with the passing `CI` run of the push to `main`.
   It checks each bundle with its own `BaselineTool`, the source revision and annotated tag, the reader results against each bundle's jars and case counts, the controls and their rejection messages, the CI run and its `CI passed` job, the commit that the run's source-freezing job actually checked out, the bundle names against their artifact lines and Protobuf profiles, member names that `baseline-fetch` accepts, the README and dispositions against the credential rules, and the review, which must explain exactly what a new scan of the work directory finds, so a reviewed file or match whose bytes changed fails, and writes `<work>/release/` with the archives, `INVENTORY.json`, `FINAL-BASELINE.json`, the verification archive, the README, and `SHA256SUMS`, whose digest it prints.
6. After the maintainer approves publication, push the tag, create the GitHub pre-release with every file in `<work>/release/`, and run `mise x -- just baseline-release verify v0.Y.Z <work>`, which downloads every asset and compares it with the local bytes.
   The pre-release does not trigger the documentation deployment; [add the milestone to the site](../development.md#documentation-site-and-publication) with `gh workflow run ci.yaml --ref main`.
7. Pin the printed `SHA256SUMS` digest in `scripts/baseline-fetch.py`, document the retained baseline here, and run `baseline-fetch` against the published release.

Keep the work directory as described in [retention](#retention) until the published assets are verified; the release assets then become the durable shared copy.
`just baseline-release-test` runs the assembly checks on synthetic bundles and requires `baseline-fetch` to accept the assembled release.

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

## Retained v0.2.0 baseline

The [`v0.2.0` pre-release](https://github.com/flink-gcp/flink-datastream-protobuf/releases/tag/v0.2.0) is the durable shared copy of the final v0.2.0 development baseline, assembled with [`baseline-release`](#publish-a-milestone-baseline) for [#63](https://github.com/flink-gcp/flink-datastream-protobuf/issues/63).
Its source is annotated tag `v0.2.0`, commit `e697ee957000f3258ba4b6fd5940458514fada5c`, and its `SHA256SUMS` digest is pinned in `scripts/baseline-fetch.py`.
The assets have the same names and roles as the v0.1.0 assets above, with the `v0.2.0-` prefix.
The bundles use bundle format 2: besides the original-schema fixtures, each backend retains the `evolved` and `enum` fixtures described under [inspect and restore](#inspect-and-restore), and the runtime results have 20 cases on 2.x and 21 on LTS.
The verification archive also holds the publication review, whose findings are public dependency literals and defaults, workflow keys, and synthetic test values, each with its disposition.

The embedded manifests keep `milestone.final=false` and the Maven version `0.2.0-SNAPSHOT`.
`FINAL-BASELINE.json`, the outer `SHA256SUMS`, and the completion record on #63 select these exact bundles as the milestone baseline.
Do not rename, re-tag, or replace these assets.
Obtain them with `mise x -- just baseline-fetch v0.2.0 <new-output>`; the release `README.md` lists equivalent manual commands.

## Retained v0.3.0 baseline

The [`v0.3.0` pre-release](https://github.com/flink-gcp/flink-datastream-protobuf/releases/tag/v0.3.0) is the durable shared copy of the final v0.3.0 development baseline, assembled with [`baseline-release`](#publish-a-milestone-baseline) for [#146](https://github.com/flink-gcp/flink-datastream-protobuf/issues/146).
Its source is annotated tag `v0.3.0`, commit `142862b9d86f449e3e4894715602a991a018fd63`, and its `SHA256SUMS` digest is pinned in `scripts/baseline-fetch.py`.
The assets have the same names and roles as the v0.1.0 assets above, with the `v0.3.0-` prefix.
The bundles use bundle format 3: besides the format 2 fixtures, each backend retains the `dynamic` fixture and each Flink version the DynamicMessage snapshot fixture described under [inspect and restore](#inspect-and-restore), and the runtime results have 25 cases on 2.x and 26 on LTS.
The verification archive also holds the publication review; its findings are public dependency literals and defaults, workflow keys, and synthetic test values, and its reviewed files are the tracked CI Maven settings, `.github/maven/settings.xml`, in each source archive, each with its disposition.

The embedded manifests keep `milestone.final=false` and the Maven version `0.3.0-SNAPSHOT`.
`FINAL-BASELINE.json`, the outer `SHA256SUMS`, and the completion record on #146 select these exact bundles as the milestone baseline.
Do not rename, re-tag, or replace these assets.
Obtain them with `mise x -- just baseline-fetch v0.3.0 <new-output>`; the release `README.md` lists equivalent manual commands.

## Retrieve the retained baseline

`baseline-fetch` downloads the designated release assets with `gh release download`, verifies them, and extracts the selected bundles into a new directory outside the repository.
It needs an authenticated `gh`, Python and uv from `mise.toml`, and JDK 17 or 21 for the final `baseline-check`.
Without bundle names it fetches all four bundles.
Give absolute paths: just runs the recipe from the repository root, so a relative path resolves there and is rejected.

```sh
mise x -- just baseline-fetch v0.1.0 /tmp/baseline-v0.1.0
mise x -- just baseline-fetch v0.1.0 /tmp/baseline-v0.1.0-flink2-pb3 flink2-pb3
```

The output contains `release/`, which holds `SHA256SUMS`, `FINAL-BASELINE.json`, `INVENTORY.json`, and the downloaded archives, and one directory per bundle.
With `--archives <directory>`, archives are kept in that directory instead and reused by later runs; for example, `just baseline-fetch v0.1.0 /tmp/new-output flink1-pb3 --archives /tmp/baseline-archives`.

The checks run in this order:

1. The SHA-256 of the downloaded `SHA256SUMS` must equal the digest pinned for the tag in `scripts/baseline-fetch.py`, before any archive is downloaded.
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
Add a pin for a later designated baseline only after reviewing its `SHA256SUMS` and final manifest, as the [publication procedure](#publish-a-milestone-baseline) does.

The follow-up checks consume only part of each bundle: the state archives and snapshot fixtures under `fixtures/`, the manifests (`manifest.properties` and the state and capture properties), the schemas `apps/original/schema.pb` and `apps/changed/schema.pb`, and from format 2 also `apps/enum-original/schema.pb` and `apps/enum-added/schema.pb`, `library.jar`, and the compiled consumers in `tools.jar`.
The `dependencies/` tree was resolved on macOS/aarch64.
Several of its jars carry native libraries, such as RocksDB, Netty, Snappy, and LZ4 for a set of operating systems and architectures, and that resolution and its native loading are not claimed for another host.
CI consumers must not put that tree on a classpath; they resolve their runtime dependencies for the runner instead.
For the same reason, CI does not run `baseline-read` on the retained bundles, because the reader starts the retained code with those dependency jars.

### Retained bundles in CI

The composite action `.github/actions/baseline-fetch` fetches one bundle per invocation:

```yaml
- uses: ./.github/actions/baseline-fetch
  with:
    tag: <tag>
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
The `GITHUB_TOKEN` with `contents: read` is sufficient for the download.

No Verify job currently uses this action, because the current library does not read the state of the retained 0.x bundles.
The release that retains the first published bundles adds a job that fetches them and runs the restore, API, and consumer checks against the candidate.

## Restore retained state with the current library

`baseline-upgrade` restores a verified bundle's retained state and snapshot fixtures with the current library and the current tree's runtime applications, which contain only the current generated classes:

```sh
mise x -- just baseline-upgrade /tmp/baseline/flink2-pb3 2.3.0
```

The recipe runs the bundle's `baseline-check`, then runs `RetainedBaselineUpgradeITCase` in a clean Maven build that uses the bundle's Protobuf profile, its exact Protobuf runtime and protoc version, its artifact line, and the given Flink version.
Maven resolves this build's own dependencies, so the bundle's `dependencies/` tree and its `library.jar` are never on the classpath; apart from the bundle's own `baseline-check`, the retained inputs are data: state archives, snapshot fixtures, manifests, and schemas.
The test refuses a Protobuf runtime other than the one that wrote the bundle and a Flink version that the bundle was not read with, so each run keeps the gencode/runtime pair and the Flink version fixed and changes only the library and the application schema.
It also requires the current `original` and `changed` application descriptors, and from format 2 the `enum-original` and `enum-added` ones, to be byte-identical to the bundle's schemas, which makes the restore with the writer's schema the unchanged-schema control.

For each backend, setting budget, and archive kind (the canonical savepoint and the full checkpoint), the test requires the state identity recorded in `state.properties` and restores the archive with the writer's recorded settings into the [runtime schemas](runtime-recovery.md#schema-changes) that differ from the retained writer:

| Application | Expected result |
|---|---|
| `original` | Restored values, continued processing, and a second restore from the new application's savepoint |
| `changed` | The same, with `added` empty in retained values and set in new values |
| `reserved` | The same, with the retained payload kept as an unknown field and read back with the original schema |
| `incompatible` | The restore fails and names the generated class and `Field 2 (count) ... changes type from INT32 to SINT32` |

The restore checks are those of the [schema-evolution test](runtime-recovery.md#schema-changes), including the operator ListState values and the ValueState and MapState values that later inputs replace.
Each retained archive holds all three state kinds, so the rejection shows only that whichever state the backend restores first fails; the schema-evolution test rejects each state kind from a savepoint that holds only that kind.
The current library must restore each snapshot fixture's framed value at its recorded settings; when it writes the fixture's snapshot version, it must also write byte-identical snapshot metadata, which shows that the snapshot format and test schema are unchanged.
From format 3, the DynamicMessage snapshot fixture restores without the application's descriptor, at default settings; its restored root must have the test schema's content, and its value must keep every field.

From format 2, the test also restores each backend's `evolved` and `enum` fixtures, and from format 3 its `dynamic` fixture, both archive kinds, with the [reader checks](#inspect-and-restore) of the bundle's own reader: `evolved` restores into `changed`, and its savepoint is rejected by `original`; `enum` restores into `enum-original` and `enum-added`; and `dynamic` restores into the DynamicMessage application with `original` and `changed`, with the descriptors that restored values carry as in the [DynamicMessage recovery test](runtime-recovery.md#dynamicmessage), and its savepoint is rejected with `incompatible`.
It requires their recorded state identity to equal what capture writes; bundles of an earlier format skip the cases that their format lacks.

## Retention

A failed capture leaves an incomplete destination without a successful checksum inventory; retry in a new directory.
Never repair historical provenance or replace files inside a completed bundle.
The repository commits no snapshot or savepoint fixtures; the 1.0.0 format restart removed the 0.x ones.

Keep unreviewed bundles in an access-controlled external directory, backed up outside temporary build and worktree cleanup locations when retaining them for future work.
Do not use an expiring CI artifact or `/tmp` as the sole long-term copy.
CI rehearsals stay in runner temporary storage and are not uploaded by this workflow.

Command records and build/runtime logs preserve exact local paths and may contain machine-identifying information.
Before copying any bundle to shared storage, inspect the exact inventory, archive members, source content, commands, and logs; keep credentials and raw JVM diagnostics out of capture processes.
The checksum checker is not the benchmark content gate in #41 and does not prove the absence of arbitrary secrets.
Retain original bytes privately if a separately sanitized derivative is needed, and identify that derivative with its own inventory.

[`baseline-upgrade`](#restore-retained-state-with-the-current-library) implements the check from #16: it consumes the retained schema descriptors, snapshot bytes, state archives, expected-value manifests, and application identities with separately built new-schema applications.
For #17, `api-compat` compares the retained `library.jar` with a candidate built for the same line with Protobuf 4, and `baseline-consume` runs the retained `tools.jar` with the candidate on an explicit classpath of the current tree's dependencies, without recompiling the old consumer.
Neither follow-up should rebuild a newer working tree and label it the old writer.
