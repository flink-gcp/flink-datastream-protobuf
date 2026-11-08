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

# ADR-0006: Releases stage both artifact lines on the Central Portal and publish after both validate

- Status: Accepted
- Date: 2026-10-31
- Issue: [#13](https://github.com/flink-gcp/flink-datastream-protobuf/issues/13)
- Publication wait and re-runs amended: 2026-11-01, [issue #181](https://github.com/flink-gcp/flink-datastream-protobuf/issues/181)
- Reproducible main jar added: 2026-11-08, [issue #177](https://github.com/flink-gcp/flink-datastream-protobuf/issues/177)
- Implements: The publication requirements of [ADR-0003](0003-flink-version-compatibility.md#state-and-release-acceptance)
- Implementation: The release build, bundle inspection, unsigned CI rehearsal, `Release` workflow, and Central Portal helper are implemented; version 1.0.0 is the first publication through them
- Current instructions: [Release artifacts](../development.md#release-artifacts) and [Publishing a release](../development.md#publishing-a-release)

## Context

Version 1.0.0 is the first Maven Central publication of `io.github.flink-gcp:flink-datastream-protobuf`.
Central never deletes or replaces a published version, so a defect found after publication requires a new version.
ADR-0003 requires two artifact lines from one source revision, `X.Y.Z` built at the Flink 2.x floor and `X.Y.Z-1.20` built for Flink 1.20, a guard on Maven's effective model before publication, and validation of both before either is published.

The `io.github.flink-gcp` namespace is verified on the Central Portal, and flink-connector-gcp has published its two lines there since 1.0.0.
Its [ADR-0147](https://github.com/flink-gcp/flink-connector-gcp/blob/ef78c13853d00f76ec58b923e6f8b8e8dd706b35/docs/adr/0147-releases-stage-on-the-central-portal-and-publish-by-hand.md) stages each line with `central-publishing-maven-plugin`, publishes through the Portal API only after both deployments validate, and rehearses each tag without uploading.
That repository's first `v1.2.0` tag run failed Central validation because Maven 3.10.0 wrote local-repository files into the staging directory; an upload-free rehearsal of the real bundle would have found it before the tag.

Both repositories inherit `org.apache.flink:flink-connector-parent:2.0.0`.
Its `release` profile targets the ASF repository flow: it pins maven-gpg-plugin 1.4, which cannot read a key from the environment, and attaches no sources jar.
It also sets the compiler source and target to 11, but this project's own properties keep both at 17, as `maven.compiler.release` does.
Its Javadoc configuration disables doclint, and its `java11` profile adds an `--add-exports` option that javadoc rejects together with `--release 17`, so `javadoc:javadoc` failed in this project.

## Decision

### Release build

`just release-stage <version>` re-versions a disposable checkout with `versions:set` and runs `clean deploy -Drelease -Dmaven.test.skip=true -Dmaven.install.skip=true`; a `-1.20` version adds `-Dflink.compat=flink1`.
`-Drelease` activates the parent's `release` profile and this project's `central-release` profile.
The second profile attaches the sources jar, signs with maven-gpg-plugin 3.x's Bouncy Castle signer from `MAVEN_GPG_KEY` and `MAVEN_GPG_PASSPHRASE`, and replaces `deploy` with `central-publishing-maven-plugin` using `autoPublish=false` and `waitUntil=validated`.
It also fixes `ignorePublishedComponents=false`, because that check reads Central credentials even when the upload is skipped, and `waitForPublishCompletion=false`, because that deprecated option switches the upload to automatic publication despite `autoPublish=false`.
The release neither compiles nor runs tests, because the release workflow builds only a commit that has already passed CI.
It does not install the release version into the local repository, so a later check of the published artifacts cannot resolve a locally built copy instead of the one on Central.

The POM replaces the parent's Javadoc options with full doclint and `failOnWarnings`.
The Maven `javadoc:javadoc` goal, which also generates the site's API reference, and the release Javadoc jar therefore succeed without the incompatible export and fail on the same warnings.

The POM also replaces the parent's maven-jar-plugin 2.4, which ignores `project.build.outputTimestamp`, with 3.x.
The main jar's entries then carry the inherited fixed timestamp, so building one source tree with one JDK reproduces it byte for byte.
Compared with the 1.0.0 jar, its manifest no longer carries `Built-By`, which named the build user, `Build-Jdk` with the JDK's patch release, `Archiver-Version`, or `Implementation-Vendor-Id`, and it adds `Build-Jdk-Spec` with the building JDK's feature release and `Java-Version` with the compiler release, 17.
[ADR-0004](0004-serializer-benchmarks.md) relies on that to show that benchmark runs of both Protobuf profiles measured the same jar.

The published POM is the project POM with its release version.
Its parent declares no dependencies, and the POM declares only `provided` and `test` dependencies, so neither line adds a transitive dependency to an application.
The POM is not flattened; consequently, the `X.Y.Z-1.20` POM still names the 2.x floor as the default version of its `provided` Flink dependencies, which applications do not inherit.

### Signing key and credentials

Both repositories in the namespace use the same project release key and Central Portal token.
The key's private part and passphrase are `MAVEN_GPG_KEY` and `MAVEN_GPG_PASSPHRASE`, and the token pair is `CENTRAL_TOKEN_USERNAME` and `CENTRAL_TOKEN_PASSWORD`.
They are secrets of the `release` environment, which only the `Release` workflow's `stage` and `publish-central` jobs use and whose deployment policy admits only the `main` branch and `v*` tags, without required reviewers.
A repository ruleset restricts creating, moving, and deleting `v*` tags to repository administrators.
A run from any other ref, including a dispatch from an unmerged branch whose workflow file drops the ancestry check, therefore receives no release secret, and only an administrator can cut the tag that publishes.
Pull-request builds and the CI rehearsal read none of them.

### Effective-model guard

The `enforce-release-line-toolchain` rule in `central-release` evaluates the final effective model at `validate`, before anything is signed or uploaded.
It accepts only a bare `X.Y.Z` built with `flink2` at `release.flink.floor`, or `X.Y.Z-1.20` built with `flink1` at `release.flink.lts`, and requires Protobuf 4 for both, the release baseline in ADR-0001.
A check inside the build sees toolchain properties however they were set, including through a `-D` long form, `MAVEN_OPTS`, or `.mvn/maven.config`, which argument inspection would miss.
The execution sets `skip` and `fail` explicitly, so `-Denforcer.skip`, `-Denforcer.fail=false`, and the parent's `fast` profile cannot disable it.
The guard checks how the toolchain is configured, not who controls the command line: arguments that replace or skip the configured rules, such as `-Drules`, or that deactivate the profile with `-P!central-release` are outside its protection.
The release workflow passes none, and the rehearsal's guard check runs with the same environment, so an inherited argument that disables the guard fails the rehearsal.
`release.flink.floor` and `release.flink.lts` repeat the floor and the `flink1` profile's version as literals, so overriding `flink.version` alone does not move the expected version.
A Flink update that changes only `flink.version` fails the CI rehearsal until the matching literal moves.

### Bundle inspection and rehearsal

`just release-rehearse <version>` exports the committed HEAD, stages both lines there with `-DskipUpload=true`, and inspects the bundles that the plugin would upload.
It also stages the `-1.20` version with the 2.x adapter and requires the guard to reject it, so a change that disables the guard fails the rehearsal.
The inspection compares each bundle with the exported sources.
It accepts only the version's POM, jar, sources jar and Javadoc jar, their signatures, and their checksums.
The main jar may hold only classes compiled from the line's production source roots, the manifest, Maven metadata, and the tracked LICENSE and NOTICE.
Classes must target Java 17, no UTF-8 constant may name Kryo, Chill, Flink's Kryo serializer or `GenericTypeInfo` in internal form, and no class may reference a Flink Kryo member; dotted names in messages and the library's own member names may still mention Kryo.
The sources jar must equal the line's production sources, which also distinguishes the two adapters.
The POM must carry the coordinates and metadata that Central requires and no dependency that reaches an application.
The [development guide](../development.md#release-artifacts) lists the individual checks.

Every Verify run rehearses both lines without signatures, so a pull request that breaks the release build, the guard, or the artifact contents fails before any tag.
A signed rehearsal requires the signing variables; contributors use a throwaway key, and the release workflow's manual dispatch uses the release key.

### Publication

A `vX.Y.Z` tag on a commit contained in `main` starts the `Release` workflow; a `validate` dispatch also requires a commit in `main`, so that no unreviewed build configuration uploads with the release credentials.
The workflow first runs the signed rehearsal of both lines from the tagged commit, so that the inspection and the guard check precede any upload.
It then stages `X.Y.Z` and `X.Y.Z-1.20` as two Portal deployments with `just release-stage`, records each deployment ID before the plugin waits for validation, and publishes both only after both have validated.
After both are published, it creates the stable `vX.Y.Z` GitHub Release that the documentation hook in ADR-0002 deploys.
A manual dispatch rehearses both lines without uploading by default, or, with `mode: validate`, also uploads both, waits for validation, and drops them.
A dispatch never publishes or creates a GitHub Release.
The two Publish requests are not a transaction; recovery publishes or drops the recorded deployments instead of staging another pair under the same version.
The Portal helper is ported from flink-connector-gcp with its tests.

This repository ports ADR-0147's shape without the parts that serve a multi-module reactor: there are no uber-jars to attach to the GitHub Release, and a single module needs no module-local publication exclusions.

### Publication wait and re-runs

The `v1.0.0` tag run ([41269811182](https://github.com/flink-gcp/flink-datastream-protobuf/actions/runs/41269811182)) staged both lines in 3 minutes, and the helper requested Publish for both at 03:38:51 and 03:38:52 UTC.
Both deployments were still `PUBLISHING` when the 30-minute wait expired at 04:08:56 UTC, and `repo1.maven.org` served both versions between 06:15 and 06:20 UTC, about 2 h 41 min after the request.
The time at which the Portal reported `PUBLISHED` was not recorded; the `repo1.maven.org` directory listing dates the files 04:40 UTC.
The timeout failed the single job, which skipped the GitHub Release and the documentation; the maintainer finished both by hand.
flink-connector-gcp's `v1.3.0` run failed the same way that morning; its `v1.1.0` and `v1.2.0` publications had taken 14 and 4 minutes.
A re-run of that job would have staged and uploaded another pair, so recovery needed the Central token outside the `release` environment.

The helper now waits up to three hours, and the workflow follows flink-connector-gcp's [#1982](https://github.com/flink-gcp/flink-connector-gcp/pull/1982) in splitting into three jobs, so that a re-run no longer stages:

- `stage` rehearses, stages, and validates both lines, then hands both deployment IDs on as job outputs.
  The step that hands them on runs only after every earlier step succeeded, and the later jobs start only on this job's success.
  It holds the signing key and the Central token with `contents: read` and `actions: read`, within 120 minutes for two validations at the plugin's 30-minute default and the builds.
- `publish-central` runs `just central-portal publish <2x-id> <1x-id>`: it sends Publish only for a `VALIDATED` deployment, waits on a `PUBLISHING` one, and returns at once for `PUBLISHED` ones.
  It holds the Central token with `contents: read`, has no signing key, runs no build, and stops after 200 minutes, enough for the three-hour wait and setup.
- `github-release` creates the GitHub Release with generated notes unless an earlier attempt already published it, and stops at a draft or a prerelease, which only a maintainer creates and the documentation site skips.
  It holds `contents: write` and no release secret.

When `publish-central` or `github-release` fails, "Re-run failed jobs" runs it again with the job outputs of the original `stage` attempt.
flink-connector-gcp measured this on a throwaway workflow ([run 41286883270](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/41286883270)): the re-run job read the first attempt's output, and the succeeded job was not re-run.
Each later job is safe to repeat, so a publication that outlasts the wait, or a GitHub error during the Release, needs only that button.
A successful re-run completes the run with the `push` event, which the CI `workflow_run` hook requires; in flink-connector-gcp, each attempt's completion has fired its own `workflow_run` event, but the `conclusion` that a successful re-run attempt reports has not yet been observed, and the documented fallback is a manual CI dispatch.
"Re-run all jobs" after a successful `stage` would stage the version again, so `stage` first reads the jobs of every earlier attempt and refuses before anything is checked out or signed if it succeeded in any of them.
A refused attempt has no outputs for the later jobs, so after that mistake the run is finished by hand.
After `stage` failed in every earlier attempt, a re-run stages afresh, as before.

## Alternatives

- **Automatic publication per line.**
  `autoPublish=true` would publish the first line before the second had built or validated, which ADR-0003 forbids.
- **The `maven-release-plugin` flow.**
  It pushes version commits and the tag from the build, which makes the tag an output of CI and writes to the protected `main` branch.
  Here a maintainer cuts the tag on a commit that has passed CI, and `versions:set` changes the version only in the release checkout.
- **A separate signing key for this repository.**
  Both repositories publish to one namespace and Portal organization with the same maintainers; a second key adds rotation and keyserver work without separating any authority.
- **Splitting the build from the upload, to inspect the bundle that is uploaded.**
  The plugin builds and uploads in one goal, so uploading an inspected bundle would require building it separately and uploading it through the Portal API, replacing the plugin's upload path.
  The tag run instead inspects a separate signed build of the same commit before staging, and Central validates the uploaded bundle.
- **Flattening the published POM.**
  A flattened POM would name the 1.20 runtime for the LTS line, but it changes no resolution result while every dependency is `provided` or `test`, and it adds a plugin to the release build.
- **A resume dispatch with typed deployment IDs.**
  Proposed in [#181](https://github.com/flink-gcp/flink-datastream-protobuf/issues/181) and drafted first in flink-connector-gcp.
  A mistyped ID, or the pair of a run that failed before Publish, would be published irreversibly, and the deployments' Portal names cannot show whether that run passed its gates.
  A dispatch also runs the dispatched ref's copy of the workflow, so a tag cut before the change has no resume mode, and a dispatch is not a `push` run, so the documentation would need a wider `workflow_run` guard or an `actions: write` dispatch.
  A re-run carries the IDs from the job that captured them and needs no input.
- **Telling a version's state from its coordinates.**
  `repo1.maven.org` shows a version only once it is `PUBLISHED`, and the Portal documents no lookup by coordinates and no listing of deployments, so a run could not tell `PUBLISHING` from never uploaded.
- **Scanning bytecode with `jdeps` or `javap`.**
  Both need a JDK and a resolved classpath in the inspection; the constant-pool reader in the standard-library inspection tool needs neither.

## Consequences

- Each Verify run adds one job that builds both lines without tests, and each `Release` run builds both lines twice, once to rehearse and once to stage, plus the rehearsal's guard check, which stops at `validate`.
- The inspection allow-lists classes by their source files, so a resource, a production source root, or a second top-level class in one source file requires changing the inspection with it.
- A local rehearsal writes its exported tree and bundles to a new external directory and leaves the checkout unchanged.
- The release key and token are shared with flink-connector-gcp; rotating either requires updating both repositories' secrets.
- A re-run uses the workflow and scripts of the tagged commit, so the re-run recovery applies only to tags cut after [#181](https://github.com/flink-gcp/flink-datastream-protobuf/issues/181); `v1.0.0` and a refused re-run are recovered by hand with the Central token pair.
