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

# Development savepoint fixtures

These canonical savepoints were written by a development `0.1.0-SNAPSHOT` jar.
They are not published-release provenance.
Each Flink/Protobuf/backend directory contains `baseline` (deterministic false, 1024-byte payload limit, recursion limit 4) and `raised` (true, 2048 bytes, depth 8) settings.
These deliberately small test budgets are not the library defaults of 64 MiB and depth 100.
The writer JDK is 17; matching JDK 21 lanes also restore these fixtures on Flink 2.x.

Each fixture contains the complete `savepoint.zip`, its `manifest.properties`, and the original `runtime.proto` and descriptor set.
The job saves inputs 0 through 7 under four integer keys.
Each ValueState count is 2, each of the two string MapState user keys has count 1, and the union of the operator ListState partitions retains all eight messages.
Each message includes nested data and unknown field 100 with varint 99.
The tests verify those restored values and process inputs 8 through 15, checking the resulting state counts for both MapState user keys.
The source cursor, UIDs, state names, parallelism, and max parallelism are part of the fixture contract.

## Capture

The current Protobuf profiles select savepoints written with 3.25.9 and 4.33.6.
The current Flink/Protobuf matrix selects all 24 development fixtures.
Remove superseded development savepoints when no supported test selects them; their history remains in Git.
This rule does not remove snapshot-format baselines or published-release compatibility fixtures.
Ordinary verification selects an exact version and does not establish cross-Protobuf-version savepoint restoration.

See [Test resources and version updates](../../../../../docs/development.md#test-resources-and-version-updates) for the build/test call flow, exact-version fixture selection, and the Protobuf/Flink update checklist.
This section supplies the capture commands and the fixture-specific provenance requirements.

Capture is explicit and refuses to overwrite an existing fixture directory.
Use an output directory outside the repository, and review the resulting manifest and archive before intentionally adding or replacing a development fixture.
Ordinary verification reads fixtures without changing them.
Temporary automation belongs outside the repository.

For example, from the repository root, capture the Flink 2.2.1 / Protobuf 3 fixtures with JDK 17:

```sh
mise x -- ./mvnw -ntp clean -Pprotobuf3 -Dflink.version=2.2.1 -DskipTests package
mise x -- ./mvnw -ntp -Pprotobuf3 -Dflink.version=2.2.1 \
  -Dtest=ProtobufSavepointFixtureITCase \
  -Dtest.production.classes="$PWD/target/flink-datastream-protobuf-0.1.0-SNAPSHOT.jar" \
  -Dprotobuf.fixture.capture=/tmp/protobuf-savepoint-capture \
  -Dprotobuf.fixture.revision="$(git merge-base HEAD origin/main)" \
  surefire:test@integration-tests
```

Repeat with `-Pprotobuf4` for Protobuf 4 and `-Dflink.version=2.3.0` for the ceiling.
For LTS, use `-Dflink.compat=flink1 -Dflink.version=1.20.4` on both commands.
Clean before changing a runtime or generated-code profile.
The capture test exercises both backends and both settings, deletes its original temporary savepoints and writer checkpoint storage, and verifies restoration from the archives.

The manifest's base revision identifies the starting revision; individual source hashes identify the exact writer inputs, including uncommitted development changes.
Use a base revision reachable from `main` so that squashing the capture branch does not discard the revision pointer.
Do not rewrite old provenance hashes to match current source files.
For a released baseline, #13 must extend this procedure to verify released coordinates, the exact release tag/source and supplied jar, both artifact lines, and the new consumer classpath.
The release tracker #6 requires capture and consumer verification after actual publication.
Retain each released baseline without regenerating it with a later writer, for the supported direct and sequential upgrade tests through 0.3.0.
