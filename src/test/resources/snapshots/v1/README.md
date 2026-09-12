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

# Version-1 development format fixtures

These fixtures were written by the unpublished 0.1.0-SNAPSHOT implementation of issue #10.
They preserve the format baseline; they are not evidence from a released Maven artifact or a Flink savepoint.
Issue #11 owns complete runtime savepoints, and #13/#6 must add fixtures from both published artifact lines before release completion.
Do not overwrite these files with later writers and describe them as the original writer's output.

Each properties file contains Base64-encoded Flink snapshot-envelope bytes and one framed message, with SHA-256 checksums.
The descriptor set inside the snapshot includes `snapshot.proto` and `snapshot_dependency.proto` from `src/test/proto`.
These schemas belong to this project and use the same Apache-2.0 license as the fixtures.
The saved Envelope has id 42, kind READY, detail text "saved", child id 7, nested text "nested", and unknown field 100 with varint 99.
The properties record the snapshot class/version, settings, Flink/JDK/protoc/runtime versions, development artifact coordinates, base revision, and exact writer/schema source hashes.
The base revision identifies the starting repository state; source hashes identify the unpublished implementation added on that base, rather than asserting that it was already in that commit.

| Variant | Deterministic | Size limit | Recursion limit |
|---|---|---|---|
| default | false | 67108864 | 100 |
| raised | true | 134217728 | 200 |

Both Protobuf 3.25.8 and 4.33.6 writers are retained.
The test schemas have matching normalized descriptors across these profiles; this does not promise restore across changed built-in descriptors or Flink savepoint versions.
Tests read all four fixtures, assert values including unknown fields, and compare the current writer's snapshot/message bytes against the fixed baseline.
Snapshot compatibility tests separately cover equal/increased limits and both deterministic-mode transitions as compatible, decreased limits and schema/name changes as incompatible, and corruption as an explicit read failure.
Tests also reject lowering either limit from each saved fixture's settings.

## Reproducing the development bytes

Run from the repository root on JDK 17 at the 2.2.1 compile floor.
Copy the Java listing below to `/tmp/SnapshotFixtureWriter.java`; keep the generator outside the repository.
Use a clean build for each runtime/gencode pair, retaining both outputs in the same temporary directory:

```sh
mise x -- ./mvnw -ntp clean -Pprotobuf3 -Dprotobuf.version=3.25.8 test-compile dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile=/tmp/snapshot-fixture-classpath.txt
mkdir -p /tmp/snapshot-fixture-writer
mise x -- javac -cp "target/classes:target/test-classes:$(cat /tmp/snapshot-fixture-classpath.txt)" \
  -d /tmp/snapshot-fixture-writer /tmp/SnapshotFixtureWriter.java
mise x -- java -cp "/tmp/snapshot-fixture-writer:target/classes:target/test-classes:$(cat /tmp/snapshot-fixture-classpath.txt)" \
  io.github.flink.gcp.protobuf.SnapshotFixtureWriter /tmp/protobuf-v1-fixtures 3 3.25.8 \
  8b07681d4a5911e7508e01cfdee005f15605b197
mise x -- ./mvnw -ntp clean -Pprotobuf4 test-compile dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile=/tmp/snapshot-fixture-classpath.txt
mkdir -p /tmp/snapshot-fixture-writer
mise x -- javac -cp "target/classes:target/test-classes:$(cat /tmp/snapshot-fixture-classpath.txt)" \
  -d /tmp/snapshot-fixture-writer /tmp/SnapshotFixtureWriter.java
mise x -- java -cp "/tmp/snapshot-fixture-writer:target/classes:target/test-classes:$(cat /tmp/snapshot-fixture-classpath.txt)" \
  io.github.flink.gcp.protobuf.SnapshotFixtureWriter /tmp/protobuf-v1-fixtures 4 4.33.6 \
  8b07681d4a5911e7508e01cfdee005f15605b197
```

Compare `snapshot.base64`, `message.base64`, and their hashes with the checked-in baseline.
Source hashes and JDK provenance can differ when reproducing bytes using revised sources or another JDK patch; those results are attributable to the new writer, not to the original one.
Ordinary tests only read fixtures and never invoke this generator.
When capturing published fixtures, use the published artifact as the writer and record its coordinates/checksum and source tag/commit in a new fixture directory for each artifact line.
Do not relabel these development files as released evidence.

```java
package io.github.flink.gcp.protobuf;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataOutputSerializer;
import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotDependencies.Detail;
import io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotMessages.Envelope;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;

public class SnapshotFixtureWriter {
    public static void main(String[] args) throws Exception {
        Path destination = Path.of(args[0]);
        Files.createDirectories(destination);
        for (boolean raised : new boolean[] {false, true}) {
            ProtobufSerializerSettings settings = raised
                    ? new ProtobufSerializerSettings(true, 134217728, 200)
                    : ProtobufSerializerSettings.DEFAULT;
            ProtobufTypeSerializer<Envelope> serializer = new ProtobufTypeSerializer<>(Envelope.class, settings);
            DataOutputSerializer snapshot = new DataOutputSerializer(32);
            TypeSerializerSnapshot.writeVersionedSnapshot(snapshot, serializer.snapshotConfiguration());
            Envelope value = Envelope.newBuilder().setId(42).setKind(Envelope.Kind.READY)
                    .setDetail(Detail.newBuilder().setText("saved"))
                    .setChild(Envelope.newBuilder().setId(7))
                    .setNested(Envelope.Nested.newBuilder().setText("nested"))
                    .setUnknownFields(UnknownFieldSet.newBuilder().addField(100,
                            UnknownFieldSet.Field.newBuilder().addVarint(99).build()).build()).build();
            DataOutputSerializer message = new DataOutputSerializer(32);
            serializer.serialize(value, message);
            Map<String, String> properties = new TreeMap<>();
            properties.put("status", "development-format-fixture");
            properties.put("writer.artifact", "io.github.flink-gcp:flink-datastream-protobuf:0.1.0-SNAPSHOT");
            properties.put("writer.base.revision", args[3]);
            properties.put("writer.flink", "2.2.1");
            properties.put("writer.protobuf", args[2]);
            properties.put("writer.protoc", args[2]);
            properties.put("writer.java", System.getProperty("java.runtime.version"));
            properties.put("settings.deterministic", Boolean.toString(raised));
            properties.put("settings.maxMessageSize", Integer.toString(settings.maxMessageSize()));
            properties.put("settings.recursionLimit", Integer.toString(settings.recursionLimit()));
            properties.put("snapshot.class", serializer.snapshotConfiguration().getClass().getName());
            properties.put("snapshot.version", "1");
            properties.put("snapshot.base64", Base64.getEncoder().encodeToString(snapshot.getCopyOfBuffer()));
            properties.put("snapshot.sha256", hash(snapshot.getCopyOfBuffer()));
            properties.put("message.base64", Base64.getEncoder().encodeToString(message.getCopyOfBuffer()));
            properties.put("message.sha256", hash(message.getCopyOfBuffer()));
            for (String source : new String[] {
                    "src/main/java/io/github/flink/gcp/protobuf/ProtobufTypeSerializerSnapshot.java",
                    "src/main/java/io/github/flink/gcp/protobuf/ProtobufTypeSerializer.java",
                    "src/main/java/io/github/flink/gcp/protobuf/ProtobufSchema.java",
                    "src/main/java/io/github/flink/gcp/protobuf/ProtobufMessageType.java",
                    "src/main/java/io/github/flink/gcp/protobuf/ProtobufSerializerSettings.java",
                    "src/main/java/io/github/flink/gcp/protobuf/ProtobufMessageSize.java",
                    "src/test/proto/snapshot.proto", "src/test/proto/snapshot_dependency.proto"}) {
                properties.put("source.sha256." + source, hash(Files.readAllBytes(Path.of(source))));
            }
            StringBuilder result = new StringBuilder("# Copyright 2026 The flink-gcp authors\n#\n"
                    + "# Licensed under the Apache License, Version 2.0 (the \"License\");\n"
                    + "# you may not use this file except in compliance with the License.\n"
                    + "# You may obtain a copy of the License at\n#\n"
                    + "#     http://www.apache.org/licenses/LICENSE-2.0\n#\n"
                    + "# Unless required by applicable law or agreed to in writing, software\n"
                    + "# distributed under the License is distributed on an \"AS IS\" BASIS,\n"
                    + "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
                    + "# See the License for the specific language governing permissions and\n"
                    + "# limitations under the License.\n\n");
            properties.forEach((key, val) -> result.append(key).append('=').append(val).append('\n'));
            Files.writeString(destination.resolve("protobuf" + args[1] + "-" + (raised ? "raised" : "default") + ".properties"), result);
        }
    }

    private static String hash(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
```
