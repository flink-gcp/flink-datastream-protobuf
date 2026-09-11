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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotMessages.Envelope;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Reads fixed development-format fixtures; ordinary test runs never regenerate them. */
class ProtobufSnapshotFixtureTest {
    @Test
    void fixedV1FixturesRestoreValuesAndPreserveTheRecordedFormat() throws Exception {
        for (String profile : List.of("3", "4")) {
            for (String variant : List.of("default", "raised")) {
                String resource =
                        "/snapshots/v1/protobuf" + profile + "-" + variant + ".properties";
                Properties fixture = new Properties();
                try (InputStream in = getClass().getResourceAsStream(resource)) {
                    assertThat(in).as(resource).isNotNull();
                    fixture.load(in);
                }
                assertThat(fixture.getProperty("status")).isEqualTo("development-format-fixture");
                assertThat(fixture.getProperty("snapshot.version")).isEqualTo("1");
                byte[] snapshotBytes = bytes(fixture, "snapshot");
                byte[] messageBytes = bytes(fixture, "message");
                ProtobufSerializerSettings settings =
                        new ProtobufSerializerSettings(
                                Boolean.parseBoolean(fixture.getProperty("settings.deterministic")),
                                Integer.parseInt(fixture.getProperty("settings.maxMessageSize")),
                                Integer.parseInt(fixture.getProperty("settings.recursionLimit")));
                ProtobufTypeSerializer<Envelope> current =
                        ProtobufTypeSerializerSnapshotTest.serializer(settings);
                TypeSerializerSnapshot<Envelope> saved =
                        ProtobufTypeSerializerSnapshotTest.readVersioned(
                                snapshotBytes, getClass().getClassLoader());
                assertThat(
                                current.snapshotConfiguration()
                                        .resolveSchemaCompatibility(saved)
                                        .isCompatibleAsIs())
                        .as(resource)
                        .isTrue();
                for (ProtobufSerializerSettings lowered :
                        List.of(
                                new ProtobufSerializerSettings(
                                        settings.deterministicSerialization(),
                                        settings.maxMessageSize() - 1,
                                        settings.recursionLimit()),
                                new ProtobufSerializerSettings(
                                        settings.deterministicSerialization(),
                                        settings.maxMessageSize(),
                                        settings.recursionLimit() - 1))) {
                    assertThat(
                                    ProtobufTypeSerializerSnapshotTest.serializer(lowered)
                                            .snapshotConfiguration()
                                            .resolveSchemaCompatibility(saved)
                                            .isIncompatible())
                            .as("%s with lowered limits %s", resource, lowered)
                            .isTrue();
                }
                TypeSerializer<Envelope> restored = saved.restoreSerializer();
                assertThat(restored).isEqualTo(current);
                assertThat(restored.deserialize(new DataInputDeserializer(messageBytes)))
                        .isEqualTo(ProtobufTypeSerializerSnapshotTest.value());
                assertThat(
                                ProtobufTypeSerializerSnapshotTest.versioned(
                                        current.snapshotConfiguration()))
                        .isEqualTo(snapshotBytes);
                DataOutputSerializer output = new DataOutputSerializer(32);
                current.serialize(ProtobufTypeSerializerSnapshotTest.value(), output);
                assertThat(output.getCopyOfBuffer()).isEqualTo(messageBytes);
            }
        }
    }

    private static byte[] bytes(Properties fixture, String kind) throws Exception {
        byte[] bytes = Base64.getDecoder().decode(fixture.getProperty(kind + ".base64"));
        assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)))
                .isEqualTo(fixture.getProperty(kind + ".sha256"));
        return bytes;
    }
}
