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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.protobuf.snapshotgenerated.SnapshotMessages.Envelope;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Already-compiled application-facing API calls for later candidate-classpath checks. */
public final class BaselineConsumer {
    private BaselineConsumer() {}

    public static void main(String[] args) throws Exception {
        BaselineTool.require(args.length == 1, "Expected a new consumer-result path");
        var explicit = ProtobufTypeInformation.of(Envelope.class);
        var built =
                ProtobufTypeInformation.newBuilder(Envelope.class)
                        .deterministicSerialization(false)
                        .maxMessageSize(67108864)
                        .recursionLimit(100)
                        .build();
        assertThat(explicit).isEqualTo(built);
        assertThat(explicit.isKeyType()).isFalse();
        assertThat(explicit.getTypeClass()).isEqualTo(Envelope.class);
        assertThat(new ProtobufTypeInfoFactory().createTypeInfo(Envelope.class, Map.of()))
                .isEqualTo(explicit);
        Configuration config = new Configuration();
        config.set(PipelineOptions.GENERIC_TYPES, false);
        config.set(
                PipelineOptions.SERIALIZATION_CONFIG,
                List.of(
                        "com.google.protobuf.AbstractMessage: {type: typeinfo, class: io.github.flink.gcp.protobuf.ProtobufTypeInfoFactory}"));
        try (var env = StreamExecutionEnvironment.createLocalEnvironment(2, config)) {
            assertThat(TypeInformation.of(Envelope.class)).isEqualTo(explicit);
            assertThat(
                            explicit.createSerializer(env.getConfig().getSerializerConfig())
                                    .copy(Envelope.newBuilder().setId(42).build()))
                    .isEqualTo(Envelope.newBuilder().setId(42).build());
        }
        Properties result = new Properties();
        result.setProperty("status", "passed");
        result.setProperty(
                "library.sha256",
                BaselineTool.hash(
                        Path.of(
                                ProtobufTypeInformation.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI())));
        BaselineTool.save(result, Path.of(args[0]));
    }
}
