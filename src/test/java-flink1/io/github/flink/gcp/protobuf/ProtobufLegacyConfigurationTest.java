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

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.GlobalConfiguration;

import io.github.flink.gcp.protobuf.generated.ScalarMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufLegacyConfigurationTest {
    @TempDir Path directory;

    @Test
    void registrationListRequiresStandardYamlAndLegacyFileTakesPrecedence() throws Exception {
        String yaml =
                """
                pipeline.generic-types: false
                pipeline.serialization-config:
                  - com.google.protobuf.AbstractMessage: {type: typeinfo, class: io.github.flink.gcp.protobuf.ProtobufTypeInfoFactory}
                """;
        Files.writeString(directory.resolve("config.yaml"), yaml);
        Path legacyFile = directory.resolve("flink-conf.yaml");
        Files.writeString(legacyFile, yaml);

        var legacy = GlobalConfiguration.loadConfiguration(directory.toString());
        assertThat(GlobalConfiguration.isStandardYaml()).isFalse();
        var legacySerializerConfig = new SerializerConfigImpl();
        legacySerializerConfig.configure(legacy, getClass().getClassLoader());
        assertThat(legacySerializerConfig.hasGenericTypesDisabled()).isTrue();
        var generic = TypeInformation.of(ScalarMessage.class);
        assertThat(generic).isInstanceOf(GenericTypeInfo.class);
        assertThatThrownBy(() -> generic.createSerializer(legacySerializerConfig))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Generic types have been disabled");

        Files.delete(legacyFile);
        var standard = GlobalConfiguration.loadConfiguration(directory.toString());
        assertThat(GlobalConfiguration.isStandardYaml()).isTrue();
        var nativeSerializerConfig = new SerializerConfigImpl();
        nativeSerializerConfig.configure(standard, getClass().getClassLoader());
        assertThat(nativeSerializerConfig.hasGenericTypesDisabled()).isTrue();
        var nativeType = TypeInformation.of(ScalarMessage.class);
        assertThat(nativeType).isEqualTo(ProtobufTypeInformation.of(ScalarMessage.class));
        assertThat(nativeType.createSerializer(nativeSerializerConfig))
                .isInstanceOf(ProtobufTypeSerializer.class);
    }
}
