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

package io.github.flink.gcp.protobuf.spike;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import io.github.flink.gcp.protobuf.spike.generated.ProbeMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InterfaceRegistrationTest {
    @Test
    void interfaceRegistrationDoesNotReachGeneratedClasses() {
        Configuration config = new Configuration();
        config.set(PipelineOptions.GENERIC_TYPES, false);
        config.set(
                PipelineOptions.SERIALIZATION_CONFIG,
                List.of(
                        "com.google.protobuf.Message: {type: typeinfo, class: io.github.flink.gcp.protobuf.spike.ProbeTypeInfoFactory}"));
        SerializerConfigImpl serializerConfig = new SerializerConfigImpl();
        serializerConfig.configure(config, getClass().getClassLoader());
        assertThatThrownBy(
                        () ->
                                TypeInformation.of(ProbeMessage.class)
                                        .createSerializer(serializerConfig))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Generic types have been disabled");
    }
}
