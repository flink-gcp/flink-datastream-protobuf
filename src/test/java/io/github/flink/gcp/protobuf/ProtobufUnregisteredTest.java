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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import io.github.flink.gcp.protobuf.spike.generated.ProbeMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufUnregisteredTest {
    @Test
    void inheritedTypeHintUsesOrdinaryExtractionWithoutRegistration() {
        var info = ProtobufTypeInformation.of(new TypeHint<ProbeMessage>() {});
        assertThat(info).isInstanceOf(GenericTypeInfo.class);
        var config = new SerializerConfigImpl();
        config.setGenericTypes(false);
        assertThatThrownBy(() -> info.createSerializer(config))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Generic types have been disabled");
    }

    @Test
    void rejectsTheUnregisteredMessageWithoutGenericTypes() {
        var config = new SerializerConfigImpl();
        config.setGenericTypes(false);
        assertThatThrownBy(() -> TypeInformation.of(ProbeMessage.class).createSerializer(config))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Generic types have been disabled");
    }
}
