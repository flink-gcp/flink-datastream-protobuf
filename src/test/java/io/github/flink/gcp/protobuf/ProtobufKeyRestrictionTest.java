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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.protobuf.Message;
import io.github.flink.gcp.protobuf.generated.ScalarMessage;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ProtobufKeyRestrictionTest {
    @Test
    void inferredAndExplicitKeyByBypassTheNativeNonKeyDeclaration() throws Exception {
        var config = new Configuration();
        config.set(PipelineOptions.GENERIC_TYPES, false);
        config.set(
                PipelineOptions.SERIALIZATION_CONFIG,
                List.of(
                        "com.google.protobuf.AbstractMessage: {type: typeinfo, class: io.github.flink.gcp.protobuf.ProtobufTypeInfoFactory}"));
        try (var env = StreamExecutionEnvironment.createLocalEnvironment(2, config)) {
            var info = TypeInformation.of(ScalarMessage.class);
            assertThat(info).isInstanceOf(ProtobufTypeInformation.class);
            assertThat(info.isKeyType()).isFalse();
            var stream = env.fromData(ScalarMessage.newBuilder().setInt32Value(7).build());
            var inferred = stream.keyBy(new MessageKeySelector());
            var explicit = stream.keyBy(new MessageKeySelector(), info);
            assertThat(inferred.getKeyType()).isEqualTo(info);
            assertThat(explicit.getKeyType()).isEqualTo(info);
            assertThat(
                            inferred.getKeyType()
                                    .createSerializer(env.getConfig().getSerializerConfig()))
                    .isInstanceOf(ProtobufTypeSerializer.class);
            inferred.map(ScalarMessage::getInt32Value).print();
            explicit.map(ScalarMessage::getInt32Value).print();
            assertThat(env.getStreamGraph().getStreamNodes()).isNotEmpty();
        }
    }

    @Test
    void identicalMessageBytesCanOccupyDifferentKeyGroupsAcrossGeneratedClassloaders()
            throws Exception {
        byte[] bytes =
                ScalarMessage.newBuilder().setStringValue("same payload").build().toByteArray();
        Set<Integer> groups = new HashSet<>();
        Set<Integer> hashes = new HashSet<>();
        // Search several real classloader identities instead of relying on one particular hash
        // pair.
        for (int attempt = 0; attempt < 32 && groups.size() < 2; attempt++) {
            try (var loader = new GeneratedMessageClassLoader()) {
                Class<? extends Message> type =
                        loader.loadClass(ScalarMessage.class.getName()).asSubclass(Message.class);
                Message value =
                        type.cast(type.getMethod("parseFrom", byte[].class).invoke(null, bytes));
                assertThat(value.toByteArray()).isEqualTo(bytes);
                assertThat(value.getClass().getClassLoader()).isSameAs(loader);
                hashes.add(value.hashCode());
                groups.add(KeyGroupRangeAssignment.assignToKeyGroup(value, 32768));
            }
        }
        assertThat(hashes)
                .as("same bytes need not have the same message hash")
                .hasSizeGreaterThan(1);
        assertThat(groups).as("same bytes need not select the same Flink key group").hasSize(2);
    }

    public static final class MessageKeySelector
            implements KeySelector<ScalarMessage, ScalarMessage> {
        @Override
        public ScalarMessage getKey(ScalarMessage value) {
            return value;
        }
    }
}
