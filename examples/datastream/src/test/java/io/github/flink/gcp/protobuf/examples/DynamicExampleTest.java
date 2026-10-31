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

package io.github.flink.gcp.protobuf.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.protobuf.examples.generated.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(120)
class DynamicExampleTest {
    @Test
    void loadsTheDescriptorSetInsteadOfTheGeneratedDescriptor() throws Exception {
        var descriptor = DynamicExample.eventDescriptor();
        assertThat(descriptor.getFullName()).isEqualTo(Event.getDescriptor().getFullName());
        assertThat(descriptor).isNotSameAs(Event.getDescriptor());
        assertThat(descriptor.getFields())
                .extracting(field -> field.getName() + "=" + field.getNumber())
                .containsExactly("account=1", "amount=2");
    }

    /** The compiled example classes, including their lambdas, never name a generated class. */
    @Test
    void referencesNoGeneratedMessageClass() throws Exception {
        for (Class<?> type :
                List.of(
                        DynamicExample.class,
                        DynamicExample.ParseEvent.class,
                        DynamicExample.RunningTotal.class)) {
            String name = type.getName().substring(type.getPackageName().length() + 1);
            try (InputStream input = type.getResourceAsStream(name + ".class")) {
                assertThat(new String(input.readAllBytes(), StandardCharsets.ISO_8859_1))
                        .as("Constant pool of %s", type.getName())
                        .doesNotContain("io/github/flink/gcp/protobuf/examples/generated");
            }
        }
    }

    @Test
    void keepsDynamicMessageTotalsForEachScalarKey() throws Exception {
        try (var env =
                StreamExecutionEnvironment.createLocalEnvironment(
                        2, ExplicitExample.configuration())) {
            env.disableOperatorChaining();
            assertThat(env.getConfig().getSerializerConfig().hasGenericTypesDisabled()).isTrue();
            assertThat(DynamicExample.totals(env).executeAndCollect(4))
                    .containsExactlyInAnyOrder("alice:2", "bob:7", "alice:5", "alice:9");
        }
    }
}
