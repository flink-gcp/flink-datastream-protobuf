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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(120)
class StatefulExampleTest {
    @Test
    void keepsTotalsAndPreviousEventsSeparateForEachScalarKey() throws Exception {
        try (var env =
                StreamExecutionEnvironment.createLocalEnvironment(
                        2, ExplicitExample.configuration())) {
            env.disableOperatorChaining();
            assertThat(env.getConfig().getSerializerConfig().hasGenericTypesDisabled()).isTrue();
            assertThat(StatefulExample.totals(env).executeAndCollect(4))
                    .containsExactlyInAnyOrder("alice:2:0", "bob:7:0", "alice:5:2", "alice:9:3");
        }
    }
}
