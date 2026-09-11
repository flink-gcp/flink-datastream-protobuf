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

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks the actual runtime and production-code location selected by verification recipes. */
class RuntimeCompatibilityTest {
    @Test
    void loadsTheRequestedFlinkRuntime() throws Exception {
        String version = System.getProperty("protobuf.test.flink.version");
        assertThat(version).isNotBlank();
        Path location =
                Path.of(
                        TypeSerializer.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI());
        assertThat(location.getFileName().toString()).isEqualTo("flink-core-" + version + ".jar");
    }

    @Test
    void loadsProductionCodeFromTheConfiguredDirectoryOrJar() throws Exception {
        Path expected =
                Path.of(System.getProperty("protobuf.test.production.classes")).toRealPath();
        for (Class<?> type :
                new Class<?>[] {
                    ProtobufTypeSerializer.class,
                    ProtobufTypeInformation.class,
                    ProtobufTypeInfoFactory.class
                }) {
            Path actual =
                    Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI())
                            .toRealPath();
            assertThat(actual).as(type.getName()).isEqualTo(expected);
        }
    }
}
