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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiNegativeControlTest {
    private static final String FACTORY = "io.github.flink.gcp.protobuf.ProtobufTypeInfoFactory";
    private static final String TYPE = "io.github.flink.gcp.protobuf.ProtobufTypeInformation";

    @TempDir Path temporary;

    @Test
    void requiresOnlyTheRemovedClass() throws Exception {
        Path report = report(type(FACTORY, "REMOVED", false), type(TYPE, "UNCHANGED", true));

        assertThat(ApiNegativeControl.check(report, FACTORY)).contains(FACTORY);
        // A misconfigured scope also reports unannotated test classes that the new jar drops.
        Path wider =
                report(
                        type(FACTORY, "REMOVED", false),
                        type("io.github.flink.gcp.protobuf.BaselineTool", "REMOVED", false));
        assertThatThrownBy(() -> ApiNegativeControl.check(wider, FACTORY))
                .hasMessageContaining("BaselineTool");
        Path missing = report(type(TYPE, "UNCHANGED", true));
        assertThatThrownBy(() -> ApiNegativeControl.check(missing, FACTORY))
                .hasMessageContaining("expected only the removal of " + FACTORY);
        Path modified = report(type(FACTORY, "MODIFIED", false));
        assertThatThrownBy(() -> ApiNegativeControl.check(modified, FACTORY))
                .hasMessageContaining("expected only the removal");
    }

    @Test
    void rejectsOtherXml() throws Exception {
        Path other = Files.writeString(temporary.resolve("other.xml"), "<report/>");
        assertThatThrownBy(() -> ApiNegativeControl.read(other))
                .hasMessageContaining("Not a japicmp XML report");
    }

    private Path report(String... types) throws Exception {
        Path file = Files.createTempFile(temporary, "japicmp", ".xml");
        Files.writeString(
                file,
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><japicmp><classes>"
                        + String.join("", types)
                        + "</classes></japicmp>");
        return file;
    }

    private static String type(String name, String status, boolean compatible) {
        return "<class fullyQualifiedName=\""
                + name
                + "\" changeStatus=\""
                + status
                + "\" binaryCompatible=\""
                + compatible
                + "\" sourceCompatible=\""
                + compatible
                + "\"/>";
    }
}
