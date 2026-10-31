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
import org.w3c.dom.Document;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks documented versions against the standalone application and library build. */
class DocumentationExamplesTest {
    @Test
    void documentedLaunchVersionsMatchTheBuiltDevelopmentArtifact() throws Exception {
        var factory = DocumentBuilderFactory.newInstance();
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        var parser = factory.newDocumentBuilder();
        var library = parser.parse(Path.of("pom.xml").toFile());
        var example = parser.parse(Path.of("examples/datastream/pom.xml").toFile());
        String floor = property(library, "/project/properties/flink.version");
        String protobuf4 = property(library, "/project/properties/protobuf.version");
        String lts =
                property(
                        library, "/project/profiles/profile[id='flink1']/properties/flink.version");
        String protobuf3 =
                property(
                        library,
                        "/project/profiles/profile[id='protobuf3']/properties/protobuf.version");
        String version = property(library, "/project/version");
        assertThat(property(example, "/project/properties/protobuf.integration.version"))
                .isEqualTo(version);
        assertThat(property(example, "/project/properties/flink.version")).isEqualTo(floor);
        assertThat(property(example, "/project/properties/protobuf.version")).isEqualTo(protobuf4);
        assertThat(Files.readString(Path.of("docs/quickstart.md")))
                .contains(
                        "sets `protobuf.integration.version` to `" + version + "`",
                        "examples-verify " + floor + " 4 ",
                        "examples-verify " + lts + " 3 ",
                        "-Dflink.version=" + lts + " ",
                        "-Dprotobuf.version=" + protobuf3 + " exec:exec@explicit",
                        "| " + protobuf4 + " or " + protobuf3 + " |");
        assertThat(Files.readString(Path.of("README.md")))
                .contains(
                        "`io.github.flink-gcp:flink-datastream-protobuf:" + version + "`",
                        "protobuf-java/protoc " + protobuf4 + " and " + protobuf3);
        assertThat(Files.readString(Path.of("docs/compatibility.md")))
                .contains(
                        "The current source version is `" + version + "`",
                        "CI pairs " + protobuf4 + " generated code with runtime " + protobuf4,
                        "and " + protobuf3 + " generated code with runtime " + protobuf3);
        assertThat(Files.readString(Path.of("docs/usage.md")))
                .contains("Current `" + version + "` development builds");
    }

    private static String property(Document document, String expression) throws Exception {
        String value = XPathFactory.newInstance().newXPath().evaluate(expression, document);
        assertThat(value).as(expression).isNotBlank();
        return value;
    }
}
