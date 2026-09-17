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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks the published excerpts against the application sources compiled by the consumer build. */
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
        String protobuf3 = property(library, "/project/properties/protobuf.version");
        String lts =
                property(
                        library, "/project/profiles/profile[id='flink1']/properties/flink.version");
        String protobuf4 =
                property(
                        library,
                        "/project/profiles/profile[id='protobuf4']/properties/protobuf.version");
        assertThat(property(example, "/project/properties/protobuf.integration.version"))
                .isEqualTo(property(library, "/project/version"));
        assertThat(property(example, "/project/properties/flink.version")).isEqualTo(floor);
        assertThat(property(example, "/project/properties/protobuf.version")).isEqualTo(protobuf3);
        assertThat(Files.readString(Path.of("docs/quickstart.md")))
                .contains(
                        "examples-verify " + floor + " 3 ",
                        "examples-verify " + lts + " 4 ",
                        "-Dflink.version=" + lts + " ",
                        "-Dprotobuf.version=" + protobuf4 + " exec:exec@explicit");
    }

    private static String property(Document document, String expression) throws Exception {
        String value = XPathFactory.newInstance().newXPath().evaluate(expression, document);
        assertThat(value).as(expression).isNotBlank();
        return value;
    }

    @Test
    void documentedSnippetsMatchTheCompiledExamplesAndActualConfiguration() throws Exception {
        var snippet =
                Pattern.compile(
                        "<!-- example: ([A-Za-z0-9.#-]+) -->\\n```(?:java|yaml)\\n(.*?)\\n```\\n<!-- /example -->",
                        Pattern.DOTALL);
        List<String> checked = new ArrayList<>();
        for (String page : List.of("quickstart", "usage")) {
            var matches = snippet.matcher(Files.readString(Path.of("docs/" + page + ".md")));
            while (matches.find()) {
                String identity = matches.group(1);
                String source;
                if (identity.equals("config.yaml")) {
                    source = Files.readString(Path.of("examples/datastream/config/config.yaml"));
                    source = source.substring(source.indexOf("pipeline.generic-types:"));
                } else {
                    String[] parts = identity.split("#", 2);
                    source =
                            Files.readString(
                                    Path.of(
                                            "examples/datastream/src/main/java/io/github/flink/gcp/protobuf/examples/"
                                                    + parts[0]
                                                    + ".java"));
                    String start = "// docs:start " + parts[1] + "\n";
                    String end = "// docs:end " + parts[1];
                    assertThat(source).containsOnlyOnce(start).containsOnlyOnce(end);
                    source =
                            source.substring(
                                    source.indexOf(start) + start.length(), source.indexOf(end));
                }
                assertThat(matches.group(2))
                        .as("%s in %s", identity, page)
                        .isEqualTo(source.stripIndent().strip());
                checked.add(identity);
            }
        }
        assertThat(checked)
                .containsExactlyInAnyOrder(
                        "ExplicitExample#explicit-configuration",
                        "ExplicitExample#explicit-source",
                        "ExplicitExample#explicit-returns",
                        "config.yaml",
                        "RegisteredExample#registration",
                        "StatefulExample#scalar-state",
                        "StatefulExample#state-descriptors");
    }
}
