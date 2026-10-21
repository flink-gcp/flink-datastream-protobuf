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

import org.w3c.dom.Element;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;

/**
 * JDK-only check of the API negative-control report, also executable in Java source-file mode.
 *
 * <p>japicmp itself fails the build on incompatible changes. This check covers what its
 * configuration cannot express: that the deliberately broken fixture reports exactly its break.
 */
public final class ApiNegativeControl {
    private ApiNegativeControl() {}

    public static void main(String[] args) throws Exception {
        require(args.length == 2, "Expected <negative-control-report> <removed-class>");
        System.out.println(check(Path.of(args[0]), args[1]));
    }

    /**
     * Requires the removed class to be the only incompatible type in a negative-control report.
     *
     * @return a summary of the reported break
     */
    static String check(Path report, String removed) throws Exception {
        TreeSet<String> incompatible = new TreeSet<>();
        String status = "";
        var types = read(report).getElementsByTagName("class");
        for (int i = 0; i < types.getLength(); i++) {
            Element type = (Element) types.item(i);
            String name = type.getAttribute("fullyQualifiedName");
            if (!type.getAttribute("binaryCompatible").equals("true")
                    || !type.getAttribute("sourceCompatible").equals("true")) {
                incompatible.add(name);
            }
            if (name.equals(removed)) {
                status = type.getAttribute("changeStatus");
            }
        }
        require(
                incompatible.equals(Set.of(removed)) && status.equals("REMOVED"),
                "Negative control expected only the removal of "
                        + removed
                        + " but japicmp reported incompatible types "
                        + incompatible);
        return "Negative control reported only the removal of " + removed;
    }

    /** Parses a japicmp XML report without external entities. */
    static Element read(Path report) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        Element root = factory.newDocumentBuilder().parse(report.toFile()).getDocumentElement();
        require(root.getTagName().equals("japicmp"), "Not a japicmp XML report: " + report);
        return root;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
