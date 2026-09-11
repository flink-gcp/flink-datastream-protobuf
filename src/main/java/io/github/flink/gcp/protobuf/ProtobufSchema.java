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

import org.apache.flink.annotation.Internal;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistryLite;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/** Complete descriptor content identity; persisted snapshot encoding is implemented separately. */
@Internal
final class ProtobufSchema {
    static final int NORMALIZATION_VERSION = 1;
    // Match the supported runtimes' per-file descriptor parser budget explicitly.
    static final int DESCRIPTOR_RECURSION_LIMIT = 100;

    private ProtobufSchema() {}

    /**
     * Normalizes the complete transitive file dependency closure for content-based identity.
     *
     * <p>Only SourceCodeInfo is removed. Options are reparsed with an empty registry so custom
     * options have a consistent unknown-field representation. Declaration order and other content
     * are preserved; files are emitted in dependency order with lexical tie breaking.
     *
     * @param root file declaring the message
     * @return normalized closure including the root
     * @throws IllegalArgumentException if imports conflict, cannot resolve, or form a cycle
     */
    static FileDescriptorSet normalize(FileDescriptor root) {
        Map<String, FileDescriptorProto> files = new HashMap<>();
        Set<FileDescriptor> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        ArrayDeque<FileDescriptor> remaining = new ArrayDeque<>();
        remaining.add(root);
        while (!remaining.isEmpty()) {
            FileDescriptor file = remaining.removeFirst();
            if (!visited.add(file)) {
                continue;
            }
            FileDescriptorProto content =
                    normalizeOptions(file.toProto().toBuilder().clearSourceCodeInfo().build());
            FileDescriptorProto previous = files.putIfAbsent(file.getName(), content);
            if (previous != null && !previous.equals(content)) {
                throw new IllegalArgumentException(
                        "Conflicting descriptor file: " + file.getName());
            }
            remaining.addAll(file.getDependencies());
        }
        return order(files);
    }

    /**
     * Normalizes and validates saved descriptors without loading any generated message class.
     *
     * @param input decoded descriptor content, with options read using an empty registry
     * @param fullName fully qualified root message name, including enclosing message names
     * @return complete normalized content with validated imports and root
     * @throws IllegalArgumentException if descriptors are invalid or do not form the root's closure
     */
    static FileDescriptorSet normalize(FileDescriptorSet input, String fullName) {
        Map<String, FileDescriptorProto> files = collectNormalizedFiles(input);
        FileDescriptorSet ordered = order(files);
        Descriptor root = resolveRootMessage(ordered, fullName);
        validateImportClosure(root.getFile(), files.keySet());
        // Keep unknown content in the set itself as well as in every file and declaration.
        return input.toBuilder().clearFile().addAllFile(ordered.getFileList()).build();
    }

    private static Map<String, FileDescriptorProto> collectNormalizedFiles(
            FileDescriptorSet input) {
        Map<String, FileDescriptorProto> files = new HashMap<>();
        for (FileDescriptorProto file : input.getFileList()) {
            if (file.getName().isEmpty()) {
                throw new IllegalArgumentException("Empty descriptor file name");
            }
            FileDescriptorProto normalized =
                    normalizeOptions(file.toBuilder().clearSourceCodeInfo().build());
            FileDescriptorProto previous = files.putIfAbsent(file.getName(), normalized);
            if (previous != null && !previous.equals(normalized)) {
                throw new IllegalArgumentException(
                        "Conflicting descriptor file: " + file.getName());
            }
            for (int index : file.getWeakDependencyList()) {
                if (index < 0 || index >= file.getDependencyCount()) {
                    throw new IllegalArgumentException(
                            "Invalid weak dependency index: " + file.getName());
                }
            }
        }
        return files;
    }

    private static Descriptor resolveRootMessage(FileDescriptorSet ordered, String fullName) {
        Map<String, FileDescriptor> resolved = new HashMap<>();
        Descriptor root = null;
        try {
            for (FileDescriptorProto file : ordered.getFileList()) {
                FileDescriptor[] dependencies =
                        file.getDependencyList().stream()
                                .map(resolved::get)
                                .toArray(FileDescriptor[]::new);
                FileDescriptor descriptor = FileDescriptor.buildFrom(file, dependencies);
                resolved.put(file.getName(), descriptor);
                ArrayDeque<Descriptor> messages = new ArrayDeque<>(descriptor.getMessageTypes());
                while (!messages.isEmpty()) {
                    Descriptor message = messages.removeFirst();
                    if (message.getFullName().equals(fullName)) {
                        if (root != null) {
                            throw new IllegalArgumentException(
                                    "Ambiguous root message: " + fullName);
                        }
                        root = message;
                    }
                    messages.addAll(message.getNestedTypes());
                }
            }
        } catch (DescriptorValidationException e) {
            throw new IllegalArgumentException("Invalid descriptor graph: " + e.getMessage(), e);
        }
        if (root == null) {
            throw new IllegalArgumentException("Missing root message: " + fullName);
        }
        return root;
    }

    private static void validateImportClosure(FileDescriptor root, Set<String> expectedFiles) {
        Set<String> closure = new HashSet<>();
        ArrayDeque<FileDescriptor> remaining = new ArrayDeque<>();
        remaining.add(root);
        while (!remaining.isEmpty()) {
            FileDescriptor file = remaining.removeFirst();
            if (closure.add(file.getName())) {
                remaining.addAll(file.getDependencies());
            }
        }
        if (!closure.equals(expectedFiles)) {
            throw new IllegalArgumentException(
                    "Descriptor set contains files outside the root import closure");
        }
    }

    /**
     * Orders files with Kahn's algorithm, selecting lexically among currently ready files.
     *
     * @param files complete normalized closure keyed by file name
     * @return dependency-first file sequence
     * @throws IllegalArgumentException if names disagree or imports are missing or cyclic
     */
    static FileDescriptorSet order(Map<String, FileDescriptorProto> files) {
        Map<String, Integer> counts = new HashMap<>();
        Map<String, List<String>> dependents = new HashMap<>();
        PriorityQueue<String> ready = new PriorityQueue<>();
        for (Map.Entry<String, FileDescriptorProto> entry : files.entrySet()) {
            String name = entry.getKey();
            if (!name.equals(entry.getValue().getName())) {
                throw new IllegalArgumentException("Descriptor file name mismatch: " + name);
            }
            Set<String> dependencies = new HashSet<>(entry.getValue().getDependencyList());
            counts.put(name, dependencies.size());
            for (String dependency : dependencies) {
                if (!files.containsKey(dependency)) {
                    throw new IllegalArgumentException(
                            "Unresolved descriptor import: " + name + " -> " + dependency);
                }
                dependents.computeIfAbsent(dependency, ignored -> new ArrayList<>()).add(name);
            }
            if (dependencies.isEmpty()) {
                ready.add(name);
            }
        }
        FileDescriptorSet.Builder result = FileDescriptorSet.newBuilder();
        while (!ready.isEmpty()) {
            String name = ready.remove();
            result.addFile(files.get(name));
            for (String dependent : dependents.getOrDefault(name, List.of())) {
                int count = counts.compute(dependent, (ignored, old) -> old - 1);
                if (count == 0) {
                    ready.add(dependent);
                }
            }
        }
        if (result.getFileCount() != files.size()) {
            throw new IllegalArgumentException("Cyclic descriptor file imports");
        }
        return result.build();
    }

    /** Reparses options without registered extensions while retaining their serialized content. */
    private static FileDescriptorProto normalizeOptions(FileDescriptorProto file) {
        try {
            CodedInputStream input = file.toByteString().newCodedInput();
            input.setRecursionLimit(DESCRIPTOR_RECURSION_LIMIT);
            FileDescriptorProto normalized =
                    FileDescriptorProto.parseFrom(input, ExtensionRegistryLite.getEmptyRegistry());
            input.checkLastTagWas(0);
            return normalized;
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid descriptor content: " + file.getName(), e);
        }
    }
}
