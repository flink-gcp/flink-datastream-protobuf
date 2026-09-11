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

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;

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
            return FileDescriptorProto.parseFrom(
                    file.toByteString(), ExtensionRegistryLite.getEmptyRegistry());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Invalid descriptor content: " + file.getName(), e);
        }
    }
}
