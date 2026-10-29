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

import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Descriptor instances that no generated class backs, for DynamicMessage tests. */
final class DynamicDescriptors {
    static final String PACKAGE = "flink.protobuf.dynamic";

    private DynamicDescriptors() {}

    /**
     * Builds a distinct descriptor instance with the same content as a message's file closure.
     *
     * @param descriptor top-level or nested message to rebuild
     * @return the equally named message in newly built files
     */
    static Descriptor rebuild(Descriptor descriptor) throws DescriptorValidationException {
        FileDescriptor file = build(descriptor.getFile(), new HashMap<>());
        return find(file, descriptor.getFullName());
    }

    /** Builds files in order, each with the previously built files as its dependencies. */
    static Descriptor build(String fullName, FileDescriptorProto... files)
            throws DescriptorValidationException {
        Map<String, FileDescriptor> built = new HashMap<>();
        FileDescriptor last = null;
        for (FileDescriptorProto file : files) {
            FileDescriptor[] dependencies = new FileDescriptor[file.getDependencyCount()];
            for (int i = 0; i < dependencies.length; i++) {
                dependencies[i] = built.get(file.getDependency(i));
            }
            last = FileDescriptor.buildFrom(file, dependencies);
            built.put(file.getName(), last);
        }
        return find(last, fullName);
    }

    /** Starts a proto3 file in the test package. */
    static FileDescriptorProto.Builder file(String name) {
        return FileDescriptorProto.newBuilder()
                .setName(name)
                .setPackage(PACKAGE)
                .setSyntax("proto3");
    }

    static FieldDescriptorProto.Builder field(
            String name, int number, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    static FieldDescriptorProto.Builder messageField(String name, int number, String type) {
        return field(name, number, FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName("." + PACKAGE + "." + type);
    }

    static DescriptorProto.Builder message(String name, FieldDescriptorProto.Builder... fields) {
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName(name);
        for (FieldDescriptorProto.Builder field : fields) {
            message.addField(field);
        }
        return message;
    }

    /** Java-serializes and deserializes an object with a loader that lacks generated classes. */
    @SuppressWarnings("unchecked")
    static <T> T javaRoundTrip(T value) throws IOException, ClassNotFoundException {
        return (T)
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(value), withoutGeneratedClasses());
    }

    /** Fails to load any generated test message class, as an application without them would. */
    static ClassLoader withoutGeneratedClasses() {
        return new ClassLoader(DynamicDescriptors.class.getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                    throws ClassNotFoundException {
                if (name.startsWith("io.github.flink.gcp.protobuf.generated.")
                        || name.startsWith("io.github.flink.gcp.protobuf.snapshotgenerated.")
                        || name.startsWith("io.github.flink.gcp.protobuf.runtimeapp.")) {
                    throw new ClassNotFoundException("Generated classes are absent: " + name);
                }
                return super.loadClass(name, resolve);
            }
        };
    }

    private static FileDescriptor build(FileDescriptor file, Map<String, FileDescriptor> built)
            throws DescriptorValidationException {
        FileDescriptor existing = built.get(file.getName());
        if (existing != null) {
            return existing;
        }
        FileDescriptor[] dependencies = new FileDescriptor[file.getDependencies().size()];
        for (int i = 0; i < dependencies.length; i++) {
            dependencies[i] = build(file.getDependencies().get(i), built);
        }
        FileDescriptor result = FileDescriptor.buildFrom(file.toProto(), dependencies);
        built.put(file.getName(), result);
        return result;
    }

    private static Descriptor find(FileDescriptor file, String fullName) {
        String prefix = file.getPackage().isEmpty() ? "" : file.getPackage() + ".";
        String[] names = fullName.substring(prefix.length()).split("\\.");
        Descriptor result = file.findMessageTypeByName(names[0]);
        for (int i = 1; i < names.length; i++) {
            result = result.findNestedTypeByName(names[i]);
        }
        return result;
    }
}
