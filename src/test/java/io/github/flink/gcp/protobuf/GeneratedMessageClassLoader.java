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

import io.github.flink.gcp.protobuf.generated.ScalarMessage;

import java.net.URL;
import java.net.URLClassLoader;

/** Isolates application gencode while sharing Flink, Protobuf runtime, and library classes. */
final class GeneratedMessageClassLoader extends URLClassLoader {
    GeneratedMessageClassLoader() {
        super(
                new URL[] {ScalarMessage.class.getProtectionDomain().getCodeSource().getLocation()},
                GeneratedMessageClassLoader.class.getClassLoader());
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException {
        if (!name.startsWith("io.github.flink.gcp.protobuf.generated.")) {
            return super.loadClass(name, resolve);
        }
        Class<?> result = findLoadedClass(name);
        if (result == null) {
            result = findClass(name);
        }
        if (resolve) {
            resolveClass(result);
        }
        return result;
    }
}
