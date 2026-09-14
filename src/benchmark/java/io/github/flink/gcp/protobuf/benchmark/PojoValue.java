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

package io.github.flink.gcp.protobuf.benchmark;

import java.util.List;
import java.util.Map;

/** A mutable POJO with explicit field type information in the benchmark. */
public class PojoValue {
    public int id;
    public long timestamp;
    public boolean active;
    public String name;
    public String note;
    public byte[] payload;
    public Child child;
    public List<Long> values;
    public Map<String, Long> attributes;

    public PojoValue() {}

    public static class Child {
        public int code;
        public String name;

        public Child() {}

        public Child(int code, String name) {
            this.code = code;
            this.name = name;
        }
    }
}
