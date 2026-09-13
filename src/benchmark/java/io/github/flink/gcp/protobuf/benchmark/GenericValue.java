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

/** Deliberately not a Flink POJO: final fields and no no-argument constructor. */
public final class GenericValue {
    public final int id;
    public final long timestamp;
    public final boolean active;
    public final String name;
    public final String note;
    public final byte[] payload;
    public final PojoValue.Child child;
    public final List<Long> values;
    public final Map<String, Long> attributes;

    public GenericValue(PojoValue p) {
        id = p.id;
        timestamp = p.timestamp;
        active = p.active;
        name = p.name;
        note = p.note;
        payload = p.payload;
        child = p.child;
        values = p.values;
        attributes = p.attributes;
    }
}
