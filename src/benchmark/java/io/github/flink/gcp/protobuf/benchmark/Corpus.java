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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Deterministic logical values, independent of the representation under measurement. */
public final class Corpus {
    public static final long SEED = 30012026L;
    public static final int RECORDS = 64;
    public static final List<String> WORKLOADS =
            List.of("scalar", "text", "nested", "collections", "large");

    private Corpus() {}

    public record Child(int code, String name) {}

    public record Value(
            int id,
            long timestamp,
            boolean active,
            String name,
            String note,
            ByteBuffer payload,
            Child child,
            List<Long> values,
            Map<String, Long> attributes) {
        public Value {
            ByteBuffer owned = ByteBuffer.allocate(payload.remaining());
            owned.put(payload.duplicate()).flip();
            payload = owned.asReadOnlyBuffer();
        }

        @Override
        public ByteBuffer payload() {
            return payload.asReadOnlyBuffer();
        }

        public byte[] bytes() {
            byte[] result = new byte[payload.remaining()];
            payload().get(result);
            return result;
        }
    }

    public static Value value(String workload, int index) {
        if (!WORKLOADS.contains(workload)) {
            throw new IllegalArgumentException(workload);
        }
        Random random = new Random(SEED + index);
        int size =
                switch (workload) {
                    case "text" -> 1024;
                    case "large" -> 65536;
                    default -> 0;
                };
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) random.nextInt(256);
        }
        List<Long> values = new ArrayList<>();
        Map<String, Long> attributes = new LinkedHashMap<>();
        if (workload.equals("collections")) {
            for (int i = 0; i < 16; i++) {
                values.add(random.nextLong());
                attributes.put("key-" + (15 - i), random.nextLong());
            }
        }
        String name = workload.equals("text") ? "ASCII-日本語-é-".repeat(32) + index : "";
        Child child =
                workload.equals("nested")
                        ? new Child(index * 7, "child-日本語-" + index)
                        : new Child(0, "");
        String note =
                switch (index % 3) {
                    case 0 -> null;
                    case 1 -> "";
                    default -> "note-" + index;
                };
        return new Value(
                index - 32,
                index == 0 ? 0 : 1700000000000L + index,
                index % 2 == 0,
                name,
                note,
                ByteBuffer.wrap(bytes),
                child,
                List.copyOf(values),
                java.util.Collections.unmodifiableMap(attributes));
    }

    public static Value decoded(
            int id,
            long timestamp,
            boolean active,
            String name,
            String note,
            byte[] payload,
            Child child,
            List<Long> values,
            Map<String, Long> attributes) {
        return new Value(
                id,
                timestamp,
                active,
                name,
                note,
                ByteBuffer.wrap(payload),
                child,
                values,
                attributes);
    }
}
