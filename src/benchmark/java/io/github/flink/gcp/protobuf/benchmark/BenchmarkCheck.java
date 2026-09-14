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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.OutputStream;
import java.lang.reflect.InaccessibleObjectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.IdentityHashMap;
import java.util.Properties;

/** Separate-JVM correctness gate and observable support inventory for one lane. */
public final class BenchmarkCheck {
    private BenchmarkCheck() {}

    public static void main(String[] args) throws Exception {
        Lane lane = Lane.valueOf(args[0]);
        Properties report = new Properties();
        report.setProperty("lane", lane.name());
        report.setProperty(
                "jvm.arguments",
                java.lang.management.ManagementFactory.getRuntimeMXBean()
                        .getInputArguments()
                        .toString());
        try {
            check(lane, report);
            report.setProperty("status", "supported");
        } catch (Throwable failure) {
            if (!knownAdapterFailure(lane, failure)) {
                throw failure;
            }
            report.setProperty("status", "unsupported");
            report.setProperty("reason", failure.toString());
            failure.printStackTrace();
        }
        try (OutputStream out = Files.newOutputStream(Path.of(args[1]))) {
            report.store(out, "Benchmark support and correctness evidence");
        }
    }

    private static boolean knownAdapterFailure(Lane lane, Throwable failure) {
        if (!lane.generic()) {
            return false;
        }
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof InaccessibleObjectException && t.getMessage().contains("java.util")) {
                return true;
            }
            if ((lane == Lane.THRIFT_CHILL || lane == Lane.PROTOBUF_CHILL)
                    && (t instanceof NoSuchMethodError || t instanceof AbstractMethodError)) {
                return true;
            }
        }
        return false;
    }

    static void check(Lane lane, Properties report) throws Exception {
        TypeSerializer<Object> serializer = lane.serializer(false);
        report.setProperty(
                "serializer.chain",
                Lane.inspect(serializer, new IdentityHashMap<>(), !lane.generic()));
        report.setProperty("immutable", Boolean.toString(serializer.isImmutableType()));
        report.setProperty("serializer.source", source(serializer.getClass()));
        if (serializer instanceof KryoSerializer<?> k) {
            report.setProperty("kryo.source", source(k.getKryo().getClass()));
            Class<?> record = lane.encode(Corpus.value("scalar", 0)).getClass();
            report.setProperty(
                    "adapter.class", k.getKryo().getSerializer(record).getClass().getName());
            report.setProperty(
                    "adapter.source", source(k.getKryo().getSerializer(record).getClass()));
            report.setProperty(
                    "kryo.registrationRequired",
                    Boolean.toString(k.getKryo().isRegistrationRequired()));
            report.setProperty("kryo.references", Boolean.toString(k.getKryo().getReferences()));
            report.setProperty(
                    "kryo.record.id",
                    Integer.toString(k.getKryo().getRegistration(record).getId()));
        }
        report.setProperty(
                "record.source", source(lane.encode(Corpus.value("scalar", 0)).getClass()));
        for (String workload : Corpus.WORKLOADS) {
            long bytes = 0;
            int min = Integer.MAX_VALUE;
            int max = 0;
            int identityCopies = 0;
            int deserializeReuses = 0;
            int copyReuses = 0;
            for (int i = 0; i < Corpus.RECORDS; i++) {
                Corpus.Value expected = Corpus.value(workload, i);
                Object value = lane.encode(expected);
                DataOutputSerializer output = new DataOutputSerializer(131072);
                serializer.serialize(value, output);
                int size = output.length();
                bytes += size;
                min = Math.min(min, size);
                max = Math.max(max, size);
                Object other = lane.encode(Corpus.value(workload, (i + 1) % Corpus.RECORDS));
                serializer.serialize(other, output);
                DataInputDeserializer input = new DataInputDeserializer(output.getCopyOfBuffer());
                Object restored = serializer.deserialize(input);
                equal(lane, restored, expected);
                Lane.require(
                        input.available() == output.length() - size,
                        "First deserialize consumed an adjacent frame");
                equal(lane, serializer.deserialize(input), lane.decode(other));
                Lane.require(input.available() == 0, "Trailing input");
                input.setBuffer(output.getCopyOfBuffer());
                Object candidate = lane.encode(Corpus.value(workload, (i + 2) % Corpus.RECORDS));
                Object reused = serializer.deserialize(candidate, input);
                equal(lane, reused, expected);
                if (reused == candidate) {
                    deserializeReuses++;
                }
                Lane.require(
                        input.available() == output.length() - size,
                        "Reuse deserialize consumed an adjacent frame");
                Object copied = serializer.copy(value);
                equal(lane, copied, expected);
                if (copied == value) {
                    identityCopies++;
                }
                Object copyCandidate =
                        lane.encode(Corpus.value(workload, (i + 3) % Corpus.RECORDS));
                Object copiedReuse = serializer.copy(value, copyCandidate);
                equal(lane, copiedReuse, expected);
                if (copiedReuse == copyCandidate) {
                    copyReuses++;
                }
                if (serializer.isImmutableType()) {
                    Lane.require(
                            copied == value && copiedReuse == value,
                            "Immutable copy must share identity");
                } else {
                    Lane.require(
                            copied != value && copiedReuse != value,
                            "Mutable copy aliases the source");
                    checkIndependentCopy(lane, value, copied, expected);
                    checkIndependentCopy(lane, value, copiedReuse, expected);
                }
                input.setBuffer(output.getCopyOfBuffer());
                DataOutputSerializer streamCopy = new DataOutputSerializer(131072);
                serializer.copy(input, streamCopy);
                Lane.require(
                        input.available() == output.length() - size,
                        "Stream copy consumed an adjacent frame");
                DataInputDeserializer copiedInput =
                        new DataInputDeserializer(streamCopy.getCopyOfBuffer());
                equal(lane, serializer.deserialize(copiedInput), expected);
                Lane.require(copiedInput.available() == 0, "Copy left trailing bytes");
                equal(lane, serializer.deserialize(input), lane.decode(other));
                equal(lane, serializer.duplicate().copy(value), expected);
            }
            String prefix = workload + ".";
            report.setProperty(prefix + "records", Integer.toString(Corpus.RECORDS));
            report.setProperty(
                    prefix + "encoded.mean", Double.toString((double) bytes / Corpus.RECORDS));
            report.setProperty(prefix + "encoded.min", Integer.toString(min));
            report.setProperty(prefix + "encoded.max", Integer.toString(max));
            report.setProperty(prefix + "identityCopies", Integer.toString(identityCopies));
            report.setProperty(prefix + "deserializeReuses", Integer.toString(deserializeReuses));
            report.setProperty(prefix + "copyReuses", Integer.toString(copyReuses));
        }
        if (lane == Lane.PROTOBUF_NATIVE) {
            var deterministic = lane.serializer(true);
            var v = Corpus.value("collections", 7);
            var out = new DataOutputSerializer(4096);
            deterministic.serialize(lane.encode(v), out);
            equal(
                    lane,
                    deterministic.deserialize(new DataInputDeserializer(out.getCopyOfBuffer())),
                    v);
        }
    }

    static void checkIndependentCopy(Lane lane, Object source, Object copy, Corpus.Value expected) {
        mutate(copy);
        equal(lane, source, expected);
    }

    @SuppressWarnings("unchecked")
    private static void mutate(Object object) {
        if (object instanceof PojoValue p) {
            p.child.name = "changed";
            p.values.add(99L);
            p.attributes.put("mutation", 99L);
            if (p.payload.length > 0) {
                p.payload[0] ^= 1;
            }
        } else if (object instanceof GenericValue p) {
            p.child.name = "changed";
            p.values.add(99L);
            p.attributes.put("mutation", 99L);
            if (p.payload.length > 0) {
                p.payload[0] ^= 1;
            }
        } else if (object
                instanceof org.apache.flink.api.java.tuple.Tuple9<?, ?, ?, ?, ?, ?, ?, ?, ?> p) {
            ((org.apache.flink.api.java.tuple.Tuple2<Integer, String>) p.f6).f1 = "changed";
            ((java.util.List<Long>) p.f7).add(99L);
            ((java.util.Map<String, Long>) p.f8).put("mutation", 99L);
            if (((byte[]) p.f5).length > 0) {
                ((byte[]) p.f5)[0] ^= 1;
            }
        } else if (object instanceof org.apache.avro.generic.IndexedRecord p) {
            ((org.apache.avro.generic.IndexedRecord) p.get(6)).put(1, "changed");
            ((java.util.List<Long>) p.get(7)).add(99L);
            ((java.util.Map<String, Long>) p.get(8)).put("mutation", 99L);
            var buffer = (java.nio.ByteBuffer) p.get(5);
            if (buffer.remaining() > 0) {
                buffer.put(buffer.position(), (byte) (buffer.get(buffer.position()) ^ 1));
            }
        } else if (object
                instanceof io.github.flink.gcp.protobuf.benchmark.generated.thrift.Record p) {
            p.child.name = "changed";
            p.values.add(99L);
            p.attributes.put("mutation", 99L);
            if (p.getPayload().length > 0) {
                p.getPayload()[0] ^= 1;
            }
        }
        // Protobuf is immutable even though the enclosing KryoSerializer reports mutable.
    }

    private static void equal(Lane lane, Object actual, Corpus.Value expected) {
        Lane.require(
                expected.equals(lane.decode(actual)),
                "Decoded values differ for " + lane + ": " + expected.id());
    }

    private static String source(Class<?> type) {
        return type.getProtectionDomain().getCodeSource().getLocation().toString();
    }
}
