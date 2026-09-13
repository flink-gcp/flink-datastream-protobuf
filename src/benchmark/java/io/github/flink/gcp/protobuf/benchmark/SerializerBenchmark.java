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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/** One logical record per invocation; initialization and validation are outside timed regions. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SerializerBenchmark {
    @State(Scope.Thread)
    public static class Records {
        @Param public Lane lane;

        @Param({"scalar", "text", "nested", "collections", "large"})
        public String workload;

        @Param({"false"})
        public boolean deterministic;

        public TypeSerializer<Object> serializer;
        public Corpus.Value[] logical;
        public Object[] records;
        public byte[][] encoded;
        public DataOutputSerializer output;
        public DataInputDeserializer input;
        public Object reuse;
        private int cursor;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            BenchmarkSuite.requirePackagedLibrary();
            serializer = lane.serializer(deterministic);
            logical = new Corpus.Value[Corpus.RECORDS];
            records = new Object[Corpus.RECORDS];
            encoded = new byte[Corpus.RECORDS][];
            output = new DataOutputSerializer(131072);
            input = new DataInputDeserializer();
            for (int i = 0; i < records.length; i++) {
                logical[i] = Corpus.value(workload, i);
                records[i] = lane.encode(logical[i]);
                output.clear();
                serializer.serialize(records[i], output);
                encoded[i] = output.getCopyOfBuffer();
                input.setBuffer(encoded[i]);
                Lane.require(
                        logical[i].equals(lane.decode(serializer.deserialize(input))),
                        "JMH setup round-trip mismatch");
            }
            // Warm lazy serializer state and all immutable message caches before measurement.
            reuse = serializer.copy(records[0]);
            output.clear();
            input.setBuffer(encoded[0]);
            serializer.copy(input, output);
        }

        public int next() {
            int current = cursor;
            cursor = (cursor + 1) & (Corpus.RECORDS - 1);
            return current;
        }
    }

    @Benchmark
    public void serialize(Records s, Blackhole bh) throws Exception {
        s.output.clear();
        s.serializer.serialize(s.records[s.next()], s.output);
        bh.consume(s.output.getSharedBuffer());
        bh.consume(s.output.length());
    }

    @Benchmark
    public void serializeSameInstance(Records s, Blackhole bh) throws Exception {
        s.output.clear();
        s.serializer.serialize(s.records[0], s.output);
        bh.consume(s.output.getSharedBuffer());
        bh.consume(s.output.length());
    }

    @Benchmark
    public void buildAndSerialize(Records s, Blackhole bh) throws Exception {
        Object fresh = s.lane.encode(s.logical[s.next()]);
        s.output.clear();
        s.serializer.serialize(fresh, s.output);
        bh.consume(s.output.getSharedBuffer());
        bh.consume(s.output.length());
    }

    @Benchmark
    public Object deserialize(Records s) throws Exception {
        s.input.setBuffer(s.encoded[s.next()]);
        return s.serializer.deserialize(s.input);
    }

    @Benchmark
    public Object deserializeReuse(Records s) throws Exception {
        s.input.setBuffer(s.encoded[s.next()]);
        s.reuse = s.serializer.deserialize(s.reuse, s.input);
        return s.reuse;
    }

    @Benchmark
    public Object objectCopy(Records s) {
        return s.serializer.copy(s.records[s.next()]);
    }

    @Benchmark
    public Object objectCopyReuse(Records s) {
        s.reuse = s.serializer.copy(s.records[s.next()], s.reuse);
        return s.reuse;
    }

    @Benchmark
    public void streamCopy(Records s, Blackhole bh) throws Exception {
        s.input.setBuffer(s.encoded[s.next()]);
        s.output.clear();
        s.serializer.copy(s.input, s.output);
        bh.consume(s.output.getSharedBuffer());
        bh.consume(s.output.length());
    }
}
