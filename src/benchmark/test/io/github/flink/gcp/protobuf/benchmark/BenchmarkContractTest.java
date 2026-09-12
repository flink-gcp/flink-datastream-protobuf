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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkContractTest {
    @Test
    void corpusPreservesAbsentAndPresentDefaultValues() {
        assertThat(Corpus.value("scalar", 0).note()).isNull();
        assertThat(Corpus.value("scalar", 1).note()).isEmpty();
        assertThat(Corpus.value("scalar", 2).note()).isEqualTo("note-2");
        for (String workload : Corpus.WORKLOADS) {
            for (int i = 0; i < Corpus.RECORDS; i++) {
                assertThat(Corpus.value(workload, i)).isEqualTo(Corpus.value(workload, i));
            }
        }
    }

    @Test
    void canonicalPayloadHasContentEqualityAndIndependentViews() {
        Corpus.Value value = Corpus.value("large", 7);
        Corpus.Value same = Corpus.value("large", 7);
        assertThat(value).isEqualTo(same).hasSameHashCodeAs(same);
        value.payload().get();
        value.bytes()[0] ^= 1;
        assertThat(value).isEqualTo(same);
        assertThatThrownBy(() -> value.payload().put(0, (byte) 0))
                .isInstanceOf(java.nio.ReadOnlyBufferException.class);
    }

    @Test
    void copyIndependenceGateRejectsPayloadOnlyAliasing() throws Exception {
        var serializer = Lane.POJO.serializer(false);
        for (boolean reuse : new boolean[] {false, true}) {
            Corpus.Value expected = Corpus.value("text", 7);
            PojoValue source = (PojoValue) Lane.POJO.encode(expected);
            PojoValue copy =
                    (PojoValue)
                            (reuse
                                    ? serializer.copy(source, Lane.POJO.encode(expected))
                                    : serializer.copy(source));
            // Model a copy that owns every mutable field except its payload.
            copy.payload = source.payload;
            assertThatThrownBy(
                            () ->
                                    BenchmarkCheck.checkIndependentCopy(
                                            Lane.POJO, source, copy, expected))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Decoded values differ for POJO");
        }
    }

    @Test
    void nestedKryoCannotPassTheNativeSelectionGate() throws Exception {
        var config = new org.apache.flink.api.common.ExecutionConfig().getSerializerConfig();
        var kryo =
                new org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer<>(
                        GenericValue.class, config);
        var nested = new org.apache.flink.api.common.typeutils.base.ListSerializer<>(kryo);
        assertThatThrownBy(() -> Lane.inspect(nested, new java.util.IdentityHashMap<>(), true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Nested Kryo");
    }

    @Test
    void forkIntervalsRetainUncertainty() {
        assertThat(BenchmarkReport.interval(new double[] {0, 0, 0, 0, 0})).containsExactly(1, 1, 1);
        double[] r = BenchmarkReport.interval(new double[] {-.2, .2, -.1, .1, 0});
        assertThat(r[1]).isLessThan(1.1);
        assertThat(r[2]).isGreaterThan(1.1);
        assertThatThrownBy(() -> BenchmarkReport.interval(new double[] {0}))
                .isInstanceOf(IllegalStateException.class);
    }
}
