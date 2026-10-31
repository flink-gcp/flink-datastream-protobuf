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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Summarizes raw JMH results without treating within-fork iterations as independent samples. */
public final class BenchmarkReport {
    private BenchmarkReport() {}

    public static void main(String[] args) throws Exception {
        Path directory = Path.of(args[0]);
        ObjectMapper mapper = new ObjectMapper();
        List<String> rows =
                new ArrayList<>(
                        List.of(
                                "lane,workload,operation,deterministic,block,ns_per_op,derived_ops_per_second,allocated_bytes_per_op"));
        List<String> files = Files.readAllLines(directory.resolve("completed-results.txt"));
        for (String name : files) {
            JsonNode r = mapper.readTree(directory.resolve(name).toFile()).get(0);
            var p = r.get("params");
            double ns = r.get("primaryMetric").get("score").asDouble();
            rows.add(
                    String.join(
                            ",",
                            p.get("lane").asText(),
                            p.get("workload").asText(),
                            r.get("benchmark")
                                    .asText()
                                    .replace(SerializerBenchmark.class.getName() + ".", ""),
                            p.get("deterministic").asText(),
                            name.substring(name.lastIndexOf('-') + 1, name.length() - 5),
                            Double.toString(ns),
                            Double.toString(1e9 / ns),
                            r.get("secondaryMetrics")
                                    .get("gc.alloc.rate.norm")
                                    .get("score")
                                    .asText()));
        }
        Files.write(directory.resolve("measurements.csv"), rows);
        List<String> parity =
                new ArrayList<>(
                        List.of("workload,operation,native_over_chill,lower95,upper95,verdict"));
        // Descriptive DynamicMessage ratios; ADR-0004 declares no tolerance for them.
        List<String> dynamic =
                new ArrayList<>(
                        List.of(
                                "workload,operation,dynamic_over_generated,dynamic_lower95,"
                                        + "dynamic_upper95,foreign_over_own,foreign_lower95,"
                                        + "foreign_upper95"));
        for (String workload : Corpus.WORKLOADS) {
            for (String operation : BenchmarkSuite.OPERATIONS) {
                double[] interval =
                        ratio(
                                directory,
                                mapper,
                                workload,
                                operation,
                                Lane.PROTOBUF_NATIVE,
                                Lane.PROTOBUF_CHILL);
                if (interval == null) {
                    parity.add(workload + "," + operation + ",,,,unsupported");
                } else {
                    String verdict =
                            interval[2] <= 1.10
                                    ? "established"
                                    : interval[1] > 1.10 ? "missed" : "inconclusive";
                    parity.add(workload + "," + operation + "," + join(interval) + "," + verdict);
                }
                double[] dynamicOverGenerated =
                        ratio(
                                directory,
                                mapper,
                                workload,
                                operation,
                                Lane.PROTOBUF_DYNAMIC,
                                Lane.PROTOBUF_NATIVE);
                double[] foreignOverOwn =
                        ratio(
                                directory,
                                mapper,
                                workload,
                                operation,
                                Lane.PROTOBUF_DYNAMIC_FOREIGN,
                                Lane.PROTOBUF_DYNAMIC);
                if (dynamicOverGenerated != null && foreignOverOwn != null) {
                    dynamic.add(
                            workload
                                    + ","
                                    + operation
                                    + ","
                                    + join(dynamicOverGenerated)
                                    + ","
                                    + join(foreignOverOwn));
                }
            }
        }
        // Pair mode measures no DynamicMessage lane and writes no DynamicMessage report.
        if (dynamic.size() > 1) {
            Files.write(directory.resolve("dynamic.csv"), dynamic);
        }
        Files.write(directory.resolve("parity.csv"), parity);
    }

    /** Returns the paired-block interval of numerator/denominator time, or null if absent. */
    static double[] ratio(
            Path directory,
            ObjectMapper mapper,
            String workload,
            String operation,
            Lane numerator,
            Lane denominator)
            throws IOException {
        double[] logRatios = new double[5];
        for (int block = 0; block < 5; block++) {
            String suffix = "-" + workload + "-" + operation + "-" + block + ".json";
            Path n = directory.resolve(numerator + suffix);
            Path d = directory.resolve(denominator + suffix);
            if (!Files.exists(n) || !Files.exists(d)) {
                return null;
            }
            logRatios[block] = Math.log(score(mapper, n) / score(mapper, d));
        }
        return interval(logRatios);
    }

    private static double score(ObjectMapper mapper, Path file) throws IOException {
        return mapper.readTree(file.toFile()).get(0).get("primaryMetric").get("score").asDouble();
    }

    private static String join(double[] interval) {
        return interval[0] + "," + interval[1] + "," + interval[2];
    }

    static double[] interval(double[] logRatios) {
        Lane.require(logRatios.length == 5, "Five independent paired blocks are required");
        double mean = 0;
        for (double v : logRatios) {
            Lane.require(Double.isFinite(v), "Non-finite ratio");
            mean += v / 5;
        }
        double sum = 0;
        for (double v : logRatios) {
            sum += (v - mean) * (v - mean);
        }
        double margin = 2.7764451051977987 * Math.sqrt(sum / 4 / 5);
        return new double[] {Math.exp(mean), Math.exp(mean - margin), Math.exp(mean + margin)};
    }
}
