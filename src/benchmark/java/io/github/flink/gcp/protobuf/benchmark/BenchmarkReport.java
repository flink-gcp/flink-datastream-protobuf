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
        for (String workload : Corpus.WORKLOADS) {
            for (String operation : BenchmarkSuite.OPERATIONS) {
                double[] logRatios = new double[5];
                boolean supported = true;
                for (int block = 0; block < 5; block++) {
                    Path n =
                            directory.resolve(
                                    "PROTOBUF_NATIVE-"
                                            + workload
                                            + "-"
                                            + operation
                                            + "-"
                                            + block
                                            + ".json");
                    Path c =
                            directory.resolve(
                                    "PROTOBUF_CHILL-"
                                            + workload
                                            + "-"
                                            + operation
                                            + "-"
                                            + block
                                            + ".json");
                    if (!Files.exists(n) || !Files.exists(c)) {
                        supported = false;
                        break;
                    }
                    logRatios[block] =
                            Math.log(
                                    mapper.readTree(n.toFile())
                                                    .get(0)
                                                    .get("primaryMetric")
                                                    .get("score")
                                                    .asDouble()
                                            / mapper.readTree(c.toFile())
                                                    .get(0)
                                                    .get("primaryMetric")
                                                    .get("score")
                                                    .asDouble());
                }
                if (!supported) {
                    parity.add(workload + "," + operation + ",,,,unsupported");
                    continue;
                }
                double[] interval = interval(logRatios);
                String verdict =
                        interval[2] <= 1.10
                                ? "established"
                                : interval[1] > 1.10 ? "missed" : "inconclusive";
                parity.add(
                        workload
                                + ","
                                + operation
                                + ","
                                + interval[0]
                                + ","
                                + interval[1]
                                + ","
                                + interval[2]
                                + ","
                                + verdict);
            }
        }
        Files.write(directory.resolve("parity.csv"), parity);
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
