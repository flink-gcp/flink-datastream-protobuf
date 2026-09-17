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

package io.github.flink.gcp.protobuf.examples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.protobuf.ProtobufTypeInformation;
import io.github.flink.gcp.protobuf.examples.generated.Event;

/** Explicit source and transformation types without process-global factory registration. */
public final class ExplicitExample {
    private ExplicitExample() {}

    public static Configuration configuration() {
        // docs:start explicit-configuration
        Configuration config = new Configuration();
        config.set(PipelineOptions.GENERIC_TYPES, false);
        // docs:end explicit-configuration
        return config;
    }

    public static DataStream<Event> messages(StreamExecutionEnvironment env) {
        // docs:start explicit-source
        var eventType = ProtobufTypeInformation.of(Event.class);
        var events =
                env.fromData(
                        eventType,
                        Event.newBuilder().setAccount("alice").setAmount(2).build(),
                        Event.newBuilder().setAccount("bob").setAmount(7).build());
        // docs:end explicit-source
        // docs:start explicit-returns
        return events.map(event -> event.toBuilder().setAmount(event.getAmount() + 1).build())
                .returns(eventType);
        // docs:end explicit-returns
    }

    public static void main(String[] args) throws Exception {
        try (var env = StreamExecutionEnvironment.getExecutionEnvironment(configuration())) {
            env.setParallelism(2);
            messages(env)
                    .rebalance()
                    .map(event -> event.getAccount() + ":" + event.getAmount())
                    .returns(Types.STRING)
                    .print();
            env.execute("Explicit Protobuf values");
        }
    }
}
