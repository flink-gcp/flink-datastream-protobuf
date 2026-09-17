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
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.protobuf.examples.generated.Event;

/** Factory selection from config.yaml before any message type extraction. */
public final class RegisteredExample {
    private RegisteredExample() {}

    public static DataStream<Event> messages(StreamExecutionEnvironment env) {
        return env.fromData(
                Event.newBuilder().setAccount("alice").setAmount(2).build(),
                Event.newBuilder().setAccount("bob").setAmount(7).build());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Pass the directory containing config.yaml");
        }
        // docs:start registration
        var config = GlobalConfiguration.loadConfiguration(args[0]);
        try (var env = StreamExecutionEnvironment.getExecutionEnvironment(config)) {
            env.setParallelism(2);
            messages(env)
                    .rebalance()
                    .map(event -> event.getAccount() + ":" + event.getAmount())
                    .returns(Types.STRING)
                    .print();
            env.execute("Registered Protobuf values");
        }
        // docs:end registration
    }
}
