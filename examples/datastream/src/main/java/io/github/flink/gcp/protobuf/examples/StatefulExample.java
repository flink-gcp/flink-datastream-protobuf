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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.protobuf.ProtobufTypeInformation;
import io.github.flink.gcp.protobuf.examples.generated.Event;

/** A bounded example of Protobuf state values with scalar partitioning and MapState keys. */
public final class StatefulExample {
    private StatefulExample() {}

    public static DataStream<String> totals(StreamExecutionEnvironment env) {
        var eventType = ProtobufTypeInformation.of(Event.class);
        var events =
                env.fromData(
                                eventType,
                                Event.newBuilder().setAccount("alice").setAmount(2).build(),
                                Event.newBuilder().setAccount("bob").setAmount(7).build(),
                                Event.newBuilder().setAccount("alice").setAmount(3).build(),
                                Event.newBuilder().setAccount("alice").setAmount(4).build())
                        .setParallelism(1)
                        .uid("events");
        // docs:start scalar-state
        return events.keyBy(Event::getAccount, Types.STRING)
                .process(new RunningTotal())
                .returns(Types.STRING)
                .uid("account-totals");
        // docs:end scalar-state
    }

    public static void main(String[] args) throws Exception {
        try (var env =
                StreamExecutionEnvironment.getExecutionEnvironment(
                        ExplicitExample.configuration())) {
            env.setParallelism(2);
            totals(env).print().uid("output");
            env.execute("Protobuf state values");
        }
    }

    public static final class RunningTotal extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Event> total;
        private transient MapState<String, Event> recent;

        @Override
        public void open(OpenContext context) throws Exception {
            // docs:start state-descriptors
            var eventType = ProtobufTypeInformation.of(Event.class);
            total = getRuntimeContext().getState(new ValueStateDescriptor<>("total", eventType));
            recent =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>("recent", Types.STRING, eventType));
            // docs:end state-descriptors
        }

        @Override
        public void processElement(Event event, Context context, Collector<String> out)
                throws Exception {
            Event previousTotal = total.value();
            Event previousEvent = recent.get("latest");
            long amount =
                    (previousTotal == null ? 0 : previousTotal.getAmount()) + event.getAmount();
            total.update(event.toBuilder().setAmount(amount).build());
            recent.put("latest", event);
            out.collect(
                    event.getAccount()
                            + ":"
                            + amount
                            + ":"
                            + (previousEvent == null ? 0 : previousEvent.getAmount()));
        }
    }
}
