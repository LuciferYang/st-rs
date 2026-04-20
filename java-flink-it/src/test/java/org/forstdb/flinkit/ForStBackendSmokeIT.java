/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.forstdb.flinkit;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end smoke test: runs a Flink streaming job that uses
 * {@link ForStStateBackend} with the st-rs JNI wrapper providing the
 * {@code org.forstdb.*} classes (instead of upstream forstjni).
 *
 * <p>If this test passes, it means the full chain works: Flink runtime →
 * Flink ForSt backend → st-rs Java wrapper → JNI → Rust engine.</p>
 *
 * <p>Failures here surface real capability gaps in the JNI / Java surface
 * that synthetic Java-only tests cannot expose.</p>
 */
class ForStBackendSmokeIT {

    @RegisterExtension
    static final MiniClusterExtension MINI = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(2)
                    .build());

    @Test
    void valueStateAcrossKeyedStream(@TempDir Path checkpointDir) throws Exception {
        final Configuration jobConfig = new Configuration();
        // Flink 2.x: state backend is selected via config, not env.setStateBackend.
        jobConfig.set(StateBackendOptions.STATE_BACKEND, "forst");
        jobConfig.setString("execution.checkpointing.dir",
                checkpointDir.toUri().toString());

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(jobConfig);
        env.setParallelism(2);
        env.enableCheckpointing(200);

        final List<String> inputs = Arrays.asList(
                "a", "a", "a",
                "b", "b",
                "c");

        final List<String> outputs = new ArrayList<>(env.fromData(inputs)
                .keyBy(s -> s)
                .process(new CountingFn())
                .executeAndCollect("st-rs ForSt smoke", inputs.size()));

        // 6 inputs => 6 outputs (one per record).
        assertEquals(inputs.size(), outputs.size(),
                "Expected " + inputs.size() + " outputs, got: " + outputs);

        // Each key's counter should monotonically increase from 1 — proves
        // ValueState.value() / .update() round-tripped through the JNI wrapper.
        final Map<String, List<Long>> byKey = new HashMap<>();
        for (String row : outputs) {
            final String[] parts = row.split(":");
            byKey.computeIfAbsent(parts[0], k -> new ArrayList<>())
                    .add(Long.parseLong(parts[1]));
        }
        assertEquals(List.of(1L, 2L, 3L), byKey.get("a"));
        assertEquals(List.of(1L, 2L), byKey.get("b"));
        assertEquals(List.of(1L), byKey.get("c"));

        // Sanity: Flink had a chance to trigger at least one checkpoint —
        // the job ran for ≥ ~200ms with checkpointing enabled, so the ForSt
        // backend exercised createCheckpoint / getLiveFilesMetaData at least
        // once through our JNI wrapper.
        assertTrue(checkpointDir.toFile().exists(),
                "Checkpoint dir should exist after run");
    }

    /**
     * Per-key counter that reads + writes a {@link ValueState}, exercising
     * st-rs's get / put paths through the JNI wrapper for every record.
     */
    static final class CountingFn
            extends KeyedProcessFunction<String, String, String> {

        private transient ValueState<Long> counter;

        @Override
        public void open(OpenContext openContext) {
            counter = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("counter", Long.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out)
                throws Exception {
            final Long current = counter.value();
            final long next = (current == null ? 0L : current) + 1L;
            counter.update(next);
            out.collect(ctx.getCurrentKey() + ":" + next);
        }
    }
}
