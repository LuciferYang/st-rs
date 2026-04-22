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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Two-phase IT proving that ForSt-backed state survives a savepoint +
 * restore cycle: phase 1 builds state and triggers a savepoint, phase 2
 * starts from that savepoint and asserts that the restored counters
 * continue from where phase 1 left off.
 *
 * <p>Failures here surface gaps in the restore-side JNI surface — the
 * checkpoint side was covered by {@link ForStBackendSmokeIT}.
 */
class ForStBackendCheckpointRestoreIT {

    @RegisterExtension
    static final MiniClusterExtension MINI = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(2)
                    .build());

    private static Configuration baseConfig(final Path checkpointDir) {
        final Configuration cfg = new Configuration();
        cfg.set(StateBackendOptions.STATE_BACKEND, "forst");
        cfg.setString("execution.checkpointing.dir",
                checkpointDir.toUri().toString());
        cfg.setString("restart-strategy.type", "none");
        return cfg;
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES,
            threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void valueStateSurvivesSavepointRestore(
            @TempDir Path checkpointDir,
            @TempDir Path savepointDir) throws Exception {

        // ---------- Phase 1: build state, take savepoint, cancel ----------
        final Configuration cfg1 = baseConfig(checkpointDir);
        final StreamExecutionEnvironment env1 =
                StreamExecutionEnvironment.getExecutionEnvironment(cfg1);
        env1.setParallelism(2);
        env1.enableCheckpointing(500);

        // Throttled, effectively unbounded source so the job stays
        // alive long enough to reach a savepoint.
        final DataGeneratorSource<String> src1 = new DataGeneratorSource<>(
                index -> KEY_RING[(int) (index % KEY_RING.length)],
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(20),
                Types.STRING);

        env1.fromSource(src1, WatermarkStrategy.noWatermarks(), "datagen")
                .keyBy(s -> s)
                .process(new CountingFn())
                .print();

        System.out.println("[IT] phase=1 step=executeAsync.before");
        final JobClient client1 = env1.executeAsync("phase 1 - build state");
        System.out.println("[IT] phase=1 jobId=" + client1.getJobID());

        // Let some elements flow + at least one checkpoint complete.
        Thread.sleep(2000);

        // Trigger a savepoint to a fresh dir; result is the fully-qualified
        // savepoint path that phase 2 will restore from.
        System.out.println("[IT] phase=1 step=triggerSavepoint");
        final String savepointPath = client1
                .triggerSavepoint(savepointDir.toUri().toString(),
                        SavepointFormatType.CANONICAL)
                .get(60, TimeUnit.SECONDS);
        System.out.println("[IT] phase=1 savepoint=" + savepointPath);

        client1.cancel().get(30, TimeUnit.SECONDS);
        // Wait for terminal status before moving on.
        long deadline = System.currentTimeMillis() + 30_000;
        JobStatus last = null;
        while (System.currentTimeMillis() < deadline) {
            final JobStatus s = client1.getJobStatus().get(5, TimeUnit.SECONDS);
            if (s != last) {
                System.out.println("[IT] phase=1 jobStatus=" + s);
                last = s;
            }
            if (s.isGloballyTerminalState() || s == JobStatus.CANCELED) {
                break;
            }
            Thread.sleep(200);
        }

        // ---------- Phase 2: start from savepoint, verify continuity ----
        final Configuration cfg2 = baseConfig(checkpointDir);
        cfg2.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
        final StreamExecutionEnvironment env2 =
                StreamExecutionEnvironment.getExecutionEnvironment(cfg2);
        env2.setParallelism(2);

        // Bounded source; one record per key so each key's counter
        // ticks exactly once. If state was restored, the next value
        // for each key must be > 1 — proving phase 1's writes survived.
        env2.fromData(Arrays.asList(KEY_RING))
                .keyBy(s -> s)
                .process(new RestoredCountingFn())
                .print();

        System.out.println("[IT] phase=2 step=executeAsync.before");
        final JobClient client2 = env2.executeAsync("phase 2 - restore");
        System.out.println("[IT] phase=2 jobId=" + client2.getJobID());

        try {
            final JobExecutionResult r = client2.getJobExecutionResult()
                    .get(60, TimeUnit.SECONDS);
            System.out.println("[IT] phase=2 finished runtimeMs="
                    + r.getNetRuntime());
        } catch (final java.util.concurrent.ExecutionException e) {
            System.out.println("[IT] phase=2 failed cause=");
            e.getCause().printStackTrace(System.out);
            throw e;
        }
    }

    /** Keys we recycle through in both phases. */
    private static final String[] KEY_RING = {"a", "b", "c"};

    /**
     * Phase-1 counter: bumps a per-key {@link ValueState} every record.
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
        public void processElement(String value, Context ctx,
                Collector<String> out) throws Exception {
            final Long current = counter.value();
            final long next = (current == null ? 0L : current) + 1L;
            counter.update(next);
            out.collect(ctx.getCurrentKey() + ":" + next);
        }
    }

    /**
     * Phase-2 counter: same descriptor name as phase 1 ("counter"), so
     * Flink restores the value. Asserts {@code current != null} on the
     * first element per key — that's the proof state survived.
     */
    static final class RestoredCountingFn
            extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<Long> counter;

        @Override
        public void open(OpenContext openContext) {
            counter = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("counter", Long.class));
        }

        @Override
        public void processElement(String value, Context ctx,
                Collector<String> out) throws Exception {
            final String key = ctx.getCurrentKey();
            final Long current = counter.value();
            System.out.println("[IT] phase=2 key=" + key
                    + " restoredCurrent=" + current);
            if (current == null) {
                throw new IllegalStateException(
                        "Restore lost state for key=" + key
                                + " — counter.value() returned null");
            }
            final long next = current + 1L;
            counter.update(next);
            out.collect(key + ":" + next);
        }
    }
}
