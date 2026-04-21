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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.JobClient;
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
 * End-to-end smoke test: runs a Flink streaming job that uses the ForSt
 * state backend with the st-rs JNI wrapper providing the
 * {@code org.forstdb.*} classes (instead of upstream forstjni).
 *
 * <p>Init failures during ForSt backend setup show up as a real exception
 * from {@link JobClient#getJobExecutionResult()} — much easier to diagnose
 * than the silent hang you get with {@code DataStream.executeAndCollect}.
 *
 * <p>State assertions are a follow-up; M1's job is to prove the full
 * Flink → ForSt → JNI → Rust path initializes and runs without error.
 */
class ForStBackendSmokeIT {

    @RegisterExtension
    static final MiniClusterExtension MINI = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(2)
                    .build());

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void valueStateAcrossKeyedStream(@TempDir Path checkpointDir) throws Exception {
        System.out.println("[IT] step=config.start");
        final Configuration jobConfig = new Configuration();
        jobConfig.set(StateBackendOptions.STATE_BACKEND, "forst");
        jobConfig.setString("execution.checkpointing.dir",
                checkpointDir.toUri().toString());

        System.out.println("[IT] step=env.create");
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(jobConfig);
        env.setParallelism(2);
        env.enableCheckpointing(200);

        final List<String> inputs = Arrays.asList(
                "a", "a", "a",
                "b", "b",
                "c");

        env.fromData(inputs)
                .keyBy(s -> s)
                .process(new CountingFn())
                .print();

        System.out.println("[IT] step=executeAsync.before");
        final JobClient client = env.executeAsync("st-rs ForSt smoke");
        System.out.println("[IT] step=executeAsync.after jobId=" + client.getJobID());

        // Block on terminal status. If the ForSt backend fails during init,
        // get() throws ExecutionException with the underlying Flink failure
        // — full stack trace, not a silent collect-poll hang.
        final JobExecutionResult result =
                client.getJobExecutionResult().get(60, TimeUnit.SECONDS);
        System.out.println("[IT] step=jobFinished runtimeMs=" + result.getNetRuntime());
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
            System.out.println("[IT] CountingFn.open subtask="
                    + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
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
