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
package org.apache.beam.runners.flink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the state and timer integration of {@link
 * org.apache.beam.runners.flink.translation.wrappers.streaming.ExecutableStageDoFnOperator}.
 *
 * <p>The test sets the same timers multiple times per key. This tests that only the latest version
 * of a given timer is run.
 */
@RunWith(Parameterized.class)
public class PortableTimersExecutionTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(PortableTimersExecutionTest.class);

  @Parameters(name = "streaming: {0}")
  public static Object[] testModes() {
    return new Object[] {true, false};
  }

  @Parameter public boolean isStreaming;

  private static ListeningExecutorService flinkJobExecutor;

  @BeforeClass
  public static void setup() {
    // Restrict this to only one thread to avoid multiple Flink clusters up at the same time
    // which is not suitable for memory-constraint environments, i.e. Jenkins.
    flinkJobExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
  }

  @AfterClass
  public static void tearDown() throws InterruptedException {
    flinkJobExecutor.shutdown();
    flinkJobExecutor.awaitTermination(10, TimeUnit.SECONDS);
    if (!flinkJobExecutor.isShutdown()) {
      LOG.warn("Could not shutdown Flink job executor");
    }
    flinkJobExecutor = null;
  }

  @Test(timeout = 120_000)
  public void testTimerExecution() throws Exception {
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs("--experiments=beam_fn_api").as(FlinkPipelineOptions.class);
    options.setRunner(CrashingRunner.class);
    options.setFlinkMaster("[local]");
    options.setStreaming(isStreaming);
    options.setParallelism(2);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);

    final String timerId = "foo";
    final String stateId = "sizzle";
    final int offset = 5000;
    final int timerOutput = 4093;
    // Enough keys that we exercise interesting code paths
    int numKeys = 50;
    int numDuplicateTimers = 15;
    List<KV<String, Integer>> input = new ArrayList<>();
    List<KV<String, Integer>> expectedOutput = new ArrayList<>();

    for (Integer key = 0; key < numKeys; ++key) {
      // Each key should have just one final output at GC time
      expectedOutput.add(KV.of(key.toString(), timerOutput));

      for (int i = 0; i < numDuplicateTimers; ++i) {
        // Each input should be output with the offset added
        input.add(KV.of(key.toString(), i));
        expectedOutput.add(KV.of(key.toString(), i + offset));
      }
    }

    Collections.shuffle(input);

    DoFn<byte[], KV<String, Integer>> inputFn =
        new DoFn<byte[], KV<String, Integer>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            for (KV<String, Integer> stringIntegerKV : input) {
              context.output(stringIntegerKV);
            }
          }
        };

    DoFn<KV<String, Integer>, KV<String, Integer>> testFn =
        new DoFn<KV<String, Integer>, KV<String, Integer>>() {

          @TimerId(timerId)
          private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @StateId(stateId)
          private final StateSpec<ValueState<String>> stateSpec =
              StateSpecs.value(StringUtf8Coder.of());

          @ProcessElement
          public void processElement(
              ProcessContext context,
              @TimerId(timerId) Timer timer,
              @StateId(stateId) ValueState<String> state,
              BoundedWindow window) {
            timer.set(window.maxTimestamp());
            state.write(context.element().getKey());
            context.output(
                KV.of(context.element().getKey(), context.element().getValue() + offset));
          }

          @OnTimer(timerId)
          public void onTimer(
              @StateId(stateId) ValueState<String> state, OutputReceiver<KV<String, Integer>> r) {
            String read = Objects.requireNonNull(state.read(), "State must not be null");
            KV<String, Integer> of = KV.of(read, timerOutput);
            r.output(of);
          }
        };

    final Pipeline pipeline = Pipeline.create(options);
    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("Impulse", Impulse.create())
            .apply("Input", ParDo.of(inputFn))
            .apply("Timers", ParDo.of(testFn));
    PAssert.that(output).containsInAnyOrder(expectedOutput);

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);

    JobInvocation jobInvocation =
        FlinkJobInvoker.create(null)
            .createJobInvocation(
                "id",
                "none",
                flinkJobExecutor,
                pipelineProto,
                options,
                new FlinkPipelineRunner(options, null, Collections.emptyList()));

    jobInvocation.start();
    while (jobInvocation.getState() != JobState.Enum.DONE) {
      Thread.sleep(1000);
    }
    assertThat(jobInvocation.getState(), is(JobState.Enum.DONE));
  }
}
