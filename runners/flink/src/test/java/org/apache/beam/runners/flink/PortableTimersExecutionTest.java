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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests the state and timer integration of {@link
 * org.apache.beam.runners.flink.translation.wrappers.streaming.ExecutableStageDoFnOperator}.
 */
@RunWith(Parameterized.class)
public class PortableTimersExecutionTest implements Serializable {

  @Parameters
  public static Object[] testModes() {
    return new Object[] {true, false};
  }

  @Parameter public boolean isStreaming;

  private transient ListeningExecutorService flinkJobExecutor;

  private static List<KV<String, Integer>> results = new ArrayList<>();

  @Before
  public void setup() {
    results.clear();
    flinkJobExecutor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
  }

  @After
  public void tearDown() {
    flinkJobExecutor.shutdown();
  }

  @Test(timeout = 60_000)
  public void testTimerExecution() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(CrashingRunner.class);
    options.as(FlinkPipelineOptions.class).setFlinkMaster("[local]");
    options.as(FlinkPipelineOptions.class).setStreaming(isStreaming);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);

    final String timerId = "foo";
    final String stateId = "sizzle";
    final int offset = 5000;
    final int timerOutput = 4093;
    // Enough keys that we exercise interesting code paths
    int numKeys = 50;
    List<KV<String, Integer>> input = new ArrayList<>();
    List<KV<String, Integer>> expectedOutput = new ArrayList<>();

    for (Integer key = 0; key < numKeys; ++key) {
      // Each key should have just one final output at GC time
      expectedOutput.add(KV.of(key.toString(), timerOutput));

      for (int i = 0; i < 15; ++i) {
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
            r.output(KV.of(state.read(), timerOutput));
          }
        };

    DoFn<KV<String, Integer>, Void> collectResults =
        new DoFn<KV<String, Integer>, Void>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            results.add(context.element());
          }
        };

    final Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(Impulse.create())
        .apply(ParDo.of(inputFn))
        .apply(ParDo.of(testFn))
        .apply(ParDo.of(collectResults));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);

    FlinkJobInvocation jobInvocation =
        FlinkJobInvocation.create(
            "id",
            "none",
            flinkJobExecutor,
            pipelineProto,
            options.as(FlinkPipelineOptions.class),
            null,
            Collections.emptyList());

    jobInvocation.start();
    long timeout = System.currentTimeMillis() + 60 * 1000;
    while (jobInvocation.getState() != Enum.DONE && System.currentTimeMillis() < timeout) {
      Thread.sleep(1000);
    }
    assertThat(jobInvocation.getState(), is(Enum.DONE));
    assertThat(results, containsInAnyOrder(expectedOutput.toArray()));
  }
}
