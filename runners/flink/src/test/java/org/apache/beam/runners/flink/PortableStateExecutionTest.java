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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests the State server integration of {@link
 * org.apache.beam.runners.flink.translation.wrappers.streaming.ExecutableStageDoFnOperator}.
 */
@RunWith(Parameterized.class)
public class PortableStateExecutionTest implements Serializable {

  @Parameters
  public static Object[] data() {
    return new Object[] {true, false};
  }

  @Parameter public boolean isStreaming;

  private transient ListeningExecutorService flinkJobExecutor;

  @Before
  public void setup() {
    flinkJobExecutor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
  }

  @After
  public void tearDown() {
    flinkJobExecutor.shutdown();
  }

  // State -> Key -> Value
  private static final Map<String, Map<String, Integer>> stateValuesMap = new HashMap<>();

  @Before
  public void before() {
    stateValuesMap.clear();
    stateValuesMap.put("valueState", new HashMap<>());
    stateValuesMap.put("valueState2", new HashMap<>());
  }

  // Special values which clear / write out state
  private static final int CLEAR_STATE = -1;
  private static final int WRITE_STATE_TO_MAP = -2;

  @Test
  public void testExecution() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(CrashingRunner.class);
    options.as(FlinkPipelineOptions.class).setFlinkMaster("[local]");
    options.as(FlinkPipelineOptions.class).setStreaming(isStreaming);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    Pipeline p = Pipeline.create(options);
    p.apply(Impulse.create())
        .apply(
            ParDo.of(
                new DoFn<byte[], KV<String, Integer>>() {
                  @ProcessElement
                  public void process(ProcessContext ctx) {
                    // Values == -1 will clear the state
                    ctx.output(KV.of("clearedState", 1));
                    ctx.output(KV.of("clearedState", CLEAR_STATE));
                    // values >= 1 will be added on top of each other
                    ctx.output(KV.of("bla1", 42));
                    ctx.output(KV.of("bla", 23));
                    ctx.output(KV.of("bla2", 64));
                    ctx.output(KV.of("bla", 1));
                    ctx.output(KV.of("bla", 1));
                    // values == -2 will write the state to a map
                    ctx.output(KV.of("bla", WRITE_STATE_TO_MAP));
                    ctx.output(KV.of("bla1", WRITE_STATE_TO_MAP));
                    ctx.output(KV.of("bla2", WRITE_STATE_TO_MAP));
                    ctx.output(KV.of("clearedState", -2));
                  }
                }))
        .apply(
            "statefulDoFn",
            ParDo.of(
                new DoFn<KV<String, Integer>, String>() {
                  @StateId("valueState")
                  private final StateSpec<ValueState<Integer>> valueStateSpec =
                      StateSpecs.value(VarIntCoder.of());

                  @StateId("valueState2")
                  private final StateSpec<ValueState<Integer>> valueStateSpec2 =
                      StateSpecs.value(VarIntCoder.of());

                  @ProcessElement
                  public void process(
                      ProcessContext ctx,
                      @StateId("valueState") ValueState<Integer> valueState,
                      @StateId("valueState2") ValueState<Integer> valueState2) {
                    performStateUpdates("valueState", ctx, valueState);
                    performStateUpdates("valueState2", ctx, valueState2);
                  }

                  private void performStateUpdates(
                      String stateId, ProcessContext ctx, ValueState<Integer> valueState) {
                    Map<String, Integer> stateValues = stateValuesMap.get(stateId);
                    Integer value = ctx.element().getValue();
                    if (value == null) {
                      throw new IllegalStateException();
                    }
                    switch (value) {
                      case CLEAR_STATE:
                        valueState.clear();
                        break;
                      case WRITE_STATE_TO_MAP:
                        stateValues.put(ctx.element().getKey(), valueState.read());
                        break;
                      default:
                        Integer currentState = valueState.read();
                        if (currentState == null) {
                          currentState = value;
                        } else {
                          currentState += value;
                        }
                        valueState.write(currentState);
                    }
                  }
                }));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);

    FlinkJobInvocation jobInvocation =
        FlinkJobInvocation.create(
            "id",
            "none",
            flinkJobExecutor,
            pipelineProto,
            options.as(FlinkPipelineOptions.class),
            Collections.emptyList());

    jobInvocation.start();
    long timeout = System.currentTimeMillis() + 60 * 1000;
    while (jobInvocation.getState() != Enum.DONE && System.currentTimeMillis() < timeout) {
      Thread.sleep(1000);
    }
    assertThat(jobInvocation.getState(), is(Enum.DONE));

    Map<String, Integer> expected = new HashMap<>();
    expected.put("bla", 25);
    expected.put("bla1", 42);
    expected.put("bla2", 64);
    expected.put("clearedState", null);

    for (Map<String, Integer> statesValues : stateValuesMap.values()) {
      assertThat(statesValues, equalTo(expected));
    }
  }
}
