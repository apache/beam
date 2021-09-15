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

import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
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
 * Tests the State server integration of {@link
 * org.apache.beam.runners.flink.translation.wrappers.streaming.ExecutableStageDoFnOperator}.
 */
@RunWith(Parameterized.class)
public class PortableStateExecutionTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(PortableStateExecutionTest.class);

  @Parameters(name = "streaming: {0}")
  public static Object[] data() {
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

  // Special values which clear / write out state
  private static final int CLEAR_STATE = -1;
  private static final int WRITE_STATE = -2;

  @Test(timeout = 120_000)
  public void testExecution() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.fromArgs("--experiments=beam_fn_api").create();
    options.setRunner(CrashingRunner.class);
    options.as(FlinkPipelineOptions.class).setFlinkMaster("[local]");
    options.as(FlinkPipelineOptions.class).setStreaming(isStreaming);
    options.as(FlinkPipelineOptions.class).setParallelism(2);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, String>> output =
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
                        // values == -2 will write the current state to the output
                        ctx.output(KV.of("bla", WRITE_STATE));
                        ctx.output(KV.of("bla1", WRITE_STATE));
                        ctx.output(KV.of("bla2", WRITE_STATE));
                        ctx.output(KV.of("clearedState", WRITE_STATE));
                      }
                    }))
            .apply(
                "statefulDoFn",
                ParDo.of(
                    new DoFn<KV<String, Integer>, KV<String, String>>() {
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
                        performStateUpdates(ctx, valueState);
                        performStateUpdates(ctx, valueState2);
                      }

                      private void performStateUpdates(
                          ProcessContext ctx, ValueState<Integer> valueState) {
                        Integer value = ctx.element().getValue();
                        if (value == null) {
                          throw new IllegalStateException();
                        }
                        switch (value) {
                          case CLEAR_STATE:
                            valueState.clear();
                            break;
                          case WRITE_STATE:
                            Integer read = valueState.read();
                            ctx.output(
                                KV.of(
                                    ctx.element().getKey(),
                                    read == null ? "null" : read.toString()));
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

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("bla", "25"),
            KV.of("bla1", "42"),
            KV.of("bla2", "64"),
            KV.of("clearedState", "null"),
            KV.of("bla", "25"),
            KV.of("bla1", "42"),
            KV.of("bla2", "64"),
            KV.of("clearedState", "null"));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);

    JobInvocation jobInvocation =
        FlinkJobInvoker.create(null)
            .createJobInvocation(
                "id",
                "none",
                flinkJobExecutor,
                pipelineProto,
                options.as(FlinkPipelineOptions.class),
                new FlinkPipelineRunner(
                    options.as(FlinkPipelineOptions.class), null, Collections.emptyList()));

    jobInvocation.start();

    while (jobInvocation.getState() != JobState.Enum.DONE) {
      Thread.sleep(1000);
    }
  }
}
