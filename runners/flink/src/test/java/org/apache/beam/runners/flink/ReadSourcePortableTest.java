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
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
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

/** Tests that Read translation is supported in portable pipelines. */
@RunWith(Parameterized.class)
public class ReadSourcePortableTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(PortableExecutionTest.class);

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
    PCollection<Long> result = p.apply(GenerateSequence.from(0L).to(10L));
    PAssert.that(result)
        .containsInAnyOrder(ImmutableList.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);

    // execute the pipeline
    JobInvocation jobInvocation =
        FlinkJobInvoker.create(null)
            .createJobInvocation(
                "fakeId",
                "fakeRetrievalToken",
                flinkJobExecutor,
                pipelineProto,
                options.as(FlinkPipelineOptions.class),
                new FlinkPipelineRunner(
                    options.as(FlinkPipelineOptions.class), null, Collections.emptyList()));
    jobInvocation.start();
    while (jobInvocation.getState() != JobState.Enum.DONE) {
      Thread.sleep(100);
    }
  }
}
