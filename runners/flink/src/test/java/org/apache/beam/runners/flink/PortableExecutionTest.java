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
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
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
 * Tests the execution of a pipeline from specification to execution on the portable Flink runner.
 * Exercises job invocation, executable stage translation and deployment with embedded Flink for
 * batch and streaming.
 */
@RunWith(Parameterized.class)
public class PortableExecutionTest implements Serializable {

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
    options.as(FlinkPipelineOptions.class).setNumberOfExecutionRetries(0);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, Iterable<Long>>> result =
        p.apply("impulse", Impulse.create())
            .apply(
                "create",
                ParDo.of(
                    new DoFn<byte[], String>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {
                        ctxt.output("zero");
                        ctxt.output("one");
                        ctxt.output("two");
                      }
                    }))
            .apply(
                "len",
                ParDo.of(
                    new DoFn<String, Long>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {
                        ctxt.output((long) ctxt.element().length());
                      }
                    }))
            .apply("addKeys", WithKeys.of("foo"))
            // Use some unknown coders
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()))
            // Force the output to be materialized
            .apply("gbk", GroupByKey.create());

    PAssert.that(result).containsInAnyOrder(KV.of("foo", ImmutableList.of(4L, 3L, 3L)));

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
      Thread.sleep(1000);
    }
  }
}
