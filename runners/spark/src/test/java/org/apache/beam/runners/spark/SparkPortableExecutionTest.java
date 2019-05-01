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
package org.apache.beam.runners.spark;

import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.JavaReadViaImpulse;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.MoreExecutors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the execution of a pipeline from specification to execution on the portable Spark runner.
 */
public class SparkPortableExecutionTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPortableExecutionTest.class);

  private static ListeningExecutorService sparkJobExecutor;

  @BeforeClass
  public static void setup() {
    // Restrict this to only one thread to avoid multiple Spark clusters up at the same time
    // which is not suitable for memory-constraint environments, i.e. Jenkins.
    sparkJobExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
  }

  @AfterClass
  public static void tearDown() throws InterruptedException {
    sparkJobExecutor.shutdown();
    sparkJobExecutor.awaitTermination(10, TimeUnit.SECONDS);
    if (!sparkJobExecutor.isShutdown()) {
      LOG.warn("Could not shut down Spark job executor");
    }
    sparkJobExecutor = null;
  }

  @Test(timeout = 120_000)
  public void testExecution() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(CrashingRunner.class);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);

    Pipeline p = Pipeline.create(options);

    final PCollectionView<Long> view =
        p.apply("impulse23", Impulse.create())
            .apply(
                "create23",
                ParDo.of(
                    new DoFn<byte[], Long>() {
                      @ProcessElement
                      public void process(ProcessContext context) {
                        context.output(23L);
                      }
                    }))
            .apply(View.asSingleton());

    PCollection<KV<String, Iterable<Long>>> result =
        p.apply("impulse", Impulse.create())
            .apply(
                "create",
                ParDo.of(
                    new DoFn<byte[], String>() {
                      @ProcessElement
                      public void process(ProcessContext context) {
                        context.output("zero");
                        context.output("one");
                        context.output("two");
                      }
                    }))
            .apply(
                "len",
                ParDo.of(
                    new DoFn<String, Long>() {
                      @ProcessElement
                      public void process(ProcessContext context) {
                        context.output((long) context.element().length());
                      }
                    }))
            .apply("addKeys", WithKeys.of("foo"))
            // First GBK just to verify GBK works
            .apply("gbk", GroupByKey.create())
            .apply(
                "print",
                ParDo.of(
                        new DoFn<KV<String, Iterable<Long>>, KV<String, Long>>() {
                          @ProcessElement
                          public void process(ProcessContext context) {
                            context.output(KV.of("bar", context.sideInput(view)));
                            for (Long i : context.element().getValue()) {
                              context.output(KV.of(context.element().getKey(), i));
                            }
                          }
                        })
                    .withSideInputs(view))
            // Second GBK forces the output to be materialized
            .apply("gbk", GroupByKey.create());

    PAssert.that(result)
        .containsInAnyOrder(
            KV.of("foo", ImmutableList.of(4L, 3L, 3L)), KV.of("bar", ImmutableList.of(23L)));

    // This is line below required to convert the PAssert's read to an impulse, which is expected
    // by the GreedyPipelineFuser.
    p.replaceAll(Collections.singletonList(JavaReadViaImpulse.boundedOverride()));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);

    JobInvocation jobInvocation =
        SparkJobInvoker.createJobInvocation(
            "fakeId",
            "fakeRetrievalToken",
            sparkJobExecutor,
            pipelineProto,
            options.as(SparkPipelineOptions.class));
    jobInvocation.start();
    while (jobInvocation.getState() != Enum.DONE) {
      Thread.sleep(1000);
    }
  }
}
