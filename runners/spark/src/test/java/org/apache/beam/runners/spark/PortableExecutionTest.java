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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.transforms.Impulse;
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
public class PortableExecutionTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(PortableExecutionTest.class);

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
  public void testImpulse() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(CrashingRunner.class);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    Pipeline p = Pipeline.create(options);
    p.apply("impulse", Impulse.create());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);

    SparkPipelineRunner pipelineRunner =
        new SparkPipelineRunner(options.as(SparkPipelineOptions.class));
    JobInvocation jobInvocation =
        new JobInvocation("fakeId", sparkJobExecutor, pipelineProto, pipelineRunner);
    jobInvocation.start();
    // For now, ensure the pipeline fails because impulse is not yet implemented.
    while (jobInvocation.getState() != Enum.FAILED) {
      Thread.sleep(1000);
    }
  }
}
