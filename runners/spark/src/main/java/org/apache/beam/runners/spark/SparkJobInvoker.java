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

import java.util.UUID;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.runners.jobsubmission.JobInvoker;
import org.apache.beam.runners.jobsubmission.PortablePipelineJarCreator;
import org.apache.beam.runners.jobsubmission.PortablePipelineRunner;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates a job invocation to manage the Spark runner's execution of a portable pipeline. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SparkJobInvoker extends JobInvoker {

  private static final Logger LOG = LoggerFactory.getLogger(SparkJobInvoker.class);

  private SparkJobServerDriver.SparkServerConfiguration configuration;

  public static SparkJobInvoker create(
      SparkJobServerDriver.SparkServerConfiguration configuration) {
    return new SparkJobInvoker(configuration);
  }

  private SparkJobInvoker(SparkJobServerDriver.SparkServerConfiguration configuration) {
    super("spark-runner-job-invoker-%d");
    this.configuration = configuration;
  }

  @Override
  protected JobInvocation invokeWithExecutor(
      Pipeline pipeline,
      Struct options,
      @Nullable String retrievalToken,
      ListeningExecutorService executorService) {
    LOG.trace("Parsing pipeline options");
    SparkPipelineOptions sparkOptions =
        PipelineOptionsTranslation.fromProto(options).as(SparkPipelineOptions.class);

    String invocationId =
        String.format("%s_%s", sparkOptions.getJobName(), UUID.randomUUID().toString());
    LOG.info("Invoking job {}", invocationId);

    if (sparkOptions.getSparkMaster().equals(SparkPipelineOptions.DEFAULT_MASTER_URL)) {
      sparkOptions.setSparkMaster(configuration.getSparkMasterUrl());
    }

    // Options can't be translated to proto if runner class is unresolvable, so set it to null.
    sparkOptions.setRunner(null);

    if (sparkOptions.getAppName() == null) {
      LOG.debug("App name was null. Using invocationId {}", invocationId);
      sparkOptions.setAppName(invocationId);
    }

    return createJobInvocation(
        invocationId, retrievalToken, executorService, pipeline, sparkOptions);
  }

  static JobInvocation createJobInvocation(
      String invocationId,
      String retrievalToken,
      ListeningExecutorService executorService,
      Pipeline pipeline,
      SparkPipelineOptions sparkOptions) {
    JobInfo jobInfo =
        JobInfo.create(
            invocationId,
            sparkOptions.getJobName(),
            retrievalToken,
            PipelineOptionsTranslation.toProto(sparkOptions));
    PortablePipelineRunner pipelineRunner;
    if (Strings.isNullOrEmpty(
        sparkOptions.as(PortablePipelineOptions.class).getOutputExecutablePath())) {
      pipelineRunner = new SparkPipelineRunner(sparkOptions);
    } else {
      pipelineRunner = new PortablePipelineJarCreator(SparkPipelineRunner.class);
    }
    return new JobInvocation(jobInfo, executorService, pipeline, pipelineRunner);
  }
}
