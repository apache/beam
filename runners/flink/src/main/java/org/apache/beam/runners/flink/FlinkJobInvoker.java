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

import static org.apache.beam.sdk.util.construction.resources.PipelineResources.detectClassPathResourcesToStage;

import java.util.UUID;
import org.apache.beam.model.pipeline.v1.RunnerApi;
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

/** Job Invoker for the {@link FlinkRunner}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FlinkJobInvoker extends JobInvoker {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobInvoker.class);

  public static FlinkJobInvoker create(FlinkJobServerDriver.FlinkServerConfiguration serverConfig) {
    return new FlinkJobInvoker(serverConfig);
  }

  private final FlinkJobServerDriver.FlinkServerConfiguration serverConfig;

  protected FlinkJobInvoker(FlinkJobServerDriver.FlinkServerConfiguration serverConfig) {
    super("flink-runner-job-invoker-%d");
    this.serverConfig = serverConfig;
  }

  @Override
  protected JobInvocation invokeWithExecutor(
      RunnerApi.Pipeline pipeline,
      Struct options,
      @Nullable String retrievalToken,
      ListeningExecutorService executorService) {

    // TODO: How to make Java/Python agree on names of keys and their values?
    LOG.trace("Parsing pipeline options");
    FlinkPipelineOptions flinkOptions =
        PipelineOptionsTranslation.fromProto(options).as(FlinkPipelineOptions.class);

    String invocationId =
        String.format("%s_%s", flinkOptions.getJobName(), UUID.randomUUID().toString());

    if (FlinkPipelineOptions.AUTO.equals(flinkOptions.getFlinkMaster())) {
      flinkOptions.setFlinkMaster(serverConfig.getFlinkMaster());
    }

    PortablePipelineOptions portableOptions = flinkOptions.as(PortablePipelineOptions.class);

    PortablePipelineRunner pipelineRunner;
    if (Strings.isNullOrEmpty(portableOptions.getOutputExecutablePath())) {
      pipelineRunner =
          new FlinkPipelineRunner(
              flinkOptions,
              serverConfig.getFlinkConfDir(),
              detectClassPathResourcesToStage(
                  FlinkJobInvoker.class.getClassLoader(), flinkOptions));
    } else {
      pipelineRunner = new PortablePipelineJarCreator(FlinkPipelineRunner.class);
    }

    flinkOptions.setRunner(null);

    LOG.info("Invoking job {} with pipeline runner {}", invocationId, pipelineRunner);
    return createJobInvocation(
        invocationId, retrievalToken, executorService, pipeline, flinkOptions, pipelineRunner);
  }

  protected JobInvocation createJobInvocation(
      String invocationId,
      String retrievalToken,
      ListeningExecutorService executorService,
      RunnerApi.Pipeline pipeline,
      FlinkPipelineOptions flinkOptions,
      PortablePipelineRunner pipelineRunner) {
    JobInfo jobInfo =
        JobInfo.create(
            invocationId,
            flinkOptions.getJobName(),
            retrievalToken,
            PipelineOptionsTranslation.toProto(flinkOptions));
    return new JobInvocation(jobInfo, executorService, pipeline, pipelineRunner);
  }
}
