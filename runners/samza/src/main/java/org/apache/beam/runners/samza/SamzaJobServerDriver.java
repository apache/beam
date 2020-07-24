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
package org.apache.beam.runners.samza;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.InMemoryJobService;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.runners.jobsubmission.JobInvoker;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Driver program that starts a job server. */
// TODO extend JobServerDriver
public class SamzaJobServerDriver {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaJobServerDriver.class);

  private final SamzaPortablePipelineOptions pipelineOptions;

  private SamzaJobServerDriver(SamzaPortablePipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  public static void main(String[] args) throws Exception {
    SamzaPortablePipelineOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(args).as(SamzaPortablePipelineOptions.class);
    fromOptions(pipelineOptions).run();
  }

  public static SamzaJobServerDriver fromOptions(SamzaPortablePipelineOptions pipelineOptions) {
    Map<String, String> overrideConfig =
        pipelineOptions.getConfigOverride() != null
            ? pipelineOptions.getConfigOverride()
            : new HashMap<>();
    overrideConfig.put(SamzaRunnerOverrideConfigs.IS_PORTABLE_MODE, String.valueOf(true));
    overrideConfig.put(
        SamzaRunnerOverrideConfigs.FN_CONTROL_PORT,
        String.valueOf(pipelineOptions.getControlPort()));
    pipelineOptions.setConfigOverride(overrideConfig);
    return new SamzaJobServerDriver(pipelineOptions);
  }

  private static InMemoryJobService createJobService(SamzaPortablePipelineOptions pipelineOptions)
      throws IOException {
    JobInvoker jobInvoker =
        new JobInvoker("samza-job-invoker") {
          @Override
          protected JobInvocation invokeWithExecutor(
              RunnerApi.Pipeline pipeline,
              Struct options,
              @Nullable String retrievalToken,
              ListeningExecutorService executorService)
              throws IOException {
            String invocationId =
                String.format("%s_%s", pipelineOptions.getJobName(), UUID.randomUUID().toString());
            SamzaPipelineRunner pipelineRunner = new SamzaPipelineRunner(pipelineOptions);
            JobInfo jobInfo =
                JobInfo.create(
                    invocationId,
                    pipelineOptions.getJobName(),
                    retrievalToken,
                    PipelineOptionsTranslation.toProto(pipelineOptions));
            return new JobInvocation(jobInfo, executorService, pipeline, pipelineRunner);
          }
        };
    return InMemoryJobService.create(
        null,
        session -> session,
        stagingSessionToken -> {},
        jobInvoker,
        InMemoryJobService.DEFAULT_MAX_INVOCATION_HISTORY);
  }

  public void run() throws Exception {
    final InMemoryJobService service = createJobService(pipelineOptions);
    final GrpcFnServer<InMemoryJobService> jobServiceGrpcFnServer =
        GrpcFnServer.allocatePortAndCreateFor(
            service, ServerFactory.createWithPortSupplier(pipelineOptions::getJobPort));
    LOG.info("JobServer started on {}", jobServiceGrpcFnServer.getApiServiceDescriptor().getUrl());
    try {
      jobServiceGrpcFnServer.getServer().awaitTermination();
    } finally {
      LOG.info("JobServer closing");
      jobServiceGrpcFnServer.close();
    }
  }
}
