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
import java.net.InetAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.jobsubmission.InMemoryJobService;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.runners.jobsubmission.JobInvoker;
import org.apache.beam.sdk.expansion.service.ExpansionServer;
import org.apache.beam.sdk.expansion.service.ExpansionService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Driver program that starts a job server. */
// TODO(BEAM-8510): extend JobServerDriver
public class SamzaJobServerDriver {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaJobServerDriver.class);

  private final SamzaPortablePipelineOptions pipelineOptions;
  private ExpansionServer expansionServer;

  protected SamzaJobServerDriver(SamzaPortablePipelineOptions pipelineOptions) {
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
    overrideConfig.put(SamzaRunnerOverrideConfigs.FS_TOKEN_PATH, pipelineOptions.getFsTokenPath());

    pipelineOptions.setConfigOverride(overrideConfig);
    return new SamzaJobServerDriver(pipelineOptions);
  }

  private InMemoryJobService createJobService() throws IOException {
    JobInvoker jobInvoker =
        new JobInvoker("samza-job-invoker") {
          @Override
          protected JobInvocation invokeWithExecutor(
              RunnerApi.Pipeline pipeline,
              Struct options,
              @Nullable String retrievalToken,
              ListeningExecutorService executorService)
              throws IOException {
            return new SamzaJobInvocation(pipeline, pipelineOptions);
          }
        };
    return InMemoryJobService.create(
        null,
        session -> session,
        stagingSessionToken -> {},
        jobInvoker,
        InMemoryJobService.DEFAULT_MAX_INVOCATION_HISTORY);
  }

  private ExpansionServer createExpansionService(String host, int expansionPort)
      throws IOException {
    if (host == null) {
      host = InetAddress.getLoopbackAddress().getHostName();
    }
    ExpansionServer expansionServer =
        ExpansionServer.create(new ExpansionService(), host, expansionPort);
    LOG.info(
        "Java ExpansionService started on {}:{}",
        expansionServer.getHost(),
        expansionServer.getPort());
    return expansionServer;
  }

  public void run() throws Exception {
    // Create services
    InMemoryJobService service = createJobService();
    GrpcFnServer<InMemoryJobService> jobServiceGrpcFnServer =
        GrpcFnServer.allocatePortAndCreateFor(
            service, ServerFactory.createWithPortSupplier(pipelineOptions::getJobPort));
    String jobServerUrl = jobServiceGrpcFnServer.getApiServiceDescriptor().getUrl();
    LOG.info("JobServer started on {}", jobServerUrl);
    URI uri = new URI(jobServerUrl);
    expansionServer = createExpansionService(uri.getHost(), pipelineOptions.getExpansionPort());

    try {
      jobServiceGrpcFnServer.getServer().awaitTermination();
    } finally {
      LOG.info("JobServer closing");
      jobServiceGrpcFnServer.close();
      if (expansionServer != null) {
        try {
          expansionServer.close();
          LOG.info(
              "Expansion stopped on {}:{}", expansionServer.getHost(), expansionServer.getPort());
          expansionServer = null;
        } catch (Exception e) {
          LOG.error("Error while closing the Expansion Service.", e);
        }
      }
    }
  }
}
