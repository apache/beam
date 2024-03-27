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
package org.apache.beam.runners.portability;

import static org.apache.beam.sdk.util.construction.resources.PipelineResources.detectClassPathResourcesToStage;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.beam.fn.harness.ExternalWorkerService;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.runners.portability.CloseableResource.CloseException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.construction.DefaultArtifactResolver;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link PipelineRunner} a {@link Pipeline} against a {@code JobService}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PortableRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(PortableRunner.class);

  /** Provided pipeline options. */
  private final PipelineOptions options;
  /** Job API endpoint. */
  private final String endpoint;
  /** Channel factory used to create communication channel with job and staging services. */
  private final ManagedChannelFactory channelFactory;

  /**
   * Constructs a runner from the provided options.
   *
   * @param options Properties which configure the runner.
   * @return The newly created runner.
   */
  public static PortableRunner fromOptions(PipelineOptions options) {
    return create(options, ManagedChannelFactory.createDefault());
  }

  @VisibleForTesting
  static PortableRunner create(PipelineOptions options, ManagedChannelFactory channelFactory) {
    PortablePipelineOptions portableOptions =
        PipelineOptionsValidator.validate(PortablePipelineOptions.class, options);

    String endpoint = portableOptions.getJobEndpoint();

    return new PortableRunner(options, endpoint, channelFactory);
  }

  private PortableRunner(
      PipelineOptions options, String endpoint, ManagedChannelFactory channelFactory) {
    this.options = options;
    this.endpoint = endpoint;
    this.channelFactory = channelFactory;
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    Runnable cleanup;
    if (Environments.ENVIRONMENT_LOOPBACK.equals(
        options.as(PortablePipelineOptions.class).getDefaultEnvironmentType())) {
      GrpcFnServer<ExternalWorkerService> workerService;
      try {
        workerService = new ExternalWorkerService(options).start();
      } catch (Exception exn) {
        throw new RuntimeException("Failed to start GrpcFnServer for ExternalWorkerService", exn);
      }
      LOG.info("Starting worker service at {}", workerService.getApiServiceDescriptor().getUrl());
      options
          .as(PortablePipelineOptions.class)
          .setDefaultEnvironmentConfig(workerService.getApiServiceDescriptor().getUrl());
      cleanup =
          () -> {
            try {
              LOG.warn("closing worker service {}", workerService);
              workerService.close();
            } catch (Exception exn) {
              throw new RuntimeException(exn);
            }
          };
    } else {
      cleanup = null;
    }

    ImmutableList.Builder<String> filesToStageBuilder = ImmutableList.builder();
    List<String> stagingFiles = options.as(PortablePipelineOptions.class).getFilesToStage();
    if (stagingFiles == null) {
      List<String> classpathResources =
          detectClassPathResourcesToStage(Environments.class.getClassLoader(), options);
      if (classpathResources.isEmpty()) {
        throw new IllegalArgumentException("No classpath elements found.");
      }
      LOG.debug(
          "PortablePipelineOptions.filesToStage was not specified. "
              + "Defaulting to files from the classpath: {}",
          classpathResources.size());
      filesToStageBuilder.addAll(classpathResources);
    } else {
      filesToStageBuilder.addAll(stagingFiles);
    }

    // TODO(heejong): remove jar_packages experimental flag when cross-language dependency
    //   management is implemented for all runners.
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    if (experiments != null) {
      Optional<String> jarPackages =
          experiments.stream()
              .filter((String flag) -> flag.startsWith("jar_packages="))
              .findFirst();
      jarPackages.ifPresent(
          s ->
              filesToStageBuilder.addAll(
                  Arrays.asList(s.replaceFirst("jar_packages=", "").split(","))));
    }
    options.as(PortablePipelineOptions.class).setFilesToStage(filesToStageBuilder.build());

    RunnerApi.Pipeline pipelineProto =
        PipelineTranslation.toProto(pipeline, SdkComponents.create(options));
    pipelineProto = DefaultArtifactResolver.INSTANCE.resolveArtifacts(pipelineProto);

    PrepareJobRequest prepareJobRequest =
        PrepareJobRequest.newBuilder()
            .setJobName(options.getJobName())
            .setPipeline(pipelineProto)
            .setPipelineOptions(PipelineOptionsTranslation.toProto(options))
            .build();

    LOG.info("Using job server endpoint: {}", endpoint);
    ManagedChannel jobServiceChannel =
        channelFactory.forDescriptor(ApiServiceDescriptor.newBuilder().setUrl(endpoint).build());

    JobServiceBlockingStub jobService = JobServiceGrpc.newBlockingStub(jobServiceChannel);
    try (CloseableResource<JobServiceBlockingStub> wrappedJobService =
        CloseableResource.of(jobService, unused -> jobServiceChannel.shutdown())) {

      final int jobServerTimeout = options.as(PortablePipelineOptions.class).getJobServerTimeout();
      PrepareJobResponse prepareJobResponse =
          jobService
              .withDeadlineAfter(jobServerTimeout, TimeUnit.SECONDS)
              .withWaitForReady()
              .prepare(prepareJobRequest);
      LOG.info("PrepareJobResponse: {}", prepareJobResponse);

      ApiServiceDescriptor artifactStagingEndpoint =
          prepareJobResponse.getArtifactStagingEndpoint();
      String stagingSessionToken = prepareJobResponse.getStagingSessionToken();

      try (CloseableResource<ManagedChannel> artifactChannel =
          CloseableResource.of(
              channelFactory.forDescriptor(artifactStagingEndpoint), ManagedChannel::shutdown)) {

        ArtifactStagingService.offer(
            new ArtifactRetrievalService(),
            ArtifactStagingServiceGrpc.newStub(artifactChannel.get()),
            stagingSessionToken);
      } catch (CloseableResource.CloseException e) {
        LOG.warn("Error closing artifact staging channel", e);
        // CloseExceptions should only be thrown while closing the channel.
      } catch (Exception e) {
        throw new RuntimeException("Error staging files.", e);
      }

      RunJobRequest runJobRequest =
          RunJobRequest.newBuilder()
              .setPreparationId(prepareJobResponse.getPreparationId())
              .build();

      // Run the job and wait for a result, we don't set a timeout here because
      // it may take a long time for a job to complete and streaming
      // jobs never return a response.
      RunJobResponse runJobResponse = jobService.run(runJobRequest);

      LOG.info("RunJobResponse: {}", runJobResponse);
      ByteString jobId = runJobResponse.getJobIdBytes();

      return new JobServicePipelineResult(
          jobId, jobServerTimeout, wrappedJobService.transfer(), cleanup);
    } catch (CloseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "PortableRunner#" + hashCode();
  }
}
