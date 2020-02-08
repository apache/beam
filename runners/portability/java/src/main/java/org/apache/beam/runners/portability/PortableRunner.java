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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Map;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ArtifactServiceStager;
import org.apache.beam.runners.core.construction.ArtifactServiceStager.StagedFile;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.portability.CloseableResource.CloseException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link PipelineRunner} a {@link Pipeline} against a {@code JobService}. */
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

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);

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

      PrepareJobResponse prepareJobResponse = jobService.prepare(prepareJobRequest);
      LOG.info("PrepareJobResponse: {}", prepareJobResponse);

      ApiServiceDescriptor artifactStagingEndpoint =
          prepareJobResponse.getArtifactStagingEndpoint();
      String stagingSessionToken = prepareJobResponse.getStagingSessionToken();

      ImmutableMap.Builder<String, String> retrievalTokenMapBuilder = ImmutableMap.builder();
      try (CloseableResource<ManagedChannel> artifactChannel =
          CloseableResource.of(
              channelFactory.forDescriptor(artifactStagingEndpoint), ManagedChannel::shutdown)) {
        ArtifactServiceStager stager = ArtifactServiceStager.overChannel(artifactChannel.get());

        for (Map.Entry<String, RunnerApi.Environment> entry :
            pipelineProto.getComponents().getEnvironmentsMap().entrySet()) {
          ImmutableList.Builder<StagedFile> filesToStageBuilder = ImmutableList.builder();
          for (RunnerApi.ArtifactInformation info : entry.getValue().getDependenciesList()) {
            if (!BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE)
                .equals(info.getTypeUrn())) {
              throw new RuntimeException(
                  String.format("unsupported artifact type %s", info.getTypeUrn()));
            }
            if (!BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO)
                .equals(info.getRoleUrn())) {
              throw new RuntimeException(
                  String.format("unsupported role type %s", info.getRoleUrn()));
            }
            RunnerApi.ArtifactFilePayload filePayload;
            try {
              filePayload = RunnerApi.ArtifactFilePayload.parseFrom(info.getTypePayload());
            } catch (InvalidProtocolBufferException e) {
              throw new RuntimeException("Error parsing artifact file payload.", e);
            }
            RunnerApi.ArtifactStagingToRolePayload stagingRolePayload;
            try {
              stagingRolePayload =
                  RunnerApi.ArtifactStagingToRolePayload.parseFrom(info.getRolePayload());
            } catch (InvalidProtocolBufferException e) {
              throw new RuntimeException("Error parsing artifact role payload.", e);
            }
            filesToStageBuilder.add(
                StagedFile.of(new File(filePayload.getPath()), stagingRolePayload.getStagedName()));
          }
          ImmutableList<StagedFile> filesToStage = filesToStageBuilder.build();
          LOG.debug("Actual files staged: {}", filesToStage);
          String retrievalToken = stager.stage(stagingSessionToken, filesToStage);
          retrievalTokenMapBuilder.put(entry.getKey(), retrievalToken);
        }
      } catch (CloseableResource.CloseException e) {
        LOG.warn("Error closing artifact staging channel", e);
        // CloseExceptions should only be thrown while closing the channel.
        checkState(!retrievalTokenMapBuilder.build().isEmpty());
      } catch (Exception e) {
        throw new RuntimeException("Error staging files.", e);
      }

      RunJobRequest runJobRequest =
          RunJobRequest.newBuilder()
              .setPreparationId(prepareJobResponse.getPreparationId())
              .putAllRetrievalTokens(retrievalTokenMapBuilder.build())
              .build();

      RunJobResponse runJobResponse = jobService.run(runJobRequest);

      LOG.info("RunJobResponse: {}", runJobResponse);
      ByteString jobId = runJobResponse.getJobIdBytes();

      return new JobServicePipelineResult(jobId, wrappedJobService.transfer(), cleanup);
    } catch (CloseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "PortableRunner#" + hashCode();
  }

  /** Create a filename-friendly artifact name for the given path. */
  // TODO: Are we missing any commonly allowed path characters that are disallowed in file names?
  private static String escapePath(String path) {
    StringBuilder result = new StringBuilder(2 * path.length());
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      switch (c) {
        case '_':
          result.append("__");
          break;
        case '/':
          result.append("_.");
          break;
        case '\\':
          result.append("._");
          break;
        case '.':
          result.append("..");
          break;
        default:
          result.append(c);
      }
    }
    return result.toString();
  }
}
