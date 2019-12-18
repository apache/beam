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

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.core.construction.ArtifactServiceStager;
import org.apache.beam.runners.core.construction.ArtifactServiceStager.StagedFile;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.JavaReadViaImpulse;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.portability.CloseableResource.CloseException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.ZipFiles;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link PipelineRunner} a {@link Pipeline} against a {@code JobService}. */
public class PortableRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(PortableRunner.class);

  /** Provided pipeline options. */
  private final PipelineOptions options;
  /** Job API endpoint. */
  private final String endpoint;
  /** Files to stage to artifact staging service. They will ultimately be added to the classpath. */
  private final Collection<StagedFile> filesToStage;
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

    // Deduplicate artifacts.
    Set<String> pathsToStage = Sets.newHashSet();
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    if (experiments != null) {
      Optional<String> jarPackages =
          experiments.stream()
              .filter((String flag) -> flag.startsWith("jar_packages="))
              .findFirst();
      jarPackages.ifPresent(
          s -> pathsToStage.addAll(Arrays.asList(s.replaceFirst("jar_packages=", "").split(","))));
    }
    if (portableOptions.getFilesToStage() == null) {
      pathsToStage.addAll(
          detectClassPathResourcesToStage(PortableRunner.class.getClassLoader(), options));
      if (pathsToStage.isEmpty()) {
        throw new IllegalArgumentException("No classpath elements found.");
      }
      LOG.debug(
          "PortablePipelineOptions.filesToStage was not specified. "
              + "Defaulting to files from the classpath: {}",
          pathsToStage.size());
    } else {
      pathsToStage.addAll(portableOptions.getFilesToStage());
    }

    ImmutableList.Builder<StagedFile> filesToStage = ImmutableList.builder();
    for (String path : pathsToStage) {
      File file = new File(path);
      if (new File(path).exists()) {
        // Spurious items get added to the classpath. Filter by just those that exist.
        if (file.isDirectory()) {
          // Zip up directories so we can upload them to the artifact service.
          try {
            filesToStage.add(createStagingFile(zipDirectory(file)));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } else {
          filesToStage.add(createStagingFile(file));
        }
      }
    }

    return new PortableRunner(options, endpoint, filesToStage.build(), channelFactory);
  }

  private PortableRunner(
      PipelineOptions options,
      String endpoint,
      Collection<StagedFile> filesToStage,
      ManagedChannelFactory channelFactory) {
    this.options = options;
    this.endpoint = endpoint;
    this.filesToStage = filesToStage;
    this.channelFactory = channelFactory;
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    pipeline.replaceAll(ImmutableList.of(JavaReadViaImpulse.boundedOverride()));

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

    LOG.debug("Initial files to stage: " + filesToStage);

    PrepareJobRequest prepareJobRequest =
        PrepareJobRequest.newBuilder()
            .setJobName(options.getJobName())
            .setPipeline(PipelineTranslation.toProto(pipeline))
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

      String retrievalToken = null;
      try (CloseableResource<ManagedChannel> artifactChannel =
          CloseableResource.of(
              channelFactory.forDescriptor(artifactStagingEndpoint), ManagedChannel::shutdown)) {
        ArtifactServiceStager stager = ArtifactServiceStager.overChannel(artifactChannel.get());
        LOG.debug("Actual files staged: {}", filesToStage);
        retrievalToken = stager.stage(stagingSessionToken, filesToStage);
      } catch (CloseableResource.CloseException e) {
        LOG.warn("Error closing artifact staging channel", e);
        // CloseExceptions should only be thrown while closing the channel.
        checkState(retrievalToken != null);
      } catch (Exception e) {
        throw new RuntimeException("Error staging files.", e);
      }

      RunJobRequest runJobRequest =
          RunJobRequest.newBuilder()
              .setPreparationId(prepareJobResponse.getPreparationId())
              .setRetrievalToken(retrievalToken)
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

  private static File zipDirectory(File directory) throws IOException {
    File zipFile = File.createTempFile(directory.getName(), ".zip");
    try (FileOutputStream fos = new FileOutputStream(zipFile)) {
      ZipFiles.zipDirectory(directory, fos);
    }
    return zipFile;
  }

  private static StagedFile createStagingFile(File file) {
    // TODO: https://issues.apache.org/jira/browse/BEAM-4109 Support arbitrary names in the staging
    // service itself.
    // HACK: Encode the path name ourselves because the local artifact staging service currently
    // assumes artifact names correspond to a flat directory. Artifact staging services should
    // generally accept arbitrary artifact names.
    // NOTE: Base64 url encoding does not work here because the stage artifact names tend to be long
    // and exceed file length limits on the artifact stager.
    return StagedFile.of(file, UUID.randomUUID().toString());
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
