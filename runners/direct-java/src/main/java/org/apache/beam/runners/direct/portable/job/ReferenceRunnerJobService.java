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
package org.apache.beam.runners.direct.portable.job;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceImplBase;
import org.apache.beam.runners.direct.portable.ReferenceRunner;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.BeamFileSystemArtifactStagingService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This JobService implements the grpc calls for running jobs by using the {@code ReferenceRunner}
 * as an engine.
 */
public class ReferenceRunnerJobService extends JobServiceImplBase implements FnService {

  /** A configuration object for constructing the {@code ReferenceRunnerJobService}. */
  public static class Configuration {
    public String artifactStagingPath;
    public boolean keepArtifacts;
  }

  private static final Logger LOG = LoggerFactory.getLogger(ReferenceRunnerJobService.class);
  private static final int WAIT_MS = 1000;

  public static ReferenceRunnerJobService create(
      final ServerFactory serverFactory, Configuration configuration) {
    LOG.info("Starting {}", ReferenceRunnerJobService.class);
    return new ReferenceRunnerJobService(serverFactory, configuration);
  }

  private final ServerFactory serverFactory;
  private final Configuration configuration;

  private final ConcurrentMap<String, PreparingJob> unpreparedJobs;
  private final ConcurrentMap<String, ReferenceRunner> runningJobs;
  private final ConcurrentMap<String, JobState.Enum> jobStates;
  private final ExecutorService executor;
  private final ConcurrentLinkedQueue<GrpcFnServer<BeamFileSystemArtifactStagingService>>
      artifactStagingServices;

  private ReferenceRunnerJobService(ServerFactory serverFactory, Configuration configuration) {
    this.serverFactory = serverFactory;
    this.configuration = configuration;
    unpreparedJobs = new ConcurrentHashMap<>();
    runningJobs = new ConcurrentHashMap<>();
    jobStates = new ConcurrentHashMap<>();
    executor =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("reference-runner-pipeline-%s")
                .build());
    artifactStagingServices = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void prepare(
      JobApi.PrepareJobRequest request,
      StreamObserver<JobApi.PrepareJobResponse> responseObserver) {
    try {
      LOG.trace("{} {}", PrepareJobResponse.class.getSimpleName(), request);

      String preparationId = request.getJobName() + ThreadLocalRandom.current().nextInt();
      GrpcFnServer<BeamFileSystemArtifactStagingService> artifactStagingService =
          createArtifactStagingService();
      artifactStagingServices.add(artifactStagingService);
      String stagingSessionToken =
          BeamFileSystemArtifactStagingService.generateStagingSessionToken(
              preparationId, configuration.artifactStagingPath);
      PreparingJob existingJob =
          unpreparedJobs.putIfAbsent(
              preparationId,
              PreparingJob.builder()
                  .setArtifactStagingServer(artifactStagingService)
                  .setPipeline(request.getPipeline())
                  .setOptions(request.getPipelineOptions())
                  .setStagingSessionToken(stagingSessionToken)
                  .build());
      checkArgument(
          existingJob == null, "Unexpected existing job with preparation ID %s", preparationId);

      responseObserver.onNext(
          PrepareJobResponse.newBuilder()
              .setPreparationId(preparationId)
              .setArtifactStagingEndpoint(artifactStagingService.getApiServiceDescriptor())
              .setStagingSessionToken(stagingSessionToken)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Could not prepare job with name {}", request.getJobName(), e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  private GrpcFnServer<BeamFileSystemArtifactStagingService> createArtifactStagingService()
      throws Exception {
    BeamFileSystemArtifactStagingService service = new BeamFileSystemArtifactStagingService();
    return GrpcFnServer.allocatePortAndCreateFor(service, serverFactory);
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored") // Run API does not block on execution
  public void run(
      JobApi.RunJobRequest request, StreamObserver<JobApi.RunJobResponse> responseObserver) {
    try {
      LOG.trace("{} {}", RunJobRequest.class.getSimpleName(), request);
      String preparationId = request.getPreparationId();
      PreparingJob preparingJob = unpreparedJobs.get(preparationId);
      if (preparingJob == null) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(String.format("Unknown Preparation Id %s", preparationId))
                .asException());
        return;
      }
      try {
        // Close any preparation-time only resources.
        preparingJob.close();
      } catch (Exception e) {
        responseObserver.onError(e);
      }

      ReferenceRunner runner =
          ReferenceRunner.forPipeline(
              preparingJob.getPipeline(), preparingJob.getOptions(), request.getRetrievalToken());
      String jobId = "job-" + Integer.toString(ThreadLocalRandom.current().nextInt());
      responseObserver.onNext(RunJobResponse.newBuilder().setJobId(jobId).build());
      responseObserver.onCompleted();
      runningJobs.put(jobId, runner);
      jobStates.putIfAbsent(jobId, Enum.RUNNING);
      executor.submit(
          () -> {
            try {
              jobStates.computeIfPresent(jobId, (id, status) -> Enum.RUNNING);
              runner.execute();
              jobStates.computeIfPresent(jobId, (id, status) -> Enum.DONE);
            } catch (Exception e) {
              jobStates.computeIfPresent(jobId, (id, status) -> Enum.FAILED);
              throw e;
            }

            // Delete artifacts after job is done.
            if (!configuration.keepArtifacts) {
              String stagingSessionToken = preparingJob.getStagingSessionToken();
              try {
                preparingJob
                    .getArtifactStagingServer()
                    .getService()
                    .removeArtifacts(stagingSessionToken);
              } catch (Exception e) {
                LOG.error(
                    "Failed to remove job staging directory for token {}: {}",
                    stagingSessionToken,
                    e);
              }
            }
            return null;
          });
    } catch (StatusRuntimeException e) {
      responseObserver.onError(e);
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void getState(
      GetJobStateRequest request, StreamObserver<GetJobStateResponse> responseObserver) {
    LOG.trace("{} {}", GetJobStateRequest.class.getSimpleName(), request);
    try {
      responseObserver.onNext(
          GetJobStateResponse.newBuilder()
              .setState(jobStates.getOrDefault(request.getJobId(), Enum.UNRECOGNIZED))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      String errMessage =
          String.format("Encountered Unexpected Exception for Invocation %s", request.getJobId());
      LOG.error(errMessage, e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void getStateStream(
      GetJobStateRequest request, StreamObserver<GetJobStateResponse> responseObserver) {
    LOG.trace("{} {}", GetJobStateRequest.class.getSimpleName(), request);
    String invocationId = request.getJobId();
    try {
      Thread.sleep(WAIT_MS);
      Enum state = jobStates.getOrDefault(request.getJobId(), Enum.UNRECOGNIZED);
      responseObserver.onNext(GetJobStateResponse.newBuilder().setState(state).build());
      while (Enum.RUNNING.equals(state)) {
        Thread.sleep(WAIT_MS);
        state = jobStates.getOrDefault(request.getJobId(), Enum.UNRECOGNIZED);
      }
      responseObserver.onNext(GetJobStateResponse.newBuilder().setState(state).build());
    } catch (Exception e) {
      String errMessage =
          String.format("Encountered Unexpected Exception for Invocation %s", invocationId);
      LOG.error(errMessage, e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
    responseObserver.onCompleted();
  }

  @Override
  public void describePipelineOptions(
      JobApi.DescribePipelineOptionsRequest request,
      StreamObserver<JobApi.DescribePipelineOptionsResponse> responseObserver) {
    LOG.trace("{} {}", JobApi.DescribePipelineOptionsRequest.class.getSimpleName(), request);
    try {
      JobApi.DescribePipelineOptionsResponse response =
          JobApi.DescribePipelineOptionsResponse.newBuilder()
              .addAllOptions(
                  PipelineOptionsFactory.describe(PipelineOptionsFactory.getRegisteredOptions()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error describing pipeline options", e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void getMessageStream(
      JobMessagesRequest request, StreamObserver<JobMessagesResponse> responseObserver) {
    // Not implemented
    LOG.trace("{} {}", JobMessagesRequest.class.getSimpleName(), request);
  }

  @Override
  public void cancel(CancelJobRequest request, StreamObserver<CancelJobResponse> responseObserver) {
    LOG.trace("{} {}", CancelJobRequest.class.getSimpleName(), request);
    responseObserver.onError(
        Status.NOT_FOUND
            .withDescription(String.format("Unknown Job ID %s", request.getJobId()))
            .asException());
  }

  @Override
  public void close() throws Exception {
    for (PreparingJob preparingJob : ImmutableList.copyOf(unpreparedJobs.values())) {
      try {
        preparingJob.close();
      } catch (Exception e) {
        LOG.warn("Exception while closing preparing job {}", preparingJob);
      }
    }
    while (!artifactStagingServices.isEmpty()) {
      GrpcFnServer<BeamFileSystemArtifactStagingService> artifactStagingService =
          artifactStagingServices.remove();
      try {
        artifactStagingService.close();
      } catch (Exception e) {
        LOG.error(
            "Unable to close staging sevice started on %s",
            artifactStagingService.getApiServiceDescriptor().getUrl(), e);
      }
    }
  }
}
