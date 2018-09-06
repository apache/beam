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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
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
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceImplBase;
import org.apache.beam.runners.direct.portable.ReferenceRunner;
import org.apache.beam.runners.direct.portable.artifact.LocalFileSystemArtifactStagerService;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.vendor.grpc.v1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The ReferenceRunner uses the portability framework to execute a Pipeline on a single machine. */
public class ReferenceRunnerJobService extends JobServiceImplBase implements FnService {
  private static final Logger LOG = LoggerFactory.getLogger(ReferenceRunnerJobService.class);

  public static ReferenceRunnerJobService create(final ServerFactory serverFactory) {
    LOG.info("Starting {}", ReferenceRunnerJobService.class);
    return new ReferenceRunnerJobService(
        serverFactory, () -> Files.createTempDirectory("reference-runner-staging"));
  }

  private final ServerFactory serverFactory;
  private final Callable<Path> stagingPathCallable;

  private final ConcurrentMap<String, PreparingJob> unpreparedJobs;
  private final ConcurrentMap<String, ReferenceRunner> runningJobs;
  private final ConcurrentMap<String, JobState.Enum> jobStates;
  private final ExecutorService executor;
  private final ConcurrentLinkedQueue<GrpcFnServer<LocalFileSystemArtifactStagerService>>
      artifactStagingServices;

  private ReferenceRunnerJobService(
      ServerFactory serverFactory, Callable<Path> stagingPathCallable) {
    this.serverFactory = serverFactory;
    this.stagingPathCallable = stagingPathCallable;
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

  public ReferenceRunnerJobService withStagingPathSupplier(Callable<Path> supplier) {
    return new ReferenceRunnerJobService(serverFactory, supplier);
  }

  @Override
  public void prepare(
      JobApi.PrepareJobRequest request,
      StreamObserver<JobApi.PrepareJobResponse> responseObserver) {
    try {
      LOG.trace("{} {}", PrepareJobResponse.class.getSimpleName(), request);

      String preparationId = request.getJobName() + ThreadLocalRandom.current().nextInt();
      Path tempDir = stagingPathCallable.call();
      GrpcFnServer<LocalFileSystemArtifactStagerService> artifactStagingService =
          createArtifactStagingService(tempDir);
      artifactStagingServices.add(artifactStagingService);
      PreparingJob previous =
          unpreparedJobs.putIfAbsent(
              preparationId,
              PreparingJob.builder()
                  .setArtifactStagingServer(artifactStagingService)
                  .setPipeline(request.getPipeline())
                  .setOptions(request.getPipelineOptions())
                  .setStagingLocation(tempDir)
                  .build());
      checkArgument(
          previous == null, "Unexpected existing job with preparation ID %s", preparationId);

      responseObserver.onNext(
          PrepareJobResponse.newBuilder()
              .setPreparationId(preparationId)
              .setArtifactStagingEndpoint(artifactStagingService.getApiServiceDescriptor())
              // ReferenceRunner uses LocalFileSystemArtifactStagerService which only need local
              // artifact directory.
              .setStagingSessionToken(tempDir.toFile().getAbsolutePath())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Could not prepare job with name {}", request.getJobName(), e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  private GrpcFnServer<LocalFileSystemArtifactStagerService> createArtifactStagingService(
      Path stagingPath) throws Exception {
    LocalFileSystemArtifactStagerService service =
        LocalFileSystemArtifactStagerService.forRootDirectory(stagingPath.toFile());
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
              preparingJob.getPipeline(),
              preparingJob.getOptions(),
              preparingJob.getStagingLocation().toFile());
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
              return null;
            } catch (Exception e) {
              jobStates.computeIfPresent(jobId, (id, status) -> Enum.FAILED);
              throw e;
            }
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
    responseObserver.onNext(
        GetJobStateResponse.newBuilder()
            .setState(jobStates.getOrDefault(request.getJobId(), Enum.UNRECOGNIZED))
            .build());
    responseObserver.onCompleted();
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
      GrpcFnServer<LocalFileSystemArtifactStagerService> artifactStagingService =
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
