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
package org.apache.beam.runners.jobsubmission;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobInfo;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessage;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.sdk.fn.server.FnService;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.fn.stream.SynchronizedStreamObserver;
import org.apache.beam.sdk.function.ThrowingConsumer;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.graph.PipelineValidator;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.StatusException;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A InMemoryJobService that prepares and runs jobs on behalf of a client using a {@link
 * JobInvoker}.
 *
 * <p>Job management is handled in-memory rather than any persistent storage, running the risk of
 * leaking jobs if the InMemoryJobService crashes.
 *
 * <p>TODO: replace in-memory job management state with persistent solution.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class InMemoryJobService extends JobServiceGrpc.JobServiceImplBase implements FnService {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryJobService.class);

  /** The default maximum number of completed invocations to keep. */
  public static final int DEFAULT_MAX_INVOCATION_HISTORY = 10;

  /**
   * Creates an InMemoryJobService.
   *
   * @param stagingService The staging service.
   * @param stagingServiceTokenProvider Function mapping a preparationId to a staging service token.
   * @param cleanupJobFn A cleanup function to run, parameterized with the staging token of a job.
   * @param invoker A JobInvoker which creates the jobs.
   * @return A new InMemoryJobService.
   */
  public static InMemoryJobService create(
      GrpcFnServer<ArtifactStagingService> stagingService,
      Function<String, String> stagingServiceTokenProvider,
      ThrowingConsumer<Exception, String> cleanupJobFn,
      JobInvoker invoker) {
    return new InMemoryJobService(
        stagingService,
        stagingServiceTokenProvider,
        cleanupJobFn,
        invoker,
        DEFAULT_MAX_INVOCATION_HISTORY);
  }

  /**
   * Creates an InMemoryJobService.
   *
   * @param stagingService The staging service.
   * @param stagingServiceTokenProvider Function mapping a preparationId to a staging service token.
   * @param cleanupJobFn A cleanup function to run, parameterized with the staging token of a job.
   * @param invoker A JobInvoker which creates the jobs.
   * @param maxInvocationHistory The maximum number of completed invocations to keep.
   * @return A new InMemoryJobService.
   */
  public static InMemoryJobService create(
      GrpcFnServer<ArtifactStagingService> stagingService,
      Function<String, String> stagingServiceTokenProvider,
      ThrowingConsumer<Exception, String> cleanupJobFn,
      JobInvoker invoker,
      int maxInvocationHistory) {
    return new InMemoryJobService(
        stagingService, stagingServiceTokenProvider, cleanupJobFn, invoker, maxInvocationHistory);
  }

  /** Map of preparationId to preparation. */
  private final ConcurrentHashMap<String, JobPreparation> preparations;
  /** Map of preparationId to staging token. */
  private final ConcurrentHashMap<String, String> stagingSessionTokens;
  /** Map of invocationId to invocation. */
  private final ConcurrentHashMap<String, JobInvocation> invocations;
  /** InvocationIds of completed invocations in least-recently-completed order. */
  private final ConcurrentLinkedDeque<String> completedInvocationsIds;

  private final GrpcFnServer<ArtifactStagingService> stagingService;
  private final Endpoints.ApiServiceDescriptor stagingServiceDescriptor;
  private final Function<String, String> stagingServiceTokenProvider;
  private final ThrowingConsumer<Exception, String> cleanupJobFn;
  private final JobInvoker invoker;

  /** The maximum number of past invocations to keep. */
  private final int maxInvocationHistory;

  private InMemoryJobService(
      GrpcFnServer<ArtifactStagingService> stagingService,
      Function<String, String> stagingServiceTokenProvider,
      ThrowingConsumer<Exception, String> cleanupJobFn,
      JobInvoker invoker,
      int maxInvocationHistory) {
    this.stagingService = stagingService;
    this.stagingServiceDescriptor = stagingService.getApiServiceDescriptor();
    this.stagingServiceTokenProvider = stagingServiceTokenProvider;
    this.cleanupJobFn = cleanupJobFn;
    this.invoker = invoker;

    this.preparations = new ConcurrentHashMap<>();
    this.invocations = new ConcurrentHashMap<>();
    this.stagingSessionTokens = new ConcurrentHashMap<>();
    this.completedInvocationsIds = new ConcurrentLinkedDeque<>();
    Preconditions.checkArgument(maxInvocationHistory >= 0);
    this.maxInvocationHistory = maxInvocationHistory;
  }

  @Override
  public void prepare(
      PrepareJobRequest request, StreamObserver<PrepareJobResponse> responseObserver) {
    try {
      LOG.trace("{} {}", PrepareJobRequest.class.getSimpleName(), request);
      // insert preparation
      String preparationId =
          String.format("%s_%s", request.getJobName(), UUID.randomUUID().toString());
      Struct pipelineOptions = request.getPipelineOptions();
      if (pipelineOptions == null) {
        throw new NullPointerException("Encountered null pipeline options.");
      }
      LOG.trace("PIPELINE OPTIONS {} {}", pipelineOptions.getClass(), pipelineOptions);
      JobPreparation preparation =
          JobPreparation.builder()
              .setId(preparationId)
              .setPipeline(request.getPipeline())
              .setOptions(pipelineOptions)
              .build();
      JobPreparation previous = preparations.putIfAbsent(preparationId, preparation);
      if (previous != null) {
        // this should never happen with a UUID
        String errMessage =
            String.format("A job with the preparation ID \"%s\" already exists.", preparationId);
        StatusException exception = Status.NOT_FOUND.withDescription(errMessage).asException();
        responseObserver.onError(exception);
        return;
      }

      String stagingSessionToken = stagingServiceTokenProvider.apply(preparationId);
      stagingSessionTokens.putIfAbsent(preparationId, stagingSessionToken);

      stagingService
          .getService()
          .registerJob(stagingSessionToken, extractDependencies(request.getPipeline()));

      // send response
      PrepareJobResponse response =
          PrepareJobResponse.newBuilder()
              .setPreparationId(preparationId)
              .setArtifactStagingEndpoint(stagingServiceDescriptor)
              .setStagingSessionToken(stagingSessionToken)
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Could not prepare job with name {}", request.getJobName(), e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void run(RunJobRequest request, StreamObserver<RunJobResponse> responseObserver) {
    LOG.trace("{} {}", RunJobRequest.class.getSimpleName(), request);

    String preparationId = request.getPreparationId();
    try {
      // retrieve job preparation
      JobPreparation preparation = preparations.get(preparationId);
      if (preparation == null) {
        String errMessage = String.format("Unknown Preparation Id \"%s\".", preparationId);
        StatusException exception = Status.NOT_FOUND.withDescription(errMessage).asException();
        responseObserver.onError(exception);
        return;
      }
      try {
        PipelineValidator.validate(preparation.pipeline());
      } catch (Exception e) {
        LOG.warn("Encountered Unexpected Exception during validation", e);
        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withCause(e)));
        return;
      }

      // create new invocation
      JobInvocation invocation =
          invoker.invoke(
              resolveDependencies(preparation.pipeline(), stagingSessionTokens.get(preparationId)),
              preparation.options(),
              request.getRetrievalToken());
      String invocationId = invocation.getId();

      invocation.addStateListener(
          event -> {
            if (!JobInvocation.isTerminated(event.getState())) {
              return;
            }
            String stagingSessionToken = stagingSessionTokens.get(preparationId);
            stagingSessionTokens.remove(preparationId);
            try {
              if (cleanupJobFn != null) {
                cleanupJobFn.accept(stagingSessionToken);
              }
            } catch (Exception e) {
              LOG.warn(
                  "Failed to remove job staging directory for token {}.", stagingSessionToken, e);
            } finally {
              onFinishedInvocationCleanup(invocationId);
            }
          });

      invocation.start();
      invocations.put(invocationId, invocation);
      // Cleanup this preparation because we are running it now.
      // If we fail, we need to prepare again.
      preparations.remove(preparationId);
      RunJobResponse response = RunJobResponse.newBuilder().setJobId(invocationId).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      LOG.warn("Encountered Status Exception", e);
      responseObserver.onError(e);
    } catch (Exception e) {
      String errMessage =
          String.format("Encountered Unexpected Exception for Preparation %s", preparationId);
      LOG.error(errMessage, e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  private Map<String, List<RunnerApi.ArtifactInformation>> extractDependencies(
      RunnerApi.Pipeline pipeline) {
    Map<String, List<RunnerApi.ArtifactInformation>> dependencies = new HashMap<>();
    for (Map.Entry<String, RunnerApi.Environment> entry :
        pipeline.getComponents().getEnvironmentsMap().entrySet()) {
      List<RunnerApi.Environment> subEnvs = Environments.expandAnyOfEnvironments(entry.getValue());
      for (int i = 0; i < subEnvs.size(); i++) {
        dependencies.put(i + ":" + entry.getKey(), subEnvs.get(i).getDependenciesList());
      }
    }
    return dependencies;
  }

  private RunnerApi.Pipeline resolveDependencies(RunnerApi.Pipeline pipeline, String stagingToken) {
    Map<String, List<RunnerApi.ArtifactInformation>> resolvedDependencies =
        stagingService.getService().getStagedArtifacts(stagingToken);
    Map<String, RunnerApi.Environment> newEnvironments = new HashMap<>();
    for (Map.Entry<String, RunnerApi.Environment> entry :
        pipeline.getComponents().getEnvironmentsMap().entrySet()) {
      List<RunnerApi.Environment> subEnvs = Environments.expandAnyOfEnvironments(entry.getValue());
      List<RunnerApi.Environment> newSubEnvs = new ArrayList<>();
      for (int i = 0; i < subEnvs.size(); i++) {
        RunnerApi.Environment subEnv = subEnvs.get(i);
        if (subEnv.getDependenciesCount() > 0 && resolvedDependencies == null) {
          throw new RuntimeException(
              "Artifact dependencies provided but not staged for " + entry.getKey());
        }
        newSubEnvs.add(
            subEnv.getDependenciesCount() == 0
                ? subEnv
                : subEnv
                    .toBuilder()
                    .clearDependencies()
                    .addAllDependencies(resolvedDependencies.get(i + ":" + entry.getKey()))
                    .build());
      }
      if (newSubEnvs.size() == 1) {
        newEnvironments.put(entry.getKey(), newSubEnvs.get(0));
      } else {
        newEnvironments.put(
            entry.getKey(),
            Environments.createAnyOfEnvironment(
                newSubEnvs.toArray(new RunnerApi.Environment[newSubEnvs.size()])));
      }
    }
    RunnerApi.Pipeline.Builder builder = pipeline.toBuilder();
    builder.getComponentsBuilder().clearEnvironments().putAllEnvironments(newEnvironments);
    return builder.build();
  }

  @Override
  public void getJobs(GetJobsRequest request, StreamObserver<GetJobsResponse> responseObserver) {
    LOG.trace("{} {}", GetJobsRequest.class.getSimpleName(), request);

    try {
      List<JobInfo> result = new ArrayList<>();
      for (JobInvocation invocation : invocations.values()) {
        result.add(invocation.toProto());
      }
      GetJobsResponse response = GetJobsResponse.newBuilder().addAllJobInfo(result).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Encountered Unexpected Exception", e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void getState(GetJobStateRequest request, StreamObserver<JobStateEvent> responseObserver) {
    LOG.trace("{} {}", GetJobStateRequest.class.getSimpleName(), request);
    String invocationId = request.getJobId();
    try {
      JobInvocation invocation = getInvocation(invocationId);
      JobStateEvent response = invocation.getStateEvent();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException | StatusException e) {
      responseObserver.onError(e);
    } catch (Exception e) {
      String errMessage =
          String.format("Encountered Unexpected Exception for Invocation %s", invocationId);
      LOG.error(errMessage, e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void getPipeline(
      GetJobPipelineRequest request, StreamObserver<GetJobPipelineResponse> responseObserver) {
    LOG.trace("{} {}", GetJobPipelineRequest.class.getSimpleName(), request);
    String invocationId = request.getJobId();
    try {
      JobInvocation invocation = getInvocation(invocationId);
      RunnerApi.Pipeline pipeline = invocation.getPipeline();
      GetJobPipelineResponse response =
          GetJobPipelineResponse.newBuilder().setPipeline(pipeline).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException | StatusException e) {
      responseObserver.onError(e);
    } catch (Exception e) {
      String errMessage =
          String.format("Encountered Unexpected Exception for Invocation %s", invocationId);
      LOG.error(errMessage, e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void cancel(CancelJobRequest request, StreamObserver<CancelJobResponse> responseObserver) {
    LOG.trace("{} {}", CancelJobRequest.class.getSimpleName(), request);
    String invocationId = request.getJobId();
    try {
      JobInvocation invocation = getInvocation(invocationId);
      invocation.cancel();
      JobState.Enum state = invocation.getState();
      CancelJobResponse response = CancelJobResponse.newBuilder().setState(state).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException | StatusException e) {
      responseObserver.onError(e);
    } catch (Exception e) {
      String errMessage =
          String.format("Encountered Unexpected Exception for Invocation %s", invocationId);
      LOG.error(errMessage, e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void getStateStream(
      GetJobStateRequest request, StreamObserver<JobStateEvent> responseObserver) {
    LOG.trace("{} {}", GetJobStateRequest.class.getSimpleName(), request);
    String invocationId = request.getJobId();
    try {
      JobInvocation invocation = getInvocation(invocationId);

      Consumer<JobStateEvent> stateListener =
          event -> {
            responseObserver.onNext(event);
            if (JobInvocation.isTerminated(event.getState())) {
              responseObserver.onCompleted();
            }
          };
      invocation.addStateListener(stateListener);
    } catch (StatusRuntimeException | StatusException e) {
      responseObserver.onError(e);
    } catch (Exception e) {
      String errMessage =
          String.format("Encountered Unexpected Exception for Invocation %s", invocationId);
      LOG.error(errMessage, e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void getMessageStream(
      JobMessagesRequest request, StreamObserver<JobMessagesResponse> responseObserver) {
    String invocationId = request.getJobId();
    try {
      JobInvocation invocation = getInvocation(invocationId);
      // synchronization is necessary since we use this stream observer in both the state listener
      // and message listener.
      StreamObserver<JobMessagesResponse> syncResponseObserver =
          SynchronizedStreamObserver.wrapping(responseObserver);
      Consumer<JobStateEvent> stateListener =
          event -> {
            syncResponseObserver.onNext(
                JobMessagesResponse.newBuilder().setStateResponse(event).build());
            // The terminal state is always updated after the last message, that's
            // why we can end the stream here.
            if (JobInvocation.isTerminated(invocation.getStateEvent().getState())) {
              responseObserver.onCompleted();
            }
          };
      Consumer<JobMessage> messageListener =
          message ->
              syncResponseObserver.onNext(
                  JobMessagesResponse.newBuilder().setMessageResponse(message).build());

      invocation.addMessageListener(messageListener);
      // The order matters here. Make sure to send all the message first because the stream
      // will be ended by the terminal state request.
      invocation.addStateListener(stateListener);

    } catch (StatusRuntimeException | StatusException e) {
      responseObserver.onError(e);
    } catch (Exception e) {
      String errMessage =
          String.format("Encountered Unexpected Exception for Invocation %s", invocationId);
      LOG.error(errMessage, e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void getJobMetrics(
      JobApi.GetJobMetricsRequest request,
      StreamObserver<JobApi.GetJobMetricsResponse> responseObserver) {

    String invocationId = request.getJobId();
    LOG.info("Getting job metrics for {}", invocationId);

    try {
      JobInvocation invocation = getInvocation(invocationId);
      JobApi.MetricResults metrics = invocation.getMetrics();
      JobApi.GetJobMetricsResponse response =
          JobApi.GetJobMetricsResponse.newBuilder().setMetrics(metrics).build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException | StatusException e) {
      responseObserver.onError(e);
    } catch (Exception e) {
      LOG.error(String.format("Encountered exception for job invocation %s", invocationId), e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
    LOG.info("Finished getting job metrics for {}", invocationId);
  }

  @Override
  public void describePipelineOptions(
      DescribePipelineOptionsRequest request,
      StreamObserver<DescribePipelineOptionsResponse> responseObserver) {
    LOG.trace("{} {}", DescribePipelineOptionsRequest.class.getSimpleName(), request);
    try {
      DescribePipelineOptionsResponse response =
          DescribePipelineOptionsResponse.newBuilder()
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
  public void close() throws Exception {
    // TODO: throw error if jobs are running
  }

  private JobInvocation getInvocation(String invocationId) throws StatusException {
    JobInvocation invocation = invocations.get(invocationId);
    if (invocation == null) {
      throw Status.NOT_FOUND.asException();
    }
    return invocation;
  }

  private void onFinishedInvocationCleanup(String invocationId) {
    completedInvocationsIds.addLast(invocationId);
    while (completedInvocationsIds.size() > maxInvocationHistory) {
      // Clean up invocations
      // "preparations" is cleaned up when adding to "invocations"
      // "stagingTokens" is cleaned up when the invocation finishes
      invocations.remove(completedInvocationsIds.removeFirst());
    }
  }
}
