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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Throwables.getRootCause;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Throwables.getStackTraceAsString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessage;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.FutureCallback;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.flink.api.common.JobExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Invocation of a Flink Job via {@link FlinkRunner}. */
public class FlinkJobInvocation implements JobInvocation {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobInvocation.class);

  public static FlinkJobInvocation create(
      String id,
      String retrievalToken,
      ListeningExecutorService executorService,
      Pipeline pipeline,
      FlinkPipelineOptions pipelineOptions,
      @Nullable String confDir,
      List<String> filesToStage) {
    return new FlinkJobInvocation(
        id, retrievalToken, executorService, pipeline, pipelineOptions, confDir, filesToStage);
  }

  private final String id;
  private final String retrievalToken;
  private final ListeningExecutorService executorService;
  private final RunnerApi.Pipeline pipeline;
  private final FlinkPipelineOptions pipelineOptions;
  private final String confDir;
  private final List<String> filesToStage;
  private JobState.Enum jobState;
  private List<Consumer<JobState.Enum>> stateObservers;
  private List<Consumer<JobMessage>> messageObservers;

  @Nullable private ListenableFuture<PipelineResult> invocationFuture;

  private FlinkJobInvocation(
      String id,
      String retrievalToken,
      ListeningExecutorService executorService,
      Pipeline pipeline,
      FlinkPipelineOptions pipelineOptions,
      @Nullable String confDir,
      List<String> filesToStage) {
    this.id = id;
    this.retrievalToken = retrievalToken;
    this.executorService = executorService;
    this.pipeline = pipeline;
    this.pipelineOptions = pipelineOptions;
    this.confDir = confDir;
    this.filesToStage = filesToStage;
    this.invocationFuture = null;
    this.jobState = JobState.Enum.STOPPED;
    this.stateObservers = new ArrayList<>();
    this.messageObservers = new ArrayList<>();
  }

  private PipelineResult runPipeline() throws Exception {
    MetricsEnvironment.setMetricsSupported(false);

    FlinkPortablePipelineTranslator<?> translator;
    if (!pipelineOptions.isStreaming() && !hasUnboundedPCollections(pipeline)) {
      // TODO: Do we need to inspect for unbounded sources before fusing?
      translator = FlinkBatchPortablePipelineTranslator.createTranslator();
    } else {
      translator = new FlinkStreamingPortablePipelineTranslator();
    }
    return runPipelineWithTranslator(translator);
  }

  private <T extends FlinkPortablePipelineTranslator.TranslationContext>
      PipelineResult runPipelineWithTranslator(FlinkPortablePipelineTranslator<T> translator)
          throws Exception {
    LOG.info("Translating pipeline to Flink program.");

    // Don't let the fuser fuse any subcomponents of native transforms.
    // TODO(BEAM-6327): Remove the need for this.
    RunnerApi.Pipeline trimmedPipeline =
        makeKnownUrnsPrimitives(
            pipeline,
            Sets.difference(
                translator.knownUrns(),
                ImmutableSet.of(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)));

    // Fused pipeline proto.
    // TODO: Consider supporting partially-fused graphs.
    RunnerApi.Pipeline fusedPipeline =
        trimmedPipeline.getComponents().getTransformsMap().values().stream()
                .anyMatch(proto -> ExecutableStage.URN.equals(proto.getSpec().getUrn()))
            ? trimmedPipeline
            : GreedyPipelineFuser.fuse(trimmedPipeline).toPipeline();
    JobInfo jobInfo =
        JobInfo.create(
            id,
            pipelineOptions.getJobName(),
            retrievalToken,
            PipelineOptionsTranslation.toProto(pipelineOptions));

    FlinkPortablePipelineTranslator.Executor executor =
        translator.translate(
            translator.createTranslationContext(jobInfo, pipelineOptions, confDir, filesToStage),
            fusedPipeline);
    final JobExecutionResult result = executor.execute(pipelineOptions.getJobName());

    return FlinkRunner.createPipelineResult(result, pipelineOptions);
  }

  private RunnerApi.Pipeline makeKnownUrnsPrimitives(
      RunnerApi.Pipeline pipeline, Set<String> knownUrns) {
    RunnerApi.Pipeline.Builder trimmedPipeline = pipeline.toBuilder();
    for (String ptransformId : pipeline.getComponents().getTransformsMap().keySet()) {
      if (knownUrns.contains(
          pipeline.getComponents().getTransformsOrThrow(ptransformId).getSpec().getUrn())) {
        LOG.debug("Removing descendants of known PTransform {}" + ptransformId);
        removeDescendants(trimmedPipeline, ptransformId);
      }
    }
    return trimmedPipeline.build();
  }

  private void removeDescendants(RunnerApi.Pipeline.Builder pipeline, String parentId) {
    RunnerApi.PTransform parentProto =
        pipeline.getComponents().getTransformsOrDefault(parentId, null);
    if (parentProto != null) {
      for (String childId : parentProto.getSubtransformsList()) {
        removeDescendants(pipeline, childId);
        pipeline.getComponentsBuilder().removeTransforms(childId);
      }
      pipeline
          .getComponentsBuilder()
          .putTransforms(parentId, parentProto.toBuilder().clearSubtransforms().build());
    }
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting job invocation {}", getId());
    if (getState() != JobState.Enum.STOPPED) {
      throw new IllegalStateException(String.format("Job %s already running.", getId()));
    }
    setState(JobState.Enum.STARTING);
    invocationFuture = executorService.submit(this::runPipeline);
    // TODO: Defer transitioning until the pipeline is up and running.
    setState(JobState.Enum.RUNNING);
    Futures.addCallback(
        invocationFuture,
        new FutureCallback<PipelineResult>() {
          @Override
          public void onSuccess(@Nullable PipelineResult pipelineResult) {
            if (pipelineResult != null) {
              checkArgument(
                  pipelineResult.getState() == PipelineResult.State.DONE,
                  "Success on non-Done state: " + pipelineResult.getState());
              setState(JobState.Enum.DONE);
            } else {
              setState(JobState.Enum.UNSPECIFIED);
            }
          }

          @Override
          public void onFailure(Throwable throwable) {
            String message = String.format("Error during job invocation %s.", getId());
            LOG.error(message, throwable);
            sendMessage(
                JobMessage.newBuilder()
                    .setMessageText(getStackTraceAsString(throwable))
                    .setImportance(JobMessage.MessageImportance.JOB_MESSAGE_DEBUG)
                    .build());
            sendMessage(
                JobMessage.newBuilder()
                    .setMessageText(getRootCause(throwable).toString())
                    .setImportance(JobMessage.MessageImportance.JOB_MESSAGE_ERROR)
                    .build());
            setState(JobState.Enum.FAILED);
          }
        },
        executorService);
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public synchronized void cancel() {
    LOG.info("Canceling job invocation {}", getId());
    if (this.invocationFuture != null) {
      this.invocationFuture.cancel(true /* mayInterruptIfRunning */);
      Futures.addCallback(
          invocationFuture,
          new FutureCallback<PipelineResult>() {
            @Override
            public void onSuccess(@Nullable PipelineResult pipelineResult) {
              if (pipelineResult != null) {
                try {
                  pipelineResult.cancel();
                } catch (IOException exn) {
                  throw new RuntimeException(exn);
                }
              }
            }

            @Override
            public void onFailure(Throwable throwable) {}
          },
          executorService);
    }
  }

  @Override
  public JobState.Enum getState() {
    return this.jobState;
  }

  @Override
  public synchronized void addStateListener(Consumer<JobState.Enum> stateStreamObserver) {
    stateStreamObserver.accept(getState());
    stateObservers.add(stateStreamObserver);
  }

  @Override
  public synchronized void addMessageListener(Consumer<JobMessage> messageStreamObserver) {
    messageObservers.add(messageStreamObserver);
  }

  private synchronized void setState(JobState.Enum state) {
    this.jobState = state;
    for (Consumer<JobState.Enum> observer : stateObservers) {
      observer.accept(state);
    }
  }

  private synchronized void sendMessage(JobMessage message) {
    for (Consumer<JobMessage> observer : messageObservers) {
      observer.accept(message);
    }
  }

  /** Indicates whether the given pipeline has any unbounded PCollections. */
  private static boolean hasUnboundedPCollections(RunnerApi.Pipeline pipeline) {
    checkNotNull(pipeline);
    Collection<RunnerApi.PCollection> pCollecctions =
        pipeline.getComponents().getPcollectionsMap().values();
    // Assume that all PCollections are consumed at some point in the pipeline.
    return pCollecctions.stream()
        .anyMatch(pc -> pc.getIsBounded() == RunnerApi.IsBounded.Enum.UNBOUNDED);
  }
}
