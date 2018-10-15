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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessage;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
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
      List<String> filesToStage) {
    return new FlinkJobInvocation(
        id, retrievalToken, executorService, pipeline, pipelineOptions, filesToStage);
  }

  private final String id;
  private final String retrievalToken;
  private final ListeningExecutorService executorService;
  private final RunnerApi.Pipeline pipeline;
  private final FlinkPipelineOptions pipelineOptions;
  private final List<String> filesToStage;
  private JobState.Enum jobState;
  private List<Consumer<JobState.Enum>> stateObservers;

  @Nullable private ListenableFuture<PipelineResult> invocationFuture;

  private FlinkJobInvocation(
      String id,
      String retrievalToken,
      ListeningExecutorService executorService,
      Pipeline pipeline,
      FlinkPipelineOptions pipelineOptions,
      List<String> filesToStage) {
    this.id = id;
    this.retrievalToken = retrievalToken;
    this.executorService = executorService;
    this.pipeline = pipeline;
    this.pipelineOptions = pipelineOptions;
    this.filesToStage = filesToStage;
    this.invocationFuture = null;
    this.jobState = JobState.Enum.STOPPED;
    this.stateObservers = new ArrayList<>();
  }

  private PipelineResult runPipeline() throws Exception {
    MetricsEnvironment.setMetricsSupported(false);

    LOG.info("Translating pipeline to Flink program.");
    // Fused pipeline proto.
    RunnerApi.Pipeline fusedPipeline = GreedyPipelineFuser.fuse(pipeline).toPipeline();
    JobInfo jobInfo =
        JobInfo.create(
            id,
            pipelineOptions.getJobName(),
            retrievalToken,
            PipelineOptionsTranslation.toProto(pipelineOptions));
    final JobExecutionResult result;

    if (!pipelineOptions.isStreaming() && !hasUnboundedPCollections(fusedPipeline)) {
      // TODO: Do we need to inspect for unbounded sources before fusing?
      // batch translation
      FlinkBatchPortablePipelineTranslator translator =
          FlinkBatchPortablePipelineTranslator.createTranslator();
      FlinkBatchPortablePipelineTranslator.BatchTranslationContext context =
          FlinkBatchPortablePipelineTranslator.createTranslationContext(
              jobInfo, pipelineOptions, filesToStage);
      translator.translate(context, fusedPipeline);
      result = context.getExecutionEnvironment().execute(pipelineOptions.getJobName());
    } else {
      // streaming translation
      FlinkStreamingPortablePipelineTranslator translator =
          new FlinkStreamingPortablePipelineTranslator();
      FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context =
          FlinkStreamingPortablePipelineTranslator.createTranslationContext(
              jobInfo, pipelineOptions, filesToStage);
      translator.translate(context, fusedPipeline);
      result = context.getExecutionEnvironment().execute(pipelineOptions.getJobName());
    }

    return FlinkRunner.createPipelineResult(result, pipelineOptions);
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
    LOG.warn("addMessageObserver() not yet implemented.");
  }

  private synchronized void setState(JobState.Enum state) {
    this.jobState = state;
    for (Consumer<JobState.Enum> observer : stateObservers) {
      observer.accept(state);
    }
  }

  /** Indicates whether the given pipeline has any unbounded PCollections. */
  private static boolean hasUnboundedPCollections(RunnerApi.Pipeline pipeline) {
    checkNotNull(pipeline);
    Collection<RunnerApi.PCollection> pCollecctions =
        pipeline.getComponents().getPcollectionsMap().values();
    // Assume that all PCollections are consumed at some point in the pipeline.
    return pCollecctions
        .stream()
        .anyMatch(pc -> pc.getIsBounded() == RunnerApi.IsBounded.Enum.UNBOUNDED);
  }
}
