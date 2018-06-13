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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessage;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.flink.api.common.JobExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Invocation of a Flink Job via {@link FlinkRunner}.
 */
public class FlinkJobInvocation implements JobInvocation {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobInvocation.class);

  public static FlinkJobInvocation create(String id, ListeningExecutorService executorService,
      RunnerApi.Pipeline pipeline, FlinkPipelineOptions pipelineOptions) {
    return new FlinkJobInvocation(id, executorService, pipeline, pipelineOptions);
  }

  private final String id;
  private final ListeningExecutorService executorService;
  private final RunnerApi.Pipeline pipeline;
  private final FlinkPipelineOptions pipelineOptions;
  private Enum jobState;
  private List<Consumer<Enum>> stateObservers;

  @Nullable
  private ListenableFuture<PipelineResult> invocationFuture;

  private FlinkJobInvocation(String id, ListeningExecutorService executorService,
      RunnerApi.Pipeline pipeline, FlinkPipelineOptions pipelineOptions) {
    this.id = id;
    this.executorService = executorService;
    this.pipeline = pipeline;
    this.pipelineOptions = pipelineOptions;
    this.invocationFuture = null;
    this.jobState = Enum.STOPPED;
    this.stateObservers = new ArrayList<>();
  }

  private PipelineResult runPipeline() throws Exception {
    LOG.trace("Translating pipeline from proto");

    MetricsEnvironment.setMetricsSupported(true);

    LOG.info("Translating pipeline to Flink program.");
    // Fused pipeline proto.
    RunnerApi.Pipeline fusedPipeline = GreedyPipelineFuser.fuse(pipeline).toPipeline();
    JobInfo jobInfo = JobInfo.create(
        id, pipelineOptions.getJobName(), PipelineOptionsTranslation.toProto(pipelineOptions));
    final JobExecutionResult result;

    if (!pipelineOptions.isStreaming() && !hasUnboundedPCollections(fusedPipeline)) {
      // TODO: Do we need to inspect for unbounded sources before fusing?
      // batch translation
      FlinkBatchPortablePipelineTranslator translator =
          FlinkBatchPortablePipelineTranslator.createTranslator();
      FlinkBatchPortablePipelineTranslator.BatchTranslationContext context =
          FlinkBatchPortablePipelineTranslator.createTranslationContext(jobInfo);
      translator.translate(context, fusedPipeline);
      result = context.getExecutionEnvironment().execute(pipelineOptions.getJobName());
    } else {
      // streaming translation
      FlinkStreamingPortablePipelineTranslator translator =
          new FlinkStreamingPortablePipelineTranslator();
      FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context =
          FlinkStreamingPortablePipelineTranslator.createTranslationContext(jobInfo);
      translator.translate(context, fusedPipeline);
      result = context.getExecutionEnvironment().execute(pipelineOptions.getJobName());
    }

    return FlinkRunner.createPipelineResult(result, pipelineOptions);
  }

  @Override
  public void start() {
    LOG.trace("Starting job invocation {}", getId());
    synchronized (this) {
      setState(Enum.STARTING);
      invocationFuture = executorService.submit(this::runPipeline);
      setState(Enum.RUNNING);
      Futures.addCallback(
          invocationFuture,
          new FutureCallback<PipelineResult>() {
            @Override
            public void onSuccess(
                @Nullable PipelineResult pipelineResult) {
              setState(Enum.DONE);
            }

            @Override
            public void onFailure(Throwable throwable) {
              String message = String.format("Error during job invocation %s.", getId());
              LOG.error(message, throwable);
              setState(Enum.FAILED);
            }
          },
          executorService
      );
    }
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void cancel() {
    LOG.trace("Canceling job invocation {}", getId());
    synchronized (this) {
      if (this.invocationFuture != null) {
        this.invocationFuture.cancel(true /* mayInterruptIfRunning */);
      }
    }
  }

  @Override
  public synchronized Enum getState() {
    return this.jobState;
  }

  @Override
  public synchronized void addStateListener(Consumer<Enum> stateStreamObserver) {
    stateStreamObserver.accept(getState());
    stateObservers.add(stateStreamObserver);
  }

  @Override
  public synchronized void addMessageListener(Consumer<JobMessage> messageStreamObserver) {
    LOG.warn("addMessageObserver() not yet implemented.");
  }

  private synchronized void setState(Enum state) {
    this.jobState = state;
    for (Consumer<Enum> observer : stateObservers) {
      observer.accept(state);
    }
  }

  /** Indicates whether the given pipeline has any unbounded PCollections. */
  private static boolean hasUnboundedPCollections(RunnerApi.Pipeline pipeline) {
    checkArgument(pipeline != null);
    Collection<RunnerApi.PCollection> pCollecctions = pipeline.getComponents()
        .getPcollectionsMap().values();
    // Assume that all PCollections are consumed at some point in the pipeline.
    return pCollecctions.stream()
        .anyMatch(pc -> pc.getIsBounded() == RunnerApi.IsBounded.Enum.UNBOUNDED);
  }

}
