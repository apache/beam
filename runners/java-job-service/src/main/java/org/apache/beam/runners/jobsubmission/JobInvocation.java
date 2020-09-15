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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables.getRootCause;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables.getStackTraceAsString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessage;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.util.Timestamps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.FutureCallback;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Internal representation of a Job which has been invoked (prepared and run) by a client. */
public class JobInvocation {

  private static final Logger LOG = LoggerFactory.getLogger(JobInvocation.class);

  private final RunnerApi.Pipeline pipeline;
  private final PortablePipelineRunner pipelineRunner;
  private final JobInfo jobInfo;
  private final ListeningExecutorService executorService;
  private final List<JobStateEvent> stateHistory;
  private final List<JobMessage> messageHistory;
  private final List<Consumer<JobStateEvent>> stateObservers;
  private final List<Consumer<JobMessage>> messageObservers;
  private JobApi.MetricResults metrics;
  private PortablePipelineResult resultHandle;
  private @Nullable ListenableFuture<PortablePipelineResult> invocationFuture;

  public JobInvocation(
      JobInfo jobInfo,
      ListeningExecutorService executorService,
      Pipeline pipeline,
      PortablePipelineRunner pipelineRunner) {
    this.jobInfo = jobInfo;
    this.executorService = executorService;
    this.pipeline = pipeline;
    this.pipelineRunner = pipelineRunner;
    this.stateObservers = new ArrayList<>();
    this.messageObservers = new ArrayList<>();
    this.invocationFuture = null;
    this.stateHistory = new ArrayList<>();
    this.messageHistory = new ArrayList<>();
    this.metrics = JobApi.MetricResults.newBuilder().build();
    this.setState(JobState.Enum.STOPPED);
  }

  private PortablePipelineResult runPipeline() throws Exception {
    return pipelineRunner.run(pipeline, jobInfo);
  }

  /** Start the job. */
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
        new FutureCallback<PortablePipelineResult>() {
          @Override
          public void onSuccess(PortablePipelineResult pipelineResult) {
            if (pipelineResult != null) {
              PipelineResult.State state = pipelineResult.getState();

              if (state.isTerminal()) {
                metrics = pipelineResult.portableMetrics();
              } else {
                resultHandle = pipelineResult;
              }

              switch (state) {
                case DONE:
                  setState(Enum.DONE);
                  break;
                case RUNNING:
                  setState(Enum.RUNNING);
                  break;
                case CANCELLED:
                  setState(Enum.CANCELLED);
                  break;
                case FAILED:
                  setState(Enum.FAILED);
                  break;
                default:
                  setState(JobState.Enum.UNSPECIFIED);
              }
            } else {
              setState(JobState.Enum.UNSPECIFIED);
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable throwable) {
            if (throwable instanceof CancellationException) {
              // We have canceled execution, just update the job state
              setState(JobState.Enum.CANCELLED);
              return;
            }
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

  /** @return Unique identifier for the job invocation. */
  public String getId() {
    return jobInfo.jobId();
  }

  /** Cancel the job. */
  public synchronized void cancel() {
    LOG.info("Canceling job invocation {}", getId());
    if (this.invocationFuture != null) {
      this.invocationFuture.cancel(true /* mayInterruptIfRunning */);
      Futures.addCallback(
          invocationFuture,
          new FutureCallback<PortablePipelineResult>() {
            @Override
            public void onSuccess(PortablePipelineResult pipelineResult) {
              // Do not cancel when we are already done.
              if (pipelineResult != null
                  && pipelineResult.getState() != PipelineResult.State.DONE) {
                try {
                  pipelineResult.cancel();
                  setState(JobState.Enum.CANCELLED);
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

  public JobApi.MetricResults getMetrics() {
    if (resultHandle != null) {
      metrics = resultHandle.portableMetrics();
    }
    return metrics;
  }

  /** Retrieve the job's current state. */
  public JobState.Enum getState() {
    return getStateEvent().getState();
  }

  /** Retrieve the job's current state. */
  public JobStateEvent getStateEvent() {
    return stateHistory.get(stateHistory.size() - 1);
  }

  /** Retrieve the job's pipeline. */
  public RunnerApi.Pipeline getPipeline() {
    return this.pipeline;
  }

  /** Listen for job state changes with a {@link Consumer}. */
  public synchronized void addStateListener(Consumer<JobStateEvent> stateStreamObserver) {
    for (JobStateEvent event : stateHistory) {
      stateStreamObserver.accept(event);
    }
    stateObservers.add(stateStreamObserver);
  }

  /** Listen for job messages with a {@link Consumer}. */
  public synchronized void addMessageListener(Consumer<JobMessage> messageStreamObserver) {
    for (JobMessage msg : messageHistory) {
      messageStreamObserver.accept(msg);
    }
    messageObservers.add(messageStreamObserver);
  }

  /** Convert to {@link JobApi.JobInfo}. */
  public JobApi.JobInfo toProto() {
    return JobApi.JobInfo.newBuilder()
        .setJobId(jobInfo.jobId())
        .setJobName(jobInfo.jobName())
        .setPipelineOptions(jobInfo.pipelineOptions())
        .setState(getState())
        .build();
  }

  private synchronized void setState(JobState.Enum state) {
    JobStateEvent event =
        JobStateEvent.newBuilder()
            .setState(state)
            .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
            .build();
    this.stateHistory.add(event);
    for (Consumer<JobStateEvent> observer : stateObservers) {
      observer.accept(event);
    }
  }

  private synchronized void sendMessage(JobMessage message) {
    messageHistory.add(message);
    for (Consumer<JobMessage> observer : messageObservers) {
      observer.accept(message);
    }
  }

  public static Boolean isTerminated(Enum state) {
    switch (state) {
      case DONE:
      case FAILED:
      case CANCELLED:
      case DRAINED:
        return true;
      default:
        return false;
    }
  }
}
