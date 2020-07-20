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
package org.apache.beam.runners.dataflow;

import static org.apache.beam.runners.dataflow.util.TimeUtil.fromCloudTime;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.MetricUpdate;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.runners.dataflow.util.MonitoringUtil.JobMessagesHandler;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A DataflowPipelineJob represents a job submitted to Dataflow using {@link DataflowRunner}. */
public class DataflowPipelineJob implements PipelineResult {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowPipelineJob.class);

  /** The id for the job. */
  protected String jobId;

  /** The {@link DataflowPipelineOptions} for the job. */
  private final DataflowPipelineOptions dataflowOptions;

  /**
   * Client for the Dataflow service. This can be used to query the service for information about
   * the job.
   */
  private final DataflowClient dataflowClient;

  /**
   * MetricResults object for Dataflow Runner. It allows for querying of metrics from the Dataflow
   * service.
   */
  private final DataflowMetrics dataflowMetrics;

  /** The state the job terminated in or {@code null} if the job has not terminated. */
  private @Nullable State terminalState = null;

  /** The job that replaced this one or {@code null} if the job has not been replaced. */
  private @Nullable DataflowPipelineJob replacedByJob = null;

  protected BiMap<AppliedPTransform<?, ?, ?>, String> transformStepNames;

  /** The Metric Updates retrieved after the job was in a terminal state. */
  private List<MetricUpdate> terminalMetricUpdates;

  /** The latest timestamp up to which job messages have been retrieved. */
  private long lastTimestamp = Long.MIN_VALUE;

  /** The polling interval for job status and messages information. */
  static final Duration MESSAGES_POLLING_INTERVAL = Duration.standardSeconds(2);

  static final Duration STATUS_POLLING_INTERVAL = Duration.standardSeconds(2);

  static final Duration DEFAULT_MAX_BACKOFF = Duration.standardMinutes(2);

  static final double DEFAULT_BACKOFF_EXPONENT = 1.5;

  /** The amount of polling retries for job status and messages information. */
  static final int MESSAGES_POLLING_RETRIES = 11;

  static final int STATUS_POLLING_RETRIES = 4;

  private static final FluentBackoff MESSAGES_BACKOFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(MESSAGES_POLLING_INTERVAL)
          .withMaxRetries(MESSAGES_POLLING_RETRIES)
          .withExponent(DEFAULT_BACKOFF_EXPONENT)
          .withMaxBackoff(DEFAULT_MAX_BACKOFF);

  protected static final FluentBackoff STATUS_BACKOFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(STATUS_POLLING_INTERVAL)
          .withMaxRetries(STATUS_POLLING_RETRIES)
          .withExponent(DEFAULT_BACKOFF_EXPONENT);

  /**
   * Constructs the job.
   *
   * @param jobId the job id
   * @param dataflowOptions used to configure the client for the Dataflow Service
   * @param transformStepNames a mapping from AppliedPTransforms to Step Names
   */
  public DataflowPipelineJob(
      DataflowClient dataflowClient,
      String jobId,
      DataflowPipelineOptions dataflowOptions,
      Map<AppliedPTransform<?, ?, ?>, String> transformStepNames) {
    this.dataflowClient = dataflowClient;
    this.jobId = jobId;
    this.dataflowOptions = dataflowOptions;
    this.transformStepNames = HashBiMap.create(firstNonNull(transformStepNames, ImmutableMap.of()));
    this.dataflowMetrics = new DataflowMetrics(this, this.dataflowClient);
  }

  /** Get the id of this job. */
  public String getJobId() {
    return jobId;
  }

  /** Get the project this job exists in. */
  public String getProjectId() {
    return dataflowOptions.getProject();
  }

  public DataflowPipelineOptions getDataflowOptions() {
    return dataflowOptions;
  }

  /** Get the region this job exists in. */
  public String getRegion() {
    return dataflowOptions.getRegion();
  }

  /**
   * Returns a new {@link DataflowPipelineJob} for the job that replaced this one, if applicable.
   *
   * @throws IllegalStateException if called before the job has terminated or if the job terminated
   *     but was not updated
   */
  public DataflowPipelineJob getReplacedByJob() {
    if (terminalState == null) {
      throw new IllegalStateException("getReplacedByJob() called before job terminated");
    }
    if (replacedByJob == null) {
      throw new IllegalStateException("getReplacedByJob() called for job that was not replaced");
    }
    return replacedByJob;
  }

  @Override
  public @Nullable State waitUntilFinish() {
    return waitUntilFinish(Duration.millis(-1));
  }

  @Override
  public @Nullable State waitUntilFinish(Duration duration) {
    try {
      return waitUntilFinish(duration, new MonitoringUtil.LoggingHandler());
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Waits until the pipeline finishes and returns the final status.
   *
   * @param duration The time to wait for the job to finish. Provide a value less than 1 ms for an
   *     infinite wait.
   * @param messageHandler If non null this handler will be invoked for each batch of messages
   *     received.
   * @return The final state of the job or null on timeout or if the thread is interrupted.
   * @throws IOException If there is a persistent problem getting job information.
   */
  @Nullable
  @VisibleForTesting
  public State waitUntilFinish(Duration duration, MonitoringUtil.JobMessagesHandler messageHandler)
      throws IOException, InterruptedException {
    // We ignore the potential race condition here (Ctrl-C after job submission but before the
    // shutdown hook is registered). Even if we tried to do something smarter (eg., SettableFuture)
    // the run method (which produces the job) could fail or be Ctrl-C'd before it had returned a
    // job. The display of the command to cancel the job is best-effort anyways -- RPC's could fail,
    // etc. If the user wants to verify the job was cancelled they should look at the job status.
    Thread shutdownHook =
        new Thread(
            () ->
                LOG.warn(
                    "Job is already running in Google Cloud Platform, Ctrl-C will not cancel it.\n"
                        + "To cancel the job in the cloud, run:\n> {}",
                    MonitoringUtil.getGcloudCancelCommand(dataflowOptions, getJobId())));

    try {
      Runtime.getRuntime().addShutdownHook(shutdownHook);
      return waitUntilFinish(
          duration,
          messageHandler,
          Sleeper.DEFAULT,
          NanoClock.SYSTEM,
          new MonitoringUtil(dataflowClient));
    } finally {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }
  }

  @Nullable
  @VisibleForTesting
  State waitUntilFinish(
      Duration duration,
      MonitoringUtil.@Nullable JobMessagesHandler messageHandler,
      Sleeper sleeper,
      NanoClock nanoClock)
      throws IOException, InterruptedException {
    return waitUntilFinish(
        duration, messageHandler, sleeper, nanoClock, new MonitoringUtil(dataflowClient));
  }

  private static BackOff getMessagesBackoff(Duration duration) {
    FluentBackoff factory = MESSAGES_BACKOFF_FACTORY;

    if (!duration.isShorterThan(Duration.ZERO)) {
      factory = factory.withMaxCumulativeBackoff(duration);
    }

    return BackOffAdapter.toGcpBackOff(factory.backoff());
  }

  /**
   * Waits until the pipeline finishes and returns the final status.
   *
   * @param duration The time to wait for the job to finish. Provide a value less than 1 ms for an
   *     infinite wait.
   * @param messageHandler If non null this handler will be invoked for each batch of messages
   *     received.
   * @param sleeper A sleeper to use to sleep between attempts.
   * @param nanoClock A nanoClock used to time the total time taken.
   * @return The final state of the job or null on timeout.
   * @throws IOException If there is a persistent problem getting job information.
   * @throws InterruptedException if the thread is interrupted.
   */
  @Nullable
  @VisibleForTesting
  State waitUntilFinish(
      Duration duration,
      MonitoringUtil.@Nullable JobMessagesHandler messageHandler,
      Sleeper sleeper,
      NanoClock nanoClock,
      MonitoringUtil monitor)
      throws IOException, InterruptedException {

    BackOff backoff = getMessagesBackoff(duration);

    // This function tracks the cumulative time from the *first request* to enforce the wall-clock
    // limit. Any backoff instance could, at best, track the the time since the first attempt at a
    // given request. Thus, we need to track the cumulative time ourselves.
    long startNanos = nanoClock.nanoTime();

    State state = State.UNKNOWN;
    Exception exception;
    do {
      exception = null;
      try {
        // Get the state of the job before listing messages. This ensures we always fetch job
        // messages after the job finishes to ensure we have all them.
        state =
            getStateWithRetries(
                BackOffAdapter.toGcpBackOff(STATUS_BACKOFF_FACTORY.withMaxRetries(0).backoff()),
                sleeper);
      } catch (IOException e) {
        exception = e;
        LOG.warn("Failed to get job state: {}", e.getMessage());
        LOG.debug("Failed to get job state: {}", e);
        continue;
      }

      exception = processJobMessages(messageHandler, monitor);

      if (exception != null) {
        continue;
      }

      // We can stop if the job is done.
      if (state.isTerminal()) {
        logTerminalState(state);
        return state;
      }

      // Reset attempts count and update cumulative wait time.
      backoff = resetBackoff(duration, nanoClock, startNanos);
    } while (BackOffUtils.next(sleeper, backoff));

    // At this point Backoff decided that we retried enough times.
    // This can be either due to exceeding allowed timeout for job to complete, or receiving
    // error multiple times in a row.

    if (exception == null) {
      LOG.warn("No terminal state was returned within allotted timeout. State value {}", state);
    } else {
      LOG.error("Failed to fetch job metadata with error: {}", exception);
    }

    return null;
  }

  private void logTerminalState(State state) {
    switch (state) {
      case DONE:
      case CANCELLED:
        LOG.info("Job {} finished with status {}.", getJobId(), state);
        break;
      case UPDATED:
        LOG.info(
            "Job {} has been updated and is running as the new job with id {}. "
                + "To access the updated job on the Dataflow monitoring console, "
                + "please navigate to {}",
            getJobId(),
            getReplacedByJob().getJobId(),
            MonitoringUtil.getJobMonitoringPageURL(
                getReplacedByJob().getProjectId(), getRegion(), getReplacedByJob().getJobId()));
        break;
      default:
        LOG.info("Job {} failed with status {}.", getJobId(), state);
    }
  }

  /**
   * Reset backoff. If duration is limited, calculate time remaining, otherwise just reset retry
   * count.
   *
   * <p>If a total duration for all backoff has been set, update the new cumulative sleep time to be
   * the remaining total backoff duration, stopping if we have already exceeded the allotted time.
   */
  private static BackOff resetBackoff(Duration duration, NanoClock nanoClock, long startNanos) {
    BackOff backoff;
    if (duration.isLongerThan(Duration.ZERO)) {
      long nanosConsumed = nanoClock.nanoTime() - startNanos;
      Duration consumed = Duration.millis((nanosConsumed + 999999) / 1000000);
      Duration remaining = duration.minus(consumed);
      if (remaining.isLongerThan(Duration.ZERO)) {
        backoff = getMessagesBackoff(remaining);
      } else {
        backoff = BackOff.STOP_BACKOFF;
      }
    } else {
      backoff = getMessagesBackoff(duration);
    }
    return backoff;
  }

  /**
   * Process all the job messages that have accumulated so far.
   *
   * @return Exception that caused failure to process messages or null.
   */
  private Exception processJobMessages(
      @Nullable JobMessagesHandler messageHandler, MonitoringUtil monitor) throws IOException {
    if (messageHandler != null) {
      try {
        List<JobMessage> allMessages = monitor.getJobMessages(getJobId(), lastTimestamp);

        if (!allMessages.isEmpty()) {
          lastTimestamp =
              fromCloudTime(allMessages.get(allMessages.size() - 1).getTime()).getMillis();
          messageHandler.process(allMessages);
        }
      } catch (GoogleJsonResponseException | SocketTimeoutException e) {
        LOG.warn("Failed to get job messages: {}", e.getMessage());
        LOG.debug("Failed to get job messages: {}", e);
        return e;
      }
    }
    return null;
  }

  private AtomicReference<FutureTask<State>> cancelState = new AtomicReference<>();

  @Override
  public State cancel() throws IOException {
    // Enforce that a cancel() call on the job is done at most once - as
    // a workaround for Dataflow service's current bugs with multiple
    // cancellation, where it may sometimes return an error when cancelling
    // a job that was already cancelled, but still report the job state as
    // RUNNING.
    // To partially work around these issues, we absorb duplicate cancel()
    // calls. This, of course, doesn't address the case when the job terminates
    // externally almost concurrently to calling cancel(), but at least it
    // makes it possible to safely call cancel() multiple times and from
    // multiple threads in one program.
    FutureTask<State> tentativeCancelTask =
        new FutureTask<>(
            () -> {
              Job content = new Job();
              content.setProjectId(getProjectId());
              String currentJobId = getJobId();
              content.setId(currentJobId);
              content.setRequestedState("JOB_STATE_CANCELLED");
              try {
                Job job = dataflowClient.updateJob(currentJobId, content);
                return MonitoringUtil.toState(job.getCurrentState());
              } catch (IOException e) {
                State state = getState();
                if (state.isTerminal()) {
                  LOG.warn("Cancel failed because job is already terminated. State is {}", state);
                  return state;
                } else if (e.getMessage().contains("has terminated")) {
                  // This handles the case where the getState() call above returns RUNNING but the
                  // cancel was rejected because the job is in fact done. Hopefully, someday we can
                  // delete this code if there is better consistency between the State and whether
                  // Cancel succeeds.
                  //
                  // Example message:
                  //    Workflow modification failed. Causes: (7603adc9e9bff51e): Cannot perform
                  //    operation 'cancel' on Job: 2017-04-01_22_50_59-9269855660514862348. Job has
                  //    terminated in state SUCCESS: Workflow job:
                  //    2017-04-01_22_50_59-9269855660514862348 succeeded.
                  LOG.warn("Cancel failed because job is already terminated.", e);
                  return state;
                } else {
                  String errorMsg =
                      String.format(
                          "Failed to cancel job in state %s, "
                              + "please go to the Developers Console to cancel it manually: %s",
                          state,
                          MonitoringUtil.getJobMonitoringPageURL(
                              getProjectId(), getRegion(), getJobId()));
                  LOG.warn(errorMsg);
                  throw new IOException(errorMsg, e);
                }
              }
            });
    if (cancelState.compareAndSet(null, tentativeCancelTask)) {
      // This thread should perform cancellation, while others will
      // only wait for the result.
      cancelState.get().run();
    }
    try {
      return cancelState.get().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public State getState() {
    if (terminalState != null) {
      return terminalState;
    }

    return getStateWithRetriesOrUnknownOnException(
        BackOffAdapter.toGcpBackOff(STATUS_BACKOFF_FACTORY.backoff()), Sleeper.DEFAULT);
  }

  /**
   * Attempts to get the state. Uses exponential backoff on failure up to the maximum number of
   * passed in attempts.
   *
   * @param attempts The amount of attempts to make.
   * @param sleeper Object used to do the sleeps between attempts.
   * @return The state of the job or State.UNKNOWN in case of failure.
   */
  @VisibleForTesting
  State getStateWithRetriesOrUnknownOnException(BackOff attempts, Sleeper sleeper) {
    try {
      return getStateWithRetries(attempts, sleeper);
    } catch (IOException exn) {
      // The only IOException that getJobWithRetries is permitted to throw is the final IOException
      // that caused the failure of retry. Other exceptions are wrapped in an unchecked exceptions
      // and will propagate.
      return State.UNKNOWN;
    }
  }

  State getStateWithRetries(BackOff attempts, Sleeper sleeper) throws IOException {
    if (terminalState != null) {
      return terminalState;
    }
    Job job = getJobWithRetries(attempts, sleeper);
    return MonitoringUtil.toState(job.getCurrentState());
  }

  /**
   * Attempts to get the underlying {@link Job}. Uses exponential backoff on failure up to the
   * maximum number of passed in attempts.
   *
   * @param backoff the {@link BackOff} used to control retries.
   * @param sleeper Object used to do the sleeps between attempts.
   * @return The underlying {@link Job} object.
   * @throws IOException When the maximum number of retries is exhausted, the last exception is
   *     thrown.
   */
  private Job getJobWithRetries(BackOff backoff, Sleeper sleeper) throws IOException {
    // Retry loop ends in return or throw
    while (true) {
      try {
        Job job = dataflowClient.getJob(getJobId());
        State currentState = MonitoringUtil.toState(job.getCurrentState());
        if (currentState.isTerminal()) {
          terminalState = currentState;
          replacedByJob =
              new DataflowPipelineJob(
                  dataflowClient, job.getReplacedByJobId(), dataflowOptions, transformStepNames);
        }
        return job;
      } catch (IOException exn) {
        LOG.warn("There were problems getting current job status: {}.", exn.getMessage());
        LOG.debug("Exception information:", exn);

        if (!nextBackOff(sleeper, backoff)) {
          throw exn;
        }
      }
    }
  }

  /** Identical to {@link BackOffUtils#next} but without checked exceptions. */
  private boolean nextBackOff(Sleeper sleeper, BackOff backoff) {
    try {
      return BackOffUtils.next(sleeper, backoff);
    } catch (InterruptedException | IOException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public MetricResults metrics() {
    return dataflowMetrics;
  }
}
