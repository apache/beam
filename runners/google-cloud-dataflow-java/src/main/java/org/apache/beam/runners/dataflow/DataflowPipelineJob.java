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

import org.apache.beam.runners.dataflow.internal.DataflowAggregatorTransforms;
import org.apache.beam.runners.dataflow.internal.DataflowMetricUpdateExtractor;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.runners.AggregatorRetrievalException;
import org.apache.beam.sdk.runners.AggregatorValues;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.util.AttemptAndTimeBoundedExponentialBackOff;
import org.apache.beam.sdk.util.AttemptBoundedExponentialBackOff;
import org.apache.beam.sdk.util.MapAggregatorValues;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.common.annotations.VisibleForTesting;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * A DataflowPipelineJob represents a job submitted to Dataflow using
 * {@link DataflowRunner}.
 */
public class DataflowPipelineJob implements PipelineResult {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowPipelineJob.class);

  /**
   * The id for the job.
   */
  private String jobId;

  /**
   * Google cloud project to associate this pipeline with.
   */
  private String projectId;

  /**
   * Client for the Dataflow service. This can be used to query the service
   * for information about the job.
   */
  private DataflowPipelineOptions dataflowOptions;

  /**
   * The state the job terminated in or {@code null} if the job has not terminated.
   */
  @Nullable
  private State terminalState = null;

  /**
   * The job that replaced this one or {@code null} if the job has not been replaced.
   */
  @Nullable
  private DataflowPipelineJob replacedByJob = null;

  private DataflowAggregatorTransforms aggregatorTransforms;

  /**
   * The Metric Updates retrieved after the job was in a terminal state.
   */
  private List<MetricUpdate> terminalMetricUpdates;

  /**
   * The polling interval for job status and messages information.
   */
  static final long MESSAGES_POLLING_INTERVAL = TimeUnit.SECONDS.toMillis(2);
  static final long STATUS_POLLING_INTERVAL = TimeUnit.SECONDS.toMillis(2);

  /**
   * The amount of polling attempts for job status and messages information.
   */
  static final int MESSAGES_POLLING_ATTEMPTS = 12;
  static final int STATUS_POLLING_ATTEMPTS = 5;

  /**
   * Constructs the job.
   *
   * @param projectId the project id
   * @param jobId the job id
   * @param dataflowOptions used to configure the client for the Dataflow Service
   * @param aggregatorTransforms a mapping from aggregators to PTransforms
   */
  public DataflowPipelineJob(
      String projectId,
      String jobId,
      DataflowPipelineOptions dataflowOptions,
      DataflowAggregatorTransforms aggregatorTransforms) {
    this.projectId = projectId;
    this.jobId = jobId;
    this.dataflowOptions = dataflowOptions;
    this.aggregatorTransforms = aggregatorTransforms;
  }

  /**
   * Get the id of this job.
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * Get the project this job exists in.
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * Returns a new {@link DataflowPipelineJob} for the job that replaced this one, if applicable.
   *
   * @throws IllegalStateException if called before the job has terminated or if the job terminated
   * but was not updated
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
  @Nullable
  public State waitUntilFinish() throws IOException, InterruptedException {
    return waitUntilFinish(Duration.millis(-1));
  }

  @Override
  @Nullable
  public State waitUntilFinish(Duration duration)
          throws IOException, InterruptedException {
    return waitUntilFinish(duration, new MonitoringUtil.LoggingHandler());
  }

  /**
   * Waits until the pipeline finishes and returns the final status.
   *
   * @param duration The time to wait for the job to finish.
   *     Provide a value less than 1 ms for an infinite wait.
   *
   * @param messageHandler If non null this handler will be invoked for each
   *   batch of messages received.
   * @return The final state of the job or null on timeout or if the
   *   thread is interrupted.
   * @throws IOException If there is a persistent problem getting job
   *   information.
   * @throws InterruptedException
   */
  @Nullable
  @VisibleForTesting
  public State waitUntilFinish(
      Duration duration,
      MonitoringUtil.JobMessagesHandler messageHandler) throws IOException, InterruptedException {
    return waitUntilFinish(duration, messageHandler, Sleeper.DEFAULT, NanoClock.SYSTEM);
  }

  /**
   * Waits until the pipeline finishes and returns the final status.
   *
   * @param duration The time to wait for the job to finish.
   *     Provide a value less than 1 ms for an infinite wait.
   *
   * @param messageHandler If non null this handler will be invoked for each
   *   batch of messages received.
   * @param sleeper A sleeper to use to sleep between attempts.
   * @param nanoClock A nanoClock used to time the total time taken.
   * @return The final state of the job or null on timeout.
   * @throws IOException If there is a persistent problem getting job
   *   information.
   * @throws InterruptedException if the thread is interrupted.
   */
  @Nullable
  @VisibleForTesting
  State waitUntilFinish(
      Duration duration,
      MonitoringUtil.JobMessagesHandler messageHandler,
      Sleeper sleeper,
      NanoClock nanoClock)
          throws IOException, InterruptedException {
    MonitoringUtil monitor = new MonitoringUtil(projectId, dataflowOptions.getDataflowClient());

    long lastTimestamp = 0;
    BackOff backoff =
        duration.getMillis() > 0
            ? new AttemptAndTimeBoundedExponentialBackOff(
                MESSAGES_POLLING_ATTEMPTS,
                MESSAGES_POLLING_INTERVAL,
                duration.getMillis(),
                AttemptAndTimeBoundedExponentialBackOff.ResetPolicy.ATTEMPTS,
                nanoClock)
            : new AttemptBoundedExponentialBackOff(
                MESSAGES_POLLING_ATTEMPTS, MESSAGES_POLLING_INTERVAL);
    State state;
    do {
      // Get the state of the job before listing messages. This ensures we always fetch job
      // messages after the job finishes to ensure we have all them.
      state = getStateWithRetries(1, sleeper);
      boolean hasError = state == State.UNKNOWN;

      if (messageHandler != null && !hasError) {
        // Process all the job messages that have accumulated so far.
        try {
          List<JobMessage> allMessages = monitor.getJobMessages(
              jobId, lastTimestamp);

          if (!allMessages.isEmpty()) {
            lastTimestamp =
                fromCloudTime(allMessages.get(allMessages.size() - 1).getTime()).getMillis();
            messageHandler.process(allMessages);
          }
        } catch (GoogleJsonResponseException | SocketTimeoutException e) {
          hasError = true;
          LOG.warn("There were problems getting current job messages: {}.", e.getMessage());
          LOG.debug("Exception information:", e);
        }
      }

      if (!hasError) {
        backoff.reset();
        // Check if the job is done.
        if (state.isTerminal()) {
          return state;
        }
      }
    } while(BackOffUtils.next(sleeper, backoff));
    LOG.warn("No terminal state was returned.  State value {}", state);
    return null;  // Timed out.
  }

  @Override
  public State cancel() throws IOException {
    Job content = new Job();
    content.setProjectId(projectId);
    content.setId(jobId);
    content.setRequestedState("JOB_STATE_CANCELLED");
    dataflowOptions.getDataflowClient().projects().jobs()
        .update(projectId, jobId, content)
        .execute();
    return State.CANCELLED;
  }

  @Override
  public State getState() {
    if (terminalState != null) {
      return terminalState;
    }

    return getStateWithRetries(STATUS_POLLING_ATTEMPTS, Sleeper.DEFAULT);
  }

  /**
   * Attempts to get the state. Uses exponential backoff on failure up to the maximum number
   * of passed in attempts.
   *
   * @param attempts The amount of attempts to make.
   * @param sleeper Object used to do the sleeps between attempts.
   * @return The state of the job or State.UNKNOWN in case of failure.
   */
  @VisibleForTesting
  State getStateWithRetries(int attempts, Sleeper sleeper) {
    if (terminalState != null) {
      return terminalState;
    }
    try {
      Job job = getJobWithRetries(attempts, sleeper);
      return MonitoringUtil.toState(job.getCurrentState());
    } catch (IOException exn) {
      // The only IOException that getJobWithRetries is permitted to throw is the final IOException
      // that caused the failure of retry. Other exceptions are wrapped in an unchecked exceptions
      // and will propagate.
      return State.UNKNOWN;
    }
  }

  /**
   * Attempts to get the underlying {@link Job}. Uses exponential backoff on failure up to the
   * maximum number of passed in attempts.
   *
   * @param attempts The amount of attempts to make.
   * @param sleeper Object used to do the sleeps between attempts.
   * @return The underlying {@link Job} object.
   * @throws IOException When the maximum number of retries is exhausted, the last exception is
   * thrown.
   */
  @VisibleForTesting
  Job getJobWithRetries(int attempts, Sleeper sleeper) throws IOException {
    AttemptBoundedExponentialBackOff backoff =
        new AttemptBoundedExponentialBackOff(attempts, STATUS_POLLING_INTERVAL);

    // Retry loop ends in return or throw
    while (true) {
      try {
        Job job = dataflowOptions.getDataflowClient()
            .projects()
            .jobs()
            .get(projectId, jobId)
            .execute();
        State currentState = MonitoringUtil.toState(job.getCurrentState());
        if (currentState.isTerminal()) {
          terminalState = currentState;
          replacedByJob = new DataflowPipelineJob(
              getProjectId(), job.getReplacedByJobId(), dataflowOptions, aggregatorTransforms);
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

  /**
   * Identical to {@link BackOffUtils#next} but without checked exceptions.
   */
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
  public <OutputT> AggregatorValues<OutputT> getAggregatorValues(Aggregator<?, OutputT> aggregator)
      throws AggregatorRetrievalException {
    try {
      return new MapAggregatorValues<>(fromMetricUpdates(aggregator));
    } catch (IOException e) {
      throw new AggregatorRetrievalException(
          "IOException when retrieving Aggregator values for Aggregator " + aggregator, e);
    }
  }

  private <OutputT> Map<String, OutputT> fromMetricUpdates(Aggregator<?, OutputT> aggregator)
      throws IOException {
    if (aggregatorTransforms.contains(aggregator)) {
      List<MetricUpdate> metricUpdates;
      if (terminalMetricUpdates != null) {
        metricUpdates = terminalMetricUpdates;
      } else {
        boolean terminal = getState().isTerminal();
        JobMetrics jobMetrics = dataflowOptions.getDataflowClient()
            .projects().jobs().getMetrics(projectId, jobId).execute();
        metricUpdates = jobMetrics.getMetrics();
        if (terminal && jobMetrics.getMetrics() != null) {
          terminalMetricUpdates = metricUpdates;
        }
      }

      return DataflowMetricUpdateExtractor.fromMetricUpdates(
          aggregator, aggregatorTransforms, metricUpdates);
    } else {
      throw new IllegalArgumentException(
          "Aggregator " + aggregator + " is not used in this pipeline");
    }
  }
}
