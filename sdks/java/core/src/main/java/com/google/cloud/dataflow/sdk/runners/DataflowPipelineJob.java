/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners;

import static com.google.cloud.dataflow.sdk.util.TimeUtil.fromCloudTime;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.runners.dataflow.DataflowAggregatorTransforms;
import com.google.cloud.dataflow.sdk.runners.dataflow.DataflowMetricUpdateExtractor;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.util.AttemptAndTimeBoundedExponentialBackOff;
import com.google.cloud.dataflow.sdk.util.AttemptBoundedExponentialBackOff;
import com.google.cloud.dataflow.sdk.util.MapAggregatorValues;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

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
 * {@link DataflowPipelineRunner}.
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
  private Dataflow dataflowClient;

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
  static final int MESSAGES_POLLING_ATTEMPTS = 10;
  static final int STATUS_POLLING_ATTEMPTS = 5;

  /**
   * Constructs the job.
   *
   * @param projectId the project id
   * @param jobId the job id
   * @param dataflowClient the client for the Dataflow Service
   */
  public DataflowPipelineJob(String projectId, String jobId, Dataflow dataflowClient,
      DataflowAggregatorTransforms aggregatorTransforms) {
    this.projectId = projectId;
    this.jobId = jobId;
    this.dataflowClient = dataflowClient;
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

  /**
   * Get the Cloud Dataflow API Client used by this job.
   */
  public Dataflow getDataflowClient() {
    return dataflowClient;
  }

  /**
   * Waits for the job to finish and return the final status.
   *
   * @param timeToWait The time to wait in units timeUnit for the job to finish.
   *     Provide a value less than 1 ms for an infinite wait.
   * @param timeUnit The unit of time for timeToWait.
   * @param messageHandler If non null this handler will be invoked for each
   *   batch of messages received.
   * @return The final state of the job or null on timeout or if the
   *   thread is interrupted.
   * @throws IOException If there is a persistent problem getting job
   *   information.
   * @throws InterruptedException
   */
  @Nullable
  public State waitToFinish(
      long timeToWait,
      TimeUnit timeUnit,
      MonitoringUtil.JobMessagesHandler messageHandler)
          throws IOException, InterruptedException {
    return waitToFinish(timeToWait, timeUnit, messageHandler, Sleeper.DEFAULT, NanoClock.SYSTEM);
  }

  /**
   * Wait for the job to finish and return the final status.
   *
   * @param timeToWait The time to wait in units timeUnit for the job to finish.
   *     Provide a value less than 1 ms for an infinite wait.
   * @param timeUnit The unit of time for timeToWait.
   * @param messageHandler If non null this handler will be invoked for each
   *   batch of messages received.
   * @param sleeper A sleeper to use to sleep between attempts.
   * @param nanoClock A nanoClock used to time the total time taken.
   * @return The final state of the job or null on timeout or if the
   *   thread is interrupted.
   * @throws IOException If there is a persistent problem getting job
   *   information.
   * @throws InterruptedException
   */
  @Nullable
  @VisibleForTesting
  State waitToFinish(
      long timeToWait,
      TimeUnit timeUnit,
      MonitoringUtil.JobMessagesHandler messageHandler,
      Sleeper sleeper,
      NanoClock nanoClock)
          throws IOException, InterruptedException {
    MonitoringUtil monitor = new MonitoringUtil(projectId, dataflowClient);

    long lastTimestamp = 0;
    BackOff backoff =
        timeUnit.toMillis(timeToWait) > 0
            ? new AttemptAndTimeBoundedExponentialBackOff(
                MESSAGES_POLLING_ATTEMPTS,
                MESSAGES_POLLING_INTERVAL,
                timeUnit.toMillis(timeToWait),
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

  /**
   * Cancels the job.
   * @throws IOException if there is a problem executing the cancel request.
   */
  public void cancel() throws IOException {
    Job content = new Job();
    content.setProjectId(projectId);
    content.setId(jobId);
    content.setRequestedState("JOB_STATE_CANCELLED");
    dataflowClient.projects().jobs()
        .update(projectId, jobId, content)
        .execute();
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
        Job job = dataflowClient
            .projects()
            .jobs()
            .get(projectId, jobId)
            .execute();
        State currentState = MonitoringUtil.toState(job.getCurrentState());
        if (currentState.isTerminal()) {
          terminalState = currentState;
          replacedByJob = new DataflowPipelineJob(
              getProjectId(), job.getReplacedByJobId(), dataflowClient, aggregatorTransforms);
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
      throw Throwables.propagate(e);
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
        JobMetrics jobMetrics =
            dataflowClient.projects().jobs().getMetrics(projectId, jobId).execute();
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
