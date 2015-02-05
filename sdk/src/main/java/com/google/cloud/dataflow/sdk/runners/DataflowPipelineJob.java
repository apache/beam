/*
 * Copyright (C) 2014 Google Inc.
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
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil.JobState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.List;
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
  private String project;

  /**
   * Client for the Dataflow service. This can be used to query the service
   * for information about the job.
   */
  private Dataflow dataflowClient;

  /**
   * Construct the job.
   *
   * @param projectId the project id
   * @param jobId the job id
   * @param client the workflow client
   */
  public DataflowPipelineJob(
      String projectId, String jobId, Dataflow client) {
    project = projectId;
    this.jobId = jobId;
    dataflowClient = client;
  }

  public String getJobId() {
    return jobId;
  }

  public String getProjectId() {
    return project;
  }

  public Dataflow getDataflowClient() {
    return dataflowClient;
  }

  /**
   * Wait for the job to finish and return the final status.
   *
   * @param timeToWait The time to wait in units timeUnit for the job to finish.
   * @param timeUnit The unit of time for timeToWait.
   *     Provide a negative value for an infinite wait.
   * @param messageHandler If non null this handler will be invoked for each
   *   batch of messages received.
   * @return The final state of the job or null on timeout or if the
   *   thread is interrupted.
   * @throws IOException If there is a persistent problem getting job
   *   information.
   * @throws InterruptedException
   */
  @Nullable
  public JobState waitToFinish(
      long timeToWait,
      TimeUnit timeUnit,
      MonitoringUtil.JobMessagesHandler messageHandler)
          throws IOException, InterruptedException {
    // The polling interval for job status information.
    long interval = TimeUnit.SECONDS.toMillis(2);

    // The time at which to stop.
    long endTime = timeToWait >= 0
        ? System.currentTimeMillis() + timeUnit.toMillis(timeToWait)
        : Long.MAX_VALUE;

    MonitoringUtil monitor = new MonitoringUtil(project, dataflowClient);

    long lastTimestamp = 0;
    int errorGettingMessages = 0;
    int errorGettingJobStatus = 0;
    while (true) {
      // Get the state of the job before listing messages. This ensures we always fetch job
      // messages after the job finishes to ensure we have all them.
      Job job = null;
      try {
        job = dataflowClient.v1b3().projects().jobs().get(project, jobId).execute();
      } catch (GoogleJsonResponseException | SocketTimeoutException e) {
        if (++errorGettingJobStatus > 5) {
          // We want to continue to wait for the job to finish so
          // we ignore this error, but warn occasionally if it keeps happening.
          LOG.warn("There were problems getting job status: ", e);
          errorGettingJobStatus = 0;
        }
      }

      if (messageHandler != null) {
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
          if (++errorGettingMessages > 5) {
            // We want to continue to wait for the job to finish so
            // we ignore this error, but warn occasionally if it keeps happening.
            LOG.warn("There are problems accessing job messages: ", e);
            errorGettingMessages = 0;
          }
        }
      }

      // Check if the job is done.
      JobState state = JobState.toState(job.getCurrentState());
      if (state.isTerminal()) {
        return state;
      }

      if (System.currentTimeMillis() >= endTime) {
        // Timed out.
        return null;
      }

      // Job not yet done.  Wait a little, then check again.
      long sleepTime = Math.min(
          endTime - System.currentTimeMillis(), interval);
      TimeUnit.MILLISECONDS.sleep(sleepTime);
    }
  }
}
