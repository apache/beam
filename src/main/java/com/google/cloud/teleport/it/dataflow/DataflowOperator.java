/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.dataflow;

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobState;
import com.google.common.base.Strings;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for managing Dataflow jobs. */
public final class DataflowOperator {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowOperator.class);

  /** The result of running an operation. */
  public enum Result {
    CONDITION_MET,
    JOB_FINISHED,
    JOB_FAILED,
    TIMEOUT
  }

  private final DataflowClient client;

  public DataflowOperator(DataflowClient client) {
    this.client = client;
  }

  /**
   * Waits until the given job is done, timing out it if runs for too long.
   *
   * <p>If the job is a batch job, it should complete eventually. If it is a streaming job, this
   * will time out unless the job is explicitly cancelled or drained.
   *
   * @param config the configuration for performing the operation
   * @return the result, which will be {@link Result#JOB_FINISHED}, {@link Result#JOB_FAILED} or
   *     {@link Result#TIMEOUT}
   */
  public Result waitUntilDone(Config config) {
    return finishOrTimeout(
        config, () -> false, () -> jobIsDone(config.project(), config.region(), config.jobId()));
  }

  /**
   * Waits until the given job is done, timing out it if runs for too long. In cases of timeout, the
   * dataflow job is drained.
   *
   * <p>If the job is a batch job, it should complete eventually. If it is a streaming job, this
   * will time out unless the job is explicitly cancelled or drained. After timeout, the job will be
   * drained.
   *
   * @param config the configuration for performing the operation
   * @return the result, which will be {@link Result#JOB_FINISHED}, {@link Result#JOB_FAILED} or
   *     {@link Result#TIMEOUT}
   */
  public Result waitUntilDoneAndFinish(Config config) throws IOException {
    Result result = waitUntilDone(config);
    if (result == Result.TIMEOUT) {
      drainJobAndFinish(config);
    }
    return result;
  }

  /**
   * Waits until a given condition is met OR when the job enters a state that indicates that it is
   * done or ready to be done.
   *
   * @param config the configuration for performing operations
   * @param conditionCheck a {@link Supplier} that will be called periodically to check if the
   *     condition is met
   * @return the result, which could be any value in {@link Result}
   */
  public Result waitForCondition(Config config, Supplier<Boolean> conditionCheck) {
    return finishOrTimeout(
        config,
        conditionCheck,
        () -> jobIsDoneOrFinishing(config.project(), config.region(), config.jobId()));
  }

  /**
   * Waits until a given condition is met OR when a job enters a state that indicates that it is
   * done or ready to be done.
   *
   * <p>If the condition was met before the job entered a done or finishing state, then this will
   * cancel the job and wait for the job to enter a done state.
   *
   * @param config the configuration for performing operations
   * @param conditionCheck a {@link Supplier} that will be called periodically to check if the
   *     condition is met
   * @return the result of waiting for the condition, not of waiting for the job to be done
   * @throws IOException if there is an issue cancelling the job
   */
  public Result waitForConditionAndFinish(Config config, Supplier<Boolean> conditionCheck)
      throws IOException {
    Result conditionStatus = waitForCondition(config, conditionCheck);
    if (conditionStatus != Result.JOB_FINISHED && conditionStatus != Result.JOB_FAILED) {
      drainJobAndFinish(config);
    }
    return conditionStatus;
  }

  /**
   * Drains the job and waits till its drained.
   *
   * @param config the configuration for performing operations
   * @return the result of waiting for the condition
   * @throws IOException
   */
  public Result drainJobAndFinish(Config config) throws IOException {
    client.drainJob(config.project(), config.region(), config.jobId());
    return waitUntilDone(config);
  }

  private static Result finishOrTimeout(
      Config config, Supplier<Boolean> conditionCheck, Supplier<Boolean> stopChecking) {
    Instant start = Instant.now();

    LOG.info("Making initial finish check.");
    if (conditionCheck.get()) {
      return Result.CONDITION_MET;
    }

    LOG.info("Job was not already finished. Starting to wait between requests.");
    while (timeIsLeft(start, config.timeoutAfter())) {
      try {
        Thread.sleep(config.checkAfter().toMillis());
      } catch (InterruptedException e) {
        LOG.warn("Wait interrupted. Checking now.");
      }

      LOG.info("Checking if condition is met.");
      if (conditionCheck.get()) {
        LOG.info("Condition met!");
        return Result.CONDITION_MET;
      }
      LOG.info("Condition not met. Checking if job is finished.");
      if (stopChecking.get()) {
        LOG.info("Detected that we should stop checking.");
        return Result.JOB_FINISHED;
      }
      LOG.info(
          "Job not finished. Will check again in {} seconds", config.checkAfter().getSeconds());
    }

    LOG.warn("Neither the condition or job completion were fulfilled on time.");
    return Result.TIMEOUT;
  }

  private boolean jobIsDone(String project, String region, String jobId) {
    try {
      JobState state = client.getJobStatus(project, region, jobId);
      LOG.info("Job is in state {}", state);
      if (JobState.FAILED_STATES.contains(state)) {
        throw new RuntimeException(
            String.format(
                "Job ID %s under %s failed. Please check cloud console for more details.",
                jobId, project));
      }
      return JobState.DONE_STATES.contains(state);
    } catch (IOException e) {
      LOG.error("Failed to get current job state. Assuming not done.", e);
      return false;
    }
  }

  private boolean jobIsDoneOrFinishing(String project, String region, String jobId) {
    try {
      JobState state = client.getJobStatus(project, region, jobId);
      LOG.info("Job is in state {}", state);
      if (JobState.FAILED_STATES.contains(state)) {
        throw new RuntimeException(
            String.format(
                "Job ID %s under %s failed. Please check cloud console for more details.",
                jobId, project));
      }
      return JobState.DONE_STATES.contains(state) || JobState.FINISHING_STATES.contains(state);
    } catch (IOException e) {
      LOG.error("Failed to get current job state. Assuming not done.", e);
      return false;
    }
  }

  private static boolean timeIsLeft(Instant start, Duration maxWaitTime) {
    return Duration.between(start, Instant.now()).minus(maxWaitTime).isNegative();
  }

  /** Configuration for running an operation. */
  @AutoValue
  public abstract static class Config {

    public abstract String project();

    public abstract String jobId();

    public abstract String region();

    public abstract Duration checkAfter();

    public abstract Duration timeoutAfter();
    // TODO(zhoufek): Also let users set the maximum number of exceptions.

    public static Builder builder() {
      return new AutoValue_DataflowOperator_Config.Builder()
          .setCheckAfter(Duration.ofSeconds(15))
          .setTimeoutAfter(Duration.ofMinutes(15));
    }

    /** Builder for a {@link Config}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setProject(String value);

      public abstract Builder setRegion(String value);

      public abstract Builder setJobId(String value);

      public abstract Builder setCheckAfter(Duration value);

      public abstract Builder setTimeoutAfter(Duration value);

      abstract Config autoBuild();

      public Config build() {
        Config config = autoBuild();
        checkState(!Strings.isNullOrEmpty(config.project()), "Project must be set");
        checkState(!Strings.isNullOrEmpty(config.region()), "Region must be set");
        checkState(!Strings.isNullOrEmpty(config.jobId()), "Job id must be set");
        return config;
      }
    }
  }
}
