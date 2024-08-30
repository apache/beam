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
package org.apache.beam.it.common;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.apache.beam.it.common.PipelineLauncher.JobState;
import org.apache.beam.sdk.function.ThrowingConsumer;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for managing Dataflow jobs. */
public final class PipelineOperator {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineOperator.class);

  /** The result of running an operation. */
  public enum Result {
    CONDITION_MET,
    LAUNCH_FINISHED,
    LAUNCH_FAILED,
    TIMEOUT
  }

  private final PipelineLauncher client;

  public PipelineOperator(PipelineLauncher client) {
    this.client = client;
  }

  /**
   * Waits until the given job is done, timing out it if runs for too long.
   *
   * <p>If the job is a batch job, it should complete eventually. If it is a streaming job, this
   * will time out unless the job is explicitly cancelled or drained.
   *
   * @param config the configuration for performing the operation
   * @return the result, which will be {@link Result#LAUNCH_FINISHED}, {@link Result#LAUNCH_FAILED}
   *     or {@link Result#TIMEOUT}
   */
  @SuppressWarnings("rawtypes")
  public Result waitUntilDone(Config config) {
    return finishOrTimeout(
        config,
        new Supplier[] {() -> false},
        () -> jobIsDone(config.project(), config.region(), config.jobId()));
  }

  /**
   * Waits until the given job is done, timing out it if runs for too long. In cases of timeout, the
   * dataflow job is drained.
   *
   * <p>If the job is a batch job, it should complete eventually. If it is a streaming job, this
   * will time out unless the job is explicitly cancelled or drained. After timeout, the job will be
   * drained.
   *
   * <p>If the job is drained, this method will return once the drain call is finalized and the job
   * is fully drained.
   *
   * @param config the configuration for performing the operation
   * @return the result, which will be {@link Result#LAUNCH_FINISHED}, {@link Result#LAUNCH_FAILED}
   *     or {@link Result#TIMEOUT}
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
  public Result waitForCondition(Config config, Supplier<Boolean>... conditionCheck) {
    return finishOrTimeout(
        config,
        conditionCheck,
        () -> jobIsDoneOrFinishing(config.project(), config.region(), config.jobId()));
  }

  /**
   * Waits until a given condition is met OR when a job enters a state that indicates that it is
   * done or ready to be done.
   *
   * @see #waitForConditionsAndFinish(Config, Supplier[])
   */
  public Result waitForConditionAndFinish(Config config, Supplier<Boolean> conditionCheck)
      throws IOException {
    return waitForConditionsAndFinish(config, conditionCheck);
  }

  /**
   * Waits until a given condition is met OR when a job enters a state that indicates that it is
   * done or ready to be done.
   *
   * <p>If the condition was met before the job entered a done or finishing state, then this will
   * drain the job and wait for the job to enter a done state.
   *
   * @param config the configuration for performing operations
   * @param conditionChecks {@link Supplier} varargs that will be called periodically to check if
   *     the condition is met
   * @return the result of waiting for the condition, not of waiting for the job to be done
   * @throws IOException if there is an issue cancelling the job
   */
  public Result waitForConditionsAndFinish(Config config, Supplier<Boolean>... conditionChecks)
      throws IOException {
    return waitForConditionAndExecute(config, conditionChecks, this::drainJobAndFinish);
  }

  /** Similar to {@link #waitForConditionAndFinish} but cancels the job instead of draining. */
  public Result waitForConditionAndCancel(Config config, Supplier<Boolean>... conditionCheck)
      throws IOException {
    return waitForConditionAndExecute(config, conditionCheck, this::cancelJobAndFinish);
  }

  private Result waitForConditionAndExecute(
      Config config,
      Supplier<Boolean>[] conditionCheck,
      ThrowingConsumer<IOException, Config> executable)
      throws IOException {
    Result conditionStatus = waitForCondition(config, conditionCheck);
    if (conditionStatus != Result.LAUNCH_FINISHED && conditionStatus != Result.LAUNCH_FAILED) {
      executable.accept(config);
    }
    return conditionStatus;
  }

  /**
   * Drains the job and waits till it's drained.
   *
   * @param config the configuration for performing operations
   * @return the result of waiting for the condition
   * @throws IOException if DataflowClient fails while sending a request
   */
  public Result drainJobAndFinish(Config config) throws IOException {
    client.drainJob(config.project(), config.region(), config.jobId());
    return waitUntilDone(config);
  }

  /** Similar to {@link #drainJobAndFinish} but cancels the job instead of draining. */
  public Result cancelJobAndFinish(Config config) throws IOException {
    client.cancelJob(config.project(), config.region(), config.jobId());
    return waitUntilDone(config);
  }

  private static Result finishOrTimeout(
      Config config, Supplier<Boolean>[] conditionCheck, Supplier<Boolean>... stopChecking) {
    Instant start = Instant.now();

    boolean launchFinished = false;

    while (timeIsLeft(start, config.timeoutAfter())) {
      LOG.debug("Checking if condition is met.");
      try {
        if (allMatch(conditionCheck)) {
          LOG.info("Condition met!");
          return Result.CONDITION_MET;
        }
      } catch (Exception e) {
        LOG.warn("Error happened when checking for condition", e);
      }

      LOG.info("Condition was not met yet. Checking if job is finished.");
      if (launchFinished) {
        LOG.info("Launch was finished, stop checking.");
        return Result.LAUNCH_FINISHED;
      }

      if (allMatch(stopChecking)) {
        LOG.info("Detected that launch was finished, checking conditions once more.");
        launchFinished = true;
      } else {
        LOG.info(
            "Job not finished and conditions not met. Will check again in {} seconds (total wait: {}s of max {}s)",
            config.checkAfter().getSeconds(),
            Duration.between(start, Instant.now()).getSeconds(),
            config.timeoutAfter().getSeconds());
      }
      try {
        Thread.sleep(config.checkAfter().toMillis());
      } catch (InterruptedException e) {
        LOG.warn("Wait interrupted. Checking now.");
      }
    }

    LOG.warn("Neither the condition or job completion were fulfilled on time.");
    return Result.TIMEOUT;
  }

  private boolean jobIsDone(String project, String region, String jobId) {
    try {
      JobState state = client.getJobStatus(project, region, jobId);
      LOG.info("Job {} is in state {}", jobId, state);
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
      LOG.info("Job {} is in state {}", jobId, state);
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

  /**
   * Check if all checks return true, but makes sure that all of them are executed. This is
   * important to have complete feedback of integration tests progress.
   *
   * @param checks Varargs with all checks to run.
   * @return If all checks meet the criteria.
   */
  private static boolean allMatch(Supplier<Boolean>... checks) {
    boolean match = true;
    for (Supplier<Boolean> check : checks) {
      if (!check.get()) {
        match = false;
      }
    }
    return match;
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
      return new AutoValue_PipelineOperator_Config.Builder()
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
