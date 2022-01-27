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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/** Client for working with Flex templates. */
public interface DataflowTemplateClient {
  /** Enum representing known Dataflow job states. */
  enum JobState {
    UNKNOWN("JOB_STATE_UNKNOWN"),
    STOPPED("JOB_STATE_STOPPED"),
    RUNNING("JOB_STATE_RUNNING"),
    DONE("JOB_STATE_DONE"),
    FAILED("JOB_STATE_FAILED"),
    CANCELLED("JOB_STATE_CANCELLED"),
    UPDATED("JOB_STATE_UPDATED"),
    DRAINING("JOB_STATE_DRAINING"),
    DRAINED("JOB_STATE_DRAINED"),
    PENDING("JOB_STATE_PENDING"),
    CANCELLING("JOB_STATE_CANCELLING"),
    QUEUED("JOB_STATE_QUEUED"),
    RESOURCE_CLEANING_UP("JOB_STATE_RESOURCE_CLEANING_UP");

    private static final String DATAFLOW_PREFIX = "JOB_STATE_";

    /** States that indicate the job is running or getting ready to run. */
    public static final ImmutableSet<JobState> RUNNING_STATES =
        ImmutableSet.of(RUNNING, UPDATED, PENDING, QUEUED);

    /** States that indicate that the job is done. */
    public static final ImmutableSet<JobState> DONE_STATES =
        ImmutableSet.of(CANCELLED, DONE, DRAINED, FAILED, STOPPED);

    /** States that indicate that the job is in the process of finishing. */
    public static final ImmutableSet<JobState> FINISHING_STATES =
        ImmutableSet.of(DRAINING, CANCELLING);

    private final String text;

    JobState(String text) {
      this.text = text;
    }

    /**
     * Parses the state from Dataflow.
     *
     * <p>Always use this in place of valueOf.
     */
    public static JobState parse(String fromDataflow) {
      return valueOf(fromDataflow.replace(DATAFLOW_PREFIX, ""));
    }

    @Override
    public String toString() {
      return text;
    }
  }

  /** LaunchOptions for starting a Dataflow job. */
  class LaunchOptions {
    private final String jobName;
    private final ImmutableMap<String, String> parameters;
    private final String specPath;

    private LaunchOptions(Builder builder) {
      this.jobName = builder.jobName;
      this.parameters = ImmutableMap.copyOf(builder.parameters);
      this.specPath = builder.specPath;
    }

    public String jobName() {
      return jobName;
    }

    public ImmutableMap<String, String> parameters() {
      return parameters;
    }

    public String specPath() {
      return specPath;
    }

    public static Builder builder(String jobName, String specPath) {
      return new Builder(jobName, specPath);
    }

    /** Builder for the {@link LaunchOptions}. */
    public static final class Builder {
      private final String jobName;
      private final Map<String, String> parameters;
      private final String specPath;

      private Builder(String jobName, String specPath) {
        this.jobName = jobName;
        this.parameters = new HashMap<>();
        this.specPath = specPath;
      }

      public String getJobName() {
        return jobName;
      }

      @Nullable
      public String getParameter(String key) {
        return parameters.get(key);
      }

      public Builder addParameter(String key, String value) {
        parameters.put(key, value);
        return this;
      }

      public String getSpecPath() {
        return specPath;
      }

      public LaunchOptions build() {
        return new LaunchOptions(this);
      }
    }
  }

  /** Info about the job from what Dataflow returned. */
  @AutoValue
  abstract class JobInfo {
    public abstract String jobId();

    public abstract JobState state();

    public static Builder builder() {
      return new AutoValue_DataflowTemplateClient_JobInfo.Builder();
    }

    /** Builder for {@link JobInfo}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setJobId(String value);

      public abstract Builder setState(JobState value);

      public abstract JobInfo build();
    }
  }

  /**
   * Launches a new job.
   *
   * @param project the project to run the job in
   * @param region the region to run the job in (e.g. us-east1)
   * @param options options for configuring the job
   * @return info about the request to launch a new job
   * @throws IOException if there is an issue sending the request
   */
  JobInfo launchTemplate(String project, String region, LaunchOptions options) throws IOException;

  /**
   * Gets the current status of a job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job
   * @return the current state of the job
   * @throws IOException if there is an issue sending the request
   */
  JobState getJobStatus(String project, String region, String jobId) throws IOException;

  /**
   * Cancels the given job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job to cancel
   * @throws IOException if there is an issue sending the request
   */
  void cancelJob(String project, String region, String jobId) throws IOException;
}
