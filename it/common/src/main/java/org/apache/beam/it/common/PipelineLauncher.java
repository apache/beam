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

import static org.apache.beam.it.common.utils.PipelineUtils.createJobName;

import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;

/** Client for working with Cloud Dataflow. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/27438)
})
public interface PipelineLauncher {
  /** Enum representing Apache Beam SDKs. */
  enum Sdk {
    JAVA("JAVA"),
    PYTHON("PYTHON"),
    GO("GO");

    private final String text;

    Sdk(String text) {
      this.text = text;
    }

    @Override
    public String toString() {
      return text;
    }
  }

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

    /** States that indicate the job is getting ready to run. */
    public static final ImmutableSet<JobState> PENDING_STATES = ImmutableSet.of(PENDING, QUEUED);

    /** States that indicate the job is running. */
    public static final ImmutableSet<JobState> ACTIVE_STATES = ImmutableSet.of(RUNNING, UPDATED);

    /** States that indicate that the job is done. */
    public static final ImmutableSet<JobState> DONE_STATES =
        ImmutableSet.of(CANCELLED, DONE, DRAINED, STOPPED);

    /** States that indicate that the job has failed. */
    public static final ImmutableSet<JobState> FAILED_STATES = ImmutableSet.of(FAILED);

    /** States that indicate that the job is in the process of finishing. */
    public static final ImmutableSet<JobState> FINISHING_STATES =
        ImmutableSet.of(DRAINING, CANCELLING, RESOURCE_CLEANING_UP);

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
      if (fromDataflow == null) {
        return null;
      }
      return valueOf(fromDataflow.replace(DATAFLOW_PREFIX, ""));
    }

    @Override
    public String toString() {
      return text;
    }
  }

  /** Config for starting a Dataflow job. */
  class LaunchConfig {

    /** The default number of random characters to use in the generated job names. */
    public static final int JOB_NAME_DEFAULT_CHARS_SUFFIX = 8;

    private final String jobName;
    private final ImmutableMap<String, String> parameters;
    private final ImmutableMap<String, Object> environment;
    private final @Nullable String specPath;
    private final @Nullable Sdk sdk;
    private final @Nullable String executable;
    private final @Nullable String requirementsFile;
    private final @Nullable Pipeline pipeline;

    private LaunchConfig(Builder builder) {
      this.jobName = builder.jobName;
      this.parameters = ImmutableMap.copyOf(builder.parameters);
      this.environment = ImmutableMap.copyOf(builder.environment);
      this.specPath = builder.specPath;
      this.sdk = builder.sdk;
      this.executable = builder.executable;
      this.requirementsFile = builder.requirementsFile;
      this.pipeline = builder.pipeline;
    }

    public String jobName() {
      return jobName;
    }

    public ImmutableMap<String, String> parameters() {
      return parameters;
    }

    public ImmutableMap<String, Object> environment() {
      return environment;
    }

    public @Nullable String getParameter(String key) {
      return parameters.get(key);
    }

    public @Nullable String specPath() {
      return specPath;
    }

    public @Nullable Sdk sdk() {
      return sdk;
    }

    public @Nullable String executable() {
      return executable;
    }

    public @Nullable String requirementsFile() {
      return requirementsFile;
    }

    public @Nullable Pipeline pipeline() {
      return pipeline;
    }

    public static Builder builderWithName(String jobName, String specPath) {
      return new Builder(jobName, specPath);
    }

    public static Builder builder(String testName, String specPath) {
      return new Builder(createJobName(testName, JOB_NAME_DEFAULT_CHARS_SUFFIX), specPath);
    }

    public static Builder builder(String jobName) {
      return builder(jobName, null);
    }

    /** Builder for the {@link LaunchConfig}. */
    public static final class Builder {
      private final String jobName;
      private final String specPath;
      private final Map<String, Object> environment;
      private Map<String, String> parameters;
      private Sdk sdk;
      private String executable;
      private String requirementsFile;
      private Pipeline pipeline;

      private Builder(String jobName, String specPath) {
        this.jobName = jobName;
        this.parameters = new HashMap<>();
        this.environment = new HashMap<>();
        this.specPath = specPath;
      }

      public String getJobName() {
        return jobName;
      }

      public @Nullable String getParameter(String key) {
        return parameters.get(key);
      }

      public Builder setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
        return this;
      }

      public Builder addParameter(String key, String value) {
        parameters.put(key, value);
        return this;
      }

      public @Nullable Object getEnvironment(String key) {
        return environment.get(key);
      }

      public Builder addEnvironment(String key, Object value) {
        environment.put(key, value);
        return this;
      }

      public @Nullable String getSpecPath() {
        return specPath;
      }

      public @Nullable Sdk getSdk() {
        return sdk;
      }

      public Builder setSdk(Sdk sdk) {
        this.sdk = sdk;
        return this;
      }

      public @Nullable String getExecutable() {
        return executable;
      }

      public Builder setExecutable(String executable) {
        this.executable = executable;
        return this;
      }

      public @Nullable String getRequirementsFile() {
        return requirementsFile;
      }

      public Builder setRequirementsFile(String requirementsFile) {
        this.requirementsFile = requirementsFile;
        return this;
      }

      public @Nullable Pipeline getPipeline() {
        return pipeline;
      }

      public Builder setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
        return this;
      }

      public LaunchConfig build() {
        return new LaunchConfig(this);
      }
    }
  }

  /** Info about the job from what Dataflow returned. */
  @AutoValue
  abstract class LaunchInfo {
    public abstract String jobId();

    public abstract String projectId();

    public abstract String region();

    public abstract JobState state();

    public abstract String createTime();

    public abstract String sdk();

    public abstract String version();

    public abstract String jobType();

    public abstract String runner();

    public abstract @Nullable String templateName();

    public abstract @Nullable String templateType();

    public abstract @Nullable String templateVersion();

    public abstract @Nullable String pipelineName();

    public abstract ImmutableMap<String, String> parameters();

    public static Builder builder() {
      return new AutoValue_PipelineLauncher_LaunchInfo.Builder();
    }

    /** Builder for {@link LaunchInfo}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setProjectId(String value);

      public abstract Builder setJobId(String value);

      public abstract Builder setRegion(String value);

      public abstract Builder setState(JobState value);

      public abstract Builder setCreateTime(String value);

      public abstract Builder setSdk(String value);

      public abstract Builder setVersion(String value);

      public abstract Builder setJobType(String value);

      public abstract Builder setRunner(String value);

      public abstract Builder setTemplateName(@Nullable String value);

      public abstract Builder setTemplateType(@Nullable String value);

      public abstract Builder setTemplateVersion(@Nullable String value);

      public abstract Builder setPipelineName(@Nullable String value);

      public abstract Builder setParameters(ImmutableMap<String, String> value);

      public abstract LaunchInfo build();
    }
  }

  /**
   * Launches a new Dataflow job.
   *
   * @param project the project to run the job in
   * @param region the region to run the job in (e.g. us-east1)
   * @param options options for configuring the job
   * @return info about the request to launch a new job
   * @throws IOException if there is an issue sending the request
   */
  LaunchInfo launch(String project, String region, LaunchConfig options) throws IOException;

  /**
   * Gets information of a job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job
   * @return dataflow job information
   * @throws IOException if there is an issue sending the request
   */
  Job getJob(String project, String region, String jobId) throws IOException;

  /**
   * Gets information of a job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job
   * @param jobView the level of information requested in response.
   * @return dataflow job information
   * @throws IOException if there is an issue sending the request
   */
  Job getJob(String project, String region, String jobId, String jobView) throws IOException;

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
   * Gets the messages of a job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job
   * @param minimumImportance indicates the importance of the message.
   * @return the list of messages of the job.
   */
  List<JobMessage> listMessages(
      String project, String region, String jobId, String minimumImportance);

  /**
   * Cancels the given job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job to cancel
   * @throws IOException if there is an issue sending the request
   * @return Updated job instance
   */
  Job cancelJob(String project, String region, String jobId) throws IOException;

  /**
   * Drains the given job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job to drain
   * @throws IOException if there is an issue sending the request
   * @return Updated job instance
   */
  Job drainJob(String project, String region, String jobId) throws IOException;

  /**
   * Get the specified metric of the given job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job to query
   * @param metricName metric name to query from dataflow
   * @return value of the metric or null
   * @throws IOException if there is an issue sending the request
   */
  @Nullable
  Double getMetric(String project, String region, String jobId, String metricName)
      throws IOException;

  /**
   * Get all metrics of the given job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job to query
   * @return all metrics of the given job
   * @throws IOException if there is an issue sending the request
   */
  Map<String, Double> getMetrics(String project, String region, String jobId) throws IOException;

  /** Cancel all active jobs. */
  void cleanupAll() throws IOException;
}
