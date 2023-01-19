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

import static com.google.cloud.teleport.it.dataflow.DataflowClient.JobState.FAILED;
import static com.google.cloud.teleport.it.dataflow.DataflowClient.JobState.PENDING_STATES;
import static com.google.cloud.teleport.it.logging.LogStrings.formatForLogging;

import com.google.api.client.util.ArrayMap;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.teleport.it.logging.LogStrings;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class covering the common operations between Classic and Flex templates.
 *
 * <p>Generally, the methods here are the ones that focus more on the Dataflow jobs rather than
 * launching a specific type of template.
 */
abstract class AbstractDataflowClient implements DataflowClient {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractDataflowClient.class);
  private static final Pattern CURRENT_METRICS = Pattern.compile(".*Current.*");

  protected final Dataflow client;

  AbstractDataflowClient(Dataflow client) {
    this.client = client;
  }

  @Override
  public Job getJob(String project, String region, String jobId) throws IOException {
    LOG.info("Getting the status of {} under {}", jobId, project);
    Job job = client.projects().locations().jobs().get(project, region, jobId).execute();
    LOG.info("Received job on get request for {}:\n{}", jobId, LogStrings.formatForLogging(job));
    return job;
  }

  @Override
  public JobState getJobStatus(String project, String region, String jobId) throws IOException {
    LOG.info("Getting the status of {} under {}", jobId, project);

    Job job = client.projects().locations().jobs().get(project, region, jobId).execute();
    LOG.info("Received job on get request for {}:\n{}", jobId, LogStrings.formatForLogging(job));
    return handleJobState(job);
  }

  @Override
  public void cancelJob(String project, String region, String jobId) throws IOException {
    LOG.info("Cancelling {} under {}", jobId, project);
    Job job = new Job().setRequestedState(JobState.CANCELLED.toString());
    LOG.info("Sending job to update {}:\n{}", jobId, LogStrings.formatForLogging(job));
    client.projects().locations().jobs().update(project, region, jobId, job).execute();
  }

  @Override
  public void drainJob(String project, String region, String jobId) throws IOException {
    LOG.info("Draining {} under {}", jobId, project);
    Job job = new Job().setRequestedState(JobState.DRAINED.toString());
    LOG.info("Sending job to update {}:\n{}", jobId, LogStrings.formatForLogging(job));
    client.projects().locations().jobs().update(project, region, jobId, job).execute();
  }

  @Override
  public Double getMetric(String project, String region, String jobId, String metricName)
      throws IOException {
    LOG.info("Getting '{}' metric for {} under {}", metricName, jobId, project);
    List<MetricUpdate> metrics =
        client
            .projects()
            .locations()
            .jobs()
            .getMetrics(project, region, jobId)
            .execute()
            .getMetrics();
    if (metrics == null) {
      LOG.warn("No metrics received for the job {} under {}.", jobId, project);
      return null;
    }
    for (MetricUpdate metricUpdate : metrics) {
      String currentMetricName = metricUpdate.getName().getName();
      String currentMetricOriginalName = metricUpdate.getName().getContext().get("original_name");
      if (Objects.equals(metricName, currentMetricName)
          || Objects.equals(metricName, currentMetricOriginalName)) {
        // only return if the metric is a scalar
        if (metricUpdate.getScalar() != null) {
          return ((Number) metricUpdate.getScalar()).doubleValue();
        } else {
          LOG.warn(
              "The given metric '{}' is not a scalar metric. Please use getMetrics instead.",
              metricName);
          return null;
        }
      }
    }
    LOG.warn(
        "Unable to find '{}' metric for {} under {}. Please check the metricName and try again!",
        metricName,
        jobId,
        project);
    return null;
  }

  @Override
  public Map<String, Double> getMetrics(String project, String region, String jobId)
      throws IOException {
    LOG.info("Getting metrics for {} under {}", jobId, project);
    List<MetricUpdate> metrics =
        client
            .projects()
            .locations()
            .jobs()
            .getMetrics(project, region, jobId)
            .execute()
            .getMetrics();
    Map<String, Double> result = new HashMap<>();
    for (MetricUpdate metricUpdate : metrics) {
      String metricName = metricUpdate.getName().getName();
      Matcher matcher = CURRENT_METRICS.matcher(metricName);
      // Since we query metrics after the job finishes, we can ignore tentative and step metrics
      if (metricUpdate.getName().getContext().containsKey("tentative")
          || metricUpdate.getName().getContext().containsKey("execution_step")
          || metricUpdate.getName().getContext().containsKey("step")
          || metricName.equals("MeanByteCount")
          || metricName.equals("ElementCount")
          || matcher.find()) {
        continue;
      }

      if (result.containsKey(metricName)) {
        LOG.warn("Key {} already exists in metrics. Something might be wrong.", metricName);
      }

      if (metricUpdate.getScalar() != null) {
        result.put(metricName, ((Number) metricUpdate.getScalar()).doubleValue());
      } else if (metricUpdate.getDistribution() != null) {
        // currently, reporting distribution metrics as 4 separate scalar metrics
        ArrayMap distributionMap = (ArrayMap) metricUpdate.getDistribution();
        result.put(metricName + "_COUNT", ((Number) distributionMap.get("count")).doubleValue());
        result.put(metricName + "_MIN", ((Number) distributionMap.get("min")).doubleValue());
        result.put(metricName + "_MAX", ((Number) distributionMap.get("max")).doubleValue());
        result.put(metricName + "_SUM", ((Number) distributionMap.get("sum")).doubleValue());
      } else if (metricUpdate.getGauge() != null) {
        LOG.warn("Gauge metric {} cannot be handled.", metricName);
        // not sure how to handle gauge metrics
      }
    }
    return result;
  }

  protected void printJobResponse(Job job) {
    LOG.info("Received job response: {}", formatForLogging(job));

    LOG.info(
        "Dataflow Console: https://console.cloud.google.com/dataflow/jobs/{}/{}?project={}",
        job.getLocation(),
        job.getId(),
        job.getProjectId());
  }

  /** Parses the job state if available or returns {@link JobState#UNKNOWN} if not given. */
  protected JobState handleJobState(Job job) {
    String currentState = job.getCurrentState();
    return Strings.isNullOrEmpty(currentState) ? JobState.UNKNOWN : JobState.parse(currentState);
  }

  /** Creates a JobInfo object from the provided parameters. */
  protected JobInfo getJobInfo(LaunchConfig options, JobState state, Job job, String runner) {
    Map<String, String> labels = job.getLabels();
    JobInfo.Builder builder =
        JobInfo.builder()
            .setProjectId(job.getProjectId())
            .setJobId(job.getId())
            .setRegion(job.getLocation())
            .setCreateTime(job.getCreateTime())
            .setSdk(job.getJobMetadata().getSdkVersion().getVersionDisplayName())
            .setVersion(job.getJobMetadata().getSdkVersion().getVersion())
            .setJobType(job.getType())
            .setRunner(runner)
            .setParameters(options.parameters())
            .setState(state);
    if (labels != null && !labels.isEmpty()) {
      builder
          .setTemplateType(
              job.getLabels().getOrDefault("goog-dataflow-provided-template-type", null))
          .setTemplateVersion(
              job.getLabels().getOrDefault("goog-dataflow-provided-template-version", null))
          .setTemplateName(
              job.getLabels().getOrDefault("goog-dataflow-provided-template-name", null));
    }
    return builder.build();
  }

  /** Waits until the specified job is not in a pending state. */
  public JobState waitUntilActive(String project, String region, String jobId) throws IOException {
    JobState state = getJobStatus(project, region, jobId);
    while (PENDING_STATES.contains(state)) {
      LOG.info("Job still pending. Will check again in 15 seconds");
      try {
        TimeUnit.SECONDS.sleep(15);
      } catch (InterruptedException e) {
        LOG.warn("Wait interrupted. Checking now.");
      }
      state = getJobStatus(project, region, jobId);
    }
    if (state == FAILED) {
      throw new RuntimeException(
          String.format(
              "The job failed before launch! For more "
                  + "information please check if the job log for Job ID: %s, under project %s.",
              jobId, project));
    }
    return state;
  }
}
