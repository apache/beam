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
package org.apache.beam.it.gcp.dataflow;

import static org.apache.beam.it.common.logging.LogStrings.formatForLogging;

import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.auth.Credentials;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link PipelineLauncher} interface which invokes the template class using
 * DirectRunner, and manages the state in memory.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/27438)
})
public class DirectRunnerClient implements PipelineLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(DirectRunnerClient.class);

  private final Map<String, DirectRunnerJobThread> managedJobs = new HashMap<>();

  private final Class<?> mainClass;

  DirectRunnerClient(Builder builder) {
    this.mainClass = builder.getMainClass();
  }

  public static DirectRunnerClient.Builder builder(Class<?> mainClass) {
    return new DirectRunnerClient.Builder(mainClass);
  }

  @Override
  public LaunchInfo launch(String project, String region, LaunchConfig options) throws IOException {

    LOG.info("Getting ready to launch {} in {} under {}", options.jobName(), region, project);
    LOG.info("Using parameters:\n{}", formatForLogging(options.parameters()));

    try {
      List<String> cmd = new ArrayList<>();

      for (String parameter : options.parameters().keySet()) {
        cmd.add(String.format("--%s=%s", parameter, options.getParameter(parameter)));
      }
      cmd.add(String.format("--project=%s", project));
      cmd.add(String.format("--region=%s", region));

      String jobId =
          "direct-"
              + new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss").format(new Date())
              + "-"
              + System.currentTimeMillis();

      DirectRunnerJobThread jobThread =
          new DirectRunnerJobThread(project, region, jobId, mainClass, cmd);
      managedJobs.put(jobId, jobThread);
      jobThread.start();

      return LaunchInfo.builder()
          .setJobId(jobId)
          .setProjectId(project)
          .setRegion(region)
          .setCreateTime("")
          .setSdk("DirectBeam")
          .setVersion("0.0.1")
          .setJobType("JOB_TYPE_BATCH")
          .setRunner("DirectRunner")
          .setParameters(options.parameters())
          .setState(JobState.RUNNING)
          .build();
    } catch (Exception e) {
      throw new RuntimeException("Error launching DirectRunner test", e);
    }
  }

  @Override
  public Job getJob(String project, String region, String jobId) {
    return managedJobs.get(jobId).getJob();
  }

  @Override
  public Job getJob(String project, String region, String jobId, String jobView) {
    return managedJobs.get(jobId).getJob();
  }

  @Override
  public JobState getJobStatus(String project, String region, String jobId) {
    return managedJobs.get(jobId).getJobState();
  }

  @Override
  public List<JobMessage> listMessages(
      String project, String region, String jobId, String minimumImportance) {
    return new ArrayList<>();
  }

  @Override
  public Job cancelJob(String project, String region, String jobId) {
    LOG.warn("Cancelling direct runner job {}.", jobId);

    managedJobs.get(jobId).cancel();
    return new Job().setId(jobId).setRequestedState(JobState.CANCELLED.toString());
  }

  @Override
  public Job drainJob(String project, String region, String jobId) {
    LOG.warn("Cannot drain a direct runner job. Cancelling the job instead.");
    return cancelJob(project, region, jobId);
  }

  @Override
  public Double getMetric(String project, String region, String jobId, String metricName) {
    return null;
  }

  @Override
  public Map<String, Double> getMetrics(String project, String region, String jobId)
      throws IOException {
    return null;
  }

  @Override
  public synchronized void cleanupAll() throws IOException {
    // Cancel / terminate all threads for DirectRunnerClient
    for (DirectRunnerJobThread jobs : managedJobs.values()) {
      jobs.cancel();
    }
  }

  /** Builder for {@link DirectRunnerClient}. */
  public static final class Builder {

    private Credentials credentials;
    private final Class<?> mainClass;

    private Builder(Class<?> mainClass) {
      this.mainClass = mainClass;
    }

    public Credentials getCredentials() {
      return credentials;
    }

    public Class<?> getMainClass() {
      return mainClass;
    }

    public DirectRunnerClient.Builder setCredentials(Credentials value) {
      credentials = value;
      return this;
    }

    public DirectRunnerClient build() {
      return new DirectRunnerClient(this);
    }
  }

  static class DirectRunnerJobThread extends Thread {

    private final Job currentJob;
    private final Class<?> mainClass;
    private final List<String> commandLines;
    private Throwable throwable;
    private boolean cancelled;

    public DirectRunnerJobThread(
        String projectId,
        String location,
        String jobId,
        Class<?> mainClass,
        List<String> commandLines) {
      this.currentJob =
          new Job()
              .setProjectId(projectId)
              .setLocation(location)
              .setId(jobId)
              .setCurrentState(JobState.QUEUED.toString());
      this.mainClass = mainClass;
      this.commandLines = commandLines;
    }

    @Override
    public void run() {

      try {
        String[] args = commandLines.toArray(new String[0]);
        Method mainMethod = mainClass.getMethod("main", String[].class);
        currentJob.setCurrentState(JobState.RUNNING.toString());

        LOG.info("Starting job {}...", currentJob.getId());
        mainMethod.setAccessible(true);
        mainMethod.invoke(null, (Object) args);

        currentJob.setCurrentState(JobState.DONE.toString());
      } catch (Throwable e) {

        // Errors are acceptable if thread was cancelled
        if (!cancelled) {
          LOG.warn("Error occurred with job {}", currentJob.getId(), e);
          this.throwable = e;
          currentJob.setCurrentState(JobState.FAILED.toString());
        }
      }
    }

    public Job getJob() {
      return currentJob;
    }

    public Throwable getThrowable() {
      return throwable;
    }

    public JobState getJobState() {
      return JobState.parse(currentJob.getCurrentState());
    }

    public void cancel() {
      if (this.cancelled || !isAlive()) {
        return;
      }

      LOG.info("Finishing job {}...", currentJob.getId());

      this.cancelled = true;
      currentJob.setCurrentState(JobState.CANCELLED.toString());

      try {
        this.stop();
      } catch (Exception e) {
        LOG.warn("Error cancelling job", e);
      }
    }
  }
}
