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

import static com.google.cloud.teleport.it.logging.LogStrings.formatForLogging;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link DataflowClient} interface which invokes the template class using
 * DirectRunner, and manages the state in-memory.
 */
public class DirectRunnerClient extends AbstractDataflowClient {
  private static final Logger LOG = LoggerFactory.getLogger(DirectRunnerClient.class);

  private static final Map<String, DirectRunnerJobThread> MANAGED_JOBS = new HashMap<>();

  private final Class<?> mainClass;

  DirectRunnerClient(Builder builder) {
    super(new DirectRunnerDataflowFacade(builder.getMainClass(), builder.getCredentials()));
    this.mainClass = builder.getMainClass();
  }

  public static DirectRunnerClient.Builder builder(Class<?> mainClass) {
    return new DirectRunnerClient.Builder(mainClass);
  }

  @Override
  public JobInfo launch(String project, String region, LaunchConfig options) throws IOException {

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
      MANAGED_JOBS.put(jobId, jobThread);
      jobThread.start();

      return JobInfo.builder()
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

  /** Builder for {@link DirectRunnerClient}. */
  public static final class Builder {
    private Credentials credentials;
    private Class<?> mainClass;

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

  static class DirectRunnerDataflowFacade extends Dataflow {

    public DirectRunnerDataflowFacade(Class<?> mainClass, Credentials credentials) {
      super(
          Utils.getDefaultTransport(),
          Utils.getDefaultJsonFactory(),
          new HttpCredentialsAdapter(credentials));
    }

    @Override
    public Projects projects() {
      return new DirectRunnerProjects();
    }

    class DirectRunnerProjects extends Projects {

      @Override
      public Locations locations() {
        return new DirectRunnerLocations();
      }

      class DirectRunnerLocations extends Locations {

        @Override
        public Jobs jobs() {
          return new DirectRunnerLocationJobs();
        }

        class DirectRunnerLocationJobs extends Jobs {

          @Override
          public Get get(String projectId, String location, String jobId) throws IOException {
            return new DirectRunnerGetJob(projectId, location, jobId);
          }

          @Override
          public Update update(String projectId, String location, String jobId, Job content)
              throws IOException {
            return new DirectRunnerUpdateJob(projectId, location, jobId, content);
          }

          class DirectRunnerGetJob extends Get {

            private String jobId;

            public DirectRunnerGetJob(String projectId, String location, String jobId) {
              super(projectId, location, jobId);
              this.jobId = jobId;
            }

            @Override
            public Job execute() throws IOException {
              return MANAGED_JOBS.get(jobId).getJob();
            }
          }

          private class DirectRunnerUpdateJob extends Update {

            private final String jobId;
            private final Job updateJob;

            public DirectRunnerUpdateJob(
                String projectId, String location, String jobId, Job updateJob) {
              super(projectId, location, jobId, updateJob);
              this.jobId = jobId;
              this.updateJob = updateJob;
            }

            @Override
            public Job execute() throws IOException {
              DirectRunnerJobThread jobThread = MANAGED_JOBS.get(jobId);
              Job job = jobThread.getJob();
              job.putAll(updateJob);

              // Make state transitions instant
              String requestedState = (String) job.get("requestedState");
              String currentState = (String) job.get("currentState");
              if (requestedState != null && !requestedState.equals(currentState)) {
                job.put("currentState", requestedState);

                JobState jobState = JobState.parse(requestedState);
                if (jobState == JobState.CANCELLED || jobState == JobState.DRAINED) {
                  jobThread.cancel();
                }
              }

              return job;
            }
          }
        }
      }
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
      this.currentJob = new Job().setProjectId(projectId).setLocation(location).setId(jobId);
      this.mainClass = mainClass;
      this.commandLines = commandLines;
    }

    @Override
    public void run() {

      try {
        String[] args = commandLines.toArray(new String[0]);
        Method mainMethod = mainClass.getDeclaredMethod("main", String[].class);
        currentJob.setCurrentState(JobState.RUNNING.toString());

        LOG.info("Starting job {}...", currentJob.getId());
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

    public void cancel() {
      if (!isAlive()) {
        return;
      }

      LOG.info("Finishing job {}...", currentJob.getId());

      this.cancelled = true;

      try {
        this.stop();
      } catch (Exception e) {
        LOG.warn("Error cancelling job", e);
      }
    }
  }
}
