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
import static org.apache.beam.it.common.utils.RetryUtil.clientRetryPolicy;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.CreateJobFromTemplateRequest;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import dev.failsafe.Failsafe;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for interacting with Dataflow classic templates using the Dataflow SDK. */
public final class ClassicTemplateClient extends AbstractPipelineLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(ClassicTemplateClient.class);

  private ClassicTemplateClient(Builder builder) {
    super(
        new Dataflow(
            Utils.getDefaultTransport(),
            Utils.getDefaultJsonFactory(),
            new HttpCredentialsAdapter(builder.getCredentials())));
  }

  private ClassicTemplateClient(Dataflow dataflow) {
    super(dataflow);
  }

  public static ClassicTemplateClient withDataflowClient(Dataflow dataflow) {
    return new ClassicTemplateClient(dataflow);
  }

  public static Builder builder(Credentials credentials) {
    return new Builder(credentials);
  }

  @Override
  public LaunchInfo launch(String project, String region, LaunchConfig options) throws IOException {
    checkState(
        options.specPath() != null,
        "Cannot launch a template job without specPath. Please specify specPath and try again!");
    LOG.info("Getting ready to launch {} in {} under {}", options.jobName(), region, project);
    LOG.info("Using the spec at {}", options.specPath());
    LOG.info("Using parameters:\n{}", formatForLogging(options.parameters()));

    @SuppressWarnings("nullness")
    CreateJobFromTemplateRequest parameter =
        new CreateJobFromTemplateRequest()
            .setJobName(options.jobName())
            .setParameters(options.parameters())
            .setLocation(region)
            .setGcsPath(options.specPath())
            .setEnvironment(buildEnvironment(options));
    LOG.info("Sending request:\n{}", formatForLogging(parameter));

    Job job =
        Failsafe.with(clientRetryPolicy())
            .get(
                () ->
                    client
                        .projects()
                        .locations()
                        .templates()
                        .create(project, region, parameter)
                        .execute());
    printJobResponse(job);

    // Wait until the job is active to get more information
    JobState state = waitUntilActive(project, region, job.getId());
    job = getJob(project, region, job.getId(), "JOB_VIEW_DESCRIPTION");
    LOG.info("Received classic template job {}: {}", job.getId(), formatForLogging(job));

    launchedJobs.add(job.getId());
    return getJobInfo(options, state, job);
  }

  private RuntimeEnvironment buildEnvironment(LaunchConfig options) {
    RuntimeEnvironment environment = new RuntimeEnvironment();
    environment.putAll(options.environment());
    return environment;
  }

  /** Builder for {@link ClassicTemplateClient}. */
  public static final class Builder {

    private Credentials credentials;

    private Builder(Credentials credentials) {
      this.credentials = credentials;
    }

    public Credentials getCredentials() {
      return credentials;
    }

    public Builder setCredentials(Credentials value) {
      credentials = value;
      return this;
    }

    public ClassicTemplateClient build() {
      return new ClassicTemplateClient(this);
    }
  }
}
