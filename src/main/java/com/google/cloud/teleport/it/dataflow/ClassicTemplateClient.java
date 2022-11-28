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
import com.google.api.services.dataflow.model.CreateJobFromTemplateRequest;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for interacting with Dataflow Classic Templates using the Dataflow SDK. */
public final class ClassicTemplateClient extends AbstractDataflowTemplateClient {
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

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public JobInfo launchTemplate(String project, String region, LaunchConfig options)
      throws IOException {
    LOG.info("Getting ready to launch {} in {} under {}", options.jobName(), region, project);
    LOG.info("Using the spec at {}", options.specPath());
    LOG.info("Using parameters:\n{}", formatForLogging(options.parameters()));

    CreateJobFromTemplateRequest parameter =
        new CreateJobFromTemplateRequest()
            .setJobName(options.jobName())
            .setParameters(options.parameters())
            .setLocation(region)
            .setGcsPath(options.specPath())
            .setEnvironment(buildEnvironment(options));
    LOG.info("Sending request:\n{}", formatForLogging(parameter));

    Job job =
        client.projects().locations().templates().create(project, region, parameter).execute();
    printJobResponse(job);

    // The initial response will not return the state, so need to explicitly get it
    JobState state = getJobStatus(project, region, job.getId());
    return JobInfo.builder().setJobId(job.getId()).setState(state).build();
  }

  private RuntimeEnvironment buildEnvironment(LaunchConfig options) {
    RuntimeEnvironment environment = new RuntimeEnvironment();
    environment.putAll(options.environment());
    return environment;
  }

  /** Builder for {@link ClassicTemplateClient}. */
  public static final class Builder {
    private Credentials credentials;

    private Builder() {}

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
