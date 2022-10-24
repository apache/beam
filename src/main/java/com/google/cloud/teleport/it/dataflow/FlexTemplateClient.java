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
import com.google.api.services.dataflow.model.LaunchFlexTemplateParameter;
import com.google.api.services.dataflow.model.LaunchFlexTemplateRequest;
import com.google.api.services.dataflow.model.LaunchFlexTemplateResponse;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for interacting with Dataflow Flex Templates using the Dataflow SDK. */
public final class FlexTemplateClient extends AbstractDataflowTemplateClient {
  private static final Logger LOG = LoggerFactory.getLogger(FlexTemplateClient.class);

  private FlexTemplateClient(Builder builder) {
    super(
        new Dataflow(
            Utils.getDefaultTransport(),
            Utils.getDefaultJsonFactory(),
            new HttpCredentialsAdapter(builder.getCredentials())));
  }

  private FlexTemplateClient(Dataflow dataflow) {
    super(dataflow);
  }

  public static FlexTemplateClient withDataflowClient(Dataflow dataflow) {
    return new FlexTemplateClient(dataflow);
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

    LaunchFlexTemplateParameter parameter =
        new LaunchFlexTemplateParameter()
            .setJobName(options.jobName())
            .setParameters(options.parameters())
            .setContainerSpecGcsPath(options.specPath());
    LaunchFlexTemplateRequest request =
        new LaunchFlexTemplateRequest().setLaunchParameter(parameter);
    LOG.info("Sending request:\n{}", formatForLogging(request));

    LaunchFlexTemplateResponse response =
        client.projects().locations().flexTemplates().launch(project, region, request).execute();
    LOG.info("Received response:\n{}", formatForLogging(response));

    Job job = response.getJob();

    LOG.info(
        "Dataflow Console: https://console.cloud.google.com/dataflow/jobs/{}/{}?project={}",
        job.getLocation(),
        job.getId(),
        job.getProjectId());

    // The initial response will not return the state, so need to explicitly get it
    JobState state = getJobStatus(project, region, job.getId());
    return JobInfo.builder().setJobId(job.getId()).setState(state).build();
  }

  /** Builder for {@link FlexTemplateClient}. */
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

    public FlexTemplateClient build() {
      return new FlexTemplateClient(this);
    }
  }
}
