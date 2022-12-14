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
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default class for implementation of {@link DataflowClient} interface. */
public class DefaultDataflowClient extends AbstractDataflowClient {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultDataflowClient.class);
  private static final Pattern JOB_ID_PATTERN = Pattern.compile("Submitted job: (?<jobId>\\S+)");

  DefaultDataflowClient(Dataflow client) {
    super(client);
  }

  private DefaultDataflowClient(DefaultDataflowClient.Builder builder) {
    this(
        new Dataflow(
            Utils.getDefaultTransport(),
            Utils.getDefaultJsonFactory(),
            new HttpCredentialsAdapter(builder.getCredentials())));
  }

  public static DefaultDataflowClient withDataflowClient(Dataflow client) {
    return new DefaultDataflowClient(client);
  }

  public static DefaultDataflowClient.Builder builder() {
    return new DefaultDataflowClient.Builder();
  }

  @Override
  public JobInfo launch(String project, String region, LaunchConfig options) throws IOException {
    checkState(
        options.sdk() != null,
        "Cannot launch a dataflow job "
            + "without sdk specified. Please specify sdk and try again!");
    checkState(
        options.executable() != null,
        "Cannot launch a dataflow job "
            + "without executable specified. Please specify executable and try again!");
    LOG.info("Getting ready to launch {} in {} under {}", options.jobName(), region, project);
    LOG.info("Using the executable at {}", options.executable());
    LOG.info("Using parameters:\n{}", formatForLogging(options.parameters()));
    // Create SDK specific command and execute to launch dataflow job
    List<String> cmd = new ArrayList<>();
    switch (options.sdk()) {
      case JAVA:
        checkState(
            options.mainClassname() != null,
            "Cannot launch a dataflow job "
                + "without classname specified. Please specify classname and try again!");
        cmd.add("java");
        cmd.add("-cp");
        cmd.add(options.executable());
        cmd.add(options.mainClassname());
        break;
      case PYTHON:
        cmd.add("python3");
        cmd.add(options.executable());
        break;
      case GO:
        cmd.add("go");
        cmd.add("run");
        cmd.add(options.executable());
        break;
      default:
        throw new RuntimeException(
            String.format(
                "Invalid sdk %s specified. " + "sdk can be one of java, python, or go.",
                options.sdk()));
    }
    for (String parameter : options.parameters().keySet()) {
      cmd.add(String.format("--%s=%s", parameter, options.getParameter(parameter)));
    }
    cmd.add(String.format("--project=%s", project));
    cmd.add(String.format("--region=%s", region));
    String jobId = executeCommandAndParseResponse(cmd);
    // Wait until the job is active to get more information
    JobState state = waitUntilActive(project, region, jobId);
    Job job = getJob(project, region, jobId);
    return getJobInfo(options, state, job, options.getParameter("runner"));
  }

  /** Executes the specified command and parses the response to get the Job ID. */
  private String executeCommandAndParseResponse(List<String> cmd) throws IOException {
    Process process = new ProcessBuilder().command(cmd).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    Matcher m = JOB_ID_PATTERN.matcher(output);
    if (!m.find()) {
      throw new RuntimeException(
          String.format(
              "Dataflow output in unexpected format. Failed to parse Dataflow Job ID. "
                  + "Result from process: %s",
              output));
    }
    String jobId = m.group("jobId");
    LOG.info("Submitted job: {}", jobId);
    return jobId;
  }

  /** Builder for {@link DefaultDataflowClient}. */
  public static final class Builder {
    private Credentials credentials;

    private Builder() {}

    public Credentials getCredentials() {
      return credentials;
    }

    public DefaultDataflowClient.Builder setCredentials(Credentials value) {
      credentials = value;
      return this;
    }

    public DefaultDataflowClient build() {
      return new DefaultDataflowClient(this);
    }
  }
}
