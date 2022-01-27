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

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.cloud.teleport.it.logging.LogStrings;
import com.google.common.base.Strings;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class covering the common operations between Classic and Flex templates.
 *
 * <p>Generally, the methods here are the ones that focus more on the Dataflow jobs rather than
 * launching a specific type of template.
 */
abstract class AbstractDataflowTemplateClient implements DataflowTemplateClient {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractDataflowTemplateClient.class);

  protected final Dataflow client;

  AbstractDataflowTemplateClient(Dataflow client) {
    this.client = client;
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

  /** Parses the job state if available or returns {@link JobState#UNKNOWN} if not given. */
  protected static JobState handleJobState(Job job) {
    String currentState = job.getCurrentState();
    return Strings.isNullOrEmpty(currentState) ? JobState.UNKNOWN : JobState.parse(currentState);
  }
}
