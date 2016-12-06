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
package org.apache.beam.runners.dataflow;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.Dataflow.Projects.Jobs;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.LeaseWorkItemRequest;
import com.google.api.services.dataflow.model.LeaseWorkItemResponse;
import com.google.api.services.dataflow.model.ListJobMessagesResponse;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.api.services.dataflow.model.ReportWorkItemStatusRequest;
import com.google.api.services.dataflow.model.ReportWorkItemStatusResponse;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

/**
 * Wrapper around the generated {@link Dataflow} client to provide common functionality.
 */
public class DataflowClient {

  public static DataflowClient create(DataflowPipelineOptions options) {
    return new DataflowClient(options.getDataflowClient(), options.getProject());
  }

  private final Dataflow dataflow;
  private final String projectId;

  private DataflowClient(Dataflow dataflow, String projectId) {
    this.dataflow = checkNotNull(dataflow, "dataflow");
    this.projectId = checkNotNull(projectId, "options");
  }

  /**
   * Creates the Dataflow {@link Job}.
   */
  public Job createJob(@Nonnull Job job) throws IOException {
    Jobs.Create jobsCreate = dataflow.projects().jobs().create(projectId, job);
    return jobsCreate.execute();
  }

  /**
   * Lists Dataflow {@link Job Jobs} in the project associated with
   * the {@link DataflowPipelineOptions}.
   */
  public ListJobsResponse listJobs(@Nullable String pageToken) throws IOException {
    Jobs.List jobsList = dataflow.projects().jobs()
        .list(projectId)
        .setPageToken(pageToken);
    return jobsList.execute();
  }

  /**
   * Updates the Dataflow {@link Job} with the given {@code jobId}.
   */
  public Job updateJob(@Nonnull String jobId, @Nonnull Job content) throws IOException {
    Jobs.Update jobsUpdate = dataflow.projects().jobs()
        .update(projectId, jobId, content);
    return jobsUpdate.execute();
  }

  /**
   * Gets the Dataflow {@link Job} with the given {@code jobId}.
   */
  public Job getJob(@Nonnull String jobId) throws IOException {
    Jobs.Get jobsGet = dataflow.projects().jobs()
        .get(projectId, jobId);
    return jobsGet.execute();
  }

  /**
   * Gets the {@link JobMetrics} with the given {@code jobId}.
   */
  public JobMetrics getJobMetrics(@Nonnull String jobId) throws IOException {
    Jobs.GetMetrics jobsGetMetrics = dataflow.projects().jobs()
        .getMetrics(projectId, jobId);
    return jobsGetMetrics.execute();
  }

  /**
   * Lists job messages with the given {@code jobId}.
   */
  public ListJobMessagesResponse listJobMessages(
      @Nonnull String jobId, @Nullable String pageToken) throws IOException {
    Jobs.Messages.List jobMessagesList = dataflow.projects().jobs().messages()
        .list(projectId, jobId)
        .setPageToken(pageToken);
    return jobMessagesList.execute();
  }

  /**
   * Leases the work item for {@code jobId}.
   */
  public LeaseWorkItemResponse leaseWorkItem(
      @Nonnull String jobId, @Nonnull LeaseWorkItemRequest request) throws IOException {
    Jobs.WorkItems.Lease jobWorkItemsLease = dataflow.projects().jobs().workItems()
        .lease(projectId, jobId, request);
    return jobWorkItemsLease.execute();
  }

  /**
   * Reports the status of the work item for {@code jobId}.
   */
  public ReportWorkItemStatusResponse reportWorkItemStatus(
      @Nonnull String jobId, @Nonnull ReportWorkItemStatusRequest request) throws IOException {
    Jobs.WorkItems.ReportStatus jobWorkItemsReportStatus = dataflow.projects().jobs().workItems()
        .reportStatus(projectId, jobId, request);
    return jobWorkItemsReportStatus.execute();
  }
}
