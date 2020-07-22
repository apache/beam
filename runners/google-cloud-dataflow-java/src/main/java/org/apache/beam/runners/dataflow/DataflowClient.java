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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs;
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
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Wrapper around the generated {@link Dataflow} client to provide common functionality. */
public class DataflowClient {

  public static DataflowClient create(DataflowPipelineOptions options) {
    return new DataflowClient(options.getDataflowClient(), options);
  }

  private final Dataflow dataflow;
  private final DataflowPipelineOptions options;

  private DataflowClient(Dataflow dataflow, DataflowPipelineOptions options) {
    this.dataflow = checkNotNull(dataflow, "dataflow");
    this.options = checkNotNull(options, "options");
  }

  /** Creates the Dataflow {@link Job}. */
  public Job createJob(@Nonnull Job job) throws IOException {
    checkNotNull(job, "job");
    Jobs.Create jobsCreate =
        dataflow
            .projects()
            .locations()
            .jobs()
            .create(options.getProject(), options.getRegion(), job);
    return jobsCreate.execute();
  }

  /**
   * Lists Dataflow {@link Job Jobs} in the project associated with the {@link
   * DataflowPipelineOptions}.
   */
  public ListJobsResponse listJobs(@Nullable String pageToken) throws IOException {
    Jobs.List jobsList =
        dataflow
            .projects()
            .locations()
            .jobs()
            .list(options.getProject(), options.getRegion())
            .setPageToken(pageToken);
    return jobsList.execute();
  }

  /** Updates the Dataflow {@link Job} with the given {@code jobId}. */
  public Job updateJob(@Nonnull String jobId, @Nonnull Job content) throws IOException {
    checkNotNull(jobId, "jobId");
    checkNotNull(content, "content");
    Jobs.Update jobsUpdate =
        dataflow
            .projects()
            .locations()
            .jobs()
            .update(options.getProject(), options.getRegion(), jobId, content);
    return jobsUpdate.execute();
  }

  /** Gets the Dataflow {@link Job} with the given {@code jobId}. */
  public Job getJob(@Nonnull String jobId) throws IOException {
    checkNotNull(jobId, "jobId");
    Jobs.Get jobsGet =
        dataflow
            .projects()
            .locations()
            .jobs()
            .get(options.getProject(), options.getRegion(), jobId);
    return jobsGet.execute();
  }

  /** Gets the {@link JobMetrics} with the given {@code jobId}. */
  public JobMetrics getJobMetrics(@Nonnull String jobId) throws IOException {
    checkNotNull(jobId, "jobId");
    Jobs.GetMetrics jobsGetMetrics =
        dataflow
            .projects()
            .locations()
            .jobs()
            .getMetrics(options.getProject(), options.getRegion(), jobId);
    return jobsGetMetrics.execute();
  }

  /** Lists job messages with the given {@code jobId}. */
  public ListJobMessagesResponse listJobMessages(@Nonnull String jobId, @Nullable String pageToken)
      throws IOException {
    checkNotNull(jobId, "jobId");
    Jobs.Messages.List jobMessagesList =
        dataflow
            .projects()
            .locations()
            .jobs()
            .messages()
            .list(options.getProject(), options.getRegion(), jobId)
            .setPageToken(pageToken);
    return jobMessagesList.execute();
  }

  /** Leases the work item for {@code jobId}. */
  @SuppressWarnings("unused") // used internally in the Cloud Dataflow execution environment.
  public LeaseWorkItemResponse leaseWorkItem(
      @Nonnull String jobId, @Nonnull LeaseWorkItemRequest request) throws IOException {
    checkNotNull(jobId, "jobId");
    checkNotNull(request, "request");
    Jobs.WorkItems.Lease jobWorkItemsLease =
        dataflow
            .projects()
            .locations()
            .jobs()
            .workItems()
            .lease(options.getProject(), options.getRegion(), jobId, request);
    return jobWorkItemsLease.execute();
  }

  /** Reports the status of the work item for {@code jobId}. */
  @SuppressWarnings("unused") // used internally in the Cloud Dataflow execution environment.
  public ReportWorkItemStatusResponse reportWorkItemStatus(
      @Nonnull String jobId, @Nonnull ReportWorkItemStatusRequest request) throws IOException {
    checkNotNull(jobId, "jobId");
    checkNotNull(request, "request");
    Jobs.WorkItems.ReportStatus jobWorkItemsReportStatus =
        dataflow
            .projects()
            .locations()
            .jobs()
            .workItems()
            .reportStatus(options.getProject(), options.getRegion(), jobId, request);
    return jobWorkItemsReportStatus.execute();
  }
}
