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
package org.apache.beam.testinfra.pipelines;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.dataflow.v1beta3.GetJobExecutionDetailsRequest;
import com.google.dataflow.v1beta3.GetJobMetricsRequest;
import com.google.dataflow.v1beta3.GetJobRequest;
import com.google.dataflow.v1beta3.Job;
import com.google.events.cloud.dataflow.v1beta3.JobState;
import com.google.events.cloud.dataflow.v1beta3.JobType;
import java.time.Instant;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.testinfra.pipelines.bigquery.BigQueryWriteOptions;
import org.apache.beam.testinfra.pipelines.bigquery.BigQueryWrites;
import org.apache.beam.testinfra.pipelines.conversions.ConversionError;
import org.apache.beam.testinfra.pipelines.conversions.ConversionErrorsToString;
import org.apache.beam.testinfra.pipelines.conversions.DataflowRequestErrorsToString;
import org.apache.beam.testinfra.pipelines.conversions.EventarcConversions;
import org.apache.beam.testinfra.pipelines.conversions.JobsToRow;
import org.apache.beam.testinfra.pipelines.conversions.RowConversionResult;
import org.apache.beam.testinfra.pipelines.conversions.WithAppendedDetailsToRow;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowClientFactoryConfiguration;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowFilterEventarcJobs;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowGetJobExecutionDetails;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowGetJobMetrics;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowGetJobs;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowJobsOptions;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowReadResult;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowRequestError;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowRequests;
import org.apache.beam.testinfra.pipelines.dataflow.JobMetricsWithAppendedDetails;
import org.apache.beam.testinfra.pipelines.dataflow.StageSummaryWithAppendedDetails;
import org.apache.beam.testinfra.pipelines.dataflow.WorkerDetailsWithAppendedDetails;
import org.apache.beam.testinfra.pipelines.pubsub.PubsubReadOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

public class ReadDataflowApiWriteBigQuery {

  public interface Options extends DataflowJobsOptions, PubsubReadOptions, BigQueryWriteOptions {}

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    // Use earlier retrieved Jobs to retrieve metrics.
    DataflowReadResult<JobMetricsWithAppendedDetails, DataflowRequestError<GetJobMetricsRequest>>
        getJobsMetricsResult =
        getJobsResult
            .getSuccess()
            .apply(
                tagOf(DataflowGetJobMetrics.class),
                DataflowGetJobMetrics.create(configuration)
            );

    // Use earlier retrieved Jobs to retrieve execution details.
    DataflowReadResult<StageSummaryWithAppendedDetails, DataflowRequestError<GetJobExecutionDetailsRequest>>
        getJobExecutionDetailsResult =
        getJobsResult
            .getSuccess()
            .apply(
                tagOf(DataflowGetJobExecutionDetails.class),
                DataflowGetJobExecutionDetails.create(configuration)
            );

    // Use earlier retrieved Jobs to retrieve worker details.
    DataflowReadResult<WorkerDetailsWithAppendedDetails, DataflowReadResult<>>

    // Convert Job Metrics to Rows.
    RowConversionResult<JobMetricsWithAppendedDetails>
        metricsToRowsResult = getJobsMetricsResult.getSuccess()
        .apply(
            tagOf(WithAppendedDetailsToRow.class, "Metrics"),
            WithAppendedDetailsToRow.jobMetricsWithAppendedDetailsToRow()
        );

    events.failures().apply(
        tagOf(BigQueryWrites.class, "writeConversionErrors"),
        BigQueryWrites.writeConversionErrors(options)
    );

    getJobsResult.getFailure()
        .apply(
            tagOf(DataflowRequestErrorsToString.class, "GetJobRequest"),
            DataflowRequestErrorsToString.create())
        .apply(
            tagOf(BigQueryWrites.class, "GetJobRequests Errors"),
            BigQueryWrites.dataflowGetJobsErrors(options));

    jobsToRowResult.getFailure()
        .apply(
            tagOf(ConversionErrorsToString.class, "ConversionError<Job>"),
            ConversionErrorsToString.create()
        )
        .apply(
            tagOf(BigQueryWrites.class, "ConversionError<Job>"),
            BigQueryWrites.writeConversionErrors(options)
        );

     getJobsMetricsResult.getFailure()
         .apply(
             tagOf(DataflowRequestErrorsToString.class, "GetJobMetrics Errors"),
             DataflowRequestErrorsToString.create()
         )
         .apply(
             tagOf(BigQueryWrites.class, "GetJobMetrics Errors"),
             BigQueryWrites.dataflowGetJobMetricsErrors(options)
         );

    pipeline.run();
  }

  private static PCollection<Job> jobs(Pipeline pipeline, Options options) {
    DataflowClientFactoryConfiguration configuration =
        DataflowClientFactoryConfiguration.builder(options).build();

    // Read from Eventarc published Pub/Sub events.
    PCollection<String> json =
        pipeline.apply(
            tagOf(PubsubIO.Read.class, "Eventarc"),
            PubsubIO.readStrings()
                .fromSubscription(options.getSubscription().getValue().getPath()));

    // Encode Eventarc JSON payloads into Eventarc Dataflow Jobs.
    WithFailures.Result<
        @NonNull PCollection<com.google.events.cloud.dataflow.v1beta3.Job>,
        ConversionError<String>>
        events = json.apply(
        tagOf(EventarcConversions.class, "fromJson"),
        EventarcConversions.fromJson()
    );

    // Filter Done Batch Jobs.
    PCollection<GetJobRequest> getBatchJobRequests = events.output().apply(
            tagOf(DataflowFilterEventarcJobs.class, "Done Batch Jobs"),
            DataflowFilterEventarcJobs.builder()
                .setIncludeJobStates(ImmutableList.of(
                    JobState.JOB_STATE_DONE
                ))
                .setIncludeJobType(JobType.JOB_TYPE_BATCH)
                .build())
        .apply(
            tagOf(DataflowRequests.class, "Batch GetJobRequests"),
            DataflowRequests.jobRequestsFromEventsViewAll());

    // Filter Canceled Streaming Jobs.
    PCollection<GetJobRequest> getStreamJobRequests = events.output().apply(
            tagOf(DataflowFilterEventarcJobs.class, "Canceled Streaming Jobs"),
            DataflowFilterEventarcJobs.builder()
                .setIncludeJobStates(ImmutableList.of(
                    JobState.JOB_STATE_CANCELLED
                ))
                .setIncludeJobType(JobType.JOB_TYPE_STREAMING)
                .build())
        .apply(
            tagOf(DataflowRequests.class, "Stream GetJobRequests"),
            DataflowRequests.jobRequestsFromEventsViewAll());

    // Merge Batch and Streaming Jobs.
    PCollectionList<GetJobRequest> getJobRequestList = PCollectionList.of(getBatchJobRequests).and(getStreamJobRequests);
    PCollection<GetJobRequest> getJobRequests = getJobRequestList.apply("Merge Batch and Streaming Jobs",
        Flatten.pCollections());

    // Call the Dataflow GetJobs endpoint.
    DataflowReadResult<Job, DataflowRequestError<GetJobRequest>> getJobsResult =
        getJobRequests.apply(
            tagOf(DataflowGetJobs.class),
            DataflowGetJobs.create(configuration));

    // Convert Jobs to Rows.
    RowConversionResult<Job> jobsToRowResult =
        getJobsResult.getSuccess().apply(
            tagOf(JobsToRow.class, "Job"),
            JobsToRow.create()
        );

    // Write Job Rows to BigQuery.
    jobsToRowResult.getSuccess()
        .apply(
            tagOf(BigQueryWrites.class, "Job"),
            BigQueryWrites.dataflowJobs(options)
        );

    return getJobsResult.getSuccess();
  }

  private static void metrics(PCollection<Job> jobs, Options options) {
    DataflowClientFactoryConfiguration configuration =
        DataflowClientFactoryConfiguration.builder(options).build();


  }

  private static void executionDetails(PCollection<Job> jobs, Options options) {
    DataflowClientFactoryConfiguration configuration =
        DataflowClientFactoryConfiguration.builder(options).build();


  }

  private static void stageExecutionDetails(PCollection<Job> jobs) {
    DataflowClientFactoryConfiguration configuration =
        DataflowClientFactoryConfiguration.builder(options).build();


  }
  private static String tagOf(Class<?> clazz, String... addl) {
    return clazz.getSimpleName() + " " + String.join(" ", addl);
  }
}
