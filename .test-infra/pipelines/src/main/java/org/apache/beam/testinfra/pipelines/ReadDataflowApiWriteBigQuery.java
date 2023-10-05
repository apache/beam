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

import static org.apache.beam.sdk.values.TypeDescriptors.rows;

import com.google.dataflow.v1beta3.GetJobExecutionDetailsRequest;
import com.google.dataflow.v1beta3.GetJobMetricsRequest;
import com.google.dataflow.v1beta3.GetJobRequest;
import com.google.dataflow.v1beta3.Job;
import com.google.events.cloud.dataflow.v1beta3.JobState;
import com.google.events.cloud.dataflow.v1beta3.JobType;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.testinfra.pipelines.bigquery.BigQueryWriteOptions;
import org.apache.beam.testinfra.pipelines.bigquery.BigQueryWrites;
import org.apache.beam.testinfra.pipelines.conversions.ConversionError;
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
import org.apache.beam.testinfra.pipelines.pubsub.PubsubReadOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Constructs and executes a {@link Pipeline} that reads from the Dataflow API and writes to
 * BigQuery. For internal use only.
 */
@Internal
public class ReadDataflowApiWriteBigQuery {

  public interface Options extends DataflowJobsOptions, PubsubReadOptions, BigQueryWriteOptions {}

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);
    DataflowClientFactoryConfiguration configuration =
        DataflowClientFactoryConfiguration.builder(options).build();

    // Retrieve Jobs calling the JobsV1Beta3.GetJob rpc.
    PCollection<Job> jobs = readEvents(options, pipeline);

    // Retrieve JobMetrics calling the MetricsV1Beta3.GetJobMetrics rpc.
    readDetailsFromDataflowJobs(
        options,
        BigQueryWrites.JOB_METRICS_ERRORS,
        GetJobMetricsRequest.class,
        JobMetricsWithAppendedDetails.class,
        jobs,
        DataflowGetJobMetrics.create(configuration),
        WithAppendedDetailsToRow.jobMetricsWithAppendedDetailsToRow(),
        BigQueryWrites.dataflowJobMetrics(options));

    // Retrieve StageSummary entries (from JobExecutionDetails) calling the
    // MetricsV1Beta3.GetJobExecutionDetails rpc.
    readDetailsFromDataflowJobs(
        options,
        BigQueryWrites.JOB_EXECUTION_DETAILS_ERRORS,
        GetJobExecutionDetailsRequest.class,
        StageSummaryWithAppendedDetails.class,
        jobs,
        DataflowGetJobExecutionDetails.create(configuration),
        WithAppendedDetailsToRow.stageSummaryWithAppendedDetailsToRow(),
        BigQueryWrites.dataflowJobExecutionDetails(options));

    // Retrieve WorkerDetails (from StageExecutionDetails) calling the
    // MetricsV1Beta3.GetStageExecutionDetails rpc.
    // Not working at this time and left commented out to revisit in the future.
    //    jobDetails(
    //        options,
    //        BigQueryWrites.STAGE_EXECUTION_DETAILS_ERRORS,
    //        GetStageExecutionDetailsRequest.class,
    //        WorkerDetailsWithAppendedDetails.class,
    //        jobs,
    //        DataflowGetStageExecutionDetails.create(configuration),
    //        WithAppendedDetailsToRow.workerDetailsWithAppendedDetailsToRow(),
    //        BigQueryWrites.dataflowStageExecutionDetails(options));

    pipeline.run();
  }

  private static <RequestT extends GeneratedMessageV3, ResponseT> void writeErrors(
      Options options,
      String requestErrorTableIdPrefix,
      Class<RequestT> requestTClass,
      Class<ResponseT> responseTClass,
      PCollection<DataflowRequestError> requestErrors,
      PCollection<ConversionError> responseConversionErrors) {

    // Write Dataflow API errors to BigQuery.
    PCollection<Row> requestErrorRows =
        requestErrors
            .apply(
                tagOf(ConversionError.class, DataflowRequestError.class, requestTClass),
                MapElements.into(rows()).via(DataflowRequestError.TO_ROW_FN))
            .setRowSchema(DataflowRequestError.SCHEMA);

    requestErrorRows.apply(
        tagOf(BigQueryWrites.class, requestTClass.getSimpleName(), "errors"),
        BigQueryWrites.writeDataflowRequestErrors(options, requestErrorTableIdPrefix));

    // Write conversion errors to BigQuery.
    PCollection<Row> conversionErrors =
        responseConversionErrors
            .apply(
                tagOf(ConversionError.class, responseTClass),
                MapElements.into(rows()).via(ConversionError.TO_ROW_FN))
            .setRowSchema(ConversionError.SCHEMA);

    conversionErrors.apply(
        tagOf(BigQueryWrites.class, responseTClass.getSimpleName(), "errors"),
        BigQueryWrites.writeConversionErrors(options));
  }

  private static PCollection<Job> readEvents(Options options, Pipeline pipeline) {

    // Read from Eventarc published Pub/Sub events.
    PCollection<String> json =
        pipeline.apply(
            tagOf(PubsubIO.Read.class, "Eventarc"),
            PubsubIO.readStrings()
                .fromSubscription(options.getSubscription().getValue().getPath()));

    json.apply(
        "Count Pub/Sub messages", ParDo.of(countFn(PubsubIO.Read.class, "pulled_pubsub_messages")));

    // Encode Eventarc JSON payloads into Eventarc Dataflow Jobs.
    WithFailures.Result<
            @NonNull PCollection<com.google.events.cloud.dataflow.v1beta3.Job>, ConversionError>
        events =
            json.apply(
                tagOf(EventarcConversions.class, "fromJson"), EventarcConversions.fromJson());

    events
        .output()
        .apply(
            "Count Encoded Events",
            ParDo.of(countFn(EventarcConversions.class, "encode_events_success")));
    events
        .failures()
        .apply(
            "Count Encoded Failures",
            ParDo.of(countFn(EventarcConversions.class, "encoded_events_failure")));

    // Write Eventarc encoding errors to BigQuery.
    PCollection<Row> eventConversionErrorRows =
        events
            .failures()
            .apply(
                "Event Conversion Errors To Row",
                MapElements.into(rows()).via(ConversionError.TO_ROW_FN))
            .setRowSchema(ConversionError.SCHEMA);

    eventConversionErrorRows.apply(
        tagOf(BigQueryWrites.class, com.google.events.cloud.dataflow.v1beta3.Job.class.getName()),
        BigQueryWrites.writeConversionErrors(options));

    return readDataflowJobsFromEvents(options, events.output());
  }

  private static PCollection<Job> readDataflowJobsFromEvents(
      Options options, PCollection<com.google.events.cloud.dataflow.v1beta3.Job> events) {
    DataflowClientFactoryConfiguration configuration =
        DataflowClientFactoryConfiguration.builder(options).build();

    // Filter Done Batch Jobs.
    PCollection<GetJobRequest> getBatchJobRequests =
        events
            .apply(
                tagOf(DataflowFilterEventarcJobs.class, "Done Batch Jobs"),
                DataflowFilterEventarcJobs.builder()
                    .setIncludeJobStates(ImmutableList.of(JobState.JOB_STATE_DONE))
                    .setIncludeJobType(JobType.JOB_TYPE_BATCH)
                    .build())
            .apply(
                tagOf(DataflowRequests.class, "Batch GetJobRequests"),
                DataflowRequests.jobRequestsFromEventsViewAll());

    getBatchJobRequests.apply(
        "Count Done Batch Jobs", ParDo.of(countFn(GetJobRequest.class, "done_batch_jobs")));

    // Filter Canceled Streaming Jobs.
    PCollection<GetJobRequest> getStreamCanceledJobRequests =
        events
            .apply(
                tagOf(DataflowFilterEventarcJobs.class, "Canceled Streaming Jobs"),
                DataflowFilterEventarcJobs.builder()
                    .setIncludeJobStates(ImmutableList.of(JobState.JOB_STATE_CANCELLED))
                    .setIncludeJobType(JobType.JOB_TYPE_STREAMING)
                    .build())
            .apply(
                tagOf(DataflowRequests.class, "Canceled Stream GetJobRequests"),
                DataflowRequests.jobRequestsFromEventsViewAll());

    getStreamCanceledJobRequests.apply(
        "Count Canceled Streaming Jobs",
        ParDo.of(countFn(GetJobRequest.class, "canceled_streaming_jobs")));

    // Filter Drained Streaming Jobs.
    PCollection<GetJobRequest> getStreamDrainedJobRequests =
        events
            .apply(
                tagOf(DataflowFilterEventarcJobs.class, "Drained Streaming Jobs"),
                DataflowFilterEventarcJobs.builder()
                    .setIncludeJobStates(ImmutableList.of(JobState.JOB_STATE_DRAINED))
                    .setIncludeJobType(JobType.JOB_TYPE_STREAMING)
                    .build())
            .apply(
                tagOf(DataflowRequests.class, "Drained Stream GetJobRequests"),
                DataflowRequests.jobRequestsFromEventsViewAll());

    getStreamDrainedJobRequests.apply(
        "Count Drained Streaming Jobs",
        ParDo.of(countFn(GetJobRequest.class, "drained_streaming_jobs")));

    // Merge Batch and Streaming Jobs.
    PCollectionList<GetJobRequest> getJobRequestList =
        PCollectionList.of(getBatchJobRequests)
            .and(getStreamCanceledJobRequests)
            .and(getStreamDrainedJobRequests);
    PCollection<GetJobRequest> getJobRequests =
        getJobRequestList.apply("Merge Batch and Streaming Jobs", Flatten.pCollections());

    // Call the Dataflow GetJobs endpoint.
    DataflowReadResult<Job, DataflowRequestError> getJobsResult =
        getJobRequests.apply(
            tagOf(DataflowGetJobs.class, "Read"), DataflowGetJobs.create(configuration));

    // Convert Jobs to Rows.
    RowConversionResult<Job, ConversionError> jobsToRowResult =
        getJobsResult.getSuccess().apply(tagOf(JobsToRow.class, "Job"), JobsToRow.create());

    jobsToRowResult
        .getSuccess()
        .apply(
            "Count JobsToRow Success", ParDo.of(countFn(JobsToRow.class, "jobs_to_row_success")));
    jobsToRowResult
        .getFailure()
        .apply(
            "Count JobsToRow Failure", ParDo.of(countFn(JobsToRow.class, "jobs_to_row_failure")));

    // Write Job Rows to BigQuery.
    jobsToRowResult
        .getSuccess()
        .apply(tagOf(BigQueryWrites.class, "Job"), BigQueryWrites.dataflowJobs(options));

    writeErrors(
        options,
        BigQueryWrites.JOB_ERRORS,
        GetJobRequest.class,
        Job.class,
        getJobsResult.getFailure(),
        jobsToRowResult.getFailure());

    return getJobsResult.getSuccess();
  }

  private static <RequestT extends GeneratedMessageV3, ResponseT> void readDetailsFromDataflowJobs(
      Options options,
      String requestErrorTableIdPrefix,
      Class<RequestT> requestTClass,
      Class<ResponseT> responseTClass,
      PCollection<Job> jobs,
      PTransform<PCollection<Job>, DataflowReadResult<ResponseT, DataflowRequestError>>
          callAPITransform,
      PTransform<PCollection<ResponseT>, RowConversionResult<ResponseT, ConversionError>>
          detailsToRowTransform,
      PTransform<@NonNull PCollection<Row>, @NonNull WriteResult> bigQueryWriteTransform) {

    // Call the Dataflow API to get more Job details.
    DataflowReadResult<ResponseT, DataflowRequestError> readResult =
        jobs.apply(tagOf(callAPITransform.getClass(), responseTClass), callAPITransform);

    // Convert the Job details result to Beam Rows.
    RowConversionResult<ResponseT, ConversionError> toRowResult =
        readResult
            .getSuccess()
            .apply(tagOf(detailsToRowTransform.getClass(), responseTClass), detailsToRowTransform);

    // Write result to BigQuery.
    toRowResult
        .getSuccess()
        .apply(tagOf(bigQueryWriteTransform.getClass(), responseTClass), bigQueryWriteTransform);

    // Write errors to BigQuery.
    writeErrors(
        options,
        requestErrorTableIdPrefix,
        requestTClass,
        responseTClass,
        readResult.getFailure(),
        toRowResult.getFailure());
  }

  private static String tagOf(Class<?> clazz, String... addl) {
    return clazz.getSimpleName() + " " + String.join(" ", addl);
  }

  private static String tagOf(Class<?> clazz, Class<?>... addl) {
    return String.join(
        " ",
        ImmutableList.<String>builder()
            .add(clazz.getSimpleName())
            .addAll(Arrays.stream(addl).map(Class::getSimpleName).collect(Collectors.toList()))
            .build());
  }

  private static <T> DoFn<T, T> countFn(Class<?> clazz, String name) {
    return new DoFn<T, T>() {
      final Counter counter = Metrics.counter(clazz, name);

      @ProcessElement
      public void process(@Element T ignored) {
        counter.inc();
      }
    };
  }
}
