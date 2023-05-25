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

import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.dataflow.v1beta3.GetJobRequest;
import com.google.dataflow.v1beta3.Job;
import java.time.Instant;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.testinfra.pipelines.bigquery.BigQueryWriteOptions;
import org.apache.beam.testinfra.pipelines.conversions.ConversionError;
import org.apache.beam.testinfra.pipelines.conversions.EventarcConversions;
import org.apache.beam.testinfra.pipelines.conversions.JobsToRow;
import org.apache.beam.testinfra.pipelines.conversions.RowConversionResult;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowClientFactoryConfiguration;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowGetJobs;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowJobsOptions;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowReadResult;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowRequestError;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowRequests;
import org.apache.beam.testinfra.pipelines.pubsub.PubsubReadOptions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

public class ReadDataflowApiWriteBigQuery {

  public interface Options extends DataflowJobsOptions, PubsubReadOptions, BigQueryWriteOptions {}

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    DataflowClientFactoryConfiguration configuration =
        DataflowClientFactoryConfiguration.builder(options).build();

    PCollection<String> json =
        pipeline.apply(
            "Read Eventarc Pubsub",
            PubsubIO.readStrings()
                .fromSubscription(options.getSubscription().getValue().getPath()));

    WithFailures.Result<
            @NonNull PCollection<com.google.events.cloud.dataflow.v1beta3.Job>,
            ConversionError<String>>
        events = json.apply(EventarcConversions.fromJson());

    PCollection<GetJobRequest> getJobRequests =
        events
            .output()
            .apply(
                "Map Job Event to GetJobRequest", DataflowRequests.jobRequestsFromEventsViewAll());

    DataflowReadResult<Job, DataflowRequestError<GetJobRequest>> getJobsResult =
        getJobRequests.apply("GetJobs", DataflowGetJobs.create(configuration));

    RowConversionResult<Job> jobsToRowResult = getJobsResult.getSuccess().apply(JobsToRow.create());

    PCollection<Row> jobRows = jobsToRowResult.getSuccess();

    DatasetReference datasetReference = options.getDataset().getValue();

    TableReference jobsTable =
        new TableReference()
            .setProjectId(datasetReference.getProjectId())
            .setDatasetId(datasetReference.getDatasetId())
            .setTableId(String.format("jobs_%s", Instant.now().getEpochSecond()));

    jobRows.apply(
        "Write Jobs To BigQuery",
        BigQueryIO.<Row>write()
            .to(jobsTable)
            .useBeamSchema()
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withMethod(Write.Method.STREAMING_INSERTS));

    //    DataflowReadResult<JobMetricsWithAppendedDetails,
    // DataflowRequestError<GetJobMetricsRequest>>
    //        getJobsMetricsResult =
    //            getJobsResult
    //                .getSuccess()
    //                .apply("GetJobMetrics", DataflowGetJobMetrics.create(configuration));
    //
    //    DataflowReadResult<
    //            StageSummaryWithAppendedDetails,
    // DataflowRequestError<GetJobExecutionDetailsRequest>>
    //        getJobExecutionDetailsResult =
    //            getJobsResult
    //                .getSuccess()
    //                .apply(
    //                    "GetJobExecutionDetails",
    // DataflowGetJobExecutionDetails.create(configuration));
    //
    //    DataflowReadResult<
    //            WorkerDetailsWithAppendedDetails,
    // DataflowRequestError<GetStageExecutionDetailsRequest>>
    //        getStageExecutionDetailsResult =
    //            getJobsResult
    //                .getSuccess()
    //                .apply(
    //                    "GetStageExecutionDetails",
    //                    DataflowGetStageExecutionDetails.create(configuration));

    //    getJobsResult.getSuccess().apply(BigQueryIO.<Row>write().to(jobsTable).useBeamSchema()
    //            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
    //            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    //            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API));

    //   getJobsResult.getSuccess().apply()

    // Write results to BigQuery
    //    getJobsResult.getSuccess().apply("Write Jobs", BigQueryWrites.dataflowJobs(options));
    //    getJobsMetricsResult
    //        .getSuccess()
    //        .apply("Write JobMetrics", BigQueryWrites.dataflowJobMetrics(options));
    //    getJobExecutionDetailsResult
    //        .getSuccess()
    //        .apply("Write JobExecutionDetails",
    // BigQueryWrites.dataflowJobExecutionDetails(options));
    //    getStageExecutionDetailsResult
    //        .getSuccess()
    //        .apply(
    //            "Write StageExecutionDetails",
    // BigQueryWrites.dataflowStageExecutionDetails(options));

    // Write errors to BigQuery
    //    events
    //        .failures()
    //        .setRowSchema(ConversionError.getSchema())
    //        .apply("Write Conversion Errors",
    // BigQueryWrites.writeFromJsonToJobEventsErrors(options));
    //    getJobsResult
    //        .getFailure()
    //        .apply("Write GetJob Errors", BigQueryWrites.dataflowGetJobsErrors(options));
    //    getJobExecutionDetailsResult
    //        .getFailure()
    //        .apply(
    //            "Write GetExecutionDetails Errors",
    //            BigQueryWrites.dataflowGetJobExecutionDetailsErrors(options));
    //    getJobsMetricsResult
    //        .getFailure()
    //        .apply("Write GetJobMetrics Errors",
    // BigQueryWrites.dataflowGetJobMetricsErrors(options));
    //    getStageExecutionDetailsResult
    //        .getFailure()
    //        .apply(
    //            "Write GetStageExecutionDetails Errors",
    //            BigQueryWrites.dataflowGetStageExecutionDetailsErrors(options));

    pipeline.run();
  }
}
