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
package org.apache.beam.testinfra.pipelines.bigquery;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.dataflow.v1beta3.GetJobExecutionDetailsRequest;
import com.google.dataflow.v1beta3.GetJobMetricsRequest;
import com.google.dataflow.v1beta3.GetJobRequest;
import com.google.dataflow.v1beta3.GetStageExecutionDetailsRequest;
import com.google.dataflow.v1beta3.Job;
import java.time.Instant;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowRequestError;
import org.apache.beam.testinfra.pipelines.dataflow.JobMetricsWithAppendedDetails;
import org.apache.beam.testinfra.pipelines.dataflow.StageSummaryWithAppendedDetails;
import org.apache.beam.testinfra.pipelines.dataflow.WorkerDetailsWithAppendedDetails;
import org.apache.beam.testinfra.pipelines.eventarc.ConversionError;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

public class BigQueryWrites {

  private static final TimePartitioning JOB_TIME_PARTITIONING =
      new TimePartitioning().setType("HOUR").setField("create_time");

  private static final TimePartitioning ENRICHED_TIME_PARTITIONING =
      new TimePartitioning().setType("HOUR").setField("job_create_time");

  private static final TimePartitioning OBSERVED_TIME_PARTITIONING =
      new TimePartitioning().setType("HOUR").setField("observed_time");

  private static final Clustering JOB_CLUSTERING =
      new Clustering().setFields(ImmutableList.of("type", "location"));

  private static final String CONVERSION_ERRORS_TABLE_ID_PREFIX = "conversion_errors";

  private static final String JOB_EXECUTION_DETAILS = "job_execution_details";

  private static final String JOB_EXECUTION_DETAILS_ERRORS = "job_execution_details_request_errors";

  private static final String JOB_METRICS = "job_metrics";

  private static final String JOB_METRICS_ERRORS = "job_metrics_request_errors";

  private static final String JOBS = "jobs";

  private static final String JOB_ERRORS = "jobs_request_errors";

  private static final String STAGE_EXECUTION_DETAILS = "stage_execution_details";

  private static final String STAGE_EXECUTION_DETAILS_REQUESTS_ERRORS =
      "stage_execution_details_requests_errors";

  public static PTransform<@NonNull PCollection<ConversionError<String>>, @NonNull WriteResult>
      writeFromJsonToJobEventsErrors(BigQueryWriteOptions options) {
    return withPartitioning(
        options, tableId(CONVERSION_ERRORS_TABLE_ID_PREFIX), OBSERVED_TIME_PARTITIONING);
  }

  public static PTransform<
          @NonNull PCollection<StageSummaryWithAppendedDetails>, @NonNull WriteResult>
      dataflowJobExecutionDetails(BigQueryWriteOptions options) {
    return withPartitioning(options, tableId(JOB_EXECUTION_DETAILS), ENRICHED_TIME_PARTITIONING);
  }

  public static PTransform<
          @NonNull PCollection<DataflowRequestError<GetJobExecutionDetailsRequest>>,
          @NonNull WriteResult>
      dataflowGetJobExecutionDetailsErrors(BigQueryWriteOptions options) {
    return writeDataflowRequestErrors(options, tableId(JOB_EXECUTION_DETAILS_ERRORS));
  }

  public static PTransform<
          @NonNull PCollection<JobMetricsWithAppendedDetails>, @NonNull WriteResult>
      dataflowJobMetrics(BigQueryWriteOptions options) {
    return withPartitioning(options, tableId(JOB_METRICS), ENRICHED_TIME_PARTITIONING);
  }

  public static PTransform<
          @NonNull PCollection<DataflowRequestError<GetJobMetricsRequest>>, @NonNull WriteResult>
      dataflowGetJobMetricsErrors(BigQueryWriteOptions options) {
    return writeDataflowRequestErrors(options, tableId(JOB_METRICS_ERRORS));
  }

  public static PTransform<@NonNull PCollection<Job>, @NonNull WriteResult> dataflowJobs(
      BigQueryWriteOptions options) {
    return withPartitioningAndOptionalClustering(
        options, tableId(JOBS), JOB_TIME_PARTITIONING, JOB_CLUSTERING);
  }

  public static PTransform<
          @NonNull PCollection<DataflowRequestError<GetJobRequest>>, @NonNull WriteResult>
      dataflowGetJobsErrors(BigQueryWriteOptions options) {
    return writeDataflowRequestErrors(options, tableId(JOB_ERRORS));
  }

  public static PTransform<
          @NonNull PCollection<WorkerDetailsWithAppendedDetails>, @NonNull WriteResult>
      dataflowStageExecutionDetails(BigQueryWriteOptions options) {
    return withPartitioning(options, tableId(STAGE_EXECUTION_DETAILS), ENRICHED_TIME_PARTITIONING);
  }

  public static PTransform<
          @NonNull PCollection<DataflowRequestError<GetStageExecutionDetailsRequest>>,
          @NonNull WriteResult>
      dataflowGetStageExecutionDetailsErrors(BigQueryWriteOptions options) {
    return writeDataflowRequestErrors(options, tableId(STAGE_EXECUTION_DETAILS_REQUESTS_ERRORS));
  }

  private static <RequestT>
      PTransform<@NonNull PCollection<DataflowRequestError<RequestT>>, @NonNull WriteResult>
          writeDataflowRequestErrors(BigQueryWriteOptions options, String tableIdPrefix) {
    return withPartitioning(options, tableId(tableIdPrefix), OBSERVED_TIME_PARTITIONING);
  }

  private static String tableId(String prefix) {
    return String.format("%s_%s", prefix, Instant.now().getEpochSecond());
  }

  private static <T> PTransform<@NonNull PCollection<T>, @NonNull WriteResult> withPartitioning(
      BigQueryWriteOptions options, String tableId, TimePartitioning timePartitioning) {
    return withPartitioningAndOptionalClustering(options, tableId, timePartitioning, null);
  }

  private static <T>
      PTransform<@NonNull PCollection<T>, @NonNull WriteResult>
          withPartitioningAndOptionalClustering(
              BigQueryWriteOptions options,
              String tableId,
              TimePartitioning timePartitioning,
              @Nullable Clustering clustering) {

    DatasetReference datasetReference = options.getDataset().getValue();
    TableReference tableReference =
        new TableReference()
            .setProjectId(datasetReference.getProjectId())
            .setDatasetId(datasetReference.getDatasetId())
            .setTableId(tableId);

    BigQueryIO.Write<T> write =
        BigQueryIO.<T>write()
            .to(tableReference)
            .useBeamSchema()
            .withTimePartitioning(timePartitioning)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
            .withTriggeringFrequency(Duration.standardSeconds(3L));

    if (clustering != null) {
      write = write.withClustering(clustering);
    }

    return write;
  }
}
