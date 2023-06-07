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
import com.google.dataflow.v1beta3.Job;
import java.time.Instant;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.testinfra.pipelines.conversions.ConversionError;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowRequestError;
import org.apache.beam.testinfra.pipelines.dataflow.JobMetricsWithAppendedDetails;
import org.apache.beam.testinfra.pipelines.dataflow.StageSummaryWithAppendedDetails;
import org.apache.beam.testinfra.pipelines.dataflow.WorkerDetailsWithAppendedDetails;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Convenience methods for {@link BigQueryIO.Write}s. */
@Internal
public class BigQueryWrites {

  public static final String JOB_EXECUTION_DETAILS_ERRORS = "errors_job_execution_details_requests";

  public static final String JOB_ERRORS = "errors_jobs_requests";

  public static final String JOB_METRICS_ERRORS = "errors_job_metrics_requests";

  private static final TimePartitioning JOB_TIME_PARTITIONING =
      new TimePartitioning().setType("HOUR").setField("create_time");

  private static final TimePartitioning ENRICHED_TIME_PARTITIONING =
      new TimePartitioning().setType("HOUR").setField("job_create_time");

  private static final TimePartitioning OBSERVED_TIME_PARTITIONING =
      new TimePartitioning().setType("HOUR").setField("observed_time");

  private static final Clustering JOB_CLUSTERING =
      new Clustering().setFields(ImmutableList.of("type", "location"));

  private static final String CONVERSION_ERRORS_TABLE_ID_PREFIX = "errors_conversions";

  private static final String JOB_EXECUTION_DETAILS = "job_execution_details";

  private static final String JOB_METRICS = "job_metrics";

  private static final String JOBS = "jobs";

  private static final String STAGE_EXECUTION_DETAILS = "stage_execution_details";

  /** Write {@link Row}s of {@link ConversionError}s with {@link #OBSERVED_TIME_PARTITIONING}. */
  public static PTransform<@NonNull PCollection<Row>, @NonNull WriteResult> writeConversionErrors(
      BigQueryWriteOptions options) {
    return withPartitioning(
        options, tableIdFrom(CONVERSION_ERRORS_TABLE_ID_PREFIX), OBSERVED_TIME_PARTITIONING);
  }

  /**
   * Write {@link Row}s of {@link StageSummaryWithAppendedDetails} with {@link
   * #ENRICHED_TIME_PARTITIONING}.
   */
  public static PTransform<@NonNull PCollection<Row>, @NonNull WriteResult>
      dataflowJobExecutionDetails(BigQueryWriteOptions options) {
    return withPartitioning(
        options, tableIdFrom(JOB_EXECUTION_DETAILS), ENRICHED_TIME_PARTITIONING);
  }

  /**
   * Write {@link Row}s of {@link JobMetricsWithAppendedDetails} with {@link
   * #ENRICHED_TIME_PARTITIONING}.
   */
  public static PTransform<@NonNull PCollection<Row>, @NonNull WriteResult> dataflowJobMetrics(
      BigQueryWriteOptions options) {
    return withPartitioning(options, tableIdFrom(JOB_METRICS), ENRICHED_TIME_PARTITIONING);
  }

  /**
   * Write {@link Row}s of {@link Job}s with {@link #JOB_TIME_PARTITIONING} clustered by {@link
   * #JOB_CLUSTERING}.
   */
  public static PTransform<@NonNull PCollection<Row>, @NonNull WriteResult> dataflowJobs(
      BigQueryWriteOptions options) {
    return withPartitioningAndOptionalClustering(
        options, tableIdFrom(JOBS), JOB_TIME_PARTITIONING, JOB_CLUSTERING);
  }

  /**
   * Write {@link Row}s of {@link WorkerDetailsWithAppendedDetails} with {@link
   * #ENRICHED_TIME_PARTITIONING}.
   */
  public static PTransform<@NonNull PCollection<Row>, @NonNull WriteResult>
      dataflowStageExecutionDetails(BigQueryWriteOptions options) {
    return withPartitioning(
        options, tableIdFrom(STAGE_EXECUTION_DETAILS), ENRICHED_TIME_PARTITIONING);
  }

  /**
   * Write {@link Row}s of {@link DataflowRequestError}s with {@link #OBSERVED_TIME_PARTITIONING}.
   */
  public static PTransform<@NonNull PCollection<Row>, @NonNull WriteResult>
      writeDataflowRequestErrors(BigQueryWriteOptions options, String tableIdPrefix) {
    return withPartitioning(options, tableIdFrom(tableIdPrefix), OBSERVED_TIME_PARTITIONING);
  }

  private static String tableIdFrom(String prefix) {
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
            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS);

    if (clustering != null) {
      write = write.withClustering(clustering);
    }

    return write;
  }
}
