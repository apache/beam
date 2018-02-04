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

package org.apache.beam.sdk.io.gcp.bigquery;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createJobIdToken;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createTempTableReference;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.TableReference;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Priority;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BigQuerySourceBase} for querying BigQuery tables.
 */
@VisibleForTesting
class BigQueryQuerySource<T> extends BigQuerySourceBase<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryQuerySource.class);

  static <T>BigQueryQuerySource<T> create(
      String stepUuid,
      ValueProvider<String> query,
      Boolean flattenResults,
      Boolean useLegacySql,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Priority priority) {
    return new BigQueryQuerySource<>(
        stepUuid, query, flattenResults, useLegacySql, bqServices, coder, parseFn, priority);
  }

  private final ValueProvider<String> query;
  private final Boolean flattenResults;
  private final Boolean useLegacySql;
  private transient AtomicReference<JobStatistics> dryRunJobStats;
  private final Priority priority;

  private BigQueryQuerySource(
      String stepUuid,
      ValueProvider<String> query,
      Boolean flattenResults,
      Boolean useLegacySql,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Priority priority) {
    super(stepUuid, bqServices, coder, parseFn);
    this.query = checkNotNull(query, "query");
    this.flattenResults = checkNotNull(flattenResults, "flattenResults");
    this.useLegacySql = checkNotNull(useLegacySql, "useLegacySql");
    this.dryRunJobStats = new AtomicReference<>();
    if (priority != BigQueryIO.TypedRead.Priority.BATCH
        || priority != BigQueryIO.TypedRead.Priority.INTERACTIVE) {
        this.priority = BigQueryIO.TypedRead.Priority.BATCH;
    } else {
        this.priority = priority;
    }
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    return dryRunQueryIfNeeded(bqOptions).getTotalBytesProcessed();
  }

  @Override
  protected TableReference getTableToExtract(BigQueryOptions bqOptions)
      throws IOException, InterruptedException {
    // 1. Find the location of the query.
    String location = null;
    List<TableReference> referencedTables =
        dryRunQueryIfNeeded(bqOptions).getQuery().getReferencedTables();
    DatasetService tableService = bqServices.getDatasetService(bqOptions);
    if (referencedTables != null && !referencedTables.isEmpty()) {
      TableReference queryTable = referencedTables.get(0);
      location = tableService.getTable(queryTable).getLocation();
    }

    // 2. Create the temporary dataset in the query location.
    TableReference tableToExtract = createTempTableReference(
        bqOptions.getProject(), createJobIdToken(bqOptions.getJobName(), stepUuid));

    LOG.info("Creating temporary dataset {} for query results", tableToExtract.getDatasetId());
    tableService.createDataset(
        tableToExtract.getProjectId(),
        tableToExtract.getDatasetId(),
        location,
        "Temporary tables for query results of job " + bqOptions.getJobName(),
        // Set a TTL of 1 day on the temporary tables, which ought to be enough in all cases:
        // the temporary tables are used only to immediately extract them into files.
        // They are normally cleaned up, but in case of job failure the cleanup step may not run,
        // and then they'll get deleted after the TTL.
        24 * 3600 * 1000L /* 1 day */);

    // 3. Execute the query.
    String queryJobId = createJobIdToken(bqOptions.getJobName(), stepUuid) + "-query";
    LOG.info(
        "Exporting query results into temporary table {} using job {}",
        tableToExtract,
        queryJobId);
    executeQuery(
        bqOptions.getProject(),
        queryJobId,
        tableToExtract,
        bqServices.getJobService(bqOptions));
    LOG.info("Query job {} completed", queryJobId);

    return tableToExtract;
  }

  @Override
  protected void cleanupTempResource(BigQueryOptions bqOptions) throws Exception {
    TableReference tableToRemove = createTempTableReference(
        bqOptions.getProject(), createJobIdToken(bqOptions.getJobName(), stepUuid));

    DatasetService tableService = bqServices.getDatasetService(bqOptions);
    LOG.info("Deleting temporary table with query results {}", tableToRemove);
    tableService.deleteTable(tableToRemove);
    LOG.info("Deleting temporary dataset with query results {}", tableToRemove.getDatasetId());
    tableService.deleteDataset(tableToRemove.getProjectId(), tableToRemove.getDatasetId());
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("query", query));
  }

  private synchronized JobStatistics dryRunQueryIfNeeded(BigQueryOptions bqOptions)
      throws InterruptedException, IOException {
    if (dryRunJobStats.get() == null) {
      JobStatistics jobStats = bqServices.getJobService(bqOptions).dryRunQuery(
          bqOptions.getProject(), createBasicQueryConfig());
      dryRunJobStats.compareAndSet(null, jobStats);
    }
    return dryRunJobStats.get();
  }

  private void executeQuery(
      String executingProject,
      String jobId,
      TableReference destinationTable,
      JobService jobService) throws IOException, InterruptedException {
    JobReference jobRef = new JobReference()
        .setProjectId(executingProject)
        .setJobId(jobId);

    JobConfigurationQuery queryConfig = createBasicQueryConfig()
        .setAllowLargeResults(true)
        .setCreateDisposition("CREATE_IF_NEEDED")
        .setDestinationTable(destinationTable)
        .setPriority(this.priority.name())
        .setWriteDisposition("WRITE_EMPTY");

    jobService.startQueryJob(jobRef, queryConfig);
    Job job = jobService.pollJob(jobRef, JOB_POLL_MAX_RETRIES);
    if (BigQueryHelpers.parseStatus(job) != Status.SUCCEEDED) {
      throw new IOException(String.format(
          "Query job %s failed, status: %s.", jobId,
          BigQueryHelpers.statusToPrettyString(job.getStatus())));
    }
  }

  private JobConfigurationQuery createBasicQueryConfig() {
    return new JobConfigurationQuery()
        .setFlattenResults(flattenResults)
        .setQuery(query.get())
        .setUseLegacySql(useLegacySql);
  }

  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();
    dryRunJobStats = new AtomicReference<>();
  }
}
