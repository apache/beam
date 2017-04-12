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
import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToJson;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToProjectId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;


/**
 * A {@link BigQuerySourceBase} for querying BigQuery tables.
 */
@VisibleForTesting
class BigQueryQuerySource extends BigQuerySourceBase {

  static BigQueryQuerySource create(
      ValueProvider<String> jobIdToken,
      ValueProvider<String> query,
      ValueProvider<TableReference> queryTempTableRef,
      Boolean flattenResults,
      Boolean useLegacySql,
      String extractDestinationDir,
      BigQueryServices bqServices) {
    return new BigQueryQuerySource(
        jobIdToken,
        query,
        queryTempTableRef,
        flattenResults,
        useLegacySql,
        extractDestinationDir,
        bqServices);
  }

  private final ValueProvider<String> query;
  private final ValueProvider<String> jsonQueryTempTable;
  private final Boolean flattenResults;
  private final Boolean useLegacySql;
  private transient AtomicReference<JobStatistics> dryRunJobStats;

  private BigQueryQuerySource(
      ValueProvider<String> jobIdToken,
      ValueProvider<String> query,
      ValueProvider<TableReference> queryTempTableRef,
      Boolean flattenResults,
      Boolean useLegacySql,
      String extractDestinationDir,
      BigQueryServices bqServices) {
    super(jobIdToken, extractDestinationDir, bqServices,
        NestedValueProvider.of(
            checkNotNull(queryTempTableRef, "queryTempTableRef"), new TableRefToProjectId()));
    this.query = checkNotNull(query, "query");
    this.jsonQueryTempTable = NestedValueProvider.of(
        queryTempTableRef, new TableRefToJson());
    this.flattenResults = checkNotNull(flattenResults, "flattenResults");
    this.useLegacySql = checkNotNull(useLegacySql, "useLegacySql");
    this.dryRunJobStats = new AtomicReference<>();
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    return dryRunQueryIfNeeded(bqOptions).getTotalBytesProcessed();
  }

  @Override
  public BoundedReader<TableRow> createReader(PipelineOptions options) throws IOException {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    return new BigQueryReader(this, bqServices.getReaderFromQuery(
        bqOptions, executingProject.get(), createBasicQueryConfig()));
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
    TableReference tableToExtract =
        BigQueryIO.JSON_FACTORY.fromString(jsonQueryTempTable.get(), TableReference.class);
    tableService.createDataset(
        tableToExtract.getProjectId(),
        tableToExtract.getDatasetId(),
        location,
        "Dataset for BigQuery query job temporary table");

    // 3. Execute the query.
    String queryJobId = jobIdToken.get() + "-query";
    executeQuery(
        executingProject.get(),
        queryJobId,
        tableToExtract,
        bqServices.getJobService(bqOptions));
    return tableToExtract;
  }

  @Override
  protected void cleanupTempResource(BigQueryOptions bqOptions) throws Exception {
    checkState(jsonQueryTempTable.isAccessible());
    TableReference tableToRemove =
        BigQueryIO.JSON_FACTORY.fromString(jsonQueryTempTable.get(), TableReference.class);

    DatasetService tableService = bqServices.getDatasetService(bqOptions);
    tableService.deleteTable(tableToRemove);
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
          executingProject.get(), createBasicQueryConfig());
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
        .setPriority("BATCH")
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
