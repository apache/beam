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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createJobIdToken;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createTempTableReference;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.TableReference;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link BigQuerySourceBase} for querying BigQuery tables. */
@VisibleForTesting
class BigQueryQuerySource<T> extends BigQuerySourceBase<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryQuerySource.class);

  static <T> BigQueryQuerySource<T> create(
      String stepUuid,
      ValueProvider<String> query,
      Boolean flattenResults,
      Boolean useLegacySql,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      QueryPriority priority,
      String location,
      String kmsKey) {
    return new BigQueryQuerySource<>(
        stepUuid,
        query,
        flattenResults,
        useLegacySql,
        bqServices,
        coder,
        parseFn,
        priority,
        location,
        kmsKey);
  }

  private final ValueProvider<String> query;
  private final Boolean flattenResults;
  private final Boolean useLegacySql;
  private final QueryPriority priority;
  private final String location;
  private final String kmsKey;

  private transient AtomicReference<JobStatistics> dryRunJobStats;

  private BigQueryQuerySource(
      String stepUuid,
      ValueProvider<String> query,
      Boolean flattenResults,
      Boolean useLegacySql,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      QueryPriority priority,
      String location,
      String kmsKey) {
    super(stepUuid, bqServices, coder, parseFn);
    this.query = checkNotNull(query, "query");
    this.flattenResults = checkNotNull(flattenResults, "flattenResults");
    this.useLegacySql = checkNotNull(useLegacySql, "useLegacySql");
    this.priority = priority;
    this.location = location;
    this.kmsKey = kmsKey;
    dryRunJobStats = new AtomicReference<>();
  }

  /**
   * Since the query helper reference is declared as transient, neither the AtomicReference nor the
   * structure it refers to are persisted across serialization boundaries. The code below is
   * resilient to the QueryHelper object disappearing in between method calls, but the reference
   * object must be recreated at deserialization time.
   */
  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();
    dryRunJobStats = new AtomicReference<>();
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return BigQueryQueryHelper.dryRunQueryIfNeeded(
            bqServices,
            options.as(BigQueryOptions.class),
            dryRunJobStats,
            query.get(),
            flattenResults,
            useLegacySql,
            location)
        .getQuery()
        .getTotalBytesProcessed();
  }

  @Override
  protected TableReference getTableToExtract(BigQueryOptions bqOptions)
      throws IOException, InterruptedException {
    return BigQueryQueryHelper.executeQuery(
        bqServices,
        bqOptions,
        dryRunJobStats,
        stepUuid,
        query.get(),
        flattenResults,
        useLegacySql,
        priority,
        location,
        kmsKey);
  }

  @Override
  protected void cleanupTempResource(BigQueryOptions bqOptions) throws Exception {
    TableReference tableToRemove =
        createTempTableReference(
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
}
