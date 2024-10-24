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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1.DataFormat;
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
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link org.apache.beam.sdk.io.Source} representing reading the results of a query. */
class BigQueryStorageQuerySource<T> extends BigQueryStorageSourceBase<T> {

  public static <T> BigQueryStorageQuerySource<T> create(
      String stepUuid,
      ValueProvider<String> queryProvider,
      Boolean flattenResults,
      Boolean useLegacySql,
      QueryPriority priority,
      @Nullable String location,
      @Nullable String queryTempDataset,
      @Nullable String queryTempProject,
      @Nullable String kmsKey,
      @Nullable DataFormat format,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageQuerySource<>(
        stepUuid,
        queryProvider,
        flattenResults,
        useLegacySql,
        priority,
        location,
        queryTempDataset,
        queryTempProject,
        kmsKey,
        format,
        parseFn,
        outputCoder,
        bqServices);
  }

  public static <T> BigQueryStorageQuerySource<T> create(
      String stepUuid,
      ValueProvider<String> queryProvider,
      Boolean flattenResults,
      Boolean useLegacySql,
      QueryPriority priority,
      @Nullable String location,
      @Nullable String kmsKey,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageQuerySource<>(
        stepUuid,
        queryProvider,
        flattenResults,
        useLegacySql,
        priority,
        location,
        null,
        null,
        kmsKey,
        null,
        parseFn,
        outputCoder,
        bqServices);
  }

  private final String stepUuid;
  private final ValueProvider<String> queryProvider;
  private final Boolean flattenResults;
  private final Boolean useLegacySql;
  private final QueryPriority priority;
  private final @Nullable String location;
  private final @Nullable String queryTempDataset;

  private final @Nullable String queryTempProject;
  private final @Nullable String kmsKey;

  private transient AtomicReference<@Nullable JobStatistics> dryRunJobStats;

  private BigQueryStorageQuerySource(
      String stepUuid,
      ValueProvider<String> queryProvider,
      Boolean flattenResults,
      Boolean useLegacySql,
      QueryPriority priority,
      @Nullable String location,
      @Nullable String queryTempDataset,
      @Nullable String queryTempProject,
      @Nullable String kmsKey,
      @Nullable DataFormat format,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    super(format, null, null, parseFn, outputCoder, bqServices);
    this.stepUuid = checkNotNull(stepUuid, "stepUuid");
    this.queryProvider = checkNotNull(queryProvider, "queryProvider");
    this.flattenResults = checkNotNull(flattenResults, "flattenResults");
    this.useLegacySql = checkNotNull(useLegacySql, "useLegacySql");
    this.priority = checkNotNull(priority, "priority");
    this.location = location;
    this.queryTempDataset = queryTempDataset;
    this.queryTempProject = queryTempProject;
    this.kmsKey = kmsKey;
    this.dryRunJobStats = new AtomicReference<>();
  }

  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();
    dryRunJobStats = new AtomicReference<>();
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("query", queryProvider).withLabel("Query"));
    builder.add(
        DisplayData.item("launchesBigQueryJobs", true)
            .withLabel("This transform launches BigQuery jobs to read/write elements."));
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return BigQueryQueryHelper.dryRunQueryIfNeeded(
            bqServices,
            options.as(BigQueryOptions.class),
            dryRunJobStats,
            queryProvider.get(),
            flattenResults,
            useLegacySql,
            location)
        .getQuery()
        .getTotalBytesProcessed();
  }

  @Override
  protected @Nullable Table getTargetTable(BigQueryOptions options) throws Exception {
    TableReference queryResultTable =
        BigQueryQueryHelper.executeQuery(
            bqServices,
            options,
            dryRunJobStats,
            stepUuid,
            queryProvider.get(),
            flattenResults,
            useLegacySql,
            priority,
            location,
            queryTempDataset,
            queryTempProject,
            kmsKey);
    try (DatasetService datasetService = bqServices.getDatasetService(options)) {
      return datasetService.getTable(queryResultTable);
    }
  }

  @Override
  protected @Nullable String getTargetTableId(BigQueryOptions options) throws Exception {
    return null;
  }
}
