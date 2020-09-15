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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link org.apache.beam.sdk.io.Source} representing reading the results of a query. */
@Experimental(Kind.SOURCE_SINK)
public class BigQueryStorageQuerySource<T> extends BigQueryStorageSourceBase<T> {

  public static <T> BigQueryStorageQuerySource<T> create(
      String stepUuid,
      ValueProvider<String> queryProvider,
      Boolean flattenResults,
      Boolean useLegacySql,
      QueryPriority priority,
      @Nullable String location,
      @Nullable String queryTempDataset,
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
        queryTempDataset,
        kmsKey,
        parseFn,
        outputCoder,
        bqServices);
  }

  private final String stepUuid;
  private final ValueProvider<String> queryProvider;
  private final Boolean flattenResults;
  private final Boolean useLegacySql;
  private final QueryPriority priority;
  private final String location;
  private final String queryTempDataset;
  private final String kmsKey;

  private transient AtomicReference<JobStatistics> dryRunJobStats;

  private BigQueryStorageQuerySource(
      String stepUuid,
      ValueProvider<String> queryProvider,
      Boolean flattenResults,
      Boolean useLegacySql,
      QueryPriority priority,
      @Nullable String location,
      @Nullable String queryTempDataset,
      @Nullable String kmsKey,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    super(null, null, parseFn, outputCoder, bqServices);
    this.stepUuid = checkNotNull(stepUuid, "stepUuid");
    this.queryProvider = checkNotNull(queryProvider, "queryProvider");
    this.flattenResults = checkNotNull(flattenResults, "flattenResults");
    this.useLegacySql = checkNotNull(useLegacySql, "useLegacySql");
    this.priority = checkNotNull(priority, "priority");
    this.location = location;
    this.queryTempDataset = queryTempDataset;
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
  protected Table getTargetTable(BigQueryOptions options) throws Exception {
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
            kmsKey);
    return bqServices.getDatasetService(options).getTable(queryResultTable);
  }
}
