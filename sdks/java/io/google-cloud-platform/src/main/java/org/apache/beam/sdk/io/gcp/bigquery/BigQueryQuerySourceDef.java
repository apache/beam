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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.createTempTableReference;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.JobType;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigQueryQuerySourceDef implements BigQuerySourceDef {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryQuerySourceDef.class);

  private final BigQueryServices bqServices;
  private final ValueProvider<String> query;
  private final Boolean flattenResults;
  private final Boolean useLegacySql;
  private final BigQueryIO.TypedRead.QueryPriority priority;
  private final String location;
  private final String tempDatasetId;
  private final String tempProjectId;
  private final String kmsKey;

  private transient AtomicReference<@Nullable JobStatistics> dryRunJobStats;

  static BigQueryQuerySourceDef create(
      BigQueryServices bqServices,
      ValueProvider<String> query,
      Boolean flattenResults,
      Boolean useLegacySql,
      BigQueryIO.TypedRead.QueryPriority priority,
      String location,
      String tempDatasetId,
      String tempProjectId,
      String kmsKey) {
    return new BigQueryQuerySourceDef(
        bqServices,
        query,
        flattenResults,
        useLegacySql,
        priority,
        location,
        tempDatasetId,
        tempProjectId,
        kmsKey);
  }

  private BigQueryQuerySourceDef(
      BigQueryServices bqServices,
      ValueProvider<String> query,
      Boolean flattenResults,
      Boolean useLegacySql,
      BigQueryIO.TypedRead.QueryPriority priority,
      String location,
      String tempDatasetId,
      String tempProjectId,
      String kmsKey) {
    this.query = checkNotNull(query, "query");
    this.flattenResults = checkNotNull(flattenResults, "flattenResults");
    this.useLegacySql = checkNotNull(useLegacySql, "useLegacySql");
    this.bqServices = bqServices;
    this.priority = priority;
    this.location = location;
    this.tempDatasetId = tempDatasetId;
    this.tempProjectId = tempProjectId;
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

  long getEstimatedSizeBytes(BigQueryOptions bqOptions) throws Exception {
    return BigQueryQueryHelper.dryRunQueryIfNeeded(
            bqServices,
            bqOptions,
            dryRunJobStats,
            query.get(),
            flattenResults,
            useLegacySql,
            location)
        .getQuery()
        .getTotalBytesProcessed();
  }

  TableReference getTableReference(BigQueryOptions bqOptions, String stepUuid)
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
        tempDatasetId,
        tempProjectId,
        kmsKey);
  }

  void cleanupTempResource(BigQueryOptions bqOptions, String stepUuid) throws Exception {
    Optional<String> queryTempDatasetOpt = Optional.ofNullable(tempDatasetId);
    String project = tempProjectId;
    if (project == null) {
      project =
          bqOptions.getBigQueryProject() == null
              ? bqOptions.getProject()
              : bqOptions.getBigQueryProject();
    }
    TableReference tableToRemove =
        createTempTableReference(
            project,
            BigQueryResourceNaming.createJobIdPrefix(
                bqOptions.getJobName(), stepUuid, JobType.QUERY),
            queryTempDatasetOpt);

    try (BigQueryServices.DatasetService tableService = bqServices.getDatasetService(bqOptions)) {
      LOG.info("Deleting temporary table with query results {}", tableToRemove);
      tableService.deleteTable(tableToRemove);
      boolean datasetCreatedByBeam = !queryTempDatasetOpt.isPresent();
      if (datasetCreatedByBeam) {
        // Remove temporary dataset only if it was created by Beam
        LOG.info("Deleting temporary dataset with query results {}", tableToRemove.getDatasetId());
        tableService.deleteDataset(tableToRemove.getProjectId(), tableToRemove.getDatasetId());
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> BigQuerySourceBase<T> toSource(
      String stepUuid,
      Coder<T> coder,
      SerializableFunction<TableSchema, AvroSource.DatumReaderFactory<T>> readerFactory,
      boolean useAvroLogicalTypes) {
    return BigQueryQuerySource.create(
        stepUuid, this, bqServices, coder, readerFactory, useAvroLogicalTypes);
  }

  /** {@inheritDoc} */
  @Override
  public TableSchema getTableSchema(BigQueryOptions bqOptions) {
    try {
      JobStatistics stats =
          BigQueryQueryHelper.dryRunQueryIfNeeded(
              bqServices,
              bqOptions,
              dryRunJobStats,
              query.get(),
              flattenResults,
              useLegacySql,
              location);
      return stats.getQuery().getSchema();
    } catch (IOException | InterruptedException | NullPointerException e) {
      throw new BigQuerySchemaRetrievalException(
          "Exception while trying to retrieve schema of query", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Schema getBeamSchema(BigQueryOptions bqOptions) {
    TableSchema tableSchema = getTableSchema(bqOptions);
    return BigQueryUtils.fromTableSchema(tableSchema);
  }

  ValueProvider<String> getQuery() {
    return query;
  }
}
