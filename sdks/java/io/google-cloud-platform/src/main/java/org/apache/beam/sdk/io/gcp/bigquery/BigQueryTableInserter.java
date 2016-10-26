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

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inserts rows into BigQuery.
 */
class BigQueryTableInserter {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableInserter.class);

  private final Bigquery client;

  /**
   * Constructs a new row inserter.
   *
   * @param client a BigQuery client
   * @param options a PipelineOptions object
   */
  BigQueryTableInserter(Bigquery client, PipelineOptions options) {
    this.client = client;
  }

  /**
   * Retrieves or creates the table.
   *
   * <p>The table is checked to conform to insertion requirements as specified
   * by WriteDisposition and CreateDisposition.
   *
   * <p>If table truncation is requested (WriteDisposition.WRITE_TRUNCATE), then
   * this will re-create the table if necessary to ensure it is empty.
   *
   * <p>If an empty table is required (WriteDisposition.WRITE_EMPTY), then this
   * will fail if the table exists and is not empty.
   *
   * <p>When constructing a table, a {@code TableSchema} must be available.  If a
   * schema is provided, then it will be used.  If no schema is provided, but
   * an existing table is being cleared (WRITE_TRUNCATE option above), then
   * the existing schema will be re-used.  If no schema is available, then an
   * {@code IOException} is thrown.
   */
  Table getOrCreateTable(
      TableReference ref,
      WriteDisposition writeDisposition,
      CreateDisposition createDisposition,
      @Nullable TableSchema schema) throws IOException {
    // Check if table already exists.
    Bigquery.Tables.Get get = client.tables()
        .get(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
    Table table = null;
    try {
      table = get.execute();
    } catch (IOException e) {
      ApiErrorExtractor errorExtractor = new ApiErrorExtractor();
      if (!errorExtractor.itemNotFound(e)
          || createDisposition != CreateDisposition.CREATE_IF_NEEDED) {
        // Rethrow.
        throw e;
      }
    }

    // If we want an empty table, and it isn't, then delete it first.
    if (table != null) {
      if (writeDisposition == WriteDisposition.WRITE_APPEND) {
        return table;
      }

      boolean empty = isEmpty(ref);
      if (empty) {
        if (writeDisposition == WriteDisposition.WRITE_TRUNCATE) {
          LOG.info("Empty table found, not removing {}", BigQueryIO.toTableSpec(ref));
        }
        return table;

      } else if (writeDisposition == WriteDisposition.WRITE_EMPTY) {
        throw new IOException("WriteDisposition is WRITE_EMPTY, "
            + "but table is not empty");
      }

      // Reuse the existing schema if none was provided.
      if (schema == null) {
        schema = table.getSchema();
      }

      // Delete table and fall through to re-creating it below.
      LOG.info("Deleting table {}", BigQueryIO.toTableSpec(ref));
      Bigquery.Tables.Delete delete = client.tables()
          .delete(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
      delete.execute();
    }

    if (schema == null) {
      throw new IllegalArgumentException(
          "Table schema required for new table.");
    }

    // Create the table.
    return tryCreateTable(ref, schema);
  }

  /**
   * Checks if a table is empty.
   */
  private boolean isEmpty(TableReference ref) throws IOException {
    Bigquery.Tabledata.List list = client.tabledata()
        .list(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
    list.setMaxResults(1L);
    TableDataList dataList = list.execute();

    return dataList.getRows() == null || dataList.getRows().isEmpty();
  }

  /**
   * Retry table creation up to 5 minutes (with exponential backoff) when this user is near the
   * quota for table creation. This relatively innocuous behavior can happen when BigQueryIO is
   * configured with a table spec function to use different tables for each window.
   */
  private static final int RETRY_CREATE_TABLE_DURATION_MILLIS = (int) TimeUnit.MINUTES.toMillis(5);

  /**
   * Tries to create the BigQuery table.
   * If a table with the same name already exists in the dataset, the table
   * creation fails, and the function returns null.  In such a case,
   * the existing table doesn't necessarily have the same schema as specified
   * by the parameter.
   *
   * @param schema Schema of the new BigQuery table.
   * @return The newly created BigQuery table information, or null if the table
   *     with the same name already exists.
   * @throws IOException if other error than already existing table occurs.
   */
  @Nullable
  private Table tryCreateTable(TableReference ref, TableSchema schema) throws IOException {
    LOG.info("Trying to create BigQuery table: {}", BigQueryIO.toTableSpec(ref));
    BackOff backoff =
        new ExponentialBackOff.Builder()
            .setMaxElapsedTimeMillis(RETRY_CREATE_TABLE_DURATION_MILLIS)
            .build();

    Table table = new Table().setTableReference(ref).setSchema(schema);
    return tryCreateTable(table, ref.getProjectId(), ref.getDatasetId(), backoff, Sleeper.DEFAULT);
  }

  @VisibleForTesting
  @Nullable
  Table tryCreateTable(
      Table table, String projectId, String datasetId, BackOff backoff, Sleeper sleeper)
          throws IOException {
    boolean retry = false;
    while (true) {
      try {
        return client.tables().insert(projectId, datasetId, table).execute();
      } catch (IOException e) {
        ApiErrorExtractor extractor = new ApiErrorExtractor();
        if (extractor.itemAlreadyExists(e)) {
          // The table already exists, nothing to return.
          return null;
        } else if (extractor.rateLimited(e)) {
          // The request failed because we hit a temporary quota. Back off and try again.
          try {
            if (BackOffUtils.next(sleeper, backoff)) {
              if (!retry) {
                LOG.info(
                    "Quota limit reached when creating table {}:{}.{}, retrying up to {} minutes",
                    projectId,
                    datasetId,
                    table.getTableReference().getTableId(),
                    TimeUnit.MILLISECONDS.toSeconds(RETRY_CREATE_TABLE_DURATION_MILLIS) / 60.0);
                retry = true;
              }
              continue;
            }
          } catch (InterruptedException e1) {
            // Restore interrupted state and throw the last failure.
            Thread.currentThread().interrupt();
            throw e;
          }
        }
        throw e;
      }
    }
  }
}
