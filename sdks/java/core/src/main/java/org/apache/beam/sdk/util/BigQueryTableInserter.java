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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.io.BigQueryIO;
import org.apache.beam.sdk.io.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.Aggregator;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * Inserts rows into BigQuery.
 */
public class BigQueryTableInserter {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableInserter.class);

  // Approximate amount of table data to upload per InsertAll request.
  private static final long UPLOAD_BATCH_SIZE_BYTES = 64 * 1024;

  // The maximum number of rows to upload per InsertAll request.
  private static final long MAX_ROWS_PER_BATCH = 500;

  // The maximum number of times to retry inserting rows into BigQuery.
  private static final int MAX_INSERT_ATTEMPTS = 5;

  // The initial backoff after a failure inserting rows into BigQuery.
  private static final long INITIAL_INSERT_BACKOFF_INTERVAL_MS = 200L;

  // Backoff time bounds for rate limit exceeded errors.
  private static final long INITIAL_RATE_LIMIT_EXCEEDED_BACKOFF_MS = TimeUnit.SECONDS.toMillis(1);
  private static final long MAX_RATE_LIMIT_EXCEEDED_BACKOFF_MS = TimeUnit.MINUTES.toMillis(2);

  private final Bigquery client;
  private final TableReference defaultRef;
  private final long maxRowsPerBatch;

  private static final ExecutorService executor = MoreExecutors.getExitingExecutorService(
      (ThreadPoolExecutor) Executors.newFixedThreadPool(100), 10, TimeUnit.SECONDS);

  /**
   * Constructs a new row inserter.
   *
   * @param client a BigQuery client
   */
  public BigQueryTableInserter(Bigquery client) {
    this.client = client;
    this.defaultRef = null;
    this.maxRowsPerBatch = MAX_ROWS_PER_BATCH;
  }

  /**
   * Constructs a new row inserter.
   *
   * @param client a BigQuery client
   * @param defaultRef identifies the table to insert into
   * @deprecated replaced by {@link #BigQueryTableInserter(Bigquery)}
   */
  @Deprecated
  public BigQueryTableInserter(Bigquery client, TableReference defaultRef) {
    this.client = client;
    this.defaultRef = defaultRef;
    this.maxRowsPerBatch = MAX_ROWS_PER_BATCH;
  }

  /**
   * Constructs a new row inserter.
   *
   * @param client a BigQuery client
   */
  public BigQueryTableInserter(Bigquery client, int maxRowsPerBatch) {
    this.client = client;
    this.defaultRef = null;
    this.maxRowsPerBatch = maxRowsPerBatch;
  }

  /**
   * Constructs a new row inserter.
   *
   * @param client a BigQuery client
   * @param defaultRef identifies the default table to insert into
   * @deprecated replaced by {@link #BigQueryTableInserter(Bigquery, int)}
   */
  @Deprecated
  public BigQueryTableInserter(Bigquery client, TableReference defaultRef, int maxRowsPerBatch) {
    this.client = client;
    this.defaultRef = defaultRef;
    this.maxRowsPerBatch = maxRowsPerBatch;
  }

  /**
   * Insert all rows from the given list.
   *
   * @deprecated replaced by {@link #insertAll(TableReference, List)}
   */
  @Deprecated
  public void insertAll(List<TableRow> rowList) throws IOException {
    insertAll(defaultRef, rowList, null, null);
  }

  /**
   * Insert all rows from the given list using specified insertIds if not null.
   *
   * @deprecated replaced by {@link #insertAll(TableReference, List, List)}
   */
  @Deprecated
  public void insertAll(List<TableRow> rowList,
      @Nullable List<String> insertIdList) throws IOException {
    insertAll(defaultRef, rowList, insertIdList, null);
  }

  /**
   * Insert all rows from the given list.
   */
  public void insertAll(TableReference ref, List<TableRow> rowList) throws IOException {
    insertAll(ref, rowList, null, null);
  }

  /**
   * Insert all rows from the given list using specified insertIds if not null. Track count of
   * bytes written with the Aggregator.
   */
  public void insertAll(TableReference ref, List<TableRow> rowList,
      @Nullable List<String> insertIdList, Aggregator<Long, Long> byteCountAggregator)
      throws IOException {
    checkNotNull(ref, "ref");
    if (insertIdList != null && rowList.size() != insertIdList.size()) {
      throw new AssertionError("If insertIdList is not null it needs to have at least "
          + "as many elements as rowList");
    }

    AttemptBoundedExponentialBackOff backoff = new AttemptBoundedExponentialBackOff(
        MAX_INSERT_ATTEMPTS,
        INITIAL_INSERT_BACKOFF_INTERVAL_MS);

    List<TableDataInsertAllResponse.InsertErrors> allErrors = new ArrayList<>();
    // These lists contain the rows to publish. Initially the contain the entire list. If there are
    // failures, they will contain only the failed rows to be retried.
    List<TableRow> rowsToPublish = rowList;
    List<String> idsToPublish = insertIdList;
    while (true) {
      List<TableRow> retryRows = new ArrayList<>();
      List<String> retryIds = (idsToPublish != null) ? new ArrayList<String>() : null;

      int strideIndex = 0;
      // Upload in batches.
      List<TableDataInsertAllRequest.Rows> rows = new LinkedList<>();
      int dataSize = 0;

      List<Future<List<TableDataInsertAllResponse.InsertErrors>>> futures = new ArrayList<>();
      List<Integer> strideIndices = new ArrayList<>();

      for (int i = 0; i < rowsToPublish.size(); ++i) {
        TableRow row = rowsToPublish.get(i);
        TableDataInsertAllRequest.Rows out = new TableDataInsertAllRequest.Rows();
        if (idsToPublish != null) {
          out.setInsertId(idsToPublish.get(i));
        }
        out.setJson(row.getUnknownKeys());
        rows.add(out);

        dataSize += row.toString().length();
        if (dataSize >= UPLOAD_BATCH_SIZE_BYTES || rows.size() >= maxRowsPerBatch
            || i == rowsToPublish.size() - 1) {
          TableDataInsertAllRequest content = new TableDataInsertAllRequest();
          content.setRows(rows);

          final Bigquery.Tabledata.InsertAll insert = client.tabledata()
              .insertAll(ref.getProjectId(), ref.getDatasetId(), ref.getTableId(),
                  content);

          futures.add(
              executor.submit(new Callable<List<TableDataInsertAllResponse.InsertErrors>>() {
                @Override
                public List<TableDataInsertAllResponse.InsertErrors> call() throws IOException {
                  BackOff backoff = new IntervalBoundedExponentialBackOff(
                      MAX_RATE_LIMIT_EXCEEDED_BACKOFF_MS, INITIAL_RATE_LIMIT_EXCEEDED_BACKOFF_MS);
                  while (true) {
                    try {
                      return insert.execute().getInsertErrors();
                    } catch (IOException e) {
                      if (new ApiErrorExtractor().rateLimited(e)) {
                        LOG.info("BigQuery insertAll exceeded rate limit, retrying");
                        try {
                          Thread.sleep(backoff.nextBackOffMillis());
                        } catch (InterruptedException interrupted) {
                          throw new IOException(
                              "Interrupted while waiting before retrying insertAll");
                        }
                      } else {
                        throw e;
                      }
                    }
                  }
                }
              }));
          strideIndices.add(strideIndex);

          if (byteCountAggregator != null) {
            byteCountAggregator.addValue(Long.valueOf(dataSize));
          }
          dataSize = 0;
          strideIndex = i + 1;
          rows = new LinkedList<>();
        }
      }

      try {
        for (int i = 0; i < futures.size(); i++) {
          List<TableDataInsertAllResponse.InsertErrors> errors = futures.get(i).get();
          if (errors != null) {
            for (TableDataInsertAllResponse.InsertErrors error : errors) {
              allErrors.add(error);
              if (error.getIndex() == null) {
                throw new IOException("Insert failed: " + allErrors);
              }

              int errorIndex = error.getIndex().intValue() + strideIndices.get(i);
              retryRows.add(rowsToPublish.get(errorIndex));
              if (retryIds != null) {
                retryIds.add(idsToPublish.get(errorIndex));
              }
            }
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while inserting " + rowsToPublish);
      } catch (ExecutionException e) {
        throw new RuntimeException(e.getCause());
      }

      if (!allErrors.isEmpty() && !backoff.atMaxAttempts()) {
        try {
          Thread.sleep(backoff.nextBackOffMillis());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting before retrying insert of " + retryRows);
        }
        LOG.info("Retrying failed inserts to BigQuery");
        rowsToPublish = retryRows;
        idsToPublish = retryIds;
        allErrors.clear();
      } else {
        break;
      }
    }
    if (!allErrors.isEmpty()) {
      throw new IOException("Insert failed: " + allErrors);
    }
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
  public Table getOrCreateTable(
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
  public boolean isEmpty(TableReference ref) throws IOException {
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
  public Table tryCreateTable(TableReference ref, TableSchema schema) throws IOException {
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
