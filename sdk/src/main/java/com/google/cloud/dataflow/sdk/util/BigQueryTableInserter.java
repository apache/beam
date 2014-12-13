/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Inserts rows into BigQuery.
 */
public class BigQueryTableInserter {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableInserter.class);

  // Approximate amount of table data to upload per InsertAll request.
  private static final long UPLOAD_BATCH_SIZE = 64 * 1024;

  private final Bigquery client;
  private final TableReference ref;

  /**
   * Constructs a new row inserter.
   *
   * @param client a BigQuery client
   * @param ref identifies the table to insert into
   */
  public BigQueryTableInserter(Bigquery client, TableReference ref) {
    this.client = client;
    this.ref = ref;
  }

  /**
   * Insert all rows from the given iterator.
   */
  public void insertAll(Iterator<TableRow> rowIterator) throws IOException {
    insertAll(rowIterator, null);
  }

  /**
   * Insert all rows from the given iterator using specified insertIds if not null.
   */
  public void insertAll(Iterator<TableRow> rowIterator,
      @Nullable Iterator<String> insertIdIterator) throws IOException {
    // Upload in batches.
    List<TableDataInsertAllRequest.Rows> rows = new LinkedList<>();
    int numInserted = 0;
    int dataSize = 0;
    while (rowIterator.hasNext()) {
      TableRow row = rowIterator.next();
      TableDataInsertAllRequest.Rows out = new TableDataInsertAllRequest.Rows();
      if (insertIdIterator != null) {
        if (insertIdIterator.hasNext()) {
          out.setInsertId(insertIdIterator.next());
        } else {
          throw new AssertionError("If insertIdIterator is not null it needs to have at least "
              + "as many elements as rowIterator");
        }
      }
      out.setJson(row.getUnknownKeys());
      rows.add(out);

      dataSize += row.toString().length();
      if (dataSize >= UPLOAD_BATCH_SIZE || !rowIterator.hasNext()) {
        TableDataInsertAllRequest content = new TableDataInsertAllRequest();
        content.setRows(rows);

        LOG.info("Number of rows in BigQuery insert: {}", rows.size());
        numInserted += rows.size();

        Bigquery.Tabledata.InsertAll insert = client.tabledata()
            .insertAll(ref.getProjectId(), ref.getDatasetId(), ref.getTableId(),
                content);
        TableDataInsertAllResponse response = insert.execute();
        List<TableDataInsertAllResponse.InsertErrors> errors = response
            .getInsertErrors();
        if (errors != null && !errors.isEmpty()) {
          throw new IOException("Insert failed: " + errors);
        }

        dataSize = 0;
        rows.clear();
      }
    }

    LOG.info("Number of rows written to BigQuery: {}", numInserted);
  }

  /**
   * Retrieves or creates the table.
   * <p>
   * The table is checked to conform to insertion requirements as specified
   * by WriteDisposition and CreateDisposition.
   * <p>
   * If table truncation is requested (WriteDisposition.WRITE_TRUNCATE), then
   * this will re-create the table if necessary to ensure it is empty.
   * <p>
   * If an empty table is required (WriteDisposition.WRITE_EMPTY), then this
   * will fail if the table exists and is not empty.
   * <p>
   * When constructing a table, a {@code TableSchema} must be available.  If a
   * schema is provided, then it will be used.  If no schema is provided, but
   * an existing table is being cleared (WRITE_TRUNCATE option above), then
   * the existing schema will be re-used.  If no schema is available, then an
   * {@code IOException} is thrown.
   */
  public Table getOrCreateTable(
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
      if (!errorExtractor.itemNotFound(e) ||
          createDisposition != CreateDisposition.CREATE_IF_NEEDED) {
        // Rethrow.
        throw e;
      }
    }

    // If we want an empty table, and it isn't, then delete it first.
    if (table != null) {
      if (writeDisposition == WriteDisposition.WRITE_APPEND) {
        return table;
      }

      boolean empty = isEmpty();
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
    return tryCreateTable(schema);
  }

  /**
   * Checks if a table is empty.
   */
  public boolean isEmpty() throws IOException {
    Bigquery.Tabledata.List list = client.tabledata()
        .list(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
    list.setMaxResults(1L);
    TableDataList dataList = list.execute();

    return dataList.getRows() == null || dataList.getRows().isEmpty();
  }

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
  public Table tryCreateTable(TableSchema schema) throws IOException {
    LOG.info("Trying to create BigQuery table: {}", BigQueryIO.toTableSpec(ref));

    Table content = new Table();
    content.setTableReference(ref);
    content.setSchema(schema);

    try {
      return client.tables()
          .insert(ref.getProjectId(), ref.getDatasetId(), content)
          .execute();
    } catch (IOException e) {
      if (new ApiErrorExtractor().alreadyExists(e)) {
        LOG.info("The BigQuery table already exists.");
        return null;
      }
      throw e;
    }
  }
}
