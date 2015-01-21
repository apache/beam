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

import com.google.api.client.util.Data;
import com.google.api.client.util.Preconditions;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Iterates over all rows in a table.
 */
public class BigQueryTableRowIterator implements Iterator<TableRow>, Closeable {

  private final Bigquery client;
  private final TableReference ref;
  private TableSchema schema;
  private String pageToken;
  private Iterator<TableRow> rowIterator;
  // Set true when the final page is seen from the service.
  private boolean lastPage = false;

  public BigQueryTableRowIterator(Bigquery client, TableReference ref) {
    this.client = client;
    this.ref = ref;
  }

  @Override
  public boolean hasNext() {
    try {
      if (!isOpen()) {
        open();
      }

      if (!rowIterator.hasNext() && !lastPage) {
        readNext();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return rowIterator.hasNext();
  }

  /**
   * Adjusts a field returned from the API to
   * match the type that will be seen when run on the
   * backend service. The end result is:
   *
   * <p><ul>
   *   <li> Nulls are {@code null}.
   *   <li> Repeated fields are lists.
   *   <li> Record columns are {@link TableRow}s.
   *   <li> {@code BOOLEAN} columns are JSON booleans, hence Java {@link Boolean}s.
   *   <li> {@code FLOAT} columns are JSON floats, hence Java {@link Double}s.
   *   <li> Every other atomic type is a {@link String}.
   * </ul></p>
   *
   * <p> Note that currently integers are encoded as strings to match
   * the behavior of the backend service.
   */
  private Object getTypedCellValue(TableFieldSchema fieldSchema, Object v) {
    // In the input from the BQ API, atomic types all come in as
    // strings, while on the Dataflow service they have more precise
    // types.

    if (Data.isNull(v)) {
      return null;
    }

    if (Objects.equals(fieldSchema.getMode(), "REPEATED")) {
      TableFieldSchema elementSchema = fieldSchema.clone().setMode("REQUIRED");
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> rawValues = (List<Map<String, Object>>) v;
      List<Object> values = new ArrayList<Object>(rawValues.size());
      for (Map<String, Object> element : rawValues) {
        values.add(getTypedCellValue(elementSchema, element.get("v")));
      }
      return values;
    }

    if (fieldSchema.getType().equals("RECORD")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> typedV = (Map<String, Object>) v;
      return getTypedTableRow(fieldSchema.getFields(), typedV);
    }

    if (fieldSchema.getType().equals("FLOAT")) {
      return Double.parseDouble((String) v);
    }

    if (fieldSchema.getType().equals("BOOLEAN")) {
      return Boolean.parseBoolean((String) v);
    }

    return v;
  }

  private TableRow getTypedTableRow(List<TableFieldSchema> fields, Map<String, Object> rawRow) {
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> cells = (List<Map<String, Object>>) rawRow.get("f");
    Preconditions.checkState(cells.size() == fields.size());

    Iterator<Map<String, Object>> cellIt = cells.iterator();
    Iterator<TableFieldSchema> fieldIt = fields.iterator();

    TableRow row = new TableRow();
    while (cellIt.hasNext()) {
      Map<String, Object> cell = cellIt.next();
      TableFieldSchema fieldSchema = fieldIt.next();
      row.set(fieldSchema.getName(), getTypedCellValue(fieldSchema, cell.get("v")));
    }
    return row;
  }

  @Override
  public TableRow next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    // Embed schema information into the raw row, so that values have an
    // associated key.  This matches how rows are read when using the
    // DataflowPipelineRunner.
    return getTypedTableRow(schema.getFields(), rowIterator.next());
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private void readNext() throws IOException {
    Bigquery.Tabledata.List list = client.tabledata()
        .list(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
    if (pageToken != null) {
      list.setPageToken(pageToken);
    }

    TableDataList result = list.execute();
    pageToken = result.getPageToken();
    rowIterator = result.getRows() != null ? result.getRows().iterator() :
                  Collections.<TableRow>emptyIterator();

    // The server may return a page token indefinitely on a zero-length table.
    if (pageToken == null ||
        result.getTotalRows() != null && result.getTotalRows() == 0) {
      lastPage = true;
    }
  }

  @Override
  public void close() throws IOException {
    // Prevent any further requests.
    lastPage = true;
  }

  private boolean isOpen() {
    return schema != null;
  }

  /**
   * Opens the table for read.
   * @throws IOException on failure
   */
  private void open() throws IOException {
    // Get table schema.
    Bigquery.Tables.Get get = client.tables()
        .get(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
    Table table = get.execute();
    schema = table.getSchema();

    // Read the first page of results.
    readNext();
  }
}
