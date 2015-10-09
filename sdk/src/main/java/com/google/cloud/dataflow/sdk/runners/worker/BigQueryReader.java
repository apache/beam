/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.util.BigQueryTableRowIterator;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.AbstractBoundedReaderIterator;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * A source that reads a BigQuery table and yields TableRow objects.
 *
 * <p>The source is a wrapper over the {@code BigQueryTableRowIterator} class, which issues a
 * query for all rows of a table and then iterates over the result. There is no support for
 * progress reporting because the source is used only in situations where the entire table must be
 * read by each worker (i.e. the source is used as a side input).
 */
public class BigQueryReader extends Reader<WindowedValue<TableRow>> {
  @Nullable private final TableReference tableRef;
  @Nullable private final String query;
  @Nullable private final String projectId;
  private final Bigquery bigQueryClient;

  private BigQueryReader(TableReference tableRef, String query,  String projectId,
      Bigquery bigQueryClient) {
    this.tableRef = tableRef;
    this.query = query;
    this.projectId = projectId;
    this.bigQueryClient = checkNotNull(bigQueryClient, "bigQueryClient");
  }

  /**
   * Returns a {@code BigQueryReader} that uses the specified client to read from the specified
   * table.
   */
  public static BigQueryReader fromTable(TableReference tableRef, Bigquery bigQueryClient) {
    return new BigQueryReader(tableRef, null, null, bigQueryClient);
  }

  /**
   * Returns a {@code BigQueryReader} that reads from the specified table. The {@link Bigquery}
   * client is constructed at runtime from the specified options.
   */
  public static BigQueryReader fromTableWithOptions(
      TableReference tableRef, BigQueryOptions bigQueryOptions) {
    Bigquery client = Transport.newBigQueryClient(bigQueryOptions).build();
    return new BigQueryReader(tableRef, null, null, client);
  }

  /**
   * Returns a {@code BigQueryReader} that uses the specified client to read the results from
   * executing the specified query in the specified project.
   */
  public static BigQueryReader fromQuery(String query, String projectId, Bigquery bigQueryClient) {
    return new BigQueryReader(null, query, projectId, bigQueryClient);
  }

  /**
   * Returns a {@code BigQueryReader} that reads the results from executing the specified query in
   * the specified project. The {@link Bigquery} client is constructed at runtime from the
   * specified options.
   */
  public static BigQueryReader fromQueryWithOptions(
      String query, String projectId, BigQueryOptions bigQueryOptions) {
    Bigquery client = Transport.newBigQueryClient(bigQueryOptions).build();
    return new BigQueryReader(null, query, projectId, client);
  }

  public TableReference getTableRef() {
    return tableRef;
  }

  public String getQuery() {
    return query;
  }

  @Override
  public ReaderIterator<WindowedValue<TableRow>> iterator() throws IOException {
    if (tableRef != null) {
      return new BigQueryReaderIterator(tableRef, bigQueryClient);
    } else {
      return new BigQueryReaderIterator(query, projectId, bigQueryClient);
    }
  }

  /**
   * A ReaderIterator that yields TableRow objects for each row of a BigQuery table.
   */
  private static class BigQueryReaderIterator
      extends AbstractBoundedReaderIterator<WindowedValue<TableRow>> {
    private BigQueryTableRowIterator rowIterator;

    public BigQueryReaderIterator(TableReference tableRef, Bigquery bigQueryClient) {
      rowIterator = BigQueryTableRowIterator.fromTable(tableRef, bigQueryClient);
    }

    public BigQueryReaderIterator(String query, String projectId, Bigquery bigQueryClient) {
      rowIterator = BigQueryTableRowIterator.fromQuery(query, projectId, bigQueryClient);
    }

    @Override
    protected boolean hasNextImpl() {
      return rowIterator.hasNext();
    }

    @Override
    protected WindowedValue<TableRow> nextImpl() throws IOException {
      return WindowedValue.valueInGlobalWindow(rowIterator.next());
    }

    @Override
    public Progress getProgress() {
      // For now reporting progress is not supported because this source is used only when
      // an entire table needs to be read by each worker (used as a side input for instance).
      return null;
    }

    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      // For now dynamic splitting is not supported because this source
      // is used only when an entire table needs to be read by each worker (used
      // as a side input for instance).
      return null;
    }

    @Override
    public void close() throws IOException {
      rowIterator.close();
    }
  }
}
