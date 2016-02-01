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
import com.google.cloud.dataflow.sdk.util.BigQueryTableRowIterator;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;
import com.google.common.annotations.VisibleForTesting;

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
public class BigQueryReader extends NativeReader<WindowedValue<TableRow>> {
  @Nullable private final TableReference tableRef;
  @Nullable private final String query;
  @Nullable private final String projectId;
  @Nullable private final Boolean flattenResults;
  private final Bigquery bigQueryClient;

  private BigQueryReader(TableReference tableRef, String query,  String projectId,
      Bigquery bigQueryClient, Boolean flattenResults) {
    this.tableRef = tableRef;
    this.query = query;
    this.projectId = projectId;
    this.flattenResults = flattenResults;
    this.bigQueryClient = checkNotNull(bigQueryClient, "bigQueryClient");
  }

  /**
   * Returns a {@code BigQueryReader} that uses the specified client to read from the specified
   * table.
   */
  static BigQueryReader fromTable(TableReference tableRef, Bigquery bigQueryClient) {
    return new BigQueryReader(tableRef, null, null, bigQueryClient, null);
  }

  /**
   * Returns a {@code BigQueryReader} that uses the specified client to read the results from
   * executing the specified query in the specified project.
   */
  static BigQueryReader fromQuery(
      String query, String projectId, Bigquery bigQueryClient, boolean flatten) {
    return new BigQueryReader(null, query, projectId, bigQueryClient, flatten);
  }

  public TableReference getTableRef() {
    return tableRef;
  }

  public String getQuery() {
    return query;
  }

  @Override
  public BigQueryReaderIterator iterator() throws IOException {
    if (tableRef != null) {
      return new BigQueryReaderIterator(tableRef, bigQueryClient);
    } else {
      return new BigQueryReaderIterator(query, projectId, bigQueryClient, flattenResults);
    }
  }

  /**
   * A ReaderIterator that yields TableRow objects for each row of a BigQuery table.
   */
  @VisibleForTesting
  static class BigQueryReaderIterator extends LegacyReaderIterator<WindowedValue<TableRow>> {
    private BigQueryTableRowIterator rowIterator;

    public BigQueryReaderIterator(TableReference tableRef, Bigquery bigQueryClient) {
      rowIterator = BigQueryTableRowIterator.fromTable(tableRef, bigQueryClient);
    }

    public BigQueryReaderIterator(String query, String projectId, Bigquery bigQueryClient,
        @Nullable Boolean flattenResults) {
      rowIterator = BigQueryTableRowIterator.fromQuery(query, projectId, bigQueryClient,
          flattenResults);
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
