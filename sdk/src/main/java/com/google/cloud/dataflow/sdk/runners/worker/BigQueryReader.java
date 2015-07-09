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

/**
 * A source that reads a BigQuery table and yields TableRow objects.
 *
 * <p>The source is a wrapper over the {@code BigQueryTableRowIterator} class, which issues a
 * query for all rows of a table and then iterates over the result. There is no support for
 * progress reporting because the source is used only in situations where the entire table must be
 * read by each worker (i.e. the source is used as a side input).
 */
public class BigQueryReader extends Reader<WindowedValue<TableRow>> {
  final TableReference tableRef;
  final BigQueryOptions bigQueryOptions;
  final Bigquery bigQueryClient;
  final String query;
  final String projectId;

  /** Builds a BigQuery source using pipeline options to instantiate a Bigquery client. */
  public BigQueryReader(BigQueryOptions bigQueryOptions, TableReference tableRef) {
    // Save pipeline options so that we can construct the BigQuery client on-demand whenever an
    // iterator gets created.
    this.bigQueryOptions = bigQueryOptions;
    this.tableRef = tableRef;
    this.bigQueryClient = null;
    this.query = null;
    this.projectId = null;
  }

  public BigQueryReader(BigQueryOptions bigQueryOptions, String query, String projectId) {
    this.bigQueryOptions = bigQueryOptions;
    this.tableRef = null;
    this.bigQueryClient = null;
    this.query = query;
    this.projectId = projectId;
  }

  /** Builds a BigQueryReader directly using a BigQuery client. */
  public BigQueryReader(Bigquery bigQueryClient, TableReference tableRef) {
    this.bigQueryOptions = null;
    this.tableRef = tableRef;
    this.bigQueryClient = bigQueryClient;
    this.query = null;
    this.projectId = null;
  }

  public BigQueryReader(Bigquery bigQueryClient, String query, String projectId) {
    this.bigQueryOptions = null;
    this.tableRef = null;
    this.bigQueryClient = bigQueryClient;
    this.query = query;
    this.projectId = projectId;
  }

  @Override
  public ReaderIterator<WindowedValue<TableRow>> iterator() throws IOException {
    if (tableRef != null) {
      return new BigQueryReaderIterator(
          bigQueryClient != null
              ? bigQueryClient : Transport.newBigQueryClient(bigQueryOptions).build(),
          tableRef);
    } else {
      return new BigQueryReaderIterator(
          bigQueryClient != null
              ? bigQueryClient : Transport.newBigQueryClient(bigQueryOptions).build(),
          query, projectId);
    }
  }

  /**
   * A ReaderIterator that yields TableRow objects for each row of a BigQuery table.
   */
  class BigQueryReaderIterator extends AbstractBoundedReaderIterator<WindowedValue<TableRow>> {
    private BigQueryTableRowIterator rowIterator;

    public BigQueryReaderIterator(Bigquery bigQueryClient, TableReference tableRef) {
      rowIterator = new BigQueryTableRowIterator(bigQueryClient, tableRef);
    }

    public BigQueryReaderIterator(Bigquery bigQueryClient, String query, String projectId) {
      rowIterator = new BigQueryTableRowIterator(bigQueryClient, query, projectId);
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
