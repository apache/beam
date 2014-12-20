/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.api.client.util.Preconditions.checkNotNull;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.util.BigQueryTableRowIterator;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * A source that reads a BigQuery table and yields TableRow objects.
 *
 * <p>The source is a wrapper over the {@code BigQueryTableRowIterator} class which issues a
 * query for all rows of a table and then iterates over the result. There is no support for
 * progress reporting because the source is used only in situations where the entire table must be
 * read by each worker (i.e. the source is used as a side input).
 */
public class BigQuerySource extends Source<TableRow> {
  final TableReference tableRef;
  final BigQueryOptions bigQueryOptions;
  final Bigquery bigQueryClient;

  /** Builds a BigQuery source using pipeline options to instantiate a Bigquery client. */
  public BigQuerySource(BigQueryOptions bigQueryOptions, TableReference tableRef) {
    // Save pipeline options so that we can construct the BigQuery client on-demand whenever an
    // iterator gets created.
    this.bigQueryOptions = bigQueryOptions;
    this.tableRef = tableRef;
    this.bigQueryClient = null;
  }

  /** Builds a BigQuerySource directly using a BigQuery client. */
  public BigQuerySource(Bigquery bigQueryClient, TableReference tableRef) {
    this.bigQueryOptions = null;
    this.tableRef = tableRef;
    this.bigQueryClient = bigQueryClient;
  }

  @Override
  public SourceIterator<TableRow> iterator() throws IOException {
    return new BigQuerySourceIterator(
        bigQueryClient != null
            ? bigQueryClient
            : Transport.newBigQueryClient(bigQueryOptions).build(),
        tableRef);
  }

  /**
   * A SourceIterator that yields TableRow objects for each row of a BigQuery table.
   */
  class BigQuerySourceIterator extends AbstractSourceIterator<TableRow> {

    private BigQueryTableRowIterator rowIterator;

    public BigQuerySourceIterator(Bigquery bigQueryClient, TableReference tableRef) {
      rowIterator =  new BigQueryTableRowIterator(bigQueryClient, tableRef);
    }

    @Override
    public boolean hasNext() {
      return rowIterator.hasNext();
    }

    @Override
    public TableRow next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return rowIterator.next();
    }

    @Override
    public Progress getProgress() {
      // For now reporting progress is not supported because this source is used only when
      // an entire table needs to be read by each worker (used as a side input for instance).
      throw new UnsupportedOperationException();
    }

    @Override
    public Position updateStopPosition(Progress proposedStopPosition) {
      // For now updating the stop position is not supported because this source
      // is used only when an entire table needs to be read by each worker (used
      // as a side input for instance).
      checkNotNull(proposedStopPosition);
      throw new UnsupportedOperationException();
    }
  }
}
