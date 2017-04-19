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

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.values.KV;

/**
 * Implementation of DoFn to perform streaming BigQuery write.
 */
@SystemDoFnInternal
@VisibleForTesting
class StreamingWriteFn
    extends DoFn<KV<ShardedKey<String>, TableRowInfo>, Void> {
  private final BigQueryServices bqServices;

  /** JsonTableRows to accumulate BigQuery rows in order to batch writes. */
  private transient Map<String, List<TableRow>> tableRows;

  /** The list of unique ids for each BigQuery table row. */
  private transient Map<String, List<String>> uniqueIdsForTableRows;

  /** Tracks bytes written, exposed as "ByteCount" Counter. */
  private Counter byteCounter = Metrics.counter(StreamingWriteFn.class, "ByteCount");

  StreamingWriteFn(BigQueryServices bqServices) {
    this.bqServices = bqServices;
  }

  /** Prepares a target BigQuery table. */
  @StartBundle
  public void startBundle(Context context) {
    tableRows = new HashMap<>();
    uniqueIdsForTableRows = new HashMap<>();
  }

  /** Accumulates the input into JsonTableRows and uniqueIdsForTableRows. */
  @ProcessElement
  public void processElement(ProcessContext context) {
    String tableSpec = context.element().getKey().getKey();
    List<TableRow> rows = BigQueryHelpers.getOrCreateMapListValue(tableRows, tableSpec);
    List<String> uniqueIds = BigQueryHelpers.getOrCreateMapListValue(uniqueIdsForTableRows,
        tableSpec);

    rows.add(context.element().getValue().tableRow);
    uniqueIds.add(context.element().getValue().uniqueId);
  }

  /** Writes the accumulated rows into BigQuery with streaming API. */
  @FinishBundle
  public void finishBundle(Context context) throws Exception {
    BigQueryOptions options = context.getPipelineOptions().as(BigQueryOptions.class);
    for (Map.Entry<String, List<TableRow>> entry : tableRows.entrySet()) {
      TableReference tableReference = BigQueryHelpers.parseTableSpec(entry.getKey());
      flushRows(tableReference, entry.getValue(),
          uniqueIdsForTableRows.get(entry.getKey()), options);
    }
    tableRows.clear();
    uniqueIdsForTableRows.clear();
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
  }

  /**
   * Writes the accumulated rows into BigQuery with streaming API.
   */
  private void flushRows(TableReference tableReference,
      List<TableRow> tableRows, List<String> uniqueIds, BigQueryOptions options)
          throws InterruptedException {
    if (!tableRows.isEmpty()) {
      try {
        long totalBytes = bqServices.getDatasetService(options).insertAll(
            tableReference, tableRows, uniqueIds);
        byteCounter.inc(totalBytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
