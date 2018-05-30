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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * Implementation of DoFn to perform streaming BigQuery write.
 */
@SystemDoFnInternal
@VisibleForTesting
class StreamingWriteFn
    extends DoFn<KV<ShardedKey<String>, TableRowInfo>, Void> {
  private final BigQueryServices bqServices;
  private final InsertRetryPolicy retryPolicy;
  private final TupleTag<TableRow> failedOutputTag;


  /** JsonTableRows to accumulate BigQuery rows in order to batch writes. */
  private transient Map<String, List<ValueInSingleWindow<TableRow>>> tableRows;

  /** The list of unique ids for each BigQuery table row. */
  private transient Map<String, List<String>> uniqueIdsForTableRows;

  /** Tracks bytes written, exposed as "ByteCount" Counter. */
  private Counter byteCounter = SinkMetrics.bytesWritten();

  StreamingWriteFn(BigQueryServices bqServices, InsertRetryPolicy retryPolicy,
                   TupleTag<TableRow> failedOutputTag) {
    this.bqServices = bqServices;
    this.retryPolicy = retryPolicy;
    this.failedOutputTag = failedOutputTag;
  }

  /** Prepares a target BigQuery table. */
  @StartBundle
  public void startBundle() {
    tableRows = new HashMap<>();
    uniqueIdsForTableRows = new HashMap<>();
  }

  /** Accumulates the input into JsonTableRows and uniqueIdsForTableRows. */
  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) {
    String tableSpec = context.element().getKey().getKey();
    List<ValueInSingleWindow<TableRow>> rows =
        BigQueryHelpers.getOrCreateMapListValue(tableRows, tableSpec);
    List<String> uniqueIds =
        BigQueryHelpers.getOrCreateMapListValue(uniqueIdsForTableRows, tableSpec);

    rows.add(
        ValueInSingleWindow.of(
            context.element().getValue().tableRow, context.timestamp(), window, context.pane()));
    uniqueIds.add(context.element().getValue().uniqueId);
  }

  /** Writes the accumulated rows into BigQuery with streaming API. */
  @FinishBundle
  public void finishBundle(FinishBundleContext context) throws Exception {
    List<ValueInSingleWindow<TableRow>> failedInserts = Lists.newArrayList();
    BigQueryOptions options = context.getPipelineOptions().as(BigQueryOptions.class);
    for (Map.Entry<String, List<ValueInSingleWindow<TableRow>>> entry : tableRows.entrySet()) {
      TableReference tableReference = BigQueryHelpers.parseTableSpec(entry.getKey());
      flushRows(
          tableReference,
          entry.getValue(),
          uniqueIdsForTableRows.get(entry.getKey()),
          options,
          failedInserts);
    }
    tableRows.clear();
    uniqueIdsForTableRows.clear();

    for (ValueInSingleWindow<TableRow> row : failedInserts) {
      context.output(failedOutputTag, row.getValue(), row.getTimestamp(), row.getWindow());
    }
  }

  /**
   * Writes the accumulated rows into BigQuery with streaming API.
   */
  private void flushRows(TableReference tableReference,
                         List<ValueInSingleWindow<TableRow>> tableRows,
                         List<String> uniqueIds, BigQueryOptions options,
                         List<ValueInSingleWindow<TableRow>> failedInserts)
      throws InterruptedException {
    if (!tableRows.isEmpty()) {
      try {
        long totalBytes = bqServices.getDatasetService(options).insertAll(
            tableReference, tableRows, uniqueIds, retryPolicy, failedInserts);
        byteCounter.inc(totalBytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
