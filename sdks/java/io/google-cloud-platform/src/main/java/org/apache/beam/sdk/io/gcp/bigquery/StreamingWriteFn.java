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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
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
class StreamingWriteFn<T>
    extends DoFn<KV<ShardedKey<String>, KV<String, T>>, Void> {
  private final SerializableFunction<T, TableRow> formatFunction;
  private final BigQueryServices bqServices;
  private final InsertRetryPolicy retryPolicy;
  private final TupleTag<TableRow> failedOutputTag;


  /** JsonTableRows to accumulate BigQuery rows in order to batch writes. */
  private transient ArrayListMultimap<String, ValueInSingleWindow<T>> values;

  /** The list of unique ids for each BigQuery table row. */
  private transient ArrayListMultimap<String, String> uniqueIdsForValues;

  /** Tracks bytes written, exposed as "ByteCount" Counter. */
  private Counter byteCounter = SinkMetrics.bytesWritten();

  StreamingWriteFn(
      SerializableFunction<T, TableRow> formatFunction,
      BigQueryServices bqServices,
      InsertRetryPolicy retryPolicy,
      TupleTag<TableRow> failedOutputTag) {
    this.formatFunction = formatFunction;
    this.bqServices = bqServices;
    this.retryPolicy = retryPolicy;
    this.failedOutputTag = failedOutputTag;
  }

  /** Prepares a target BigQuery table. */
  @StartBundle
  public void startBundle() {
    values = ArrayListMultimap.create();
    uniqueIdsForValues = ArrayListMultimap.create();
  }

  /** Accumulates the input into JsonTableRows and uniqueIdsForTableRows. */
  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) {
    String tableSpec = context.element().getKey().getKey();
    KV<String, T> rowInfo = context.element().getValue();
    values.put(
        tableSpec,
        ValueInSingleWindow.of(rowInfo.getValue(), context.timestamp(), window, context.pane()));
    uniqueIdsForValues.put(tableSpec, rowInfo.getKey());
  }

  /** Writes the accumulated rows into BigQuery with streaming API. */
  @FinishBundle
  public void finishBundle(FinishBundleContext context) throws Exception {
    List<ValueInSingleWindow<TableRow>> failedInserts = Lists.newArrayList();
    BigQueryOptions options = context.getPipelineOptions().as(BigQueryOptions.class);
    for (String tableSpec : values.keySet()) {
      TableReference tableReference = BigQueryHelpers.parseTableSpec(tableSpec);
      flushRows(
          tableReference,
          values.get(tableSpec),
          uniqueIdsForValues.get(tableSpec),
          options,
          failedInserts);
    }
    values.clear();
    uniqueIdsForValues.clear();

    for (ValueInSingleWindow<TableRow> row : failedInserts) {
      context.output(failedOutputTag, row.getValue(), row.getTimestamp(), row.getWindow());
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
  }

  /**
   * Writes the accumulated rows into BigQuery with streaming API.
   */
  private void flushRows(TableReference tableReference,
                         List<ValueInSingleWindow<T>> values,
                         List<String> uniqueIds, BigQueryOptions options,
                         List<ValueInSingleWindow<TableRow>> failedInserts)
      throws InterruptedException {
    if (!values.isEmpty()) {
      try {
        List<ValueInSingleWindow<TableRow>> tableRows =
            Lists.newArrayListWithExpectedSize(values.size());
        for (ValueInSingleWindow<T> value : values) {
          tableRows.add(
              ValueInSingleWindow.of(
                  formatFunction.apply(value.getValue()),
                  value.getTimestamp(),
                  value.getWindow(),
                  value.getPane()));
        }
        long totalBytes = bqServices.getDatasetService(options).insertAll(
            tableReference, tableRows, uniqueIds, retryPolicy, failedInserts);
        byteCounter.inc(totalBytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
