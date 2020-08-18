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
import java.io.IOException;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.Histogram;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.DoubleMath;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of DoFn to perform streaming BigQuery write. */
@SystemDoFnInternal
@VisibleForTesting
class StreamingWriteFn<ErrorT, ElementT>
    extends DoFn<KV<ShardedKey<String>, TableRowInfo<ElementT>>, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingWriteFn.class);

  private final BigQueryServices bqServices;
  private final InsertRetryPolicy retryPolicy;
  private final TupleTag<ErrorT> failedOutputTag;
  private final ErrorContainer<ErrorT> errorContainer;
  private final boolean skipInvalidRows;
  private final boolean ignoreUnknownValues;
  private final boolean ignoreInsertIds;
  private final SerializableFunction<ElementT, TableRow> toTableRow;

  /** JsonTableRows to accumulate BigQuery rows in order to batch writes. */
  private transient Map<String, List<ValueInSingleWindow<TableRow>>> tableRows;

  /** The list of unique ids for each BigQuery table row. */
  private transient Map<String, List<String>> uniqueIdsForTableRows;

  private transient Histogram histogram;

  /** Tracks bytes written, exposed as "ByteCount" Counter. */
  private Counter byteCounter = SinkMetrics.bytesWritten();

  StreamingWriteFn(
      BigQueryServices bqServices,
      InsertRetryPolicy retryPolicy,
      TupleTag<ErrorT> failedOutputTag,
      ErrorContainer<ErrorT> errorContainer,
      boolean skipInvalidRows,
      boolean ignoreUnknownValues,
      boolean ignoreInsertIds,
      SerializableFunction<ElementT, TableRow> toTableRow) {
    this.bqServices = bqServices;
    this.retryPolicy = retryPolicy;
    this.failedOutputTag = failedOutputTag;
    this.errorContainer = errorContainer;
    this.skipInvalidRows = skipInvalidRows;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.ignoreInsertIds = ignoreInsertIds;
    this.toTableRow = toTableRow;
  }

  @Setup
  public void setup() {
    // record latency upto 30 seconds in the resolution of 20ms
    histogram = Histogram.of(0, 30000, 1500, true);
  }

  @Teardown
  public void teardown() {
    if (histogram.getTotalCount() > 0) {
      logPercentiles();
      histogram.clear();
    }
  }

  /** Prepares a target BigQuery table. */
  @StartBundle
  public void startBundle() {
    tableRows = new HashMap<>();
    uniqueIdsForTableRows = new HashMap<>();
  }

  /** Accumulates the input into JsonTableRows and uniqueIdsForTableRows. */
  @ProcessElement
  public void processElement(
      @Element KV<ShardedKey<String>, TableRowInfo<ElementT>> element,
      @Timestamp Instant timestamp,
      BoundedWindow window,
      PaneInfo pane) {
    String tableSpec = element.getKey().getKey();
    List<ValueInSingleWindow<TableRow>> rows =
        BigQueryHelpers.getOrCreateMapListValue(tableRows, tableSpec);
    List<String> uniqueIds =
        BigQueryHelpers.getOrCreateMapListValue(uniqueIdsForTableRows, tableSpec);

    TableRow tableRow = toTableRow.apply(element.getValue().tableRow);
    rows.add(ValueInSingleWindow.of(tableRow, timestamp, window, pane));
    uniqueIds.add(element.getValue().uniqueId);
  }

  /** Writes the accumulated rows into BigQuery with streaming API. */
  @FinishBundle
  public void finishBundle(FinishBundleContext context) throws Exception {
    List<ValueInSingleWindow<ErrorT>> failedInserts = Lists.newArrayList();
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

    for (ValueInSingleWindow<ErrorT> row : failedInserts) {
      context.output(failedOutputTag, row.getValue(), row.getTimestamp(), row.getWindow());
    }

    if (histogram.getTotalCount() > options.getLatencyLoggingFrequency()) {
      logPercentiles();
      histogram.clear();
    }
  }

  private void logPercentiles() {
    LOG.info(
        "Total number of streaming insert requests: {}, P99: {}ms, P90: {}ms, P50: {}ms",
        histogram.getTotalCount(),
        DoubleMath.roundToInt(histogram.p99(), RoundingMode.HALF_UP),
        DoubleMath.roundToInt(histogram.p90(), RoundingMode.HALF_UP),
        DoubleMath.roundToInt(histogram.p50(), RoundingMode.HALF_UP));
  }

  /** Writes the accumulated rows into BigQuery with streaming API. */
  private void flushRows(
      TableReference tableReference,
      List<ValueInSingleWindow<TableRow>> tableRows,
      List<String> uniqueIds,
      BigQueryOptions options,
      List<ValueInSingleWindow<ErrorT>> failedInserts)
      throws InterruptedException {
    if (!tableRows.isEmpty()) {
      try {
        long totalBytes =
            bqServices
                .getDatasetService(options, histogram)
                .insertAll(
                    tableReference,
                    tableRows,
                    uniqueIds,
                    retryPolicy,
                    failedInserts,
                    errorContainer,
                    skipInvalidRows,
                    ignoreUnknownValues,
                    ignoreInsertIds);
        byteCounter.inc(totalBytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
