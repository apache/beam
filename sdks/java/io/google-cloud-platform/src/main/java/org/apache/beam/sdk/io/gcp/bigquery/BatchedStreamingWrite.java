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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.metrics.MetricsLogger;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.FailsafeValueInSingleWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** PTransform to perform batched streaming BigQuery write. */
class BatchedStreamingWrite<ErrorT, ElementT>
    extends PTransform<PCollection<KV<String, TableRowInfo<ElementT>>>, PCollectionTuple> {
  private static final TupleTag<Void> mainOutputTag = new TupleTag<>("mainOutput");
  static final TupleTag<TableRow> SUCCESSFUL_ROWS_TAG = new TupleTag<>("successfulRows");

  private final BigQueryServices bqServices;
  private final InsertRetryPolicy retryPolicy;
  private final TupleTag<ErrorT> failedOutputTag;
  private final AtomicCoder<ErrorT> failedOutputCoder;
  private final ErrorContainer<ErrorT> errorContainer;
  private final boolean skipInvalidRows;
  private final boolean ignoreUnknownValues;
  private final boolean ignoreInsertIds;
  private final boolean propagateSuccessful;
  private final @Nullable SerializableFunction<ElementT, TableRow> toTableRow;
  private final @Nullable SerializableFunction<ElementT, TableRow> toFailsafeTableRow;
  private final Set<String> allowedMetricUrns;

  /** Tracks bytes written, exposed as "ByteCount" Counter. */
  private Counter byteCounter = SinkMetrics.bytesWritten();

  /** Switches the method of batching. */
  private final boolean batchViaStateful;

  public BatchedStreamingWrite(
      BigQueryServices bqServices,
      InsertRetryPolicy retryPolicy,
      TupleTag<ErrorT> failedOutputTag,
      AtomicCoder<ErrorT> failedOutputCoder,
      ErrorContainer<ErrorT> errorContainer,
      boolean skipInvalidRows,
      boolean ignoreUnknownValues,
      boolean ignoreInsertIds,
      boolean propagateSuccessful,
      @Nullable SerializableFunction<ElementT, TableRow> toTableRow,
      @Nullable SerializableFunction<ElementT, TableRow> toFailsafeTableRow) {
    this.bqServices = bqServices;
    this.retryPolicy = retryPolicy;
    this.failedOutputTag = failedOutputTag;
    this.failedOutputCoder = failedOutputCoder;
    this.errorContainer = errorContainer;
    this.skipInvalidRows = skipInvalidRows;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.ignoreInsertIds = ignoreInsertIds;
    this.propagateSuccessful = propagateSuccessful;
    this.toTableRow = toTableRow;
    this.toFailsafeTableRow = toFailsafeTableRow;
    this.allowedMetricUrns = getAllowedMetricUrns();
    this.batchViaStateful = false;
  }

  private BatchedStreamingWrite(
      BigQueryServices bqServices,
      InsertRetryPolicy retryPolicy,
      TupleTag<ErrorT> failedOutputTag,
      AtomicCoder<ErrorT> failedOutputCoder,
      ErrorContainer<ErrorT> errorContainer,
      boolean skipInvalidRows,
      boolean ignoreUnknownValues,
      boolean ignoreInsertIds,
      boolean propagateSuccessful,
      @Nullable SerializableFunction<ElementT, TableRow> toTableRow,
      @Nullable SerializableFunction<ElementT, TableRow> toFailsafeTableRow,
      boolean batchViaStateful) {
    this.bqServices = bqServices;
    this.retryPolicy = retryPolicy;
    this.failedOutputTag = failedOutputTag;
    this.failedOutputCoder = failedOutputCoder;
    this.errorContainer = errorContainer;
    this.skipInvalidRows = skipInvalidRows;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.ignoreInsertIds = ignoreInsertIds;
    this.propagateSuccessful = propagateSuccessful;
    this.toTableRow = toTableRow;
    this.toFailsafeTableRow = toFailsafeTableRow;
    this.allowedMetricUrns = getAllowedMetricUrns();
    this.batchViaStateful = batchViaStateful;
  }

  private static Set<String> getAllowedMetricUrns() {
    ImmutableSet.Builder<String> setBuilder = ImmutableSet.builder();
    setBuilder.add(MonitoringInfoConstants.Urns.API_REQUEST_COUNT);
    setBuilder.add(MonitoringInfoConstants.Urns.API_REQUEST_LATENCIES);
    return setBuilder.build();
  }

  /**
   * A transform that performs batched streaming BigQuery write; input elements are batched and
   * flushed upon bundle finalization.
   */
  public BatchedStreamingWrite<ErrorT, ElementT> viaDoFnFinalization() {
    return new BatchedStreamingWrite<>(
        bqServices,
        retryPolicy,
        failedOutputTag,
        failedOutputCoder,
        errorContainer,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        propagateSuccessful,
        toTableRow,
        toFailsafeTableRow,
        false);
  }

  /**
   * A transform that performs batched streaming BigQuery write; input elements are grouped on table
   * destinations and batched via a stateful DoFn. This also enables dynamic sharding during
   * grouping to parallelize writes.
   */
  public BatchedStreamingWrite<ErrorT, ElementT> viaStateful() {
    return new BatchedStreamingWrite<>(
        bqServices,
        retryPolicy,
        failedOutputTag,
        failedOutputCoder,
        errorContainer,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        propagateSuccessful,
        toTableRow,
        toFailsafeTableRow,
        true);
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<String, TableRowInfo<ElementT>>> input) {
    return batchViaStateful
        ? input.apply(new ViaStateful())
        : input.apply(new ViaBundleFinalization());
  }

  private class ViaBundleFinalization
      extends PTransform<PCollection<KV<String, TableRowInfo<ElementT>>>, PCollectionTuple> {
    @Override
    public PCollectionTuple expand(PCollection<KV<String, TableRowInfo<ElementT>>> input) {
      PCollectionTuple result =
          input.apply(
              ParDo.of(new BatchAndInsertElements(propagateSuccessful))
                  .withOutputTags(
                      mainOutputTag,
                      propagateSuccessful
                          ? TupleTagList.of(failedOutputTag).and(SUCCESSFUL_ROWS_TAG)
                          : TupleTagList.of(failedOutputTag)));
      result.get(failedOutputTag).setCoder(failedOutputCoder);
      if (propagateSuccessful) {
        result.get(SUCCESSFUL_ROWS_TAG).setCoder(TableRowJsonCoder.of());
      }
      return result;
    }
  }

  @VisibleForTesting
  private class BatchAndInsertElements extends DoFn<KV<String, TableRowInfo<ElementT>>, Void> {

    /** JsonTableRows to accumulate BigQuery rows in order to batch writes. */
    private transient @Nullable Map<String, List<FailsafeValueInSingleWindow<TableRow, TableRow>>>
        tableRows = null;

    /** The list of unique ids for each BigQuery table row. */
    private transient @Nullable Map<String, List<String>> uniqueIdsForTableRows = null;

    private transient @Nullable DatasetService datasetService;

    private final boolean propagateSuccessfulInserts;

    BatchAndInsertElements(boolean propagateSuccessful) {
      this.propagateSuccessfulInserts = propagateSuccessful;
    }

    private DatasetService getDatasetService(PipelineOptions pipelineOptions) throws IOException {
      if (datasetService == null) {
        datasetService = bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
      }
      return datasetService;
    }

    /** Prepares a target BigQuery table. */
    @StartBundle
    public void startBundle() {
      tableRows = new HashMap<>();
      uniqueIdsForTableRows = new HashMap<>();
    }

    /** Accumulates the input into JsonTableRows and uniqueIdsForTableRows. */
    @ProcessElement
    @RequiresNonNull({"tableRows", "uniqueIdsForTableRows", "toTableRow", "toFailsafeTableRow"})
    public void processElement(
        @Element KV<String, TableRowInfo<ElementT>> element,
        @Timestamp Instant timestamp,
        BoundedWindow window,
        PaneInfo pane) {
      Map<String, List<FailsafeValueInSingleWindow<TableRow, TableRow>>> tableRows = this.tableRows;
      Map<String, List<String>> uniqueIdsForTableRows = this.uniqueIdsForTableRows;
      String tableSpec = element.getKey();
      TableRow tableRow = toTableRow.apply(element.getValue().tableRow);
      TableRow failsafeTableRow = toFailsafeTableRow.apply(element.getValue().tableRow);
      tableRows
          .computeIfAbsent(tableSpec, k -> new ArrayList<>())
          .add(FailsafeValueInSingleWindow.of(tableRow, timestamp, window, pane, failsafeTableRow));
      uniqueIdsForTableRows
          .computeIfAbsent(tableSpec, k -> new ArrayList<>())
          .add(element.getValue().uniqueId);
    }

    /** Writes the accumulated rows into BigQuery with streaming API. */
    @FinishBundle
    @RequiresNonNull({"tableRows", "uniqueIdsForTableRows"})
    public void finishBundle(FinishBundleContext context) throws Exception {
      Map<String, List<FailsafeValueInSingleWindow<TableRow, TableRow>>> tableRows = this.tableRows;
      Map<String, List<String>> uniqueIdsForTableRows = this.uniqueIdsForTableRows;
      List<ValueInSingleWindow<ErrorT>> failedInserts = Lists.newArrayList();
      List<ValueInSingleWindow<TableRow>> successfulInserts = Lists.newArrayList();
      BigQueryOptions options = context.getPipelineOptions().as(BigQueryOptions.class);
      for (Map.Entry<String, List<FailsafeValueInSingleWindow<TableRow, TableRow>>> entry :
          tableRows.entrySet()) {
        TableReference tableReference = BigQueryHelpers.parseTableSpec(entry.getKey());
        flushRows(
            getDatasetService(options),
            tableReference,
            entry.getValue(),
            uniqueIdsForTableRows.get(entry.getKey()),
            failedInserts,
            successfulInserts);
      }
      tableRows.clear();
      uniqueIdsForTableRows.clear();

      for (ValueInSingleWindow<ErrorT> row : failedInserts) {
        context.output(failedOutputTag, row.getValue(), row.getTimestamp(), row.getWindow());
      }
      if (propagateSuccessfulInserts) {
        for (ValueInSingleWindow<TableRow> row : successfulInserts) {
          context.output(SUCCESSFUL_ROWS_TAG, row.getValue(), row.getTimestamp(), row.getWindow());
        }
      }
      reportStreamingApiLogging(options);
    }

    @Teardown
    public void onTeardown() {
      try {
        if (datasetService != null) {
          datasetService.close();
          datasetService = null;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  // The max duration input records are allowed to be buffered in the state, if using ViaStateful.
  private static final Duration BATCH_MAX_BUFFERING_DURATION = Duration.millis(200);

  private class ViaStateful
      extends PTransform<PCollection<KV<String, TableRowInfo<ElementT>>>, PCollectionTuple> {
    @Override
    public PCollectionTuple expand(PCollection<KV<String, TableRowInfo<ElementT>>> input) {
      BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);
      Duration maxBufferingDuration =
          options.getMaxBufferingDurationMilliSec() > 0
              ? Duration.millis(options.getMaxBufferingDurationMilliSec())
              : BATCH_MAX_BUFFERING_DURATION;
      KvCoder<String, TableRowInfo<ElementT>> inputCoder = (KvCoder) input.getCoder();
      TableRowInfoCoder<ElementT> valueCoder =
          (TableRowInfoCoder) inputCoder.getCoderArguments().get(1);
      PCollectionTuple result =
          input
              // Apply a global window to avoid GroupIntoBatches below performs tiny grouping
              // partitioned by windows.
              .apply(
                  Window.<KV<String, TableRowInfo<ElementT>>>into(new GlobalWindows())
                      .triggering(DefaultTrigger.of())
                      .discardingFiredPanes())
              // Group and batch table rows so that we flush once we have enough elements for an
              // insert rpc batch. Also set a buffering time limit to avoid being  stuck at a
              // partial batch forever, especially in a global window.
              .apply(
                  GroupIntoBatches.<String, TableRowInfo<ElementT>>ofSize(
                          options.getMaxStreamingRowsToBatch())
                      .withMaxBufferingDuration(maxBufferingDuration)
                      .withShardedKey())
              // Undo the batching performed and flush output using finalization.  This may build up
              // batches of rows to insert beyond the batch size but that is handled with parallel
              // insert requests in the underlying BQ services.
              .apply(
                  ParDo.of(
                      new DoFn<
                          KV<ShardedKey<String>, Iterable<TableRowInfo<ElementT>>>,
                          KV<String, TableRowInfo<ElementT>>>() {
                        @ProcessElement
                        public void apply(
                            ProcessContext context,
                            @Element
                                KV<ShardedKey<String>, Iterable<TableRowInfo<ElementT>>> element) {
                          String key = element.getKey().getKey();
                          for (TableRowInfo<ElementT> value : element.getValue()) {
                            context.output(KV.of(key, value));
                          }
                        }
                      }))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), valueCoder))
              // TODO(BEAM-11408): This transform requires stable inputs. Currently it relies on the
              // fact that the upstream transform GroupIntoBatches produces stable outputs as
              // opposed to using the annotation @RequiresStableInputs, to avoid potential
              // performance penalty due to extra data shuffling.
              .apply(
                  ParDo.of(new BatchAndInsertElements(propagateSuccessful))
                      .withOutputTags(
                          mainOutputTag,
                          propagateSuccessful
                              ? TupleTagList.of(failedOutputTag).and(SUCCESSFUL_ROWS_TAG)
                              : TupleTagList.of(failedOutputTag)));
      result.get(failedOutputTag).setCoder(failedOutputCoder);
      if (propagateSuccessful) {
        result.get(SUCCESSFUL_ROWS_TAG).setCoder(TableRowJsonCoder.of());
      }
      return result;
    }
  }

  /** Writes the accumulated rows into BigQuery with streaming API. */
  private void flushRows(
      DatasetService datasetService,
      TableReference tableReference,
      List<FailsafeValueInSingleWindow<TableRow, TableRow>> tableRows,
      @Nullable List<String> uniqueIds,
      List<ValueInSingleWindow<ErrorT>> failedInserts,
      List<ValueInSingleWindow<TableRow>> successfulInserts)
      throws InterruptedException {
    if (!tableRows.isEmpty()) {
      try {
        long totalBytes =
            datasetService.insertAll(
                tableReference,
                tableRows,
                uniqueIds,
                retryPolicy,
                failedInserts,
                errorContainer,
                skipInvalidRows,
                ignoreUnknownValues,
                ignoreInsertIds,
                successfulInserts);
        byteCounter.inc(totalBytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void reportStreamingApiLogging(BigQueryOptions options) {
    MetricsContainer processWideContainer = MetricsEnvironment.getProcessWideContainer();
    if (processWideContainer instanceof MetricsLogger) {
      MetricsLogger processWideMetricsLogger = (MetricsLogger) processWideContainer;
      processWideMetricsLogger.tryLoggingMetrics(
          "API call Metrics: \n",
          this.allowedMetricUrns,
          options.getBqStreamingApiLoggingFrequencySec() * 1000L);
    }
  }
}
