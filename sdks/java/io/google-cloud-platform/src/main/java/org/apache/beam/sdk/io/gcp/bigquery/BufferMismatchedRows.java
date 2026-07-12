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

import com.google.api.services.bigquery.model.TableRow;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

class BufferMismatchedRows<DestinationT extends @NonNull Object, ElementT>
    extends PTransform<
        PCollection<KV<DestinationT, StoragePayloadWithDeadline>>, PCollectionTuple> {
  private final Coder<BigQueryStorageApiInsertError> failedRowsCoder;
  private final Coder<TableRow> successfulRowsCoder;
  private final Coder<DestinationT> destinationCoder;
  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private final StorageApiWriteUnshardedRecords.WriteRecordsDoFnImpl<DestinationT, ElementT>
      writeDoFn;
  private final TupleTag<BigQueryStorageApiInsertError> failedRowsTag;
  private final @Nullable TupleTag<TableRow> successfulRowsTag;
  // This output is effectively ignored, since we only support this code path for
  // StorageApiWriteRecordsInconsistent.
  private final TupleTag<KV<String, String>> finalizeTag = new TupleTag<>("finalizeTag");
  private static final int NUM_DEFAULT_SHARDS = 20;

  public BufferMismatchedRows(
      Coder<BigQueryStorageApiInsertError> failedRowsCoder,
      Coder<TableRow> successfulRowsCoder,
      Coder<DestinationT> destinationCoder,
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      StorageApiWriteUnshardedRecords.WriteRecordsDoFnImpl<DestinationT, ElementT> writeDoFn,
      TupleTag<BigQueryStorageApiInsertError> failedRowsTag,
      @Nullable TupleTag<TableRow> successfulRowsTag) {
    this.failedRowsCoder = failedRowsCoder;
    this.successfulRowsCoder = successfulRowsCoder;
    this.destinationCoder = destinationCoder;
    this.dynamicDestinations = dynamicDestinations;
    this.writeDoFn = writeDoFn;
    this.failedRowsTag = failedRowsTag;
    this.successfulRowsTag = successfulRowsTag;
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<DestinationT, StoragePayloadWithDeadline>> input) {
    // Append records to the Storage API streams.
    TupleTagList tupleTagList = TupleTagList.of(failedRowsTag);
    if (successfulRowsTag != null) {
      tupleTagList = tupleTagList.and(successfulRowsTag);
    }

    PCollectionTuple result =
        input
            .apply(
                "addShard",
                ParDo.of(
                    new DoFn<
                        KV<DestinationT, StoragePayloadWithDeadline>,
                        KV<ShardedKey<DestinationT>, StoragePayloadWithDeadline>>() {
                      int shardNumber;

                      @Setup
                      public void setup() {
                        shardNumber = ThreadLocalRandom.current().nextInt(NUM_DEFAULT_SHARDS);
                      }

                      @ProcessElement
                      public void process(
                          @Element KV<DestinationT, StoragePayloadWithDeadline> element,
                          OutputReceiver<KV<ShardedKey<DestinationT>, StoragePayloadWithDeadline>>
                              o) {
                        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                        buffer.putInt(++shardNumber % NUM_DEFAULT_SHARDS);
                        o.output(
                            KV.of(
                                ShardedKey.of(element.getKey(), buffer.array()),
                                element.getValue()));
                      }
                    }))
            .setCoder(
                KvCoder.of(
                    ShardedKey.Coder.of(destinationCoder), StoragePayloadWithDeadline.Coder.of()))
            .apply(
                "bufferMismatchedRows",
                ParDo.of(new BufferingDoFn(writeDoFn))
                    .withOutputTags(finalizeTag, tupleTagList)
                    .withSideInputs(dynamicDestinations.getSideInputs()));

    result.get(failedRowsTag).setCoder(failedRowsCoder);
    if (successfulRowsTag != null) {
      result.get(successfulRowsTag).setCoder(successfulRowsCoder);
    }
    return result;
  }

  class BufferingDoFn
      extends DoFn<KV<ShardedKey<DestinationT>, StoragePayloadWithDeadline>, KV<String, String>> {
    private final StorageApiWriteUnshardedRecords.WriteRecordsDoFnImpl<DestinationT, ElementT>
        writeDoFn;

    @StateId("mismatchedRows")
    private final StateSpec<BagState<StoragePayloadWithDeadline>> mismatchedRowsSpec =
        StateSpecs.bag(StoragePayloadWithDeadline.Coder.of());

    @TimerId("retryMismatchedRowsTimer")
    private final TimerSpec mismatchedRowsTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @StateId("currentMismatchedRowTimerValue")
    private final StateSpec<ValueState<Long>> currentMismatchedRowTimerValueSpec =
        StateSpecs.value();

    @StateId("minPendingTimestamp")
    private final StateSpec<ValueState<Long>> minPendingTimestampSpec = StateSpecs.value();

    private final Counter rowsSentToFailedRowsCollection =
        Metrics.counter(BufferMismatchedRows.BufferingDoFn.class, "rowsSentToFailedRowsCollection");

    public BufferingDoFn(
        StorageApiWriteUnshardedRecords.WriteRecordsDoFnImpl<DestinationT, ElementT> writeDoFn) {
      this.writeDoFn = writeDoFn;
    }

    @ProcessElement
    public void process(
        PipelineOptions pipelineOptions,
        ProcessContext processContext,
        @Element KV<ShardedKey<DestinationT>, StoragePayloadWithDeadline> element,
        @StateId("mismatchedRows") BagState<StoragePayloadWithDeadline> mismatchedRowsBag,
        @TimerId("retryMismatchedRowsTimer") Timer retryTimer,
        @StateId("currentMismatchedRowTimerValue") ValueState<Long> currentTimerValue,
        @StateId("minPendingTimestamp") ValueState<Long> minPendingTimestamp,
        MultiOutputReceiver o)
        throws Exception {
      dynamicDestinations.setSideInputAccessorFromProcessContext(processContext);
      TableDestination tableDestination = dynamicDestinations.getTable(element.getKey().getKey());

      Duration timerRetryDuration =
          Duration.millis(
              pipelineOptions.as(BigQueryOptions.class).getStorageApiMismatchRetryTimeMilliSec());
      SchemaChangeDetectorHelper.bufferMismatchedRows(
          Collections.singleton(element.getValue()),
          mismatchedRowsBag,
          retryTimer,
          currentTimerValue,
          minPendingTimestamp,
          tableDestination,
          o.get(failedRowsTag),
          null,
          rowsSentToFailedRowsCollection,
          timerRetryDuration);
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return Duration.millis(Long.MAX_VALUE);
    }

    @OnTimer("retryMismatchedRowsTimer")
    public void onTimer(
        OnTimerContext context,
        @Key ShardedKey<DestinationT> shardedDestination,
        @StateId("mismatchedRows") BagState<StoragePayloadWithDeadline> mismatchedRowsBag,
        @StateId("currentMismatchedRowTimerValue") ValueState<Long> currentTimerValue,
        @StateId("minPendingTimestamp") ValueState<Long> minPendingTimestamp,
        @TimerId("retryMismatchedRowsTimer") Timer retryTimer,
        PipelineOptions pipelineOptions,
        MultiOutputReceiver o)
        throws Exception {
      dynamicDestinations.setSideInputAccessorFromOnTimerContext(context);
      writeDoFn.startBundle();

      mismatchedRowsBag.readLater();
      currentTimerValue.readLater();
      minPendingTimestamp.readLater();

      TableDestination tableDestination = dynamicDestinations.getTable(shardedDestination.getKey());
      StorageApiDynamicDestinations.MessageConverter<?> messageConverter =
          writeDoFn.messageConverters.get(
              shardedDestination.getKey(),
              dynamicDestinations,
              pipelineOptions,
              writeDoFn.getDatasetService(pipelineOptions),
              writeDoFn.getWriteStreamService(pipelineOptions));
      messageConverter.updateSchemaFromTable();

      List<Iterable<KV<DestinationT, StoragePayloadWithDeadline>>> mismatchedRowsList =
          Lists.newArrayList();
      for (StoragePayloadWithDeadline row : mismatchedRowsBag.read()) {
        Iterable<KV<DestinationT, StoragePayloadWithDeadline>> mismatchedRows =
            writeDoFn.processElement(
                pipelineOptions, KV.of(shardedDestination.getKey(), row), null, o);
        if (!Iterables.isEmpty(mismatchedRows)) {
          mismatchedRowsList.add(mismatchedRows);
        }
      }
      // Once we're done, delegate to finishBundle to finish things.
      Iterable<KV<DestinationT, StoragePayloadWithDeadline>> mismatchedDestRows =
          writeDoFn.finishBundle(
              o.get(failedRowsTag),
              successfulRowsTag != null ? o.get(successfulRowsTag) : null,
              o.get(finalizeTag),
              null);
      if (!Iterables.isEmpty(mismatchedDestRows)) {
        mismatchedRowsList.add(mismatchedDestRows);
      }

      mismatchedRowsBag.clear();
      currentTimerValue.clear();
      minPendingTimestamp.clear();
      if (!mismatchedRowsList.isEmpty()) {
        AppendClientInfo appendClientInfo =
            AppendClientInfo.of(
                messageConverter.getTableSchema(),
                messageConverter.getDescriptor(writeDoFn.usesCdc),
                AutoCloseable::close);

        Iterable<StoragePayloadWithDeadline> mismatchedRows =
            () ->
                StreamSupport.stream(Iterables.concat(mismatchedRowsList).spliterator(), false)
                    .map(KV::getValue)
                    .iterator();

        Duration timerRetryDuration =
            Duration.millis(
                pipelineOptions.as(BigQueryOptions.class).getStorageApiMismatchRetryTimeMilliSec());
        SchemaChangeDetectorHelper.bufferMismatchedRows(
            mismatchedRows,
            mismatchedRowsBag,
            retryTimer,
            currentTimerValue,
            minPendingTimestamp,
            tableDestination,
            o.get(failedRowsTag),
            appendClientInfo,
            rowsSentToFailedRowsCollection,
            timerRetryDuration);
      }
    }

    @OnWindowExpiration
    public void onWindowExpiration(
        OnWindowExpirationContext context,
        @Key ShardedKey<DestinationT> shardedDestination,
        @Timestamp org.joda.time.Instant elementTs,
        @StateId("mismatchedRows") BagState<StoragePayloadWithDeadline> mismatchedRowsBag,
        PipelineOptions pipelineOptions,
        MultiOutputReceiver o)
        throws Exception {
      dynamicDestinations.setSideInputAccessorFromOnWindowExpirationContext(context);

      StorageApiDynamicDestinations.MessageConverter<?> messageConverter =
          writeDoFn.messageConverters.get(
              shardedDestination.getKey(),
              dynamicDestinations,
              pipelineOptions,
              writeDoFn.getDatasetService(pipelineOptions),
              writeDoFn.getWriteStreamService(pipelineOptions));
      messageConverter.updateSchemaFromTable();

      java.time.Duration waitTime =
          java.time.Duration.ofMillis(
              pipelineOptions
                  .as(BigQueryOptions.class)
                  .getStorageApiMismatchDrainRetryTimeMilliSec());

      Iterable<StoragePayloadWithDeadline> bufferedRows = mismatchedRowsBag.read();
      Instant start = Instant.now();
      while (!Iterables.isEmpty(bufferedRows) && start.plus(waitTime).isAfter(Instant.now())) {
        writeDoFn.startBundle();
        List<Iterable<KV<DestinationT, StoragePayloadWithDeadline>>> mismatchedRowsList =
            Lists.newArrayList();
        for (StoragePayloadWithDeadline row : bufferedRows) {
          Iterable<KV<DestinationT, StoragePayloadWithDeadline>> mismatchedRows =
              writeDoFn.processElement(
                  pipelineOptions, KV.of(shardedDestination.getKey(), row), null, o);
          if (!Iterables.isEmpty(mismatchedRows)) {
            mismatchedRowsList.add(mismatchedRows);
          }
        }
        // Once we're done, delegate to finishBundle to finish things.
        Iterable<KV<DestinationT, StoragePayloadWithDeadline>> mismatchedDestRows =
            writeDoFn.finishBundle(
                o.get(failedRowsTag),
                successfulRowsTag != null ? o.get(successfulRowsTag) : null,
                o.get(finalizeTag),
                null);
        if (!Iterables.isEmpty(mismatchedDestRows)) {
          mismatchedRowsList.add(mismatchedDestRows);
        }

        bufferedRows =
            () ->
                StreamSupport.stream(Iterables.concat(mismatchedRowsList).spliterator(), false)
                    .map(KV::getValue)
                    .iterator();
      }
      writeDoFn.writeFailedRows(
          shardedDestination.getKey(),
          bufferedRows,
          "Timed out waiting for table schema update in OnWindowExpiration",
          pipelineOptions,
          elementTs,
          o.get(failedRowsTag));
    }

    @Teardown
    public void onTeardown() {
      writeDoFn.teardown();
    }
  }
}
