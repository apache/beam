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

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.bigquery.model.TableRow;
import java.nio.ByteBuffer;
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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BufferMismatchedRows<DestinationT extends @NonNull Object, ElementT>
    extends PTransform<
        PCollection<KV<DestinationT, StoragePayloadWithDeadline>>, PCollectionTuple> {
  private static final Logger LOG = LoggerFactory.getLogger(BufferMismatchedRows.class);
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
  private static final byte[][] SHARD_KEYS = new byte[NUM_DEFAULT_SHARDS][];

  // Precalculate the shard keys.
  static {
    for (int i = 0; i < NUM_DEFAULT_SHARDS; i++) {
      SHARD_KEYS[i] = ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
    }
  }

  private static final String DRAIN_TIMEOUT_MESSAGE =
      "Timed out waiting for table schema update in OnWindowExpiration";

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
                      long shardNumber;

                      @Setup
                      public void setup() {
                        shardNumber = ThreadLocalRandom.current().nextLong(NUM_DEFAULT_SHARDS);
                      }

                      @ProcessElement
                      public void process(
                          @Element KV<DestinationT, StoragePayloadWithDeadline> element,
                          OutputReceiver<KV<ShardedKey<DestinationT>, StoragePayloadWithDeadline>>
                              o) {
                        byte[] shardKey = SHARD_KEYS[(int) (++shardNumber % NUM_DEFAULT_SHARDS)];
                        o.output(
                            KV.of(ShardedKey.of(element.getKey(), shardKey), element.getValue()));
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

    // Bounds the total time this DoFn instance spends blocking in onWindowExpiration,
    // per window, across every key it handles. Window expiration fires once per key and
    // runs synchronously on the worker thread, so a per-key budget multiplies by the
    // number of keys and can exceed the runner's bundle execution limit during a drain.
    //
    // Deliberately an instance field rather than static: a static cache would share one
    // budget across every BigQuery sink in the worker JVM.
    //
    // Keyed by window so a budget cannot leak from one window into the next. Today the
    // sink is always rewindowed into the global window by StorageApiLoads, so in practice
    // there is a single entry and expiration fires once, but nothing here enforces that
    // and a stale deadline would silently skip the retry and dead-letter the rows.
    //
    // transient, and initialized lazily rather than inline: Java deserialization does not
    // run field initializers, so an inline initializer would leave this null on workers.
    private transient @Nullable Cache<BoundedWindow, Instant> drainDeadlines = null;

    private Cache<BoundedWindow, Instant> getDrainDeadlines() {
      Cache<BoundedWindow, Instant> cache = drainDeadlines;
      if (cache == null) {
        cache =
            CacheBuilder.newBuilder().expireAfterAccess(java.time.Duration.ofMinutes(30)).build();
        drainDeadlines = cache;
      }
      return cache;
    }

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
        @Timestamp Instant originalElementTimestamp,
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
          timerRetryDuration,
          originalElementTimestamp);
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
        @Timestamp org.joda.time.Instant outputTimestamp,
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
      writeDoFn.invalidateAllAppendClientsForTable(
          tableDestination.getTableUrn(pipelineOptions.as(BigQueryOptions.class)));

      // TODO: An optimization would be to detect whether a schema change has been observed (e.g. by
      // storing a schema
      // hash in state) and to skip reading the bag in this case.
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
            timerRetryDuration,
            outputTimestamp);
      }
    }

    @OnWindowExpiration
    public void onWindowExpiration(
        OnWindowExpirationContext context,
        @Key ShardedKey<DestinationT> shardedDestination,
        @Timestamp org.joda.time.Instant elementTs,
        @StateId("mismatchedRows") BagState<StoragePayloadWithDeadline> mismatchedRowsBag,
        PipelineOptions pipelineOptions,
        BoundedWindow window,
        MultiOutputReceiver o)
        throws Exception {
      Iterable<StoragePayloadWithDeadline> bufferedRows = mismatchedRowsBag.read();
      if (Iterables.isEmpty(bufferedRows)) {
        return;
      }

      dynamicDestinations.setSideInputAccessorFromOnWindowExpirationContext(context);

      StorageApiDynamicDestinations.MessageConverter<?> messageConverter =
          writeDoFn.messageConverters.get(
              shardedDestination.getKey(),
              dynamicDestinations,
              pipelineOptions,
              writeDoFn.getDatasetService(pipelineOptions),
              writeDoFn.getWriteStreamService(pipelineOptions));
      messageConverter.updateSchemaFromTable();
      TableDestination tableDestination = dynamicDestinations.getTable(shardedDestination.getKey());
      writeDoFn.invalidateAllAppendClientsForTable(
          tableDestination.getTableUrn(pipelineOptions.as(BigQueryOptions.class)));

      java.time.Duration waitTime =
          java.time.Duration.ofMillis(
              pipelineOptions
                  .as(BigQueryOptions.class)
                  .getStorageApiMismatchDrainRetryTimeMilliSec());

      // Shared across every key in this window; see drainDeadlines.
      // TODO: This needs to use the internal clock instead Instant.now(), as the row deadlines are
      // based on the
      // internal clock. Add support for accessing the internal clock in OnWindowExpiration.
      Instant drainDeadline =
          getDrainDeadlines()
              .get(window, () -> Instant.now().plus(Duration.millis(waitTime.toMillis())));
      LOG.info(
          "Draining buffered schema-mismatched rows for table {}, waiting until {} for schema update.",
          tableDestination.getShortTableUrn(),
          drainDeadline);
      BackOff backoff =
          new ExponentialBackOff.Builder()
              .setMaxElapsedTimeMillis((int) waitTime.toMillis())
              .build();
      // Rows whose deadline has passed are written to the failed-rows collection as soon as they
      // are discovered, rather than accumulated into a list that lives until the drain finishes.
      // The drain can run for many rounds, and a row that has already been dead-lettered has no
      // further use here, so holding it only raises the peak heap of a handler whose failure
      // prevents the drain from ever completing.
      //
      // One exception: writeFailedRows needs an open bundle, because finishBundle nulls out the
      // destination map it looks the row's table up in. Rows that finishBundle itself reports as
      // mismatched therefore cannot be written where they are discovered, and are carried to the
      // next round's bundle (or to the final flush below). That carry is bounded by a single
      // append batch -- finishBundle only flushes whatever processElement left pending -- and not
      // by the size of the buffer, and it is emptied on every round.
      List<StoragePayloadWithDeadline> expiredDuringFlush = Lists.newArrayList();
      do {
        writeDoFn.startBundle();
        if (!expiredDuringFlush.isEmpty()) {
          writeDoFn.writeFailedRows(
              shardedDestination.getKey(),
              expiredDuringFlush,
              DRAIN_TIMEOUT_MESSAGE,
              BigQuerySinkMetrics.SCHEMA_MISMATCHED,
              pipelineOptions,
              elementTs,
              o.get(failedRowsTag));
          // Safe to clear: writeFailedRows consumes the iterable before returning.
          expiredDuringFlush.clear();
        }

        // Read the clock once per round so that every row in the round is judged against the same
        // instant, and so that the reading is taken before the round's work rather than after it.
        // This check is hard to test with DirectRunner. Ideally we should use the injected clock,
        // but I'm not sure how to access it from OnWindowExpiration (it's usually accessed via
        // the Timer object).
        Instant now = Instant.now();
        List<StoragePayloadWithDeadline> stillPending = Lists.newArrayList();
        for (StoragePayloadWithDeadline row : bufferedRows) {
          // Partitioned inline, while the bundle that produced these rows is still open. Both
          // halves must be derived from the same source: previously the pending rows were taken
          // from the whole mismatchedRowsList while the expired rows were taken only from
          // mismatchedDestRows, so a row returned by processElement whose deadline had passed was
          // filtered out of the pending set without ever being recorded as expired, and was
          // therefore never written to the failed-rows collection.
          for (KV<DestinationT, StoragePayloadWithDeadline> kv :
              writeDoFn.processElement(
                  pipelineOptions, KV.of(shardedDestination.getKey(), row), null, o)) {
            StoragePayloadWithDeadline mismatchedRow = kv.getValue();
            if (mismatchedRow.getDeadline().isAfter(now)) {
              stillPending.add(mismatchedRow);
            } else {
              // Written and dropped. An expired row never enters stillPending, so no later round
              // and no final flush can emit it a second time.
              writeDoFn.writeFailedRows(
                  shardedDestination.getKey(),
                  Collections.singletonList(mismatchedRow),
                  DRAIN_TIMEOUT_MESSAGE,
                  BigQuerySinkMetrics.SCHEMA_MISMATCHED,
                  pipelineOptions,
                  elementTs,
                  o.get(failedRowsTag));
            }
          }
        }

        // Once we're done, delegate to finishBundle to finish things. Its mismatched rows are
        // partitioned the same way, except that the bundle is closed by the time they are
        // returned, so the expired half has to wait for the next open bundle.
        for (KV<DestinationT, StoragePayloadWithDeadline> kv :
            writeDoFn.finishBundle(
                o.get(failedRowsTag),
                successfulRowsTag != null ? o.get(successfulRowsTag) : null,
                o.get(finalizeTag),
                null)) {
          StoragePayloadWithDeadline mismatchedRow = kv.getValue();
          if (mismatchedRow.getDeadline().isAfter(now)) {
            stillPending.add(mismatchedRow);
          } else {
            expiredDuringFlush.add(mismatchedRow);
          }
        }
        bufferedRows = stillPending;

        // Checked before sleeping so we never sleep past the shared deadline.
      } while (!Iterables.isEmpty(bufferedRows)
          && Instant.now().isBefore(drainDeadline)
          && BackOffUtils.next(com.google.api.client.util.Sleeper.DEFAULT, backoff));

      // Whichever way the loop exited, expiredDuringFlush holds at most the last round's tail, and
      // is written exactly once: either here, or by the next round that never came.
      if (!Iterables.isEmpty(bufferedRows) || !expiredDuringFlush.isEmpty()) {
        LOG.warn(
            "Timed out waiting for table schema update during drain for table {}; routing remaining buffered rows to failed-rows collection.",
            tableDestination.getShortTableUrn());
        writeDoFn.startBundle();
        writeDoFn.writeFailedRows(
            shardedDestination.getKey(),
            Iterables.concat(bufferedRows, expiredDuringFlush),
            DRAIN_TIMEOUT_MESSAGE,
            BigQuerySinkMetrics.SCHEMA_MISMATCHED,
            pipelineOptions,
            elementTs,
            o.get(failedRowsTag));
        writeDoFn.finishBundle(
            o.get(failedRowsTag),
            successfulRowsTag != null ? o.get(successfulRowsTag) : null,
            o.get(finalizeTag),
            null);
      }
    }

    @Teardown
    public void onTeardown() {
      writeDoFn.teardown();
    }
  }
}
