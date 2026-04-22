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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * This is a stateful DoFn that buffers elements that triggered table schema update. Once the table
 * schema has been updated, this reprocesses the messages and allows them to continue on through the
 * sink. This DoFn receives messages from both {@link ConvertMessagesDoFn} and {@link
 * PatchTableSchemaDoFn}. {@link ConvertMessagesDoFn} sends elements to be buffered. {@link
 * PatchTableSchemaDoFn} sends a null element as a sentinal to indicate that the table has recently
 * been patched, which triggers us to immediately try and reprocess the buffered elements.
 */
public class SchemaUpdateHoldingFn<DestinationT extends @NonNull Object, ElementT>
    extends DoFn<
        KV<ShardedKey<DestinationT>, @Nullable ElementT>,
        KV<DestinationT, StorageApiWritePayload>> {
  private static final Duration POLL_DURATION = Duration.standardSeconds(1);

  @StateId("bufferedElements")
  private final StateSpec<BagState<TimestampedValue<ElementT>>> bufferedSpec;

  @StateId("minBufferedTimestamp")
  private final StateSpec<CombiningState<Long, long[], Long>> minBufferedTsSpec;

  @StateId("timerTimestamp")
  private final StateSpec<ValueState<Long>> timerTsSpec;

  @TimerId("pollTimer")
  private final TimerSpec pollTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private final ConvertMessagesDoFn<DestinationT, ElementT> convertMessagesDoFn;

  public SchemaUpdateHoldingFn(
      Coder<ElementT> elementCoder,
      ConvertMessagesDoFn<DestinationT, ElementT> convertMessagesDoFn) {
    this.convertMessagesDoFn = convertMessagesDoFn;
    this.bufferedSpec = StateSpecs.bag(TimestampedValue.TimestampedValueCoder.of(elementCoder));
    this.timerTsSpec = StateSpecs.value();

    Combine.BinaryCombineLongFn minCombineFn =
        new Combine.BinaryCombineLongFn() {
          @Override
          public long identity() {
            return BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
          }

          @Override
          public long apply(long left, long right) {
            return Math.min(left, right);
          }
        };
    this.minBufferedTsSpec = StateSpecs.combining(minCombineFn);
  }

  @StartBundle
  public void startBundle() {
    convertMessagesDoFn.startBundle();
    ;
  }

  @Teardown
  public void onTeardown() {
    convertMessagesDoFn.onTeardown();
  }

  @ProcessElement
  public void processElement(
      @Element KV<ShardedKey<DestinationT>, @Nullable ElementT> element,
      @Timestamp Instant timestamp,
      @StateId("bufferedElements") BagState<TimestampedValue<ElementT>> bag,
      @StateId("minBufferedTimestamp") CombiningState<Long, long[], Long> minBufferedTimestamp,
      @StateId("timerTimestamp") ValueState<Long> timerTs,
      @TimerId("pollTimer") Timer pollTimer,
      ProcessContext context,
      BoundedWindow window,
      MultiOutputReceiver o)
      throws Exception {
    convertMessagesDoFn.getDynamicDestinations().setSideInputAccessorFromProcessContext(context);

    minBufferedTimestamp.readLater();
    timerTs.readLater();
    ElementT value = element.getValue();
    Instant newTimerTs = null;
    if (value != null) {
      // Buffer the element.
      bag.add(TimestampedValue.of(value, timestamp));
      minBufferedTimestamp.add(timestamp.getMillis());
      Long currentTimerTs = timerTs.read();
      // We always have to reset the timer to update the output timestamp, however if there already
      // is a timer then
      // we keep the current expiration.
      newTimerTs =
          currentTimerTs == null
              ? pollTimer.getCurrentRelativeTime().plus(POLL_DURATION)
              : Instant.ofEpochMilli(currentTimerTs);
    } else {
      // This means that the table schema was recently updated. Try to flush the pending elements.
      if (tryFlushBuffer(
          element.getKey().getKey(), context.getPipelineOptions(), bag, minBufferedTimestamp, o)) {
        // Nothing left in buffer. clear timer.
        pollTimer.clear();
        timerTs.clear();
      } else {
        // We just scanned the buffer, so bump the timer to the next poll duration.
        newTimerTs = pollTimer.getCurrentRelativeTime().plus(POLL_DURATION);
      }
    }
    if (newTimerTs != null) {
      pollTimer
          .withOutputTimestamp(Instant.ofEpochMilli(minBufferedTimestamp.read()))
          .set(newTimerTs);
      timerTs.write(newTimerTs.getMillis());
    }
  }

  @Override
  public Duration getAllowedTimestampSkew() {
    // This is safe because a watermark hold will always be set using timer.withOutputTimestamp.
    return Duration.millis(Long.MAX_VALUE);
  }

  @OnTimer("pollTimer")
  public void onPollTimer(
      @Key ShardedKey<DestinationT> key,
      PipelineOptions pipelineOptions,
      @StateId("bufferedElements") BagState<TimestampedValue<ElementT>> bag,
      @StateId("minBufferedTimestamp") CombiningState<Long, long[], Long> minBufferedTimestamp,
      @StateId("timerTimestamp") ValueState<Long> timerTs,
      @TimerId("pollTimer") Timer pollTimer,
      BoundedWindow window,
      MultiOutputReceiver o)
      throws Exception {
    if (tryFlushBuffer(key.getKey(), pipelineOptions, bag, minBufferedTimestamp, o)) {
      timerTs.clear();
    } else {
      // We still have buffered elements. Make sure that the polling timer keeps looping.
      Instant newTimerTs = pollTimer.getCurrentRelativeTime().plus(POLL_DURATION);
      pollTimer
          .withOutputTimestamp(Instant.ofEpochMilli(minBufferedTimestamp.read()))
          .set(newTimerTs);
      timerTs.write(newTimerTs.getMillis());
    }
  }

  @OnWindowExpiration
  public void onWindowExpiration(
      @Key ShardedKey<DestinationT> key,
      PipelineOptions pipelineOptions,
      @StateId("bufferedElements") BagState<TimestampedValue<ElementT>> bag,
      @StateId("minBufferedTimestamp") CombiningState<Long, long[], Long> minBufferedTimestamp,
      MultiOutputReceiver o)
      throws Exception {
    // This can happen on test completion or drain. We can't set any more timers in window
    // expiration, so we just have to loop until the schema is updated.
    BackOff backoff =
        new ExponentialBackOff.Builder()
            .setMaxElapsedTimeMillis((int) TimeUnit.SECONDS.toMillis(10))
            .build();
    do {
      if (tryFlushBuffer(key.getKey(), pipelineOptions, bag, minBufferedTimestamp, o)) {
        return;
      }
    } while (BackOffUtils.next(com.google.api.client.util.Sleeper.DEFAULT, backoff));
    throw new RuntimeException("Failed to flush elements on window expiration!");
  }

  // Returns true if the buffer is completely flushed.
  public boolean tryFlushBuffer(
      DestinationT destination,
      PipelineOptions pipelineOptions,
      @StateId("bufferedElements") BagState<TimestampedValue<ElementT>> bag,
      @StateId("minBufferedTimestamp") CombiningState<Long, long[], Long> minBufferedTimestamp,
      MultiOutputReceiver o)
      throws Exception {
    // Force an update of the MessageConverter schema.
    StorageApiDynamicDestinations.MessageConverter<ElementT> messageConverter =
        convertMessagesDoFn
            .getMessageConverters()
            .get(
                destination,
                convertMessagesDoFn.getDynamicDestinations(),
                pipelineOptions,
                convertMessagesDoFn.getDatasetService(pipelineOptions),
                convertMessagesDoFn.getWriteStreamService(pipelineOptions));
    messageConverter.updateSchemaFromTable();

    List<TimestampedValue<ElementT>> stillWaiting = Lists.newArrayList();
    minBufferedTimestamp.clear();

    Iterable<TimestampedValue<KV<DestinationT, ElementT>>> kvBagElements =
        Iterables.transform(
            bag.read(),
            e -> TimestampedValue.of(KV.of(destination, e.getValue()), e.getTimestamp()));

    TableRowToStorageApiProto.ErrorCollector errorCollector =
        UpgradeTableSchema.newErrorCollector();
    Iterable<TimestampedValue<KV<DestinationT, ElementT>>> unProcessed =
        convertMessagesDoFn.handleProcessElements(
            messageConverter, kvBagElements, o, errorCollector);
    if (!errorCollector.isEmpty()) {
      // Collect all elements that still fail to convert.
      unProcessed.forEach(
          tv -> {
            stillWaiting.add(TimestampedValue.of(tv.getValue().getValue(), tv.getTimestamp()));
            minBufferedTimestamp.add(tv.getTimestamp().getMillis());
          });
    }

    // Add the remaining elements back into the bag.
    bag.clear();
    stillWaiting.forEach(bag::add);

    return stillWaiting.isEmpty();
  }
}
