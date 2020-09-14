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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that batches inputs to a desired batch size. Batches will contain only
 * elements of a single key.
 *
 * <p>Elements are buffered until there are {@code batchSize} elements, at which point they are
 * emitted to the output {@link PCollection}. A {@code maxBufferingDuration} can be set to emit
 * output early and avoid waiting for a full batch forever.
 *
 * <p>Windows are preserved (batches contain elements from the same window). Batches may contain
 * elements from more than one bundle.
 *
 * <p>Example 1 (batch call a webservice and get return codes):
 *
 * <pre>{@code
 * PCollection<KV<String, String>> input = ...;
 * long batchSize = 100L;
 * PCollection<KV<String, Iterable<String>>> batched = input
 *     .apply(GroupIntoBatches.<String, String>ofSize(batchSize))
 *     .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())))
 *     .apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, KV<String, String>>() }{
 *        {@code @ProcessElement
 *         public void processElement(@Element KV<String, Iterable<String>> element,
 *             OutputReceiver<KV<String, String>> r) {
 *             r.output(KV.of(element.getKey(), callWebService(element.getValue())));
 *         }
 *     }}));
 * </pre>
 *
 * <p>Example 2 (batch unbounded input in a global window):
 *
 * <pre>{@code
 * PCollection<KV<String, String>> unboundedInput = ...;
 * long batchSize = 100L;
 * Duration maxBufferingDuration = Duration.standardSeconds(10);
 * PCollection<KV<String, Iterable<String>>> batched = unboundedInput
 *     .apply(Window.<KV<String, String>>into(new GlobalWindows())
 *         .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
 *         .discardingFiredPanes())
 *     .apply(GroupIntoBatches.<String, String>ofSize(batchSize)
 *         .withMaxBufferingDuration(maxBufferingDuration));
 * }</pre>
 */
public class GroupIntoBatches<K, InputT>
    extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>> {

  private final long batchSize;
  @Nullable private final Duration maxBufferingDuration;

  private GroupIntoBatches(long batchSize, @Nullable Duration maxBufferingDuration) {
    this.batchSize = batchSize;
    this.maxBufferingDuration = maxBufferingDuration;
  }

  public static <K, InputT> GroupIntoBatches<K, InputT> ofSize(long batchSize) {
    return new GroupIntoBatches<>(batchSize, null);
  }

  /** Returns the size of the batch. */
  public long getBatchSize() {
    return batchSize;
  }

  /**
   * Set a time limit (in processing time) on how long an incomplete batch of elements is allowed to
   * be buffered. Once a batch is flushed to output, the timer is reset.
   */
  public GroupIntoBatches<K, InputT> withMaxBufferingDuration(Duration duration) {
    checkArgument(
        duration.isLongerThan(Duration.ZERO), "max buffering duration should be a positive value");
    return new GroupIntoBatches<>(batchSize, duration);
  }

  @Override
  public PCollection<KV<K, Iterable<InputT>>> expand(PCollection<KV<K, InputT>> input) {
    Duration allowedLateness = input.getWindowingStrategy().getAllowedLateness();

    checkArgument(
        input.getCoder() instanceof KvCoder,
        "coder specified in the input PCollection is not a KvCoder");
    KvCoder inputCoder = (KvCoder) input.getCoder();
    Coder<K> keyCoder = (Coder<K>) inputCoder.getCoderArguments().get(0);
    Coder<InputT> valueCoder = (Coder<InputT>) inputCoder.getCoderArguments().get(1);

    return input.apply(
        ParDo.of(
            new GroupIntoBatchesDoFn<>(
                batchSize, allowedLateness, maxBufferingDuration, keyCoder, valueCoder)));
  }

  @VisibleForTesting
  static class GroupIntoBatchesDoFn<K, InputT>
      extends DoFn<KV<K, InputT>, KV<K, Iterable<InputT>>> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupIntoBatchesDoFn.class);
    private static final String END_OF_WINDOW_ID = "endOFWindow";
    private static final String END_OF_BUFFERING_ID = "endOfBuffering";
    private static final String BATCH_ID = "batch";
    private static final String NUM_ELEMENTS_IN_BATCH_ID = "numElementsInBatch";
    private static final String KEY_ID = "key";
    private final long batchSize;
    private final Duration allowedLateness;
    private final Duration maxBufferingDuration;

    @TimerId(END_OF_WINDOW_ID)
    private final TimerSpec windowTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId(END_OF_BUFFERING_ID)
    private final TimerSpec bufferingTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @StateId(BATCH_ID)
    private final StateSpec<BagState<InputT>> batchSpec;

    @StateId(NUM_ELEMENTS_IN_BATCH_ID)
    private final StateSpec<CombiningState<Long, long[], Long>> numElementsInBatchSpec;

    @StateId(KEY_ID)
    private final StateSpec<ValueState<K>> keySpec;

    private final long prefetchFrequency;

    GroupIntoBatchesDoFn(
        long batchSize,
        Duration allowedLateness,
        Duration maxBufferingDuration,
        Coder<K> inputKeyCoder,
        Coder<InputT> inputValueCoder) {
      this.batchSize = batchSize;
      this.allowedLateness = allowedLateness;
      this.maxBufferingDuration = maxBufferingDuration;
      this.batchSpec = StateSpecs.bag(inputValueCoder);
      this.numElementsInBatchSpec =
          StateSpecs.combining(
              new Combine.BinaryCombineLongFn() {

                @Override
                public long identity() {
                  return 0L;
                }

                @Override
                public long apply(long left, long right) {
                  return left + right;
                }
              });

      this.keySpec = StateSpecs.value(inputKeyCoder);
      // Prefetch every 20% of batchSize elements. Do not prefetch if batchSize is too little
      this.prefetchFrequency = ((batchSize / 5) <= 1) ? Long.MAX_VALUE : (batchSize / 5);
    }

    @ProcessElement
    public void processElement(
        @TimerId(END_OF_WINDOW_ID) Timer windowTimer,
        @TimerId(END_OF_BUFFERING_ID) Timer bufferingTimer,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID) CombiningState<Long, long[], Long> numElementsInBatch,
        @StateId(KEY_ID) ValueState<K> key,
        @Element KV<K, InputT> element,
        BoundedWindow window,
        OutputReceiver<KV<K, Iterable<InputT>>> receiver) {
      Instant windowEnds = window.maxTimestamp().plus(allowedLateness);
      LOG.debug("*** SET TIMER *** to point in time {} for window {}", windowEnds, window);
      windowTimer.set(windowEnds);
      key.write(element.getKey());
      LOG.debug("*** BATCH *** Add element for window {} ", window);
      batch.add(element.getValue());
      // Blind add is supported with combiningState
      numElementsInBatch.add(1L);

      long num = numElementsInBatch.read();
      if (num == 1 && maxBufferingDuration != null) {
        // This is the first element in batch. Start counting buffering time if a limit was set.
        bufferingTimer.offset(maxBufferingDuration).setRelative();
      }
      if (num % prefetchFrequency == 0) {
        // Prefetch data and modify batch state (readLater() modifies this)
        batch.readLater();
      }
      if (num >= batchSize) {
        LOG.debug("*** END OF BATCH *** for window {}", window.toString());
        flushBatch(receiver, key, batch, numElementsInBatch, bufferingTimer);
      }
    }

    @OnTimer(END_OF_BUFFERING_ID)
    public void onBufferingTimer(
        OutputReceiver<KV<K, Iterable<InputT>>> receiver,
        @Timestamp Instant timestamp,
        @StateId(KEY_ID) ValueState<K> key,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID) CombiningState<Long, long[], Long> numElementsInBatch,
        @TimerId(END_OF_BUFFERING_ID) Timer bufferingTimer) {
      LOG.debug(
          "*** END OF BUFFERING *** for timer timestamp {} with buffering duration {}",
          timestamp,
          maxBufferingDuration);
      flushBatch(receiver, key, batch, numElementsInBatch, null);
    }

    @OnTimer(END_OF_WINDOW_ID)
    public void onWindowTimer(
        OutputReceiver<KV<K, Iterable<InputT>>> receiver,
        @Timestamp Instant timestamp,
        @StateId(KEY_ID) ValueState<K> key,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID) CombiningState<Long, long[], Long> numElementsInBatch,
        @TimerId(END_OF_BUFFERING_ID) Timer bufferingTimer,
        BoundedWindow window) {
      LOG.debug(
          "*** END OF WINDOW *** for timer timestamp {} in windows {}",
          timestamp,
          window.toString());
      flushBatch(receiver, key, batch, numElementsInBatch, bufferingTimer);
    }

    private void flushBatch(
        OutputReceiver<KV<K, Iterable<InputT>>> receiver,
        ValueState<K> key,
        BagState<InputT> batch,
        CombiningState<Long, long[], Long> numElementsInBatch,
        @Nullable Timer bufferingTimer) {
      Iterable<InputT> values = batch.read();
      // When the timer fires, batch state might be empty
      if (!Iterables.isEmpty(values)) {
        receiver.output(KV.of(key.read(), values));
      }
      batch.clear();
      LOG.debug("*** BATCH *** clear");
      numElementsInBatch.clear();
      // We might reach here due to batch size being reached or window expiration. Reset the
      // buffering timer (if not null) since the state is empty now. It'll be extended again if a
      // new element arrives prior to the expiration time set here.
      // TODO(BEAM-10887): Use clear() when it's available.
      if (bufferingTimer != null && maxBufferingDuration != null) {
        bufferingTimer.offset(maxBufferingDuration).setRelative();
      }
    }
  }
}
