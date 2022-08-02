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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
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
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that batches inputs to a desired batch size. Batches will contain only
 * elements of a single key.
 *
 * <p>Elements are buffered until there are enough elements for a batch, at which point they are
 * emitted to the output {@link PCollection}. A {@code maxBufferingDuration} can be set to emit
 * output early and avoid waiting for a full batch forever.
 *
 * <p>Batches can be triggered either based on element count or byte size. {@link #ofSize} is used
 * to specify a maximum element count while {@link #ofByteSize} is used to specify a maximum byte
 * size. The single-argument {@link #ofByteSize} uses the input coder to determine the encoded byte
 * size of each element. However, this may not always be what is desired. A user may want to control
 * batching based on a different byte size (e.g. the memory usage of the decoded Java object) or the
 * input coder may not be able to efficiently determine the elements' byte size. For these cases, we
 * also provide the two-argument {@link #ofByteSize} allowing the user to pass in a function to be
 * used to determine the byte size of an element.
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
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes",
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class GroupIntoBatches<K, InputT>
    extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>> {

  /**
   * Wrapper class for batching parameters supplied by users. Shared by both {@link
   * GroupIntoBatches} and {@link GroupIntoBatches.WithShardedKey}.
   */
  @AutoValue
  public abstract static class BatchingParams<InputT> implements Serializable {
    public static <InputT> BatchingParams<InputT> create(
        long batchSize,
        long batchSizeBytes,
        SerializableFunction<InputT, Long> elementByteSize,
        Duration maxBufferingDuration) {
      return new AutoValue_GroupIntoBatches_BatchingParams(
          batchSize, batchSizeBytes, elementByteSize, maxBufferingDuration);
    }

    public abstract long getBatchSize();

    public abstract long getBatchSizeBytes();

    @Nullable
    public abstract SerializableFunction<InputT, Long> getElementByteSize();

    public abstract Duration getMaxBufferingDuration();

    public SerializableFunction<InputT, Long> getWeigher(Coder<InputT> valueCoder) {
      SerializableFunction<InputT, Long> weigher = getElementByteSize();
      if (getBatchSizeBytes() < Long.MAX_VALUE) {
        if (weigher == null) {
          // If the user didn't specify a byte-size function, then use the Coder to determine the
          // byte
          // size.
          // Note: if Coder.isRegisterByteSizeObserverCheap == false, then this will be expensive.
          weigher =
              (InputT element) -> {
                try {
                  ByteSizeObserver observer = new ByteSizeObserver();
                  valueCoder.registerByteSizeObserver(element, observer);
                  observer.advance();
                  return observer.getElementByteSize();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              };
        }
      }
      return weigher;
    }
  }

  private final BatchingParams<InputT> params;
  private static final UUID workerUuid = UUID.randomUUID();

  private GroupIntoBatches(BatchingParams<InputT> params) {
    this.params = params;
  }

  /** Aim to create batches each with the specified element count. */
  public static <K, InputT> GroupIntoBatches<K, InputT> ofSize(long batchSize) {
    Preconditions.checkState(batchSize < Long.MAX_VALUE);
    return new GroupIntoBatches<>(
        BatchingParams.create(batchSize, Long.MAX_VALUE, null, Duration.ZERO));
  }

  /**
   * Aim to create batches each with the specified byte size.
   *
   * <p>This option uses the PCollection's coder to determine the byte size of each element. This
   * may not always be what is desired (e.g. the encoded size is not the same as the memory usage of
   * the Java object). This is also only recommended if the coder returns true for
   * isRegisterByteSizeObserverCheap, otherwise the transform will perform a possibly-expensive
   * encoding of each element in order to measure its byte size. An alternate approach is to use
   * {@link #ofByteSize(long, SerializableFunction)} to specify code to calculate the byte size.
   */
  public static <K, InputT> GroupIntoBatches<K, InputT> ofByteSize(long batchSizeBytes) {
    Preconditions.checkState(batchSizeBytes < Long.MAX_VALUE);
    return new GroupIntoBatches<>(
        BatchingParams.create(Long.MAX_VALUE, batchSizeBytes, null, Duration.ZERO));
  }

  /**
   * Aim to create batches each with the specified byte size. The provided function is used to
   * determine the byte size of each element.
   */
  public static <K, InputT> GroupIntoBatches<K, InputT> ofByteSize(
      long batchSizeBytes, SerializableFunction<InputT, Long> getElementByteSize) {
    Preconditions.checkState(batchSizeBytes < Long.MAX_VALUE);
    return new GroupIntoBatches<>(
        BatchingParams.create(Long.MAX_VALUE, batchSizeBytes, getElementByteSize, Duration.ZERO));
  }

  /** Returns user supplied parameters for batching. */
  public BatchingParams<InputT> getBatchingParams() {
    return params;
  }

  /**
   * Sets a time limit (in processing time) on how long an incomplete batch of elements is allowed
   * to be buffered. Once a batch is flushed to output, the timer is reset. The provided limit must
   * be a positive duration or zero; a zero buffering duration effectively means no limit.
   */
  public GroupIntoBatches<K, InputT> withMaxBufferingDuration(Duration duration) {
    checkArgument(
        duration != null && !duration.isShorterThan(Duration.ZERO),
        "max buffering duration should be a non-negative value");
    return new GroupIntoBatches<>(
        BatchingParams.create(
            params.getBatchSize(),
            params.getBatchSizeBytes(),
            params.getElementByteSize(),
            duration));
  }

  /**
   * Outputs batched elements associated with sharded input keys. By default, keys are sharded to
   * such that the input elements with the same key are spread to all available threads executing
   * the transform. Runners may override the default sharding to do a better load balancing during
   * the execution time.
   */
  @Experimental
  public WithShardedKey withShardedKey() {
    return new WithShardedKey();
  }

  public class WithShardedKey
      extends PTransform<
          PCollection<KV<K, InputT>>, PCollection<KV<ShardedKey<K>, Iterable<InputT>>>> {
    private WithShardedKey() {}

    /** Returns user supplied parameters for batching. */
    public BatchingParams<InputT> getBatchingParams() {
      return params;
    }

    @Override
    public PCollection<KV<ShardedKey<K>, Iterable<InputT>>> expand(
        PCollection<KV<K, InputT>> input) {
      checkArgument(
          input.getCoder() instanceof KvCoder,
          "coder specified in the input PCollection is not a KvCoder");
      KvCoder<K, InputT> inputCoder = (KvCoder<K, InputT>) input.getCoder();
      Coder<K> keyCoder = (Coder<K>) inputCoder.getCoderArguments().get(0);
      Coder<InputT> valueCoder = (Coder<InputT>) inputCoder.getCoderArguments().get(1);

      return input
          .apply(
              MapElements.via(
                  new SimpleFunction<KV<K, InputT>, KV<ShardedKey<K>, InputT>>() {
                    @Override
                    public KV<ShardedKey<K>, InputT> apply(KV<K, InputT> input) {
                      long tid = Thread.currentThread().getId();
                      ByteBuffer buffer = ByteBuffer.allocate(3 * Long.BYTES);
                      buffer.putLong(workerUuid.getMostSignificantBits());
                      buffer.putLong(workerUuid.getLeastSignificantBits());
                      buffer.putLong(tid);
                      return KV.of(ShardedKey.of(input.getKey(), buffer.array()), input.getValue());
                    }
                  }))
          .setCoder(KvCoder.of(ShardedKey.Coder.of(keyCoder), valueCoder))
          .apply(new GroupIntoBatches<>(getBatchingParams()));
    }
  }

  private static class ByteSizeObserver extends ElementByteSizeObserver {
    private long elementByteSize = 0;

    @Override
    protected void reportElementSize(long elementByteSize) {
      this.elementByteSize += elementByteSize;
    }

    public long getElementByteSize() {
      return this.elementByteSize;
    }
  };

  @Override
  public PCollection<KV<K, Iterable<InputT>>> expand(PCollection<KV<K, InputT>> input) {

    checkArgument(
        input.getCoder() instanceof KvCoder,
        "coder specified in the input PCollection is not a KvCoder");
    KvCoder<K, InputT> inputCoder = (KvCoder<K, InputT>) input.getCoder();
    final Coder<InputT> valueCoder = (Coder<InputT>) inputCoder.getCoderArguments().get(1);

    SerializableFunction<InputT, Long> weigher = params.getWeigher(valueCoder);
    return input.apply(
        ParDo.of(
            new GroupIntoBatchesDoFn<>(
                params.getBatchSize(),
                params.getBatchSizeBytes(),
                weigher,
                params.getMaxBufferingDuration(),
                valueCoder)));
  }

  @VisibleForTesting
  private static class GroupIntoBatchesDoFn<K, InputT>
      extends DoFn<KV<K, InputT>, KV<K, Iterable<InputT>>> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupIntoBatchesDoFn.class);
    private final long batchSize;
    private final long batchSizeBytes;
    @Nullable private final SerializableFunction<InputT, Long> weigher;
    private final Duration maxBufferingDuration;

    // The following timer is no longer set. We maintain the spec for update compatibility.
    private static final String END_OF_WINDOW_ID = "endOFWindow";

    @TimerId(END_OF_WINDOW_ID)
    private final TimerSpec windowTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    // This timer expires when it's time to batch and output the buffered data.
    private static final String END_OF_BUFFERING_ID = "endOfBuffering";

    @TimerId(END_OF_BUFFERING_ID)
    private final TimerSpec bufferingTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    // The set of elements that will go in the next batch.
    private static final String BATCH_ID = "batch";

    @StateId(BATCH_ID)
    private final StateSpec<BagState<InputT>> batchSpec;

    // The size of the current batch.
    private static final String NUM_ELEMENTS_IN_BATCH_ID = "numElementsInBatch";

    @StateId(NUM_ELEMENTS_IN_BATCH_ID)
    private final StateSpec<CombiningState<Long, long[], Long>> batchSizeSpec;

    private static final String NUM_BYTES_IN_BATCH_ID = "numBytesInBatch";

    // The byte size of the current batch.

    @StateId(NUM_BYTES_IN_BATCH_ID)
    private final StateSpec<CombiningState<Long, long[], Long>> batchSizeBytesSpec;

    private static final String TIMER_TIMESTAMP = "timerTs";

    // The timestamp of the current active timer.

    @StateId(TIMER_TIMESTAMP)
    private final StateSpec<ValueState<Long>> timerTsSpec;

    // The minimum element timestamp currently buffered in the bag. This is used to set the output
    // timestamp
    // on the timer which ensures that the watermark correctly tracks the buffered elements.
    private static final String MIN_BUFFERED_TS = "minBufferedTs";

    @StateId(MIN_BUFFERED_TS)
    private final StateSpec<CombiningState<Long, long[], Long>> minBufferedTsSpec;

    private final long prefetchFrequency;

    GroupIntoBatchesDoFn(
        long batchSize,
        long batchSizeBytes,
        @Nullable SerializableFunction<InputT, Long> weigher,
        Duration maxBufferingDuration,
        Coder<InputT> inputValueCoder) {
      this.batchSize = batchSize;
      this.batchSizeBytes = batchSizeBytes;
      this.weigher = weigher;
      this.maxBufferingDuration = maxBufferingDuration;
      this.batchSpec = StateSpecs.bag(inputValueCoder);

      Combine.BinaryCombineLongFn sumCombineFn =
          new Combine.BinaryCombineLongFn() {
            @Override
            public long identity() {
              return 0L;
            }

            @Override
            public long apply(long left, long right) {
              return left + right;
            }
          };

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

      this.batchSizeSpec = StateSpecs.combining(sumCombineFn);
      this.batchSizeBytesSpec = StateSpecs.combining(sumCombineFn);
      this.timerTsSpec = StateSpecs.value();
      this.minBufferedTsSpec = StateSpecs.combining(minCombineFn);

      // Prefetch every 20% of batchSize elements. Do not prefetch if batchSize is too little
      this.prefetchFrequency = ((batchSize / 5) <= 1) ? Long.MAX_VALUE : (batchSize / 5);
    }

    @ProcessElement
    public void processElement(
        @TimerId(END_OF_BUFFERING_ID) Timer bufferingTimer,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID) CombiningState<Long, long[], Long> storedBatchSize,
        @StateId(NUM_BYTES_IN_BATCH_ID) CombiningState<Long, long[], Long> storedBatchSizeBytes,
        @StateId(TIMER_TIMESTAMP) ValueState<Long> timerTs,
        @StateId(MIN_BUFFERED_TS) CombiningState<Long, long[], Long> minBufferedTs,
        @Element KV<K, InputT> element,
        @Timestamp Instant elementTs,
        BoundedWindow window,
        OutputReceiver<KV<K, Iterable<InputT>>> receiver) {
      LOG.debug("*** BATCH *** Add element for window {} ", window);
      batch.add(element.getValue());
      // Blind add is supported with combiningState
      storedBatchSize.add(1L);
      if (weigher != null) {
        storedBatchSizeBytes.add(weigher.apply(element.getValue()));
        storedBatchSizeBytes.readLater();
      }

      long num;
      if (maxBufferingDuration.isLongerThan(Duration.ZERO)) {
        minBufferedTs.readLater();
        num = storedBatchSize.read();

        long oldOutputTs =
            MoreObjects.firstNonNull(
                minBufferedTs.read(), BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
        minBufferedTs.add(elementTs.getMillis());
        // If this is the first element in the batch or if the timer's output timestamp needs
        // modifying, then set a
        // timer.
        if (num == 1 || minBufferedTs.read() != oldOutputTs) {
          long targetTs =
              MoreObjects.firstNonNull(
                  timerTs.read(),
                  bufferingTimer.getCurrentRelativeTime().getMillis()
                      + maxBufferingDuration.getMillis());
          bufferingTimer
              .withOutputTimestamp(Instant.ofEpochMilli(minBufferedTs.read()))
              .set(Instant.ofEpochMilli(targetTs));
        }
      }
      num = storedBatchSize.read();

      if (num % prefetchFrequency == 0) {
        // Prefetch data and modify batch state (readLater() modifies this)
        batch.readLater();
      }

      if (num >= batchSize
          || (batchSizeBytes != Long.MAX_VALUE && storedBatchSizeBytes.read() >= batchSizeBytes)) {
        LOG.debug("*** END OF BATCH *** for window {}", window.toString());
        flushBatch(
            receiver,
            element.getKey(),
            batch,
            storedBatchSize,
            storedBatchSizeBytes,
            timerTs,
            minBufferedTs);
        bufferingTimer.clear();
      }
    }

    @OnTimer(END_OF_BUFFERING_ID)
    public void onBufferingTimer(
        OutputReceiver<KV<K, Iterable<InputT>>> receiver,
        @Timestamp Instant timestamp,
        @Key K key,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID) CombiningState<Long, long[], Long> storedBatchSize,
        @StateId(NUM_BYTES_IN_BATCH_ID) CombiningState<Long, long[], Long> storedBatchSizeBytes,
        @StateId(TIMER_TIMESTAMP) ValueState<Long> timerTs,
        @StateId(MIN_BUFFERED_TS) CombiningState<Long, long[], Long> minBufferedTs,
        @TimerId(END_OF_BUFFERING_ID) Timer bufferingTimer) {
      LOG.debug(
          "*** END OF BUFFERING *** for timer timestamp {} with buffering duration {}",
          timestamp,
          maxBufferingDuration);
      flushBatch(
          receiver, key, batch, storedBatchSize, storedBatchSizeBytes, timerTs, minBufferedTs);
    }

    @OnWindowExpiration
    public void onWindowExpiration(
        OutputReceiver<KV<K, Iterable<InputT>>> receiver,
        @Key K key,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID) CombiningState<Long, long[], Long> storedBatchSize,
        @StateId(NUM_BYTES_IN_BATCH_ID) CombiningState<Long, long[], Long> storedBatchSizeBytes,
        @StateId(TIMER_TIMESTAMP) ValueState<Long> timerTs,
        @StateId(MIN_BUFFERED_TS) CombiningState<Long, long[], Long> minBufferedTs) {
      flushBatch(
          receiver, key, batch, storedBatchSize, storedBatchSizeBytes, timerTs, minBufferedTs);
    }

    // We no longer set this timer, since OnWindowExpiration takes care of his. However we leave the
    // callback in place
    // for existing jobs that have already set these timers.
    @OnTimer(END_OF_WINDOW_ID)
    public void onWindowTimer(
        OutputReceiver<KV<K, Iterable<InputT>>> receiver,
        @Timestamp Instant timestamp,
        @Key K key,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID) CombiningState<Long, long[], Long> storedBatchSize,
        @StateId(NUM_BYTES_IN_BATCH_ID) CombiningState<Long, long[], Long> storedBatchSizeBytes,
        @StateId(TIMER_TIMESTAMP) ValueState<Long> timerTs,
        @StateId(MIN_BUFFERED_TS) CombiningState<Long, long[], Long> minBufferedTs,
        BoundedWindow window) {
      LOG.debug(
          "*** END OF WINDOW *** for timer timestamp {} in windows {}",
          timestamp,
          window.toString());
      flushBatch(
          receiver, key, batch, storedBatchSize, storedBatchSizeBytes, timerTs, minBufferedTs);
    }

    private void flushBatch(
        OutputReceiver<KV<K, Iterable<InputT>>> receiver,
        K key,
        BagState<InputT> batch,
        CombiningState<Long, long[], Long> storedBatchSize,
        CombiningState<Long, long[], Long> storedBatchSizeBytes,
        ValueState<Long> timerTs,
        CombiningState<Long, long[], Long> minBufferedTs) {
      Iterable<InputT> values = batch.read();
      // When the timer fires, batch state might be empty
      if (!Iterables.isEmpty(values)) {
        receiver.output(KV.of(key, values));
      }
      clearState(batch, storedBatchSize, storedBatchSizeBytes, timerTs, minBufferedTs);
      ;
    }

    private void clearState(
        BagState<InputT> batch,
        CombiningState<Long, long[], Long> storedBatchSize,
        CombiningState<Long, long[], Long> storedBatchSizeBytes,
        ValueState<Long> timerTs,
        CombiningState<Long, long[], Long> minBufferedTs) {
      batch.clear();
      storedBatchSize.clear();
      storedBatchSizeBytes.clear();
      timerTs.clear();
      minBufferedTs.clear();
    }
  }
}
