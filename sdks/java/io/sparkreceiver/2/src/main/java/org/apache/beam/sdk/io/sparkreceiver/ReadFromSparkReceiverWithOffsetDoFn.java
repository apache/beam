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
package org.apache.beam.sdk.io.sparkreceiver;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.receiver.Receiver;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;

/**
 * A SplittableDoFn which reads from {@link Receiver} that implements {@link HasOffset}. By default,
 * a {@link WatermarkEstimators.Manual} watermark estimator is used to track watermark.
 *
 * <p>Initial range The initial range is {@code [0, Long.MAX_VALUE)}
 *
 * <p>Resume Processing Every time the sparkConsumer.hasRecords() returns false, {@link
 * ReadFromSparkReceiverWithOffsetDoFn} will move to process the next element.
 */
@UnboundedPerElement
class ReadFromSparkReceiverWithOffsetDoFn<V> extends DoFn<byte[], V> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReadFromSparkReceiverWithOffsetDoFn.class);

  /** Constant waiting time after the {@link Receiver} starts. Required to prepare for polling */
  private static final int START_POLL_TIMEOUT_MS = 2000;

  private final SerializableFunction<Instant, WatermarkEstimator<Instant>>
      createWatermarkEstimatorFn;
  private final SerializableFunction<V, Long> getOffsetFn;
  private final SerializableFunction<V, Instant> getTimestampFn;
  private final ReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder;

  ReadFromSparkReceiverWithOffsetDoFn(SparkReceiverIO.Read<V> transform) {
    createWatermarkEstimatorFn = WatermarkEstimators.Manual::new;

    ReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder =
        transform.getSparkReceiverBuilder();
    checkStateNotNull(sparkReceiverBuilder, "Spark Receiver Builder can't be null!");
    this.sparkReceiverBuilder = sparkReceiverBuilder;

    SerializableFunction<V, Long> getOffsetFn = transform.getGetOffsetFn();
    checkStateNotNull(getOffsetFn, "Get offset fn can't be null!");
    this.getOffsetFn = getOffsetFn;

    SerializableFunction<V, Instant> getTimestampFn = transform.getTimestampFn();
    if (getTimestampFn == null) {
      getTimestampFn = input -> Instant.now();
    }
    this.getTimestampFn = getTimestampFn;
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element byte[] element) {
    return new OffsetRange(0, Long.MAX_VALUE);
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public WatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return createWatermarkEstimatorFn.apply(ensureTimestampWithinBounds(watermarkEstimatorState));
  }

  @GetSize
  public double getSize(@Element byte[] element, @Restriction OffsetRange offsetRange) {
    return restrictionTracker(element, offsetRange).getProgress().getWorkRemaining();
  }

  /**
   * {@link OffsetRangeTracker} that performs basic split only in {@link
   * OffsetRangeTracker#checkDone}. This behavior allows reading from primary range until resume,
   * and then split to {alreadyReadRange, residualRange}.
   */
  private static class CustomOffsetRangeTracker extends OffsetRangeTracker {

    public CustomOffsetRangeTracker(OffsetRange range) {
      super(range);
    }

    @SuppressWarnings("nullness") // Base method can return null
    @Override
    public SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
      if (lastAttemptedOffset != null) {
        if (range.getTo() == Long.MAX_VALUE) {
          // Do not split, just use primary range
          return null;
        } else {
          // Need to add residual range
          OffsetRange res = new OffsetRange(range.getTo(), Long.MAX_VALUE);
          this.range = new OffsetRange(range.getFrom(), range.getTo());
          return SplitResult.of(range, res);
        }
      }
      // Basic split logic when lastAttemptedOffset is null

      // Convert to BigDecimal in computation to prevent overflow, which may result in loss of
      // precision.
      BigDecimal cur =
          BigDecimal.valueOf(range.getFrom()).subtract(BigDecimal.ONE, MathContext.DECIMAL128);
      // split = cur + max(1, (range.getTo() - cur) * fractionOfRemainder)
      BigDecimal splitPos =
          cur.add(
              BigDecimal.valueOf(range.getTo())
                  .subtract(cur, MathContext.DECIMAL128)
                  .multiply(BigDecimal.valueOf(fractionOfRemainder), MathContext.DECIMAL128)
                  .max(BigDecimal.ONE),
              MathContext.DECIMAL128);

      long split = splitPos.longValue();
      if (split >= range.getTo()) {
        return null;
      }
      OffsetRange res = new OffsetRange(split, range.getTo());
      this.range = new OffsetRange(range.getFrom(), split);
      return SplitResult.of(range, res);
    }

    @Override
    public void checkDone() throws IllegalStateException {
      if (lastAttemptedOffset != null && range.getTo() == Long.MAX_VALUE) {
        // Perform basic split
        super.trySplit(0);
      }
    }
  }

  @NewTracker
  public OffsetRangeTracker restrictionTracker(
      @Element byte[] element, @Restriction OffsetRange restriction) {
    return new CustomOffsetRangeTracker(restriction);
  }

  @GetRestrictionCoder
  public Coder<OffsetRange> restrictionCoder() {
    return new OffsetRange.Coder();
  }

  // Need to do an unchecked cast from Object
  // because org.apache.spark.streaming.receiver.ReceiverSupervisor accepts Object in push methods
  @SuppressWarnings("unchecked")
  private static class SparkConsumerWithOffset<V> implements SparkConsumer<V> {
    private final Queue<V> recordsQueue;
    private @Nullable Receiver<V> sparkReceiver;
    private final Long startOffset;

    SparkConsumerWithOffset(Long startOffset) {
      this.startOffset = startOffset;
      this.recordsQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public boolean hasRecords() {
      return !recordsQueue.isEmpty();
    }

    @Override
    public @Nullable V poll() {
      return recordsQueue.poll();
    }

    @Override
    public void start(Receiver<V> sparkReceiver) {
      this.sparkReceiver = sparkReceiver;

      final SerializableFunction<Object[], Void> storeFn =
          (input) -> {
            if (input == null) {
              return null;
            }
            /*
            Use only [0] element - data.
            The other elements are not needed because they are related to Spark environment options.
             */
            Object data = input[0];

            if (data instanceof ByteBuffer) {
              final ByteBuffer byteBuffer = ((ByteBuffer) data).asReadOnlyBuffer();
              final byte[] bytes = new byte[byteBuffer.limit()];
              byteBuffer.get(bytes);
              final V record = SerializationUtils.deserialize(bytes);
              recordsQueue.offer(record);
            } else if (data instanceof Iterator) {
              final Iterator<V> iterator = (Iterator<V>) data;
              while (iterator.hasNext()) {
                V record = iterator.next();
                recordsQueue.offer(record);
              }
            } else if (data instanceof ArrayBuffer) {
              final ArrayBuffer<V> arrayBuffer = (ArrayBuffer<V>) data;
              final Iterator<V> iterator = arrayBuffer.iterator();
              while (iterator.hasNext()) {
                V record = iterator.next();
                recordsQueue.offer(record);
              }
            } else {
              V record = (V) data;
              recordsQueue.offer(record);
            }
            return null;
          };

      try {
        new WrappedSupervisor(sparkReceiver, new SparkConf(), storeFn);
      } catch (Exception e) {
        LOG.error("Can not init Spark Receiver!", e);
        throw new IllegalStateException("Spark Receiver was not initialized");
      }
      ((HasOffset) sparkReceiver).setStartOffset(startOffset);
      sparkReceiver.supervisor().startReceiver();
      try {
        TimeUnit.MILLISECONDS.sleep(START_POLL_TIMEOUT_MS);
      } catch (InterruptedException e) {
        LOG.error("SparkReceiver was interrupted before polling started", e);
        throw new IllegalStateException("Spark Receiver was interrupted before polling started");
      }
    }

    @Override
    public void stop() {
      if (sparkReceiver != null) {
        sparkReceiver.stop("SparkReceiver is stopped.");
      }
      recordsQueue.clear();
    }
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element byte[] element,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator<Instant> watermarkEstimator,
      OutputReceiver<V> receiver) {

    SparkConsumer<V> sparkConsumer;
    Receiver<V> sparkReceiver;
    try {
      sparkReceiver = sparkReceiverBuilder.build();
    } catch (Exception e) {
      LOG.error("Can not build Spark Receiver", e);
      throw new IllegalStateException("Spark Receiver was not built!");
    }
    LOG.debug("Restriction {}", tracker.currentRestriction().toString());
    sparkConsumer = new SparkConsumerWithOffset<>(tracker.currentRestriction().getFrom());
    sparkConsumer.start(sparkReceiver);

    while (true) {
      try {
        TimeUnit.MILLISECONDS.sleep(START_POLL_TIMEOUT_MS);
      } catch (InterruptedException e) {
        LOG.error("SparkReceiver was interrupted before polling started", e);
        throw new IllegalStateException("Spark Receiver was interrupted before polling started");
      }
      if (!sparkConsumer.hasRecords()) {
        sparkConsumer.stop();
        tracker.checkDone();
        LOG.debug("Resume for restriction: {}", tracker.currentRestriction().toString());
        return ProcessContinuation.resume();
      }
      while (sparkConsumer.hasRecords()) {
        V record = sparkConsumer.poll();
        if (record != null) {
          Long offset = getOffsetFn.apply(record);
          if (!tracker.tryClaim(offset)) {
            sparkConsumer.stop();
            LOG.debug("Stop for restriction: {}", tracker.currentRestriction().toString());
            return ProcessContinuation.stop();
          }
          Instant currentTimeStamp = getTimestampFn.apply(record);
          ((ManualWatermarkEstimator<Instant>) watermarkEstimator).setWatermark(currentTimeStamp);
          receiver.outputWithTimestamp(record, currentTimeStamp);
        }
      }
    }
  }

  private static Instant ensureTimestampWithinBounds(Instant timestamp) {
    if (timestamp.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
      LOG.debug("Timestamp was before MIN_VALUE({})", BoundedWindow.TIMESTAMP_MIN_VALUE);
    } else if (timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
      LOG.debug("Timestamp was after MAX_VALUE({})", BoundedWindow.TIMESTAMP_MAX_VALUE);
    }
    return timestamp;
  }
}
