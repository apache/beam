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

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * <p>By default the initial range is {@code [0, Long.MAX_VALUE)}. There is an ability to set {@code
 * startOffset}.
 *
 * <p>Resume Processing Every time the sparkConsumer.hasRecords() returns false, {@link
 * ReadFromSparkReceiverWithOffsetDoFn} will move to process the next element.
 */
@UnboundedPerElement
class ReadFromSparkReceiverWithOffsetDoFn<V> extends DoFn<byte[], V> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReadFromSparkReceiverWithOffsetDoFn.class);

  /** Waiting time after the {@link Receiver} starts. Required to prepare for polling */
  private static final long DEFAULT_START_POLL_TIMEOUT_SEC = 2;

  /** Delay between polling for new records updates. */
  private static final long DEFAULT_PULL_FREQUENCY_SEC = 0;

  /** Inclusive start offset from which the reading should be started. */
  private static final long DEFAULT_START_OFFSET = 0;

  private final SerializableFunction<Instant, WatermarkEstimator<Instant>>
      createWatermarkEstimatorFn;
  private final SerializableFunction<V, Long> getOffsetFn;
  private final SerializableFunction<V, Instant> getTimestampFn;
  private final ReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder;
  private final Long pullFrequencySec;
  private final Long startPollTimeoutSec;
  private final Long startOffset;

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

    Long pullFrequencySec = transform.getPullFrequencySec();
    if (pullFrequencySec == null) {
      pullFrequencySec = DEFAULT_PULL_FREQUENCY_SEC;
    }
    this.pullFrequencySec = pullFrequencySec;

    Long startPollTimeoutSec = transform.getStartPollTimeoutSec();
    if (startPollTimeoutSec == null) {
      startPollTimeoutSec = DEFAULT_START_POLL_TIMEOUT_SEC;
    }
    this.startPollTimeoutSec = startPollTimeoutSec;

    Long startOffset = transform.getStartOffset();
    if (startOffset == null) {
      startOffset = DEFAULT_START_OFFSET;
    }
    this.startOffset = startOffset;
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element byte[] element) {
    return new OffsetRange(startOffset, Long.MAX_VALUE);
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

  @NewTracker
  public OffsetRangeTracker restrictionTracker(
      @Element byte[] element, @Restriction OffsetRange restriction) {
    return new OffsetRangeTracker(restriction) {
      private final AtomicBoolean isCheckDoneCalled = new AtomicBoolean(false);

      @SuppressWarnings("nullness") // Base method can return null
      @Override
      public SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {

        LOG.debug("Try split");
        OffsetRange curRange = this.range;
        SplitResult<OffsetRange> split = super.trySplit(fractionOfRemainder);

        if (split != null) {
          OffsetRange primary = split.getPrimary();
          if (primary != null && !isCheckDoneCalled.get()) {
            // If there was no check done called, then the split is not needed
            LOG.debug("Split is not needed");
            isCheckDoneCalled.set(false);
            this.range = curRange;
            return null;
          }
        }
        return split;
      }

      @Override
      public void checkDone() throws IllegalStateException {
        isCheckDoneCalled.set(true);
      }
    };
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
      LOG.debug("Starting receiver");
      ((HasOffset) sparkReceiver).setStartOffset(startOffset);
      sparkReceiver.supervisor().startReceiver();
      LOG.debug("Receiver started");
    }

    @Override
    public void stop() {
      if (sparkReceiver != null) {
        sparkReceiver.stop("SparkReceiver is stopped.");
      }
      LOG.info("Clear records queue: {} records", recordsQueue.size());
      recordsQueue.clear();
    }
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element byte[] element,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator<Instant> watermarkEstimator,
      OutputReceiver<V> receiver) {

    if (tracker.currentRestriction() != null) {
      LOG.info(
          "Start processing element. Restriction = {}", tracker.currentRestriction().toString());
    }
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

    Long recordsProcessed = 0L;
    while (true) {
      LOG.debug("Start polling records");
      try {
        TimeUnit.SECONDS.sleep(startPollTimeoutSec);
      } catch (InterruptedException e) {
        LOG.error("SparkReceiver was interrupted before polling started", e);
        throw new IllegalStateException("Spark Receiver was interrupted before polling started");
      }
      if (!sparkConsumer.hasRecords()) {
        LOG.debug("No records left");
        ((HasOffset) sparkReceiver).setCheckpoint(recordsProcessed);
        sparkConsumer.stop();
        tracker.checkDone();
        if (pullFrequencySec != 0L) {
          LOG.debug("Waiting to poll for new records...");
          try {
            TimeUnit.SECONDS.sleep(pullFrequencySec);
          } catch (InterruptedException e) {
            LOG.error("SparkReceiver was interrupted while waiting to poll new records", e);
            throw new IllegalStateException(
                "Spark Receiver was interrupted while waiting to poll new records");
          }
        }
        OffsetRange currentRestriction = tracker.currentRestriction();
        if (currentRestriction != null
            && currentRestriction.getFrom() == currentRestriction.getTo()) {
          LOG.info("Stop for empty restriction: {}", currentRestriction);
          return ProcessContinuation.stop();
        } else {
          LOG.info("Resume for restriction: {}", currentRestriction);
          return ProcessContinuation.resume();
        }
      }
      while (sparkConsumer.hasRecords()) {
        V record = sparkConsumer.poll();
        if (record != null) {
          Long offset = getOffsetFn.apply(record);
          if (!tracker.tryClaim(offset)) {
            ((HasOffset) sparkReceiver).setCheckpoint(recordsProcessed);
            sparkConsumer.stop();
            LOG.info("Stop for restriction: {}", tracker.currentRestriction());
            return ProcessContinuation.stop();
          }
          Instant currentTimeStamp = getTimestampFn.apply(record);
          recordsProcessed++;
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
