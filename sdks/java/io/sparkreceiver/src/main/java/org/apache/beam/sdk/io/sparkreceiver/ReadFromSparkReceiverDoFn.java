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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.receiver.Receiver;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UnboundedPerElement
@SuppressWarnings({"rawtypes", "nullness", "UnusedVariable"})
public class ReadFromSparkReceiverDoFn<V> extends DoFn<SparkReceiverSourceDescriptor, V> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadFromSparkReceiverDoFn.class);

  private final SerializableFunction<Instant, WatermarkEstimator<Instant>>
      createWatermarkEstimatorFn;
  private ProxyReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder;
  private Receiver<V> sparkReceiver;
  private final SerializableFunction<V, Long> getOffsetFn;
  private SparkConsumer<V> sparkConsumer;

  public ReadFromSparkReceiverDoFn(SparkReceiverIO.ReadFromSparkReceiverViaSdf<V> transform) {
    createWatermarkEstimatorFn = WatermarkEstimators.Manual::new;
    sparkReceiverBuilder = transform.sparkReceiverRead.getSparkReceiverBuilder();
    getOffsetFn = transform.sparkReceiverRead.getGetOffsetFn();
    if (!isOffsetable()) {
      sparkConsumer = transform.sparkReceiverRead.getSparkConsumer();
      try {
        sparkReceiver = sparkReceiverBuilder.build();
        sparkConsumer.start(sparkReceiver);
      } catch (Exception e) {
        LOG.error("Can not build Spark Receiver", e);
      }
    }
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element SparkReceiverSourceDescriptor sourceDescriptor) {
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
  public double getSize(
      @Element SparkReceiverSourceDescriptor sourceDescriptor,
      @Restriction OffsetRange offsetRange) {
    return restrictionTracker(sourceDescriptor, offsetRange).getProgress().getWorkRemaining();
    // Before processing elements, we don't have a good estimated size of records and offset gap.
  }

  private class SparkReceiverLatestOffsetEstimator
      implements GrowableOffsetRangeTracker.RangeEndEstimator {

    public SparkReceiverLatestOffsetEstimator() {}

    @Override
    public long estimate() {
      if (isOffsetable()) {
        return ((HasOffset) sparkReceiver).getEndOffset();
      }
      return Long.MAX_VALUE;
    }
  }

  private boolean isOffsetable() {
    return HasOffset.class.isAssignableFrom(sparkReceiverBuilder.getSparkReceiverClass());
  }

  @NewTracker
  public OffsetRangeTracker restrictionTracker(
      @Element SparkReceiverSourceDescriptor sourceDescriptor,
      @Restriction OffsetRange restriction) {
    return new GrowableOffsetRangeTracker(
        restriction.getFrom(), new SparkReceiverLatestOffsetEstimator()) {};
  }

  @GetRestrictionCoder
  public Coder<OffsetRange> restrictionCoder() {
    return new OffsetRange.Coder();
  }

  @Setup
  public void setup() throws Exception {
    // Start to track record size and offset gap per bundle.
  }

  @Teardown
  public void teardown() throws Exception {
    // Closeables close
    LOG.debug("Teardown");
    if (!isOffsetable()) {
      sparkConsumer.stop();
    }
  }

  private static class SparkConsumerWithOffset<V> implements SparkConsumer<V> {
    private final Queue<V> recordsQueue = new ConcurrentLinkedQueue<>();
    private Receiver<V> sparkReceiver;
    private final Long startOffset;

    public SparkConsumerWithOffset(Long startOffset) {
      this.startOffset = startOffset;
    }

    @Override
    public boolean hasRecords() {
      return !recordsQueue.isEmpty();
    }

    @Override
    public V poll() {
      return recordsQueue.poll();
    }

    @Override
    public void start(Receiver<V> sparkReceiver) {
      this.sparkReceiver = sparkReceiver;
      initReceiver(
          objects -> {
            V record = (V) objects[0];
            recordsQueue.offer(record);
            return null;
          },
          sparkReceiver);
      ((HasOffset) sparkReceiver).setStartOffset(startOffset);
      sparkReceiver.supervisor().startReceiver();
    }

    @Override
    public void stop() {
      sparkReceiver.stop("Stopped");
      recordsQueue.clear();
    }

    private void initReceiver(
        SerializableFunction<Object[], Void> storeConsumer, Receiver<V> sparkReceiver) {
      try {
        new WrappedSupervisor<>(sparkReceiver, new SparkConf(), storeConsumer);
      } catch (Exception e) {
        LOG.error("Can not init Spark Receiver!", e);
      }
    }
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element SparkReceiverSourceDescriptor sourceDescriptor,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator watermarkEstimator,
      OutputReceiver<V> receiver) {

    if (isOffsetable()) {
      try {
        this.sparkReceiver = sparkReceiverBuilder.build();
        this.sparkConsumer = new SparkConsumerWithOffset<>(tracker.currentRestriction().getFrom());
        sparkConsumer.start(sparkReceiver);
        try {
          TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException e) {
          LOG.error("Interrupted", e);
        }
      } catch (Exception e) {
        LOG.error("Can not build Spark Receiver", e);
      }
    }

    LOG.info("Restriction: {}", tracker.currentRestriction().toString());

    while (sparkConsumer.hasRecords()) {
      V record = sparkConsumer.poll();
      Long offset = getOffsetFn.apply(record);
      if (!tracker.tryClaim(offset)) {
        if (isOffsetable()) {
          sparkConsumer.stop();
        }
        LOG.info("Process STOP {}", tracker.currentRestriction().toString());
        return ProcessContinuation.stop();
      }
      ((ManualWatermarkEstimator) watermarkEstimator).setWatermark(Instant.now());
      receiver.outputWithTimestamp(record, Instant.now());
    }
    if (isOffsetable()) {
      sparkConsumer.stop();
    } else {
      try {
        TimeUnit.MILLISECONDS.sleep(200);
      } catch (InterruptedException e) {
        LOG.error("Interrupted", e);
      }
    }
    LOG.info("Process RESUME {}", tracker.currentRestriction().toString());
    return ProcessContinuation.resume();
  }

  private static Instant ensureTimestampWithinBounds(Instant timestamp) {
    if (timestamp.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    } else if (timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
    return timestamp;
  }
}
