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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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
  private Queue<V> availableRecordsQueue;
  private AtomicLong recordsRead;
  private ProxyReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder;
  private Receiver<V> sparkReceiver;
  private final SerializableFunction<V, Long> getOffsetFn;

  public ReadFromSparkReceiverDoFn(SparkReceiverIO.ReadFromSparkReceiverViaSdf<V> transform) {
    createWatermarkEstimatorFn = WatermarkEstimators.Manual::new;
    sparkReceiverBuilder = transform.sparkReceiverRead.getSparkReceiverBuilder();
    getOffsetFn = transform.sparkReceiverRead.getGetOffsetFn();
  }

  private void initReceiver(Consumer<Object[]> storeConsumer, Receiver<V> sparkReceiver) {
    try {
      new WrappedSupervisor(sparkReceiver, new SparkConf(), storeConsumer);
    } catch (Exception e) {
      LOG.error("Can not init Spark Receiver!", e);
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
      if (sparkReceiver instanceof HubspotCustomReceiver) {
        return ((HubspotCustomReceiver) sparkReceiver).getEndOffset();
      }
      return Long.MAX_VALUE;
    }
  }

  @NewTracker
  public OffsetRangeTracker restrictionTracker(
      @Element SparkReceiverSourceDescriptor sourceDescriptor,
      @Restriction OffsetRange restriction) {
    return new GrowableOffsetRangeTracker(
        restriction.getFrom(), new SparkReceiverLatestOffsetEstimator());
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
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element SparkReceiverSourceDescriptor sourceDescriptor,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator watermarkEstimator,
      OutputReceiver<V> receiver) {

    recordsRead = new AtomicLong(0);
    AtomicLong recordsOut = new AtomicLong(0);
    availableRecordsQueue = new ArrayBlockingQueue<>(1000);
    try {
      this.sparkReceiver = sparkReceiverBuilder.build();
      initReceiver(
          objects -> {
            availableRecordsQueue.offer((V) objects[0]);
            long read = recordsRead.getAndIncrement();
            if (read % 200 == 0) {
              LOG.info("Records read = {}", read);
            }
          },
          sparkReceiver);
    } catch (Exception e) {
      LOG.error("Can not create new Hubspot Receiver", e);
    }
    if (sparkReceiver instanceof HubspotCustomReceiver) {
      long from = tracker.currentRestriction().getFrom();
      ((HubspotCustomReceiver) sparkReceiver)
          .setStartOffset(String.valueOf(from == 0L ? 0 : from - 1));
    }
    sparkReceiver.onStart();
    LOG.info(
        "Restriction: {}, {}",
        tracker.currentRestriction().getFrom(),
        tracker.currentRestriction().getTo());
    try {
      TimeUnit.MILLISECONDS.sleep(500);
    } catch (InterruptedException e) {
      LOG.error("Interrupted", e);
    }

    while (!availableRecordsQueue.isEmpty()) {
      V record = availableRecordsQueue.poll();
      if (!tracker.tryClaim(getOffsetFn.apply(record))) {
        sparkReceiver.onStop();
        availableRecordsQueue.clear();
        return ProcessContinuation.stop();
      }
      ((ManualWatermarkEstimator) watermarkEstimator).setWatermark(Instant.now());
      receiver.outputWithTimestamp(record, Instant.now());
      long out = recordsOut.incrementAndGet();
      if (out % 100 == 0) {
        LOG.info("Records out = {}", out);
      }
    }
    sparkReceiver.onStop();
    availableRecordsQueue.clear();
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
