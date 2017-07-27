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
package org.apache.beam.runners.spark.io;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.SparkWatermarks;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;


/**
 * Create an input stream from Queue. For SparkRunner tests only.
 *
 * <p>To properly compose a stream of micro-batches with their Watermarks, please keep in mind
 * that eventually there a two queues here - one for batches and another for Watermarks.
 *
 * <p>While both queues advance according to Spark's batch-interval, there is a slight difference
 * in how data is pushed into the stream compared to the advancement of Watermarks since Watermarks
 * advance onBatchCompleted hook call so if you'd want to set the watermark advance for a specific
 * batch it should be called before that batch.
 * Also keep in mind that being a queue that is polled per batch interval, if there is a need to
 * "hold" the same Watermark without advancing it it should be stated explicitly or the Watermark
 * will advance as soon as it can (in the next batch completed hook).
 *
 * <p>Example 1:
 *
 * {@code
 * CreateStream.<TimestampedValue<String>>withBatchInterval(batchDuration)
 *     .nextBatch(
 *         TimestampedValue.of("foo", endOfGlobalWindow),
 *         TimestampedValue.of("bar", endOfGlobalWindow))
 *     .advanceNextBatchWatermarkToInfinity();
 * }
 * The first batch will see the default start-of-time WM of
 * {@link BoundedWindow#TIMESTAMP_MIN_VALUE} and any following batch will see
 * the end-of-time WM {@link BoundedWindow#TIMESTAMP_MAX_VALUE}.
 *
 * <p>Example 2:
 *
 * {@code
 * CreateStream.<TimestampedValue<String>>withBatchInterval(batchDuration)
 *     .nextBatch(
 *         TimestampedValue.of(1, instant))
 *     .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(20)))
 *     .nextBatch(
 *         TimestampedValue.of(2, instant))
 *     .nextBatch(
 *         TimestampedValue.of(3, instant))
 *     .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(30)))
 * }
 * The first batch will see the start-of-time WM and the second will see the advanced (+20 min.) WM.
 * The third WM will see the WM advanced to +30 min, because this is the next advancement of the WM
 * regardless of where it ws called in the construction of CreateStream.
 * //TODO: write a proper Builder enforcing all those rules mentioned.
 * @param <T> stream type.
 */
public final class CreateStream<T> extends PTransform<PBegin, PCollection<T>> {

  private final Duration batchInterval;
  private final Queue<Iterable<TimestampedValue<T>>> batches = new LinkedList<>();
  private final Deque<SparkWatermarks> times = new LinkedList<>();
  private final Coder<T> coder;
  private Instant initialSystemTime;

  private Instant lowWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE; //for test purposes.

  private CreateStream(Duration batchInterval, Instant initialSystemTime, Coder<T> coder) {
    this.batchInterval = batchInterval;
    this.initialSystemTime = initialSystemTime;
    this.coder = coder;
  }

  /** Set the batch interval for the stream. */
  public static <T> CreateStream<T> of(Coder<T> coder, Duration batchInterval) {
    return new CreateStream<>(batchInterval, new Instant(0), coder);
  }

  /**
   * Enqueue next micro-batch elements.
   * This is backed by a {@link Queue} so stream input order would keep the population order (FIFO).
   */
  @SafeVarargs
  public final CreateStream<T> nextBatch(TimestampedValue<T>... batchElements) {
    // validate timestamps if timestamped elements.
    for (TimestampedValue<T> element: batchElements) {
      TimestampedValue timestampedValue = (TimestampedValue) element;
      checkArgument(
          timestampedValue.getTimestamp().isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE),
          "Elements must have timestamps before %s. Got: %s",
          BoundedWindow.TIMESTAMP_MAX_VALUE,
          timestampedValue.getTimestamp());
    }
    batches.offer(Arrays.asList(batchElements));
    return this;
  }

  /**
   * For non-timestamped elements.
   */
  @SafeVarargs
  public final CreateStream<T> nextBatch(T... batchElements) {
    List<TimestampedValue<T>> timestamped = Lists.newArrayListWithCapacity(batchElements.length);
    // as TimestampedValue.
    for (T element: batchElements) {
      timestamped.add(TimestampedValue.atMinimumTimestamp(element));
    }
    batches.offer(timestamped);
    return this;
  }

  /**
   * Adds an empty batch.
   */
  public CreateStream<T> emptyBatch() {
    batches.offer(Collections.<TimestampedValue<T>>emptyList());
    return this;
  }

  /** Set the initial synchronized processing time. */
  public CreateStream<T> initialSystemTimeAt(Instant initialSystemTime) {
    this.initialSystemTime = initialSystemTime;
    return this;
  }

  /**
   * Advances the watermark in the next batch.
   */
  public CreateStream<T> advanceWatermarkForNextBatch(Instant newWatermark) {
    checkArgument(
        !newWatermark.isBefore(lowWatermark), "The watermark is not allowed to decrease!");
    checkArgument(
        newWatermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE),
        "The Watermark cannot progress beyond the maximum. Got: %s. Maximum: %s",
        newWatermark,
        BoundedWindow.TIMESTAMP_MAX_VALUE);
    return advance(newWatermark);
  }

  /**
   * Advances the watermark in the next batch to the end-of-time.
   */
  public CreateStream<T> advanceNextBatchWatermarkToInfinity() {
    return advance(BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  private CreateStream<T> advance(Instant newWatermark) {
    // advance the system time.
    Instant currentSynchronizedProcessingTime = times.peekLast() == null ? initialSystemTime
        : times.peekLast().getSynchronizedProcessingTime();
    Instant nextSynchronizedProcessingTime = currentSynchronizedProcessingTime.plus(batchInterval);
    checkArgument(
        nextSynchronizedProcessingTime.isAfter(currentSynchronizedProcessingTime),
        "Synchronized processing time must always advance.");
    times.offer(new SparkWatermarks(lowWatermark, newWatermark, nextSynchronizedProcessingTime));
    lowWatermark = newWatermark;
    return this;
  }

  /** Get the underlying queue representing the mock stream of micro-batches. */
  public Queue<Iterable<TimestampedValue<T>>> getBatches() {
    return batches;
  }

  /**
   * Get times so they can be pushed into the
   * {@link org.apache.beam.runners.spark.util.GlobalWatermarkHolder}.
   */
  public Queue<SparkWatermarks> getTimes() {
    return times;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(),
        WindowingStrategy.globalDefault(),
        PCollection.IsBounded.UNBOUNDED,
        coder);
  }
}
