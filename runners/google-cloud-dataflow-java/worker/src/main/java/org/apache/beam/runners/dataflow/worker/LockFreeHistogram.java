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
package org.apache.beam.runners.dataflow.worker;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.ImmutableLongArray;

/**
 * A lock free implementation of {@link org.apache.beam.sdk.metrics.Histogram}. This class supports
 * extracting delta updates with the {@link #getSnapshotAndReset} method.
 */
@ThreadSafe
@Internal
public final class LockFreeHistogram implements Histogram {
  private final HistogramData.BucketType bucketType;
  private final AtomicLongArray buckets;
  private final MetricName name;
  private final AtomicReference<OutlierStatistic> underflowStatistic;
  private final AtomicReference<OutlierStatistic> overflowStatistic;

  /**
   * Whether this histogram has updates that have not been extracted by {@code getSnapshotAndReset}.
   * This values should be flipped to true AFTER recording a value, and flipped to false BEFORE
   * extracting a snapshot. This ensures that recorded values will always be seen by a future {@code
   * getSnapshotAndReset} call.
   */
  private final AtomicBoolean dirty;

  /** Create a histogram. */
  public LockFreeHistogram(MetricName name, HistogramData.BucketType bucketType) {
    this.name = name;
    this.bucketType = bucketType;
    this.buckets = new AtomicLongArray(bucketType.getNumBuckets());
    this.underflowStatistic =
        new AtomicReference<LockFreeHistogram.OutlierStatistic>(OutlierStatistic.EMPTY);
    this.overflowStatistic =
        new AtomicReference<LockFreeHistogram.OutlierStatistic>(OutlierStatistic.EMPTY);
    this.dirty = new AtomicBoolean(false);
  }

  /**
   * Represents the sum and mean of a collection of numbers. Used to represent the
   * underflow/overflow statistics of a histogram.
   */
  @AutoValue
  public abstract static class OutlierStatistic implements Serializable {
    abstract double sum();

    public abstract long count();

    public static final OutlierStatistic EMPTY = create(0, 0);

    public static OutlierStatistic create(double sum, long count) {
      return new AutoValue_LockFreeHistogram_OutlierStatistic(sum, count);
    }

    public OutlierStatistic combine(double value) {
      return create(sum() + value, count() + 1);
    }

    public double mean() {
      if (count() == 0) {
        return 0;
      }
      return sum() / count();
    }
  }

  /**
   * The snapshot of a histogram. The snapshot contains the overflow/underflow statistic, number of
   * values recorded in each bucket, and the BucketType of the underlying histogram.
   */
  @AutoValue
  public abstract static class Snapshot {
    public abstract OutlierStatistic underflowStatistic();

    public abstract OutlierStatistic overflowStatistic();

    public abstract ImmutableLongArray buckets();

    public abstract HistogramData.BucketType bucketType();

    public static Snapshot create(
        OutlierStatistic underflowStatistic,
        OutlierStatistic overflowStatistic,
        ImmutableLongArray buckets,
        HistogramData.BucketType bucketType) {
      return new AutoValue_LockFreeHistogram_Snapshot(
          underflowStatistic, overflowStatistic, buckets, bucketType);
    }

    @Memoized
    public long totalCount() {
      long count = 0;
      count += underflowStatistic().count();
      count += overflowStatistic().count();
      count += buckets().stream().sum();

      return count;
    }
  }

  /**
   * Extract a delta update of this histogram. Update represents values that have been recorded in
   * this histogram since the last time this method was called.
   *
   * <p>If this histogram is being updated concurrent to this method, then the returned snapshot is
   * not guarenteed to contain those updates. However, those updates are not dropped and will be
   * represented in a future call to this method.
   *
   * <p>If this histogram has not been updated since the last call to this method, an empty optional
   * is returned.
   */
  public Optional<Snapshot> getSnapshotAndReset() {
    if (!dirty.getAndSet(false)) {
      return Optional.empty();
    }

    ImmutableLongArray.Builder bucketsSnapshotBuilder =
        ImmutableLongArray.builder(buckets.length());
    for (int i = 0; i < buckets.length(); i++) {
      bucketsSnapshotBuilder.add(buckets.getAndSet(i, 0));
    }
    OutlierStatistic overflowSnapshot = overflowStatistic.getAndSet(OutlierStatistic.EMPTY);
    OutlierStatistic underflowSnapshot = underflowStatistic.getAndSet(OutlierStatistic.EMPTY);

    return Optional.of(
        Snapshot.create(
            underflowSnapshot, overflowSnapshot, bucketsSnapshotBuilder.build(), bucketType));
  }

  @Override
  public MetricName getName() {
    return name;
  }

  private void updateInternal(double value) {
    double rangeTo = bucketType.getRangeTo();
    double rangeFrom = bucketType.getRangeFrom();
    if (value >= rangeTo) {
      recordTopRecordsValue(value);
    } else if (value < rangeFrom) {
      recordBottomRecordsValue(value);
    } else {
      recordInBoundsValue(value);
    }
  }

  @Override
  public void update(double value) {
    updateInternal(value);
    dirty.set(true);
  }

  @Override
  public void update(double... values) {
    for (double value : values) {
      updateInternal(value);
    }
    dirty.set(true);
  }

  /** Record a inbounds value to the appropriate bucket. */
  private void recordInBoundsValue(double value) {
    int index = bucketType.getBucketIndex(value);
    if (index < 0 || index >= bucketType.getNumBuckets()) {
      return;
    }

    buckets.getAndIncrement(index);
  }

  /**
   * Record a new value in {@code overflowStatistic}. This method should only be called when a
   * Histogram is recording a value greater than the upper bound of it's largest bucket.
   *
   * @param value
   */
  private void recordTopRecordsValue(double value) {
    OutlierStatistic original;
    do {
      original = overflowStatistic.get();
    } while (!overflowStatistic.compareAndSet(original, original.combine(value)));
  }

  /**
   * Record a new value in {@code underflowStatistic}. This method should only be called when a
   * Histogram is recording a value smaller than the lowerbound bound of it's smallest bucket.
   */
  private void recordBottomRecordsValue(double value) {
    OutlierStatistic original;
    do {
      original = underflowStatistic.get();
    } while (!underflowStatistic.compareAndSet(original, original.combine(value)));
  }
}
