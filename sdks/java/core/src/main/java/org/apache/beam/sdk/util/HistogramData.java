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
package org.apache.beam.sdk.util;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.DoubleMath;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A histogram that supports estimated percentile with linear interpolation.
 *
 * <p>We may consider using Apache Commons or HdrHistogram library in the future for advanced
 * features such as sparsely populated histograms.
 */
public class HistogramData implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HistogramData.class);

  private final BucketType bucketType;

  // TODO(https://github.com/apache/beam/issues/20853): Update this function to remove the
  // numTopRecords and numBottomRecords
  // and include those counters in the buckets array.
  private long[] buckets;
  private long numBoundedBucketRecords;
  private long numTopRecords;
  private long numBottomRecords;

  /**
   * Create a histogram.
   *
   * @param bucketType a bucket type for a new histogram instance.
   */
  public HistogramData(BucketType bucketType) {
    this.bucketType = bucketType;
    this.buckets = new long[bucketType.getNumBuckets()];
    this.numBoundedBucketRecords = 0;
    this.numTopRecords = 0;
    this.numBottomRecords = 0;
  }

  public BucketType getBucketType() {
    return this.bucketType;
  }

  /**
   * TODO(https://github.com/apache/beam/issues/20853): Update this function to define numBuckets
   * total, including the infinite buckets. Create a histogram with linear buckets.
   *
   * @param start Lower bound of a starting bucket.
   * @param width Bucket width. Smaller width implies a better resolution for percentile estimation.
   * @param numBuckets The number of buckets. Upper bound of an ending bucket is defined by start +
   *     width * numBuckets.
   * @return a new Histogram instance.
   */
  public static HistogramData linear(double start, double width, int numBuckets) {
    return new HistogramData(LinearBuckets.of(start, width, numBuckets));
  }

  public void record(double... values) {
    for (double value : values) {
      record(value);
    }
  }

  public synchronized void update(HistogramData other) {
    synchronized (other) {
      if (!this.bucketType.equals(other.bucketType)
          || this.buckets.length != other.buckets.length) {
        LOG.warn("Failed to update HistogramData from another with a different buckets");
        return;
      }

      incTopBucketCount(other.numTopRecords);
      incBottomBucketCount(other.numBottomRecords);
      for (int i = 0; i < other.buckets.length; i++) {
        incBucketCount(i, other.buckets[i]);
      }
    }
  }

  // TODO(https://github.com/apache/beam/issues/20853): Update this function to allow incrementing
  // the infinite buckets as well.
  // and remove the incTopBucketCount and incBotBucketCount methods.
  // Using 0 and length -1 as the bucketIndex.
  public synchronized void incBucketCount(int bucketIndex, long count) {
    this.buckets[bucketIndex] += count;
    this.numBoundedBucketRecords += count;
  }

  public synchronized void incTopBucketCount(long count) {
    this.numTopRecords += count;
  }

  public synchronized void incBottomBucketCount(long count) {
    this.numBottomRecords += count;
  }

  public synchronized void clear() {
    this.buckets = new long[bucketType.getNumBuckets()];
    this.numBoundedBucketRecords = 0;
    this.numTopRecords = 0;
    this.numBottomRecords = 0;
  }

  public synchronized void record(double value) {
    double rangeTo = bucketType.getRangeTo();
    double rangeFrom = bucketType.getRangeFrom();
    if (value >= rangeTo) {
      numTopRecords++;
    } else if (value < rangeFrom) {
      numBottomRecords++;
    } else {
      buckets[bucketType.getBucketIndex(value)]++;
      numBoundedBucketRecords++;
    }
  }

  public synchronized long getTotalCount() {
    return numBoundedBucketRecords + numTopRecords + numBottomRecords;
  }

  public synchronized String getPercentileString(String elemType, String unit) {
    return String.format(
        "Total number of %s: %s, P99: %.0f %s, P90: %.0f %s, P50: %.0f %s",
        elemType, getTotalCount(), p99(), unit, p90(), unit, p50(), unit);
  }

  /**
   * TODO(https://github.com/apache/beam/issues/20853): Update this function to allow indexing the
   * -INF and INF bucket (using 0 and length -1) Get the bucket count for the given bucketIndex.
   *
   * <p>This method does not guarantee the atomicity when sequentially accessing the multiple
   * buckets i.e. other threads may alter the value between consecutive invocations. For summing the
   * total number of elements in the histogram, use `getTotalCount()` instead.
   *
   * @param bucketIndex index of the bucket
   * @return The number of elements in the specified bucket
   */
  public synchronized long getCount(int bucketIndex) {
    return buckets[bucketIndex];
  }

  public synchronized long getTopBucketCount() {
    return numTopRecords;
  }

  public synchronized long getBottomBucketCount() {
    return numBottomRecords;
  }

  public double p99() {
    return getLinearInterpolation(0.99);
  }

  public double p90() {
    return getLinearInterpolation(0.90);
  }

  public double p50() {
    return getLinearInterpolation(0.50);
  }

  /**
   * Calculate percentile estimation based on linear interpolation. It first finds the bucket which
   * includes the target percentile and projects the estimated point in the bucket by assuming all
   * the elements in the bucket are uniformly distributed.
   */
  private synchronized double getLinearInterpolation(double percentile) {
    long totalNumOfRecords = getTotalCount();
    if (totalNumOfRecords == 0) {
      return Double.NaN;
    }
    int index;
    double recordSum = numBottomRecords;
    if (recordSum / totalNumOfRecords >= percentile) {
      return Double.NEGATIVE_INFINITY;
    }
    for (index = 0; index < bucketType.getNumBuckets(); index++) {
      recordSum += buckets[index];
      if (recordSum / totalNumOfRecords >= percentile) {
        break;
      }
    }
    if (index == bucketType.getNumBuckets()) {
      return Double.POSITIVE_INFINITY;
    }
    double fracPercentile = percentile - (recordSum - buckets[index]) / totalNumOfRecords;
    double bucketPercentile = (double) buckets[index] / totalNumOfRecords;
    double fracBucketSize = fracPercentile * bucketType.getBucketSize(index) / bucketPercentile;
    return bucketType.getRangeFrom() + bucketType.getAccumulatedBucketSize(index) + fracBucketSize;
  }

  public interface BucketType extends Serializable {
    // Lower bound of a starting bucket.
    double getRangeFrom();
    // Upper bound of an ending bucket.
    double getRangeTo();
    // The number of buckets.
    int getNumBuckets();
    // Get the bucket array index for the given value.
    int getBucketIndex(double value);
    // Get the bucket size for the given bucket array index.
    double getBucketSize(int index);
    // Get the accumulated bucket size from bucket index 0 until endIndex.
    // Generally, this can be calculated as `sigma(0 <= i < endIndex) getBucketSize(i)`.
    double getAccumulatedBucketSize(int endIndex);
  }

  @AutoValue
  public abstract static class LinearBuckets implements BucketType {
    public abstract double getStart();

    public abstract double getWidth();

    @Override
    public abstract int getNumBuckets();

    public static LinearBuckets of(double start, double width, int numBuckets) {
      if (width <= 0) {
        throw new IllegalArgumentException(
            String.format("width should be greater than zero: %f", width));
      }
      if (numBuckets <= 0) {
        throw new IllegalArgumentException(
            String.format("numBuckets should be greater than zero: %d", numBuckets));
      }
      return new AutoValue_HistogramData_LinearBuckets(start, width, numBuckets);
    }

    @Override
    public int getBucketIndex(double value) {
      return DoubleMath.roundToInt((value - getStart()) / getWidth(), RoundingMode.FLOOR);
    }

    @Override
    public double getBucketSize(int index) {
      return getWidth();
    }

    @Override
    public double getAccumulatedBucketSize(int endIndex) {
      return getWidth() * endIndex;
    }

    @Override
    public double getRangeFrom() {
      return getStart();
    }

    @Override
    public double getRangeTo() {
      return getStart() + getNumBuckets() * getWidth();
    }

    // Note: equals() and hashCode() are implemented by the AutoValue.
  }

  @Override
  public synchronized boolean equals(@Nullable Object object) {
    if (object instanceof HistogramData) {
      HistogramData other = (HistogramData) object;
      synchronized (other) {
        return Objects.equals(bucketType, other.bucketType)
            && numBoundedBucketRecords == other.numBoundedBucketRecords
            && numTopRecords == other.numTopRecords
            && numBottomRecords == other.numBottomRecords
            && Arrays.equals(buckets, other.buckets);
      }
    }
    return false;
  }

  @Override
  public synchronized int hashCode() {
    return Objects.hash(
        bucketType,
        numBoundedBucketRecords,
        numBottomRecords,
        numTopRecords,
        Arrays.hashCode(buckets));
  }
}
