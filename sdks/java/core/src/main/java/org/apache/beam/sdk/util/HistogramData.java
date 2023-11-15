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
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.DoubleMath;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.IntMath;
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

  @GuardedBy("this")
  private double sumOfSquaredDeviations;

  @GuardedBy("this")
  private double mean;

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
    this.mean = 0;
    this.sumOfSquaredDeviations = 0;
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

  /**
   * Returns a histogram object with exponential boundaries. The input parameter {@code scale}
   * determines a coefficient 'base' which species bucket boundaries.
   *
   * <pre>
   * base = 2**(2**(-scale)) e.g.
   * scale=1 => base=2**(1/2)=sqrt(2)
   * scale=0 => base=2**(1)=2
   * scale=-1 => base=2**(2)=4
   * </pre>
   *
   * This bucketing strategy makes it simple/numerically stable to compute bucket indexes for
   * datapoints.
   *
   * <pre>
   * Bucket boundaries are given by the following table where n=numBuckets.
   * | 'Bucket Index' | Bucket Boundaries   |
   * |---------------|---------------------|
   * | Underflow     | (-inf, 0)           |
   * | 0             | [0, base)           |
   * | 1             | [base, base^2)      |
   * | 2             | [base^2, base^3)    |
   * | i             | [base^i, base^(i+1))|
   * | n-1           | [base^(n-1), base^n)|
   * | Overflow      | [base^n, inf)       |
   * </pre>
   *
   * <pre>
   * Example scale/boundaries:
   * When scale=1, buckets 0,1,2...i have lowerbounds 0, 2^(1/2), 2^(2/2), ... 2^(i/2).
   * When scale=0, buckets 0,1,2...i have lowerbounds 0, 2, 2^2, ... 2^(i).
   * When scale=-1, buckets 0,1,2...i have lowerbounds 0, 4, 4^2, ... 4^(i).
   * </pre>
   *
   * Scale parameter is similar to <a
   * href="https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram">
   * OpenTelemetry's notion of ExponentialHistogram</a>. Bucket boundaries are modified to make them
   * compatible with GCP's exponential histogram.
   *
   * @param numBuckets The number of buckets. Clipped so that the largest bucket's lower bound is
   *     not greater than 2^32-1 (uint32 max).
   * @param scale Integer between [-3, 3] which determines bucket boundaries. Larger values imply
   *     more fine grained buckets.
   * @return a new Histogram instance.
   */
  public static HistogramData exponential(int scale, int numBuckets) {
    return new HistogramData(ExponentialBuckets.of(scale, numBuckets));
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
      this.mean = other.mean;
      this.sumOfSquaredDeviations = other.sumOfSquaredDeviations;
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
    this.mean = 0;
    this.sumOfSquaredDeviations = 0;
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
    updateStatistics(value);
  }

  /**
   * Update 'mean' and 'sum of squared deviations' statistics with the newly recorded value <a
   * href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm">
   * Welford's Method</a>.
   *
   * @param value
   */
  private synchronized void updateStatistics(double value) {
    long count = getTotalCount();
    if (count == 1) {
      mean = value;
      return;
    }

    double oldMean = mean;
    mean = oldMean + (value - oldMean) / count;
    sumOfSquaredDeviations += (value - mean) * (value - oldMean);
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

  public synchronized double getMean() {
    return mean;
  }

  public synchronized double getSumOfSquaredDeviations() {
    return sumOfSquaredDeviations;
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
  public abstract static class ExponentialBuckets implements BucketType {

    // Minimum scale factor. Bucket boundaries can grow at a rate of at most: 2^(2^3)=2^8=256
    private static final int MINIMUM_SCALE = -3;

    // Minimum scale factor. Bucket boundaries must grow at a rate of at least 2^(2^-3)=2^(1/8)
    private static final int MAXIMUM_SCALE = 3;

    // Maximum number of buckets that is supported when 'scale' is zero.
    private static final int ZERO_SCALE_MAX_NUM_BUCKETS = 32;

    public abstract double getBase();

    public abstract int getScale();

    /**
     * Set to 2**scale which is equivalent to 1/log_2(base). Precomputed to use in {@code
     * getBucketIndexPositiveScale}
     */
    public abstract double getInvLog2GrowthFactor();

    @Override
    public abstract int getNumBuckets();

    /* Precomputed since this value is used everytime a datapoint is recorded. */
    @Override
    public abstract double getRangeTo();

    public static ExponentialBuckets of(int scale, int numBuckets) {
      if (scale < MINIMUM_SCALE) {
        throw new IllegalArgumentException(
            String.format("Scale should be greater than %d: %d", MINIMUM_SCALE, scale));
      }

      if (scale > MAXIMUM_SCALE) {
        throw new IllegalArgumentException(
            String.format("Scale should be less than %d: %d", MAXIMUM_SCALE, scale));
      }
      if (numBuckets <= 0) {
        throw new IllegalArgumentException(
            String.format("numBuckets should be positive: %d", numBuckets));
      }

      double invLog2GrowthFactor = Math.pow(2, scale);
      double base = Math.pow(2, Math.pow(2, -scale));
      int clippedNumBuckets = ExponentialBuckets.computeNumberOfBuckets(scale, numBuckets);
      double rangeTo = Math.pow(base, clippedNumBuckets);
      return new AutoValue_HistogramData_ExponentialBuckets(
          base, scale, invLog2GrowthFactor, clippedNumBuckets, rangeTo);
    }

    /**
     * numBuckets is clipped so that the largest bucket's lower bound is not greater than 2^32-1
     * (uint32 max). This value is log_base(2^32) which simplifies as follows:
     *
     * <pre>
     * log_base(2^32)
     * = log_2(2^32)/log_2(base)
     * = 32/(2**-scale)
     * = 32*(2**scale)
     * </pre>
     */
    private static int computeNumberOfBuckets(int scale, int inputNumBuckets) {
      if (scale == 0) {
        // When base=2 then the bucket at index 31 contains [2^31, 2^32).
        return Math.min(ZERO_SCALE_MAX_NUM_BUCKETS, inputNumBuckets);
      } else if (scale > 0) {
        // When scale is positive 32*(2**scale) is equivalent to a right bit-shift.
        return Math.min(inputNumBuckets, ZERO_SCALE_MAX_NUM_BUCKETS << scale);
      } else {
        // When scale is negative 32*(2**scale) is equivalent to a left bit-shift.
        return Math.min(inputNumBuckets, ZERO_SCALE_MAX_NUM_BUCKETS >> -scale);
      }
    }

    @Override
    public int getBucketIndex(double value) {
      if (value < getBase()) {
        return 0;
      }

      // When scale is non-positive, 'base' and 'bucket boundaries' will be integers.
      // In this scenario `value` and `floor(value)` will belong to the same bucket.
      int index;
      if (getScale() > 0) {
        index = getBucketIndexPositiveScale(value);
      } else if (getScale() < 0) {
        index = getBucketIndexNegativeScale(DoubleMath.roundToInt(value, RoundingMode.FLOOR));
      } else {
        index = getBucketIndexZeroScale(DoubleMath.roundToInt(value, RoundingMode.FLOOR));
      }
      // Ensure that a valid index is returned in the off chance of a numerical instability error.
      return Math.max(Math.min(index, getNumBuckets() - 1), 0);
    }

    private int getBucketIndexZeroScale(int value) {
      return IntMath.log2(value, RoundingMode.FLOOR);
    }

    private int getBucketIndexNegativeScale(int value) {
      return getBucketIndexZeroScale(value) >> (-getScale());
    }

    // This method is valid for all 'scale' values but we fallback to more efficient methods for
    // non-positive scales.
    // For a value>base we would like to find an i s.t. :
    // base^i <= value < base^(i+1)
    // i <= log_base(value) < i+1
    // i = floor(log_base(value))
    // i = floor(log_2(value)/log_2(base))
    private int getBucketIndexPositiveScale(double value) {
      return DoubleMath.roundToInt(
          getInvLog2GrowthFactor() * DoubleMath.log2(value), RoundingMode.FLOOR);
    }

    @Override
    public double getBucketSize(int index) {
      if (index < 0) {
        return 0;
      }
      if (index == 0) {
        return getBase();
      }

      // bucketSize = (base)^(i+1) - (base)^i
      //            = (base)^i(base - 1)
      return Math.pow(getBase(), index) * (getBase() - 1);
    }

    @Override
    public double getAccumulatedBucketSize(int endIndex) {
      if (endIndex < 0) {
        return 0;
      }
      return Math.pow(getBase(), endIndex + 1);
    }

    @Override
    public double getRangeFrom() {
      return 0;
    }
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
