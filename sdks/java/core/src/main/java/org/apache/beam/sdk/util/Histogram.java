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
import java.math.RoundingMode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.DoubleMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A histogram that supports estimated percentile with linear interpolation.
 *
 * <p>We may consider using Apache Commons or HdrHistogram library in the future for advanced
 * features such as sparsely populated histograms.
 */
public class Histogram {
  private static final Logger LOG = LoggerFactory.getLogger(Histogram.class);

  private final BucketType bucketType;

  private long[] buckets;
  private long numOfRecords;
  private long numTopRecords;
  private long numBottomRecords;

  private Histogram(BucketType bucketType) {
    this.bucketType = bucketType;
    this.buckets = new long[bucketType.getNumBuckets()];
    this.numOfRecords = 0;
    this.numTopRecords = 0;
    this.numBottomRecords = 0;
  }

  /**
   * Create a histogram with linear buckets.
   *
   * @param start Lower bound of a starting bucket.
   * @param width Bucket width. Smaller width implies a better resolution for percentile estimation.
   * @param numBuckets The number of buckets. Upper bound of an ending bucket is defined by start +
   *     width * numBuckets.
   * @return a new Histogram instance.
   */
  public static Histogram linear(double start, double width, int numBuckets) {
    return new Histogram(LinearBuckets.of(start, width, numBuckets));
  }

  public void record(double... values) {
    for (double value : values) {
      record(value);
    }
  }

  public synchronized void clear() {
    this.buckets = new long[bucketType.getNumBuckets()];
    this.numOfRecords = 0;
    this.numTopRecords = 0;
    this.numBottomRecords = 0;
  }

  public synchronized void record(double value) {
    double rangeTo = bucketType.getRangeTo();
    double rangeFrom = bucketType.getRangeFrom();
    if (value >= rangeTo) {
      LOG.warn("record is out of upper bound {}: {}", rangeTo, value);
      numTopRecords++;
    } else if (value < rangeFrom) {
      LOG.warn("record is out of lower bound {}: {}", rangeFrom, value);
      numBottomRecords++;
    } else {
      buckets[bucketType.getBucketIndex(value)]++;
      numOfRecords++;
    }
  }

  public synchronized long getTotalCount() {
    return numOfRecords + numTopRecords + numBottomRecords;
  }

  public synchronized long getCount(int bucketIndex) {
    return buckets[bucketIndex];
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
      throw new RuntimeException("histogram has no record.");
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

  public interface BucketType {
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
        throw new RuntimeException(String.format("width should be greater than zero: %f", width));
      }
      if (numBuckets <= 0) {
        throw new RuntimeException(
            String.format("numBuckets should be greater than zero: %d", numBuckets));
      }
      return new AutoValue_Histogram_LinearBuckets(start, width, numBuckets);
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
  }
}
