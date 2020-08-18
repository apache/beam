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

import java.math.RoundingMode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.DoubleMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A histogram that supports estimated percentile with linear interpolation. */
public class Histogram {
  private static final Logger LOG = LoggerFactory.getLogger(Histogram.class);

  private static final int DEFAULT_NUM_OF_BUCKETS = 50;

  private final double rangeFrom;
  private final double rangeTo;
  private final int numOfBuckets;

  private long[] buckets;
  private final double bucketSize;
  private long totalNumOfRecords;

  private final boolean ignoreOutOfRangeRecord;

  private Histogram(
      double rangeFrom, double rangeTo, int numOfBuckets, boolean ignoreOutOfRangeRecord) {
    if (rangeFrom < 0) {
      throw new RuntimeException(String.format("only positive range allowed: %f", rangeFrom));
    }
    if (rangeFrom >= rangeTo) {
      throw new RuntimeException(
          String.format("rangeTo should be larger than rangeFrom: [%f, %f)", rangeFrom, rangeTo));
    }
    if (numOfBuckets <= 0) {
      throw new RuntimeException(
          String.format("numOfBuckets should be greater than zero: %d", numOfBuckets));
    }
    this.rangeFrom = rangeFrom;
    this.rangeTo = rangeTo;
    this.numOfBuckets = numOfBuckets;
    this.ignoreOutOfRangeRecord = ignoreOutOfRangeRecord;
    this.buckets = new long[numOfBuckets];
    this.bucketSize = (rangeTo - rangeFrom) / numOfBuckets;
    this.totalNumOfRecords = 0;
  }

  /**
   * Create a histogram.
   *
   * @param rangeFrom The minimum value that this histogram can record. Cannot be negative.
   * @param rangeTo The maximum value that this histogram can record. Cannot be smaller than or
   *     equal to rangeFrom.
   * @param numOfBuckets The number of buckets. Larger number of buckets implies a better resolution
   *     for percentile estimation.
   * @param ignoreOutOfRangeRecord Whether the out-of-range records are discarded. It will throw
   *     RuntimeException for the out-of-range records if this is set to false.
   * @return a new Histogram instance.
   */
  public static Histogram of(
      double rangeFrom, double rangeTo, int numOfBuckets, boolean ignoreOutOfRangeRecord) {
    return new Histogram(rangeFrom, rangeTo, numOfBuckets, ignoreOutOfRangeRecord);
  }

  public static Histogram of(double rangeFrom, double rangeTo) {
    return new Histogram(rangeFrom, rangeTo, DEFAULT_NUM_OF_BUCKETS, false);
  }

  public void record(double... values) {
    for (double value : values) {
      record(value);
    }
  }

  public synchronized void clear() {
    this.buckets = new long[numOfBuckets];
    this.totalNumOfRecords = 0;
  }

  public synchronized void record(double value) {
    if (value >= rangeTo || value < rangeFrom) {
      if (ignoreOutOfRangeRecord) {
        LOG.warn("record out of range value [{}, {}): {}", rangeFrom, rangeTo, value);
        return;
      }
      throw new RuntimeException(
          String.format("out of range: %f is not in [%f, %f)", value, rangeFrom, rangeTo));
    }
    int index = DoubleMath.roundToInt((value - rangeFrom) / bucketSize, RoundingMode.FLOOR);
    buckets[index]++;
    totalNumOfRecords++;
  }

  public long getTotalCount() {
    return totalNumOfRecords;
  }

  public long getCount(int bucketIndex) {
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
  private double getLinearInterpolation(double percentile) {
    if (totalNumOfRecords == 0) {
      throw new RuntimeException("histogram has no record.");
    }
    int index;
    double recordSum = 0;
    for (index = 0; index < numOfBuckets; index++) {
      recordSum += buckets[index];
      if (recordSum / totalNumOfRecords >= percentile) {
        break;
      }
    }
    double fracPercentile = percentile - (recordSum - buckets[index]) / totalNumOfRecords;
    double bucketPercentile = (double) buckets[index] / totalNumOfRecords;
    double fracBucketSize = fracPercentile * bucketSize / bucketPercentile;
    return rangeFrom + bucketSize * index + fracBucketSize;
  }
}
