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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import org.apache.beam.sdk.transforms.Combine;

/**
 * Keep track of the moving minimum/maximum/sum of sampled long values. The minimum/maximum/sum is
 * over at most the user-specified last {@code samplePeriodMs}, and is updated every {@code
 * sampleUpdateMs}.
 */
public class MovingFunction {
  /** How frequently to update the moving function, in ms. */
  private final long sampleUpdateMs;

  /** How many buckets are considered 'significant'? */
  private final int numSignificantBuckets;

  /** How many samples are considered 'significant'? */
  private final int numSignificantSamples;

  /** Function for combining sample values. */
  private final Combine.BinaryCombineLongFn function;

  /** Minimum/maximum/sum of all values per bucket. */
  private final long[] buckets;

  /** How many samples have been added to each bucket. */
  private final int[] numSamples;

  /** Time of start of current bucket. */
  private long currentMsSinceEpoch;

  /** Index of bucket corresponding to above timestamp, or -1 if no entries. */
  private int currentIndex;

  public MovingFunction(
      long samplePeriodMs,
      long sampleUpdateMs,
      int numSignificantBuckets,
      int numSignificantSamples,
      Combine.BinaryCombineLongFn function) {
    this.sampleUpdateMs = sampleUpdateMs;
    this.numSignificantBuckets = numSignificantBuckets;
    this.numSignificantSamples = numSignificantSamples;
    this.function = function;
    int n = (int) (samplePeriodMs / sampleUpdateMs);
    buckets = new long[n];
    Arrays.fill(buckets, function.identity());
    numSamples = new int[n];
    Arrays.fill(numSamples, 0);
    currentMsSinceEpoch = -1;
    currentIndex = 0;
  }

  /** Flush stale values. */
  private void flush(long nowMsSinceEpoch) {
    checkArgument(nowMsSinceEpoch >= 0, "Only positive timestamps supported");
    checkArgument(nowMsSinceEpoch >= currentMsSinceEpoch, "Attempting to move backwards");
    int newBuckets =
        Math.min((int) ((nowMsSinceEpoch - currentMsSinceEpoch) / sampleUpdateMs), buckets.length);
    currentMsSinceEpoch = nowMsSinceEpoch - (nowMsSinceEpoch % sampleUpdateMs);
    while (newBuckets > 0) {
      currentIndex = (currentIndex + 1) % buckets.length;
      buckets[currentIndex] = function.identity();
      numSamples[currentIndex] = 0;
      newBuckets--;
    }
  }

  /** Add {@code value} at {@code nowMsSinceEpoch}. */
  public void add(long nowMsSinceEpoch, long value) {
    flush(nowMsSinceEpoch);
    buckets[currentIndex] = function.apply(buckets[currentIndex], value);
    numSamples[currentIndex]++;
  }

  /**
   * Return the minimum/maximum/sum of all retained values within samplePeriodMs of {@code
   * nowMsSinceEpoch}.
   */
  public long get(long nowMsSinceEpoch) {
    flush(nowMsSinceEpoch);
    long result = function.identity();
    for (long bucket : buckets) {
      result = function.apply(result, bucket);
    }
    return result;
  }

  /**
   * Is the current result 'significant'? Ie is it drawn from enough buckets or from enough samples?
   */
  public boolean isSignificant() {
    int totalSamples = 0;
    int activeBuckets = 0;
    for (int i = 0; i < buckets.length; i++) {
      totalSamples += numSamples[i];
      if (numSamples[i] > 0) {
        activeBuckets++;
      }
    }
    return activeBuckets >= numSignificantBuckets || totalSamples >= numSignificantSamples;
  }
}
