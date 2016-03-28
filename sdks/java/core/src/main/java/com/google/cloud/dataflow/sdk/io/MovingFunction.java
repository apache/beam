/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.common.base.Preconditions;
import java.util.Arrays;

/**
 * Keep track of the moving minimum/maximum/sum of sampled long values. The minimum/maximum/sum
 * is over at most the last {@link #samplePeriodMs}, and is updated every
 * {@link #sampleUpdateMs}.
 */
class MovingFunction {
  /**
   * How far back to retain samples, in ms.
   */
  private final long samplePeriodMs;

  /**
   * How frequently to update the moving function, in ms.
   */
  private final long sampleUpdateMs;

  /**
   * How many buckets are considered 'significant'?
   */
  private final int numSignificantBuckets;

  /**
   * How many samples are considered 'significant'?
   */
  private final int numSignificantSamples;

  /**
   * Function for combining sample values.
   */
  private final SimpleFunction function;

  /**
   * Minimum/maximum/sum of all values per bucket.
   */
  private final long[] buckets;

  /**
   * How many samples have been added to each bucket.
   */
  private final int[] numSamples;

  /**
   * Time of start of current bucket.
   */
  private long currentMsSinceEpoch;

  /**
   * Index of bucket corresponding to above timestamp, or -1 if no entries.
   */
  private int currentIndex;

  public MovingFunction(long samplePeriodMs, long sampleUpdateMs,
                        int numSignificantBuckets, int numSignificantSamples,
                        SimpleFunction function) {
    this.samplePeriodMs = samplePeriodMs;
    this.sampleUpdateMs = sampleUpdateMs;
    this.numSignificantBuckets = numSignificantBuckets;
    this.numSignificantSamples = numSignificantSamples;
    this.function = function;
    int n = (int) (samplePeriodMs / sampleUpdateMs);
    buckets = new long[n];
    Arrays.fill(buckets, function.zero());
    numSamples = new int[n];
    Arrays.fill(numSamples, 0);
    currentMsSinceEpoch = -1;
    currentIndex = -1;
  }

  /**
   * Flush stale values.
   */
  private void flush(long nowMsSinceEpoch) {
    Preconditions.checkArgument(nowMsSinceEpoch >= 0);
    if (currentIndex < 0) {
      currentMsSinceEpoch = nowMsSinceEpoch - (nowMsSinceEpoch % sampleUpdateMs);
      currentIndex = 0;
    }
    Preconditions.checkArgument(nowMsSinceEpoch >= currentMsSinceEpoch);
    int newBuckets =
        Math.min((int) ((nowMsSinceEpoch - currentMsSinceEpoch) / sampleUpdateMs),
                 buckets.length);
    while (newBuckets > 0) {
      currentIndex = (currentIndex + 1) % buckets.length;
      buckets[currentIndex] = function.zero();
      numSamples[currentIndex] = 0;
      newBuckets--;
      currentMsSinceEpoch += sampleUpdateMs;
    }
  }

  /**
   * Add {@code value} at {@code nowMsSinceEpoch}.
   */
  public void add(long nowMsSinceEpoch, long value) {
    flush(nowMsSinceEpoch);
    buckets[currentIndex] = function.f(buckets[currentIndex], value);
    numSamples[currentIndex]++;
  }

  /**
   * Return the minimum/maximum/sum of all retained values within {@link #samplePeriodMs}
   * of {@code nowMsSinceEpoch}.
   */
  public long get(long nowMsSinceEpoch) {
    flush(nowMsSinceEpoch);
    long result = function.zero();
    for (int i = 0; i < buckets.length; i++) {
      result = function.f(result, buckets[i]);
    }
    return result;
  }

  /**
   * Is the current result 'significant'? Ie is it drawn from enough buckets
   * or from enough samples?
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
