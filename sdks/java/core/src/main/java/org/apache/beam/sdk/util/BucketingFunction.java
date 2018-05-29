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

import static com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.Combine;

/**
 * Keep track of the minimum/maximum/sum of a set of timestamped long values.
 * For efficiency, bucket values by their timestamp.
 */
public class BucketingFunction {
  private static class Bucket {
    private int numSamples;
    private long combinedValue;

    public Bucket(BucketingFunction outer) {
      numSamples = 0;
      combinedValue = outer.function.identity();
    }

    public void add(BucketingFunction outer, long value) {
      combinedValue = outer.function.apply(combinedValue, value);
      numSamples++;
    }

    public boolean remove() {
      numSamples--;
      checkState(numSamples >= 0, "Lost count of samples");
      return numSamples == 0;
    }

    public long get() {
      return combinedValue;
    }
  }

  /**
   * How large a time interval to fit within each bucket.
   */
  private final long bucketWidthMs;

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
  private final Combine.BinaryCombineLongFn function;

  /**
   * Active buckets.
   */
  private final Map<Long, Bucket> buckets;

  public BucketingFunction(
      long bucketWidthMs,
      int numSignificantBuckets,
      int numSignificantSamples,
      Combine.BinaryCombineLongFn function) {
    this.bucketWidthMs = bucketWidthMs;
    this.numSignificantBuckets = numSignificantBuckets;
    this.numSignificantSamples = numSignificantSamples;
    this.function = function;
    this.buckets = new HashMap<>();
  }

  /**
   * Which bucket key corresponds to {@code timeMsSinceEpoch}.
   */
  private long key(long timeMsSinceEpoch) {
    return timeMsSinceEpoch - (timeMsSinceEpoch % bucketWidthMs);
  }

  /**
   * Add one sample of {@code value} (to bucket) at {@code timeMsSinceEpoch}.
   */
  public void add(long timeMsSinceEpoch, long value) {
    long key = key(timeMsSinceEpoch);
    Bucket bucket = buckets.computeIfAbsent(key, k -> new Bucket(this));
    bucket.add(this, value);
  }

  /**
   * Remove one sample (from bucket) at {@code timeMsSinceEpoch}.
   */
  public void remove(long timeMsSinceEpoch) {
    long key = key(timeMsSinceEpoch);
    Bucket bucket = buckets.get(key);
    if (bucket == null) {
      return;
    }
    if (bucket.remove()) {
      buckets.remove(key);
    }
  }

  /**
   * Return the (bucketized) combined value of all samples.
   */
  public long get() {
    long result = function.identity();
    for (Bucket bucket : buckets.values()) {
      result = function.apply(result, bucket.get());
    }
    return result;
  }

  /**
   * Is the current result 'significant'? Ie is it drawn from enough buckets
   * or from enough samples?
   */
  public boolean isSignificant() {
    if (buckets.size() >= numSignificantBuckets) {
      return true;
    }
    int totalSamples = 0;
    for (Bucket bucket : buckets.values()) {
      totalSamples += bucket.numSamples;
    }
    return totalSamples >= numSignificantSamples;
  }
}
