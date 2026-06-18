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
package org.apache.beam.sdk.io.components.util;

import java.util.Arrays;

/**
 * Class that keeps track of a rolling window sum.
 *
 * <p>For use in tracking recent performance of the connector.
 *
 * <p>Intended to be similar to {@link org.apache.beam.sdk.util.MovingFunction}, but for convenience
 * we expose the count of entries as well so this doubles as a moving average tracker.
 */
public class MovingSum {
  private final int numBuckets;
  private final long bucketMs;

  private int currentIndex;
  private long currentMsSinceEpoch;
  private final long[] sums;
  private final long[] counts;

  public MovingSum(long windowMs, long bucketMs) {
    if (windowMs < bucketMs || bucketMs <= 0) {
      throw new IllegalArgumentException("windowMs >= bucketMs > 0 please");
    }
    this.numBuckets = (int) Math.ceil((double) windowMs / bucketMs);
    this.bucketMs = bucketMs;
    this.sums = new long[this.numBuckets];
    this.counts = new long[this.numBuckets];
    this.currentIndex = 0;
    this.currentMsSinceEpoch = 0;
    Arrays.fill(this.sums, 0L);
    Arrays.fill(this.counts, 0L);
  }

  private void reset(long now) {
    this.currentIndex = 0;
    this.currentMsSinceEpoch = (now / bucketMs) * bucketMs;
    Arrays.fill(sums, 0L);
    Arrays.fill(counts, 0L);
  }

  private void flush(long now) {
    if (now >= (currentMsSinceEpoch + bucketMs * numBuckets)) {
      // Time moved forward so far that all currently held data is outside of
      // the window. It is faster to simply reset our data.
      reset(now);
      return;
    }

    while (now >= currentMsSinceEpoch + bucketMs) {
      // Advance time by one bucketMs, setting the new bucket's counts to 0.
      currentMsSinceEpoch += bucketMs;
      currentIndex = (currentIndex + 1) % numBuckets;
      sums[currentIndex] = 0;
      counts[currentIndex] = 0;
    }
  }

  public long sum(long now) {
    flush(now);
    long total = 0;
    for (long s : sums) {
      total += s;
    }
    return total;
  }

  public void add(long now, long inc) {
    flush(now);
    sums[currentIndex] += inc;
    counts[currentIndex] += 1;
  }

  public long count(long now) {
    flush(now);
    long total = 0;
    for (long c : counts) {
      total += c;
    }
    return total;
  }

  public boolean hasData(long now) {
    return count(now) > 0;
  }
}
