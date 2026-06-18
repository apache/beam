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
package org.apache.beam.sdk.io.components.throttling;

import java.util.Random;
import org.apache.beam.sdk.io.components.util.MovingSum;

/**
 * Implements adaptive throttling.
 *
 * <p>See
 * https://landing.google.com/sre/book/chapters/handling-overload.html#client-side-throttling-a7sYUg
 * for a full discussion of the use case and algorithm applied.
 */
public class AdaptiveThrottler {

  // The target minimum number of requests per samplePeriodMs, even if no
  // requests succeed. Must be greater than 0, else we could throttle to zero.
  // Because every decision is probabilistic, there is no guarantee that the
  // request rate in any given interval will not be zero. (This is the +1 from
  // the formula in
  // https://landing.google.com/sre/book/chapters/handling-overload.html )
  public static final int MIN_REQUESTS = 1;

  private final MovingSum allRequests;
  private final MovingSum successfulRequests;
  private final double overloadRatio;
  private final Random random;

  /**
   * Initializes AdaptiveThrottler.
   *
   * @param windowMs length of history to consider, in ms, to set throttling.
   * @param bucketMs granularity of time buckets that we store data in, in ms.
   * @param overloadRatio the target ratio between requests sent and successful requests.
   */
  public AdaptiveThrottler(long windowMs, long bucketMs, double overloadRatio) {
    this(windowMs, bucketMs, overloadRatio, new Random());
  }

  // visible for testing
  AdaptiveThrottler(long windowMs, long bucketMs, double overloadRatio, Random random) {
    if (overloadRatio <= 1.0) {
      throw new IllegalArgumentException("overloadRatio must be greater than 1.0");
    }
    this.allRequests = new MovingSum(windowMs, bucketMs);
    this.successfulRequests = new MovingSum(windowMs, bucketMs);
    this.overloadRatio = overloadRatio;
    this.random = random;
  }

  protected double throttlingProbability(long now) {
    if (!allRequests.hasData(now)) {
      return 0.0;
    }
    long allReqs = allRequests.sum(now);
    long successfulReqs = successfulRequests.sum(now);
    double prob = (allReqs - overloadRatio * successfulReqs) / (allReqs + MIN_REQUESTS);
    return Math.max(0.0, prob);
  }

  /**
   * Determines whether one RPC attempt should be throttled.
   *
   * <p>This should be called once each time the caller intends to send an RPC; if it returns true,
   * drop or delay that request (calling this function again after the delay).
   *
   * @param now time in ms since the epoch
   * @return true if the caller should throttle or delay the request.
   */
  public synchronized boolean throttleRequest(long now) {
    double prob = throttlingProbability(now);
    allRequests.add(now, 1);
    return random.nextDouble() < prob;
  }

  /**
   * Notifies the throttler of a successful request.
   *
   * <p>Must be called once for each request (for which throttleRequest was previously called) that
   * succeeded.
   *
   * @param now time in ms since the epoch
   */
  public synchronized void successfulRequest(long now) {
    successfulRequests.add(now, 1);
  }
}
