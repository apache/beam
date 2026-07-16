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
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.MovingFunction;

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

  private final MovingFunction allRequests;
  private final MovingFunction successfulRequests;
  private final double overloadRatio;
  private final Random random;

  /**
   * Initializes AdaptiveThrottler.
   *
   * @param samplePeriodMs length of history to consider, in ms, to set throttling.
   * @param sampleUpdateMs granularity of time buckets that we store data in, in ms.
   * @param overloadRatio the target ratio between requests sent and successful requests.
   */
  public AdaptiveThrottler(long samplePeriodMs, long sampleUpdateMs, double overloadRatio) {
    this(samplePeriodMs, sampleUpdateMs, overloadRatio, new Random());
  }

  // visible for testing
  AdaptiveThrottler(long samplePeriodMs, long sampleUpdateMs, double overloadRatio, Random random) {
    if (overloadRatio <= 1.0) {
      throw new IllegalArgumentException("overloadRatio must be greater than 1.0");
    }
    this.allRequests = new MovingFunction(samplePeriodMs, sampleUpdateMs, 1, 1, Sum.ofLongs());
    this.successfulRequests =
        new MovingFunction(samplePeriodMs, sampleUpdateMs, 1, 1, Sum.ofLongs());
    this.overloadRatio = overloadRatio;
    this.random = random;
  }

  protected double throttlingProbability(long nowMsSinceEpoch) {
    long allReqs = allRequests.get(nowMsSinceEpoch);
    if (!allRequests.isSignificant()) {
      return 0.0;
    }
    long successfulReqs = successfulRequests.get(nowMsSinceEpoch);
    double prob = (allReqs - overloadRatio * successfulReqs) / (allReqs + MIN_REQUESTS);
    return Math.max(0.0, prob);
  }

  /**
   * Determines whether one RPC attempt should be throttled.
   *
   * <p>This should be called once each time the caller intends to send an RPC; if it returns true,
   * drop or delay that request (calling this function again after the delay).
   *
   * @param nowMsSinceEpoch time in ms since the epoch
   * @return true if the caller should throttle or delay the request.
   */
  public synchronized boolean throttleRequest(long nowMsSinceEpoch) {
    double prob = throttlingProbability(nowMsSinceEpoch);
    allRequests.add(nowMsSinceEpoch, 1);
    return random.nextDouble() < prob;
  }

  /**
   * Notifies the throttler of a successful request.
   *
   * <p>Must be called once for each request (for which throttleRequest was previously called) that
   * succeeded.
   *
   * @param nowMsSinceEpoch time in ms since the epoch
   */
  public synchronized void successfulRequest(long nowMsSinceEpoch) {
    successfulRequests.add(nowMsSinceEpoch, 1);
  }
}
