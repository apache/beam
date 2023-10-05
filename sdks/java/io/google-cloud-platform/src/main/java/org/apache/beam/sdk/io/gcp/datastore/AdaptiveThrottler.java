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
package org.apache.beam.sdk.io.gcp.datastore;

import java.util.Random;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.MovingFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * An implementation of client-side adaptive throttling. See
 * https://landing.google.com/sre/book/chapters/handling-overload.html#client-side-throttling-a7sYUg
 * for a full discussion of the use case and algorithm applied.
 */
class AdaptiveThrottler {
  private final MovingFunction successfulRequests;
  private final MovingFunction allRequests;

  /**
   * The target ratio between requests sent and successful requests. This is "K" in the formula in
   * https://landing.google.com/sre/book/chapters/handling-overload.html
   */
  private final double overloadRatio;

  /**
   * The target minimum number of requests per samplePeriodMs, even if no requests succeed. Must be
   * greater than 0, else we could throttle to zero. Because every decision is probabilistic, there
   * is no guarantee that the request rate in any given interval will not be zero. (This is the +1
   * from the formula in https://landing.google.com/sre/book/chapters/handling-overload.html
   */
  private static final double MIN_REQUESTS = 1;

  private final Random random;

  /**
   * @param samplePeriodMs the time window to keep of request history to inform throttling
   *     decisions.
   * @param sampleUpdateMs the length of buckets within this time window.
   * @param overloadRatio the target ratio between requests sent and successful requests. You should
   *     always set this to more than 1, otherwise the client would never try to send more requests
   *     than succeeded in the past - so it could never recover from temporary setbacks.
   */
  public AdaptiveThrottler(long samplePeriodMs, long sampleUpdateMs, double overloadRatio) {
    this(samplePeriodMs, sampleUpdateMs, overloadRatio, new Random());
  }

  @VisibleForTesting
  AdaptiveThrottler(long samplePeriodMs, long sampleUpdateMs, double overloadRatio, Random random) {
    allRequests =
        new MovingFunction(
            samplePeriodMs,
            sampleUpdateMs,
            1 /* numSignificantBuckets */,
            1 /* numSignificantSamples */,
            Sum.ofLongs());
    successfulRequests =
        new MovingFunction(
            samplePeriodMs,
            sampleUpdateMs,
            1 /* numSignificantBuckets */,
            1 /* numSignificantSamples */,
            Sum.ofLongs());
    this.overloadRatio = overloadRatio;
    this.random = random;
  }

  @VisibleForTesting
  double throttlingProbability(long nowMsSinceEpoch) {
    if (!allRequests.isSignificant()) {
      return 0;
    }
    long allRequestsNow = allRequests.get(nowMsSinceEpoch);
    long successfulRequestsNow = successfulRequests.get(nowMsSinceEpoch);
    return Math.max(
        0,
        (allRequestsNow - overloadRatio * successfulRequestsNow) / (allRequestsNow + MIN_REQUESTS));
  }

  /**
   * Call this before sending a request to the remote service; if this returns true, drop the
   * request (treating it as a failure or trying it again at a later time).
   */
  public boolean throttleRequest(long nowMsSinceEpoch) {
    double delayProbability = throttlingProbability(nowMsSinceEpoch);
    // Note that we increment the count of all requests here, even if we return true - so even if we
    // tell the client not to send a request at all, it still counts as a failed request.
    allRequests.add(nowMsSinceEpoch, 1);

    return (random.nextDouble() < delayProbability);
  }

  /** Call this after {@link throttleRequest} if your request was successful. */
  public void successfulRequest(long nowMsSinceEpoch) {
    successfulRequests.add(nowMsSinceEpoch, 1);
  }
}
