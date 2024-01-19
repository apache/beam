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
package org.apache.beam.sdk.io.kinesis;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.Supplier;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implement this interface to create a {@code RateLimitPolicy}. Used to create a rate limiter for
 * each shard. The factory will be called from multiple threads, so if it returns a singleton
 * instance of RateLimitPolicy then that instance should be thread-safe, otherwise it should return
 * separate RateLimitPolicy instances.
 */
public interface RateLimitPolicyFactory extends Serializable {

  RateLimitPolicy getRateLimitPolicy();

  static RateLimitPolicyFactory withoutLimiter() {
    return () -> new RateLimitPolicy() {};
  }

  static RateLimitPolicyFactory withDefaultRateLimiter() {
    return withDefaultRateLimiter(
        Duration.millis(100), Duration.millis(500), Duration.standardSeconds(1));
  }

  static RateLimitPolicyFactory withDefaultRateLimiter(
      Duration emptySuccessBaseDelay, Duration throttledBaseDelay, Duration maxDelay) {
    return () -> new DefaultRateLimiter(emptySuccessBaseDelay, throttledBaseDelay, maxDelay);
  }

  static RateLimitPolicyFactory withFixedDelay() {
    return DelayIntervalRateLimiter::new;
  }

  static RateLimitPolicyFactory withFixedDelay(Duration delay) {
    return () -> new DelayIntervalRateLimiter(() -> delay);
  }

  static RateLimitPolicyFactory withDelay(Supplier<Duration> delay) {
    return () -> new DelayIntervalRateLimiter(delay);
  }

  class DelayIntervalRateLimiter implements RateLimitPolicy {

    private static final Supplier<Duration> DEFAULT_DELAY = () -> Duration.standardSeconds(1);

    private final Supplier<Duration> delay;

    public DelayIntervalRateLimiter() {
      this(DEFAULT_DELAY);
    }

    public DelayIntervalRateLimiter(Supplier<Duration> delay) {
      this.delay = delay;
    }

    @Override
    public void onSuccess(List<KinesisRecord> records) throws InterruptedException {
      Thread.sleep(delay.get().getMillis());
    }
  }

  /**
   * Default rate limiter that throttles reading from a shard using an exponential backoff if the
   * response is empty or if the consumer is throttled by AWS.
   */
  class DefaultRateLimiter implements RateLimitPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultRateLimiter.class);
    private final Sleeper sleeper;
    private final BackOff throttled;
    private final BackOff emptySuccess;

    @VisibleForTesting
    DefaultRateLimiter(BackOff emptySuccess, BackOff throttled, Sleeper sleeper) {
      this.emptySuccess = emptySuccess;
      this.throttled = throttled;
      this.sleeper = sleeper;
    }

    public DefaultRateLimiter(BackOff emptySuccess, BackOff throttled) {
      this(emptySuccess, throttled, Sleeper.DEFAULT);
    }

    public DefaultRateLimiter(
        Duration emptySuccessBaseDelay, Duration throttledBaseDelay, Duration maxDelay) {
      this(
          FluentBackoff.DEFAULT
              .withInitialBackoff(emptySuccessBaseDelay)
              .withMaxBackoff(maxDelay)
              .backoff(),
          FluentBackoff.DEFAULT
              .withInitialBackoff(throttledBaseDelay)
              .withMaxBackoff(maxDelay)
              .backoff());
    }

    @Override
    public void onSuccess(List<KinesisRecord> records) throws InterruptedException {
      try {
        if (records.isEmpty()) {
          BackOffUtils.next(sleeper, emptySuccess);
        } else {
          emptySuccess.reset();
        }
        throttled.reset();
      } catch (IOException e) {
        LOG.warn("Error applying onSuccess rate limit policy", e);
      }
    }

    @Override
    public void onThrottle(KinesisClientThrottledException e) throws InterruptedException {
      try {
        BackOffUtils.next(sleeper, throttled);
      } catch (IOException ioe) {
        LOG.warn("Error applying onThrottle rate limit policy", e);
      }
    }
  }
}
