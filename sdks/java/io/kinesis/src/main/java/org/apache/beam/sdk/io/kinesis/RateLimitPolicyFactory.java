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

import java.io.Serializable;
import java.util.List;
import java.util.function.Supplier;
import org.joda.time.Duration;

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
}
