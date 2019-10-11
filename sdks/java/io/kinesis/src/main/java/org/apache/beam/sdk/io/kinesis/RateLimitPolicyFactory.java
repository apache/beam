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
import org.joda.time.Duration;

/**
 * Implement this interface to create a {@code RateLimitPolicy}. Used to create a rate limiter for
 * each shard.
 */
public interface RateLimitPolicyFactory extends Serializable {

  RateLimitPolicy getRateLimitPolicy();

  static RateLimitPolicyFactory withoutLimiter() {
    return () -> new RateLimitPolicy() {};
  }

  static RateLimitPolicyFactory withFixedDelay() {
    return FixedDelayRateLimiter::new;
  }

  static RateLimitPolicyFactory withFixedDelay(Duration delay) {
    return () -> new FixedDelayRateLimiter(delay);
  }

  class FixedDelayRateLimiter implements RateLimitPolicy {

    private static final Duration DEFAULT_DELAY = Duration.standardSeconds(1);

    private final Duration delay;

    public FixedDelayRateLimiter() {
      this(DEFAULT_DELAY);
    }

    public FixedDelayRateLimiter(Duration delay) {
      this.delay = delay;
    }

    @Override
    public void onSuccess(List<KinesisRecord> records) throws InterruptedException {
      Thread.sleep(delay.getMillis());
    }
  }
}
