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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AdaptiveThrottlerTest {

  private static final long START_TIME = 1500000000000L;
  private static final long SAMPLE_PERIOD = 60000L;
  private static final long BUCKET = 1000L;
  private static final double OVERLOAD_RATIO = 2.0;

  private AdaptiveThrottler throttler;

  @Before
  public void setUp() {
    throttler = new AdaptiveThrottler(SAMPLE_PERIOD, BUCKET, OVERLOAD_RATIO);
  }

  @Test
  public void testNoInitialThrottling() {
    assertEquals(0.0, throttler.throttlingProbability(START_TIME), 0.0);
  }

  @Test
  public void testNoThrottlingIfNoErrors() {
    for (long t = START_TIME; t < START_TIME + 20; t++) {
      assertFalse(throttler.throttleRequest(t));
      throttler.successfulRequest(t);
    }
    assertEquals(0.0, throttler.throttlingProbability(START_TIME + 20), 0.0);
  }

  @Test
  public void testNoThrottlingAfterErrorsExpire() {
    for (long t = START_TIME; t < START_TIME + SAMPLE_PERIOD; t += 100) {
      throttler.throttleRequest(t);
      // No successful request
    }
    assertTrue(throttler.throttlingProbability(START_TIME + SAMPLE_PERIOD) > 0);

    for (long t = START_TIME + SAMPLE_PERIOD; t < START_TIME + SAMPLE_PERIOD * 2; t += 100) {
      throttler.throttleRequest(t);
      throttler.successfulRequest(t);
    }

    assertEquals(0.0, throttler.throttlingProbability(START_TIME + SAMPLE_PERIOD * 2), 0.0);
  }

  @Test
  public void testThrottlingAfterErrors() {
    // Inject a mocked Random
    throttler = new AdaptiveThrottler(SAMPLE_PERIOD, BUCKET, OVERLOAD_RATIO, new MockRandom());

    for (long t = START_TIME; t < START_TIME + 20; t++) {
      boolean throttled = throttler.throttleRequest(t);
      // 1/3rd of requests succeeding.
      if (t % 3 == 1) {
        throttler.successfulRequest(t);
      }

      if (t > START_TIME + 10) {
        // Roughly 1/3rd succeeding, 1/3rd failing, 1/3rd throttled.
        assertEquals(0.33, throttler.throttlingProbability(t), 0.1);
        // Given mocked random, expects 10..13 throttled, 14+ unthrottled
        assertEquals(t < START_TIME + 14, throttled);
      }
    }
  }

  private static class MockRandom extends Random {
    private int callCount = 0;

    @Override
    public double nextDouble() {
      // Return 0.0, 0.1, ..., 0.9, 0.0, 0.1 ...
      double val = (callCount % 10) / 10.0;
      callCount++;
      return val;
    }
  }
}
