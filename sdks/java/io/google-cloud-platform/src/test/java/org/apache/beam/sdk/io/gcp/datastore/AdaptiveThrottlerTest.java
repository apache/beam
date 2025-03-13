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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertFalse;

import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for {@link AdaptiveThrottler}. */
@RunWith(JUnit4.class)
public class AdaptiveThrottlerTest {

  static final long START_TIME_MS = 0;
  static final long SAMPLE_PERIOD_MS = 60000;
  static final long SAMPLE_BUCKET_MS = 1000;
  static final double OVERLOAD_RATIO = 2;

  /** Returns a throttler configured with the standard parameters above. */
  AdaptiveThrottler getThrottler() {
    return new AdaptiveThrottler(SAMPLE_PERIOD_MS, SAMPLE_BUCKET_MS, OVERLOAD_RATIO);
  }

  @Test
  public void testNoInitialThrottling() throws Exception {
    AdaptiveThrottler throttler = getThrottler();
    assertThat(throttler.throttlingProbability(START_TIME_MS), equalTo(0.0));
    assertThat(
        "first request is not throttled", throttler.throttleRequest(START_TIME_MS), equalTo(false));
  }

  @Test
  public void testNoThrottlingIfNoErrors() throws Exception {
    AdaptiveThrottler throttler = getThrottler();
    long t = START_TIME_MS;
    for (; t < START_TIME_MS + 20; t++) {
      assertFalse(throttler.throttleRequest(t));
      throttler.successfulRequest(t);
    }
    assertThat(throttler.throttlingProbability(t), equalTo(0.0));
  }

  @Test
  public void testNoThrottlingAfterErrorsExpire() throws Exception {
    AdaptiveThrottler throttler = getThrottler();
    long t = START_TIME_MS;
    for (; t < START_TIME_MS + SAMPLE_PERIOD_MS; t++) {
      throttler.throttleRequest(t);
      // and no successfulRequest.
    }
    assertThat(
        "check that we set up a non-zero probability of throttling",
        throttler.throttlingProbability(t),
        greaterThan(0.0));
    for (; t < START_TIME_MS + 2 * SAMPLE_PERIOD_MS; t++) {
      throttler.throttleRequest(t);
      throttler.successfulRequest(t);
    }
    assertThat(throttler.throttlingProbability(t), equalTo(0.0));
  }

  @Test
  public void testThrottlingAfterErrors() throws Exception {
    Random mockRandom = Mockito.mock(Random.class);
    Mockito.when(mockRandom.nextDouble())
        .thenReturn(
            0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6,
            0.7, 0.8, 0.9);
    AdaptiveThrottler throttler =
        new AdaptiveThrottler(SAMPLE_PERIOD_MS, SAMPLE_BUCKET_MS, OVERLOAD_RATIO, mockRandom);
    for (int i = 0; i < 20; i++) {
      boolean throttled = throttler.throttleRequest(START_TIME_MS + i);
      // 1/3rd of requests succeeding.
      if (i % 3 == 1) {
        throttler.successfulRequest(START_TIME_MS + i);
      }

      // Once we have some history in place, check what throttling happens.
      if (i >= 10) {
        // Expect 1/3rd of requests to be throttled. (So 1/3rd throttled, 1/3rd succeeding, 1/3rd
        // tried and failing).
        assertThat(
            String.format("for i=%d", i),
            throttler.throttlingProbability(START_TIME_MS + i),
            closeTo(0.33, /*error=*/ 0.1));
        // Requests 10..13 should be throttled, 14..19 not throttled given the mocked random numbers
        // that we fed to throttler.
        assertThat(String.format("for i=%d", i), throttled, equalTo(i < 14));
      }
    }
  }
}
