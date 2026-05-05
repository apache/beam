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
package org.apache.beam.sdk.io.components.ratelimiter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RateLimiterOptions}. */
@RunWith(JUnit4.class)
public class RateLimiterOptionsTest {

  @Test
  public void testValidOptions() {
    RateLimiterOptions options =
        RateLimiterOptions.builder()
            .setAddress("localhost:8081")
            .setTimeout(Duration.ofSeconds(1))
            .setMaxRetries(3)
            .build();

    assertEquals("localhost:8081", options.getAddress());
    assertEquals(Duration.ofSeconds(1), options.getTimeout());
    assertEquals(Integer.valueOf(3), options.getMaxRetries());
  }

  @Test
  public void testNegativeTimeout() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RateLimiterOptions.builder()
                .setAddress("localhost:8081")
                .setTimeout(Duration.ofSeconds(-1))
                .build());
  }

  @Test
  public void testZeroTimeout() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RateLimiterOptions.builder()
                .setAddress("localhost:8081")
                .setTimeout(Duration.ZERO)
                .build());
  }

  @Test
  public void testNegativeMaxRetries() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RateLimiterOptions.builder().setAddress("localhost:8081").setMaxRetries(-1).build());
  }

  @Test
  public void testNullMaxRetriesIsAllowed() {
    RateLimiterOptions options =
        RateLimiterOptions.builder().setAddress("localhost:8081").setMaxRetries(null).build();
    assertEquals(null, options.getMaxRetries());
  }
}
