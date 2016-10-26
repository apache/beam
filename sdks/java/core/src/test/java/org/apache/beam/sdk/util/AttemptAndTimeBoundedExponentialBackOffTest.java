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
package org.apache.beam.sdk.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.client.util.BackOff;
import org.apache.beam.sdk.testing.FastNanoClockAndSleeper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link AttemptAndTimeBoundedExponentialBackOff}. */
@RunWith(JUnit4.class)
@SuppressWarnings("deprecation") // test of deprecated class
public class AttemptAndTimeBoundedExponentialBackOffTest {
  @Rule public ExpectedException exception = ExpectedException.none();
  @Rule public FastNanoClockAndSleeper fastClock = new FastNanoClockAndSleeper();

  @Test
  public void testUsingInvalidInitialInterval() throws Exception {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Initial interval must be greater than zero.");
    new AttemptAndTimeBoundedExponentialBackOff(10, 0L, 1000L);
  }

  @Test
  public void testUsingInvalidTimeInterval() throws Exception {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Maximum total wait time must be greater than zero.");
    new AttemptAndTimeBoundedExponentialBackOff(10, 2L, 0L);
  }

  @Test
  public void testUsingInvalidMaximumNumberOfRetries() throws Exception {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Maximum number of attempts must be greater than zero.");
    new AttemptAndTimeBoundedExponentialBackOff(-1, 10L, 1000L);
  }

  @Test
  public void testThatFixedNumberOfAttemptsExits() throws Exception {
    BackOff backOff =
        new AttemptAndTimeBoundedExponentialBackOff(
            3,
            500L,
            1000L,
            AttemptAndTimeBoundedExponentialBackOff.ResetPolicy.ALL,
            fastClock);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
  }

  @Test
  public void testThatResettingAllowsReuse() throws Exception {
    AttemptBoundedExponentialBackOff backOff =
        new AttemptAndTimeBoundedExponentialBackOff(
            3,
            500,
            1000L,
            AttemptAndTimeBoundedExponentialBackOff.ResetPolicy.ALL,
            fastClock);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
    backOff.reset();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());

    backOff =
        new AttemptAndTimeBoundedExponentialBackOff(
            30,
            500,
            1000L,
            AttemptAndTimeBoundedExponentialBackOff.ResetPolicy.ALL,
            fastClock);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    fastClock.sleep(2000L);
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
    backOff.reset();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
  }

  @Test
  public void testThatResettingAttemptsAllowsReuse() throws Exception {
    AttemptBoundedExponentialBackOff backOff =
        new AttemptAndTimeBoundedExponentialBackOff(
            3,
            500,
            1000,
            AttemptAndTimeBoundedExponentialBackOff.ResetPolicy.ATTEMPTS,
            fastClock);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
    backOff.reset();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
  }

  @Test
  public void testThatResettingAttemptsDoesNotAllowsReuse() throws Exception {
    AttemptBoundedExponentialBackOff backOff =
        new AttemptAndTimeBoundedExponentialBackOff(
            30,
            500,
            1000L,
            AttemptAndTimeBoundedExponentialBackOff.ResetPolicy.ATTEMPTS,
            fastClock);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    fastClock.sleep(2000L);
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
    backOff.reset();
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
  }

  @Test
  public void testThatResettingTimerAllowsReuse() throws Exception {
    AttemptBoundedExponentialBackOff backOff =
        new AttemptAndTimeBoundedExponentialBackOff(
            30,
            500,
            1000L,
            AttemptAndTimeBoundedExponentialBackOff.ResetPolicy.TIMER,
            fastClock);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    fastClock.sleep(2000L);
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
    backOff.reset();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(561L), lessThan(1688L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(843L), lessThan(2531L)));
  }

  @Test
  public void testThatResettingTimerDoesNotAllowReuse() throws Exception {
    AttemptBoundedExponentialBackOff backOff =
        new AttemptAndTimeBoundedExponentialBackOff(
            3,
            500,
            1000L,
            AttemptAndTimeBoundedExponentialBackOff.ResetPolicy.TIMER,
            fastClock);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
    backOff.reset();
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
  }

  @Test
  public void testTimeBound() throws Exception {
    AttemptBoundedExponentialBackOff backOff =
        new AttemptAndTimeBoundedExponentialBackOff(
            3, 500L, 5L, AttemptAndTimeBoundedExponentialBackOff.ResetPolicy.ALL, fastClock);
    assertEquals(backOff.nextBackOffMillis(), 5L);
  }

  @Test
  public void testAtMaxAttempts() throws Exception {
    AttemptBoundedExponentialBackOff backOff =
        new AttemptAndTimeBoundedExponentialBackOff(
            3,
            500L,
            1000L,
            AttemptAndTimeBoundedExponentialBackOff.ResetPolicy.ALL,
            fastClock);
    assertFalse(backOff.atMaxAttempts());
    backOff.nextBackOffMillis();
    assertFalse(backOff.atMaxAttempts());
    backOff.nextBackOffMillis();
    assertTrue(backOff.atMaxAttempts());
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
  }

  @Test
  public void testAtMaxTime() throws Exception {
    AttemptBoundedExponentialBackOff backOff =
        new AttemptAndTimeBoundedExponentialBackOff(
            3, 500L, 1L, AttemptAndTimeBoundedExponentialBackOff.ResetPolicy.ALL, fastClock);
    fastClock.sleep(2);
    assertTrue(backOff.atMaxAttempts());
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
  }
}
