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
package org.apache.beam.sdk.transforms.windowing;

import static org.apache.beam.sdk.testing.WindowFnTestUtils.runWindowFn;
import static org.apache.beam.sdk.testing.WindowFnTestUtils.set;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.WindowFnTestUtils;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Sessions WindowFn. */
@RunWith(JUnit4.class)
public class SessionsTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSimple() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(0), new Instant(10)), set(0));
    expected.put(new IntervalWindow(new Instant(10), new Instant(20)), set(10));
    expected.put(new IntervalWindow(new Instant(101), new Instant(111)), set(101));
    assertEquals(
        expected,
        runWindowFn(Sessions.withGapDuration(Duration.millis(10)), Arrays.asList(0L, 10L, 101L)));
  }

  @Test
  public void testConsecutive() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(1), new Instant(19)), set(1, 2, 5, 9));
    expected.put(new IntervalWindow(new Instant(100), new Instant(111)), set(100, 101));
    assertEquals(
        expected,
        runWindowFn(
            Sessions.withGapDuration(Duration.millis(10)),
            Arrays.asList(1L, 2L, 5L, 9L, 100L, 101L)));
  }

  @Test
  public void testMerging() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(1), new Instant(40)), set(1, 10, 15, 22, 30));
    expected.put(new IntervalWindow(new Instant(95), new Instant(111)), set(95, 100, 101));
    assertEquals(
        expected,
        runWindowFn(
            Sessions.withGapDuration(Duration.millis(10)),
            Arrays.asList(1L, 15L, 30L, 100L, 101L, 95L, 22L, 10L)));
  }

  @Test
  public void testTimeUnit() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(1), new Instant(2000)), set(1, 2, 1000));
    expected.put(new IntervalWindow(new Instant(5000), new Instant(6001)), set(5000, 5001));
    expected.put(new IntervalWindow(new Instant(10000), new Instant(11000)), set(10000));
    assertEquals(
        expected,
        runWindowFn(
            Sessions.withGapDuration(Duration.standardSeconds(1)),
            Arrays.asList(1L, 2L, 1000L, 5000L, 5001L, 10000L)));
  }

  @Test
  public void testEquality() {
    assertTrue(
        Sessions.withGapDuration(Duration.millis(10))
            .isCompatible(Sessions.withGapDuration(Duration.millis(10))));
    assertTrue(
        Sessions.withGapDuration(Duration.millis(10))
            .isCompatible(Sessions.withGapDuration(Duration.millis(20))));
  }

  @Test
  public void testVerifyCompatibility() throws IncompatibleWindowException {
    Sessions.withGapDuration(Duration.millis(10))
        .verifyCompatibility(Sessions.withGapDuration(Duration.millis(10)));

    thrown.expect(IncompatibleWindowException.class);
    Sessions.withGapDuration(Duration.millis(10))
        .verifyCompatibility(FixedWindows.of(Duration.millis(10)));
  }

  /**
   * Test to confirm that {@link Sessions} with the default {@link TimestampCombiner} holds up the
   * watermark potentially indefinitely.
   */
  @Test
  public void testInvalidOutputAtEarliest() throws Exception {
    try {
      WindowFnTestUtils.validateGetOutputTimestamps(
          Sessions.withGapDuration(Duration.millis(10)),
          TimestampCombiner.EARLIEST,
          ImmutableList.of(
              (List<Long>) ImmutableList.of(1L, 3L),
              (List<Long>) ImmutableList.of(0L, 5L, 10L, 15L, 20L)));
    } catch (AssertionError exc) {
      assertThat(
          exc.getMessage(),
          // These are the non-volatile pieces of the error message that a timestamp
          // was not greater than what it should be.
          allOf(containsString("a value greater than"), containsString("was less than")));
    }
  }

  /**
   * When a user explicitly requests per-key aggregate values have their derived timestamp to be the
   * end of the window (instead of the earliest possible), the session here should not hold each
   * other up, even though they overlap.
   */
  @Test
  public void testValidOutputAtEndTimes() throws Exception {
    WindowFnTestUtils.validateGetOutputTimestamps(
        Sessions.withGapDuration(Duration.millis(10)),
        TimestampCombiner.END_OF_WINDOW,
        ImmutableList.of(
            (List<Long>) ImmutableList.of(1L, 3L),
            (List<Long>) ImmutableList.of(0L, 5L, 10L, 15L, 20L)));
  }

  @Test
  public void testDisplayData() {
    Duration gapDuration = Duration.standardMinutes(234);
    Sessions session = Sessions.withGapDuration(gapDuration);
    assertThat(DisplayData.from(session), hasDisplayItem("gapDuration", gapDuration));
  }
}
