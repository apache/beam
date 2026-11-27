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

import static org.apache.beam.sdk.testing.WindowFnTestUtils.assignedWindows;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.WindowFnTestUtils;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
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

  private static IntervalWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }

  /**
   * Assigns each timestamp to its session in order, merging after each one as a runner would, and
   * returns the resulting sessions with the timestamps each holds.
   */
  private static Map<IntervalWindow, Set<Long>> mergedSessions(
      Sessions sessions, long... timestamps) throws Exception {
    Map<IntervalWindow, Set<Long>> windows = new HashMap<>();
    for (long timestamp : timestamps) {
      for (IntervalWindow window : assignedWindows(sessions, timestamp)) {
        windows.computeIfAbsent(window, w -> new HashSet<>()).add(timestamp);
      }
      sessions.mergeWindows(new TestMergeContext(sessions, windows));
    }
    return windows;
  }

  /** Merges the windows Sessions picks, carrying each window's timestamps into the result. */
  private static class TestMergeContext extends WindowFn<Object, IntervalWindow>.MergeContext {
    private final Map<IntervalWindow, Set<Long>> windows;

    TestMergeContext(Sessions sessions, Map<IntervalWindow, Set<Long>> windows) {
      sessions.super();
      this.windows = windows;
    }

    @Override
    public Collection<IntervalWindow> windows() {
      return new ArrayList<>(windows.keySet());
    }

    @Override
    public void merge(Collection<IntervalWindow> toBeMerged, IntervalWindow mergeResult) {
      Set<Long> merged = new HashSet<>();
      for (IntervalWindow window : toBeMerged) {
        merged.addAll(windows.remove(window));
      }
      windows.computeIfAbsent(mergeResult, w -> new HashSet<>()).addAll(merged);
    }
  }

  @Test
  public void testSimple() throws Exception {
    assertEquals(
        ImmutableMap.of(
            window(0, 10), ImmutableSet.of(0L),
            window(10, 20), ImmutableSet.of(10L),
            window(101, 111), ImmutableSet.of(101L)),
        mergedSessions(Sessions.withGapDuration(Duration.millis(10)), 0, 10, 101));
  }

  @Test
  public void testConsecutive() throws Exception {
    assertEquals(
        ImmutableMap.of(
            window(1, 19), ImmutableSet.of(1L, 2L, 5L, 9L),
            window(100, 111), ImmutableSet.of(100L, 101L)),
        mergedSessions(Sessions.withGapDuration(Duration.millis(10)), 1, 2, 5, 9, 100, 101));
  }

  @Test
  public void testMerging() throws Exception {
    assertEquals(
        ImmutableMap.of(
            window(1, 40), ImmutableSet.of(1L, 10L, 15L, 22L, 30L),
            window(95, 111), ImmutableSet.of(95L, 100L, 101L)),
        mergedSessions(
            Sessions.withGapDuration(Duration.millis(10)), 1, 15, 30, 100, 101, 95, 22, 10));
  }

  @Test
  public void testTimeUnit() throws Exception {
    assertEquals(
        ImmutableMap.of(
            window(1, 2000), ImmutableSet.of(1L, 2L, 1000L),
            window(5000, 6001), ImmutableSet.of(5000L, 5001L),
            window(10000, 11000), ImmutableSet.of(10000L)),
        mergedSessions(
            Sessions.withGapDuration(Duration.standardSeconds(1)), 1, 2, 1000, 5000, 5001, 10000));
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
