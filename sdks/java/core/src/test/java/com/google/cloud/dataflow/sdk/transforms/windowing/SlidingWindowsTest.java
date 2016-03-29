/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms.windowing;

import static com.google.cloud.dataflow.sdk.testing.WindowFnTestUtils.runWindowFn;
import static com.google.cloud.dataflow.sdk.testing.WindowFnTestUtils.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.testing.WindowFnTestUtils;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Tests for the SlidingWindows WindowFn.
 */
@RunWith(JUnit4.class)
public class SlidingWindowsTest {

  @Test
  public void testSimple() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(-5), new Instant(5)), set(1, 2));
    expected.put(new IntervalWindow(new Instant(0), new Instant(10)), set(1, 2, 5, 9));
    expected.put(new IntervalWindow(new Instant(5), new Instant(15)), set(5, 9, 10, 11));
    expected.put(new IntervalWindow(new Instant(10), new Instant(20)), set(10, 11));
    assertEquals(
        expected,
        runWindowFn(
            SlidingWindows.of(new Duration(10)).every(new Duration(5)),
            Arrays.asList(1L, 2L, 5L, 9L, 10L, 11L)));
  }

  @Test
  public void testSlightlyOverlapping() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(-5), new Instant(2)), set(1));
    expected.put(new IntervalWindow(new Instant(0), new Instant(7)), set(1, 2, 5));
    expected.put(new IntervalWindow(new Instant(5), new Instant(12)), set(5, 9, 10, 11));
    expected.put(new IntervalWindow(new Instant(10), new Instant(17)), set(10, 11));
    assertEquals(
        expected,
        runWindowFn(
            SlidingWindows.of(new Duration(7)).every(new Duration(5)),
            Arrays.asList(1L, 2L, 5L, 9L, 10L, 11L)));
  }

  @Test
  public void testElidings() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(0), new Instant(3)), set(1, 2));
    expected.put(new IntervalWindow(new Instant(10), new Instant(13)), set(10, 11));
    expected.put(new IntervalWindow(new Instant(100), new Instant(103)), set(100));
    assertEquals(
        expected,
        runWindowFn(
            // Only look at the first 3 millisecs of every 10-millisec interval.
            SlidingWindows.of(new Duration(3)).every(new Duration(10)),
            Arrays.asList(1L, 2L, 3L, 5L, 9L, 10L, 11L, 100L)));
  }

  @Test
  public void testOffset() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(-8), new Instant(2)), set(1));
    expected.put(new IntervalWindow(new Instant(-3), new Instant(7)), set(1, 2, 5));
    expected.put(new IntervalWindow(new Instant(2), new Instant(12)), set(2, 5, 9, 10, 11));
    expected.put(new IntervalWindow(new Instant(7), new Instant(17)), set(9, 10, 11));
    assertEquals(
        expected,
        runWindowFn(
            SlidingWindows.of(new Duration(10)).every(new Duration(5)).withOffset(new Duration(2)),
            Arrays.asList(1L, 2L, 5L, 9L, 10L, 11L)));
  }

  @Test
  public void testTimeUnit() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(-5000), new Instant(5000)), set(1, 2, 1000));
    expected.put(new IntervalWindow(new Instant(0), new Instant(10000)),
        set(1, 2, 1000, 5000, 5001));
    expected.put(new IntervalWindow(new Instant(5000), new Instant(15000)), set(5000, 5001, 10000));
    expected.put(new IntervalWindow(new Instant(10000), new Instant(20000)), set(10000));
    assertEquals(
        expected,
        runWindowFn(
            SlidingWindows.of(Duration.standardSeconds(10)).every(Duration.standardSeconds(5)),
            Arrays.asList(1L, 2L, 1000L, 5000L, 5001L, 10000L)));
  }

  @Test
  public void testDefaultPeriods() throws Exception {
    assertEquals(Duration.standardHours(1),
        SlidingWindows.getDefaultPeriod(Duration.standardDays(1)));
    assertEquals(Duration.standardHours(1),
        SlidingWindows.getDefaultPeriod(Duration.standardHours(2)));
    assertEquals(Duration.standardMinutes(1),
        SlidingWindows.getDefaultPeriod(Duration.standardHours(1)));
    assertEquals(Duration.standardMinutes(1),
        SlidingWindows.getDefaultPeriod(Duration.standardMinutes(10)));
    assertEquals(Duration.standardSeconds(1),
        SlidingWindows.getDefaultPeriod(Duration.standardMinutes(1)));
    assertEquals(Duration.standardSeconds(1),
        SlidingWindows.getDefaultPeriod(Duration.standardSeconds(10)));
    assertEquals(Duration.millis(1),
        SlidingWindows.getDefaultPeriod(Duration.standardSeconds(1)));
    assertEquals(Duration.millis(1),
        SlidingWindows.getDefaultPeriod(Duration.millis(10)));
    assertEquals(Duration.millis(1),
        SlidingWindows.getDefaultPeriod(Duration.millis(1)));
  }

  @Test
  public void testEquality() {
    assertTrue(
        SlidingWindows.of(new Duration(10)).isCompatible(
            SlidingWindows.of(new Duration(10))));
    assertTrue(
        SlidingWindows.of(new Duration(10)).isCompatible(
            SlidingWindows.of(new Duration(10))));

    assertFalse(SlidingWindows.of(new Duration(10)).isCompatible(
        SlidingWindows.of(new Duration(20))));
    assertFalse(SlidingWindows.of(new Duration(10)).isCompatible(
        SlidingWindows.of(new Duration(20))));
  }

  @Test
  public void testGetSideInputWindow() {
    // [40, 1040), [340, 1340), [640, 1640) ...
    SlidingWindows slidingWindows = SlidingWindows.of(new Duration(1000))
        .every(new Duration(300)).withOffset(new Duration(40));
    // Prior
    assertEquals(
        new IntervalWindow(new Instant(340), new Instant(1340)),
        slidingWindows.getSideInputWindow(
            new IntervalWindow(new Instant(0), new Instant(1041))));
    assertEquals(
        new IntervalWindow(new Instant(340), new Instant(1340)),
        slidingWindows.getSideInputWindow(
            new IntervalWindow(new Instant(0), new Instant(1339))));
    // Align
    assertEquals(
        new IntervalWindow(new Instant(340), new Instant(1340)),
        slidingWindows.getSideInputWindow(
            new IntervalWindow(new Instant(0), new Instant(1340))));
    // After
    assertEquals(
        new IntervalWindow(new Instant(640), new Instant(1640)),
        slidingWindows.getSideInputWindow(
            new IntervalWindow(new Instant(0), new Instant(1341))));
  }

  @Test
  public void testValidOutputTimes() throws Exception {
    for (long timestamp : Arrays.asList(200, 800, 499, 500, 501, 700, 1000)) {
      WindowFnTestUtils.validateGetOutputTimestamp(
          SlidingWindows.of(new Duration(1000)).every(new Duration(500)), timestamp);
    }
  }

  @Test
  public void testOutputTimesNonInterference() throws Exception {
    for (long timestamp : Arrays.asList(200, 800, 700)) {
      WindowFnTestUtils.validateNonInterferingOutputTimes(
          SlidingWindows.of(new Duration(1000)).every(new Duration(500)), timestamp);
    }
  }
}
