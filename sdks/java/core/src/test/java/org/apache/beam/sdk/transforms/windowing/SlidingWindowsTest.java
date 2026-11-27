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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.transforms.display.DisplayData;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the SlidingWindows WindowFn. */
@RunWith(JUnit4.class)
public class SlidingWindowsTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static IntervalWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }

  /** Asserts that {@code windowFn} assigns {@code timestamp} to exactly {@code windows}. */
  private static void assertWindows(
      WindowFn<Object, IntervalWindow> windowFn, long timestamp, IntervalWindow... windows)
      throws Exception {
    assertThat(
        "windows for timestamp " + timestamp,
        assignedWindows(windowFn, timestamp),
        containsInAnyOrder(windows));
  }

  @Test
  public void testSimple() throws Exception {
    SlidingWindows windowFn = SlidingWindows.of(Duration.millis(10)).every(Duration.millis(5));
    assertWindows(windowFn, 1, window(-5, 5), window(0, 10));
    assertWindows(windowFn, 2, window(-5, 5), window(0, 10));
    assertWindows(windowFn, 5, window(0, 10), window(5, 15));
    assertWindows(windowFn, 9, window(0, 10), window(5, 15));
    assertWindows(windowFn, 10, window(5, 15), window(10, 20));
    assertWindows(windowFn, 11, window(5, 15), window(10, 20));
    assertThat(windowFn.assignsToOneWindow(), is(false));
  }

  @Test
  public void testSlightlyOverlapping() throws Exception {
    SlidingWindows windowFn = SlidingWindows.of(Duration.millis(7)).every(Duration.millis(5));
    assertWindows(windowFn, 1, window(-5, 2), window(0, 7));
    assertWindows(windowFn, 2, window(0, 7));
    assertWindows(windowFn, 5, window(0, 7), window(5, 12));
    assertWindows(windowFn, 9, window(5, 12));
    assertWindows(windowFn, 10, window(5, 12), window(10, 17));
    assertWindows(windowFn, 11, window(5, 12), window(10, 17));
    assertThat(windowFn.assignsToOneWindow(), is(false));
  }

  @Test
  public void testEqualSize() throws Exception {
    SlidingWindows windowFn = SlidingWindows.of(Duration.millis(3)).every(Duration.millis(3));
    assertWindows(windowFn, 1, window(0, 3));
    assertWindows(windowFn, 2, window(0, 3));
    assertWindows(windowFn, 3, window(3, 6));
    assertWindows(windowFn, 4, window(3, 6));
    assertWindows(windowFn, 5, window(3, 6));
    assertWindows(windowFn, 6, window(6, 9));
    assertWindows(windowFn, 7, window(6, 9));
    assertThat(windowFn.assignsToOneWindow(), is(true));
  }

  @Test
  public void testElidings() throws Exception {
    // Only look at the first 3 millisecs of every 10-millisec interval.
    SlidingWindows windowFn = SlidingWindows.of(Duration.millis(3)).every(Duration.millis(10));
    assertWindows(windowFn, 1, window(0, 3));
    assertWindows(windowFn, 2, window(0, 3));
    assertWindows(windowFn, 3);
    assertWindows(windowFn, 5);
    assertWindows(windowFn, 9);
    assertWindows(windowFn, 10, window(10, 13));
    assertWindows(windowFn, 11, window(10, 13));
    assertWindows(windowFn, 100, window(100, 103));
    assertThat(windowFn.assignsToOneWindow(), is(true));
  }

  @Test
  public void testOffset() throws Exception {
    SlidingWindows windowFn =
        SlidingWindows.of(Duration.millis(10))
            .every(Duration.millis(5))
            .withOffset(Duration.millis(2));
    assertWindows(windowFn, 1, window(-8, 2), window(-3, 7));
    assertWindows(windowFn, 2, window(-3, 7), window(2, 12));
    assertWindows(windowFn, 5, window(-3, 7), window(2, 12));
    assertWindows(windowFn, 9, window(2, 12), window(7, 17));
    assertWindows(windowFn, 10, window(2, 12), window(7, 17));
    assertWindows(windowFn, 11, window(2, 12), window(7, 17));
  }

  @Test
  public void testTimeUnit() throws Exception {
    SlidingWindows windowFn =
        SlidingWindows.of(Duration.standardSeconds(10)).every(Duration.standardSeconds(5));
    assertWindows(windowFn, 1, window(-5000, 5000), window(0, 10000));
    assertWindows(windowFn, 2, window(-5000, 5000), window(0, 10000));
    assertWindows(windowFn, 1000, window(-5000, 5000), window(0, 10000));
    assertWindows(windowFn, 5000, window(0, 10000), window(5000, 15000));
    assertWindows(windowFn, 5001, window(0, 10000), window(5000, 15000));
    assertWindows(windowFn, 10000, window(5000, 15000), window(10000, 20000));
  }

  @Test
  public void testDefaultPeriods() throws Exception {
    assertEquals(
        Duration.standardHours(1), SlidingWindows.getDefaultPeriod(Duration.standardDays(1)));
    assertEquals(
        Duration.standardHours(1), SlidingWindows.getDefaultPeriod(Duration.standardHours(2)));
    assertEquals(
        Duration.standardMinutes(1), SlidingWindows.getDefaultPeriod(Duration.standardHours(1)));
    assertEquals(
        Duration.standardMinutes(1), SlidingWindows.getDefaultPeriod(Duration.standardMinutes(10)));
    assertEquals(
        Duration.standardSeconds(1), SlidingWindows.getDefaultPeriod(Duration.standardMinutes(1)));
    assertEquals(
        Duration.standardSeconds(1), SlidingWindows.getDefaultPeriod(Duration.standardSeconds(10)));
    assertEquals(Duration.millis(1), SlidingWindows.getDefaultPeriod(Duration.standardSeconds(1)));
    assertEquals(Duration.millis(1), SlidingWindows.getDefaultPeriod(Duration.millis(10)));
    assertEquals(Duration.millis(1), SlidingWindows.getDefaultPeriod(Duration.millis(1)));
  }

  @Test
  public void testEquality() {
    assertTrue(
        SlidingWindows.of(Duration.millis(10))
            .isCompatible(SlidingWindows.of(Duration.millis(10))));
    assertTrue(
        SlidingWindows.of(Duration.millis(10))
            .isCompatible(SlidingWindows.of(Duration.millis(10))));

    assertFalse(
        SlidingWindows.of(Duration.millis(10))
            .isCompatible(SlidingWindows.of(Duration.millis(20))));
    assertFalse(
        SlidingWindows.of(Duration.millis(10))
            .isCompatible(SlidingWindows.of(Duration.millis(20))));
  }

  @Test
  public void testVerifyCompatibility() throws IncompatibleWindowException {
    SlidingWindows.of(Duration.millis(10))
        .verifyCompatibility(SlidingWindows.of(Duration.millis(10)));
    thrown.expect(IncompatibleWindowException.class);
    SlidingWindows.of(Duration.millis(10))
        .verifyCompatibility(SlidingWindows.of(Duration.millis(20)));
  }

  @Test
  public void testDefaultWindowMappingFn() {
    // [40, 1040), [340, 1340), [640, 1640) ...
    SlidingWindows slidingWindows =
        SlidingWindows.of(Duration.millis(1000))
            .every(Duration.millis(300))
            .withOffset(Duration.millis(40));
    WindowMappingFn<?> mapping = slidingWindows.getDefaultWindowMappingFn();

    assertThat(mapping.maximumLookback(), equalTo(Duration.ZERO));

    // Prior
    assertEquals(
        new IntervalWindow(new Instant(340), new Instant(1340)),
        mapping.getSideInputWindow(new IntervalWindow(new Instant(0), new Instant(1041))));
    assertEquals(
        new IntervalWindow(new Instant(340), new Instant(1340)),
        mapping.getSideInputWindow(new IntervalWindow(new Instant(0), new Instant(1339))));
    // Align
    assertEquals(
        new IntervalWindow(new Instant(340), new Instant(1340)),
        mapping.getSideInputWindow(new IntervalWindow(new Instant(0), new Instant(1340))));
    // After
    assertEquals(
        new IntervalWindow(new Instant(640), new Instant(1640)),
        mapping.getSideInputWindow(new IntervalWindow(new Instant(0), new Instant(1341))));
  }

  @Test
  public void testDisplayData() {
    Duration windowSize = Duration.standardSeconds(1234);
    Duration offset = Duration.standardSeconds(2345);
    Duration period = Duration.standardSeconds(3456);

    SlidingWindows slidingWindowFn = SlidingWindows.of(windowSize).every(period).withOffset(offset);

    DisplayData displayData = DisplayData.from(slidingWindowFn);
    assertThat(displayData, hasDisplayItem("size", windowSize));
    assertThat(displayData, hasDisplayItem("period", period));
    assertThat(displayData, hasDisplayItem("offset", offset));
  }
}
