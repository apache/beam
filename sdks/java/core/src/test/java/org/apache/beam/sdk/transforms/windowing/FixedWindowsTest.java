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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.beam.sdk.transforms.display.DisplayData;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for FixedWindows WindowFn. */
@RunWith(JUnit4.class)
public class FixedWindowsTest {
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
  public void testSimpleFixedWindow() throws Exception {
    FixedWindows windowFn = FixedWindows.of(Duration.millis(10));
    assertWindows(windowFn, 1, window(0, 10));
    assertWindows(windowFn, 2, window(0, 10));
    assertWindows(windowFn, 5, window(0, 10));
    assertWindows(windowFn, 9, window(0, 10));
    assertWindows(windowFn, 10, window(10, 20));
    assertWindows(windowFn, 11, window(10, 20));
    assertWindows(windowFn, 100, window(100, 110));
  }

  @Test
  public void testFixedOffsetWindow() throws Exception {
    FixedWindows windowFn = FixedWindows.of(Duration.millis(10)).withOffset(Duration.millis(5));
    assertWindows(windowFn, 1, window(-5, 5));
    assertWindows(windowFn, 2, window(-5, 5));
    assertWindows(windowFn, 5, window(5, 15));
    assertWindows(windowFn, 9, window(5, 15));
    assertWindows(windowFn, 10, window(5, 15));
    assertWindows(windowFn, 11, window(5, 15));
    assertWindows(windowFn, 100, window(95, 105));
  }

  @Test
  public void testTimeUnit() throws Exception {
    FixedWindows windowFn =
        FixedWindows.of(Duration.standardSeconds(10)).withOffset(Duration.standardSeconds(5));
    assertWindows(windowFn, 1, window(-5000, 5000));
    assertWindows(windowFn, 2, window(-5000, 5000));
    assertWindows(windowFn, 1000, window(-5000, 5000));
    assertWindows(windowFn, 5000, window(5000, 15000));
    assertWindows(windowFn, 5001, window(5000, 15000));
    assertWindows(windowFn, 10000, window(5000, 15000));
  }

  @Test
  public void testDefaultWindowMappingFn() {
    PartitioningWindowFn<?, ?> windowFn = FixedWindows.of(Duration.standardMinutes(20L));
    WindowMappingFn<?> mapping = windowFn.getDefaultWindowMappingFn();

    assertThat(
        mapping.getSideInputWindow(
            new BoundedWindow() {
              @Override
              public Instant maxTimestamp() {
                return new Instant(100L);
              }
            }),
        equalTo(
            new IntervalWindow(
                new Instant(0L), new Instant(0L).plus(Duration.standardMinutes(20L)))));
    assertThat(mapping.maximumLookback(), equalTo(Duration.ZERO));
  }

  /** Tests that the last hour of the universe in fact ends at the end of time. */
  @Test
  public void testEndOfTime() {
    Instant endOfGlobalWindow = GlobalWindow.INSTANCE.maxTimestamp();
    FixedWindows windowFn = FixedWindows.of(Duration.standardHours(1));

    IntervalWindow truncatedWindow =
        windowFn.assignWindow(endOfGlobalWindow.minus(Duration.millis(1)));

    assertThat(truncatedWindow.maxTimestamp(), equalTo(endOfGlobalWindow));
  }

  @Test
  public void testDefaultWindowMappingFnGlobalWindow() {
    PartitioningWindowFn<?, ?> windowFn = FixedWindows.of(Duration.standardMinutes(20L));
    WindowMappingFn<?> mapping = windowFn.getDefaultWindowMappingFn();

    thrown.expect(IllegalArgumentException.class);
    mapping.getSideInputWindow(GlobalWindow.INSTANCE);
  }

  void checkConstructionFailure(int size, int offset) {
    try {
      FixedWindows.of(Duration.standardSeconds(size)).withOffset(Duration.standardSeconds(offset));
      fail("should have failed");
    } catch (IllegalArgumentException e) {
      assertThat(
          e.toString(),
          containsString("FixedWindows WindowingStrategies must have 0 <= offset < size"));
    }
  }

  @Test
  public void testInvalidInput() throws Exception {
    checkConstructionFailure(-1, 0);
    checkConstructionFailure(1, 2);
    checkConstructionFailure(1, -1);
  }

  @Test
  public void testEquality() {
    assertTrue(
        FixedWindows.of(Duration.millis(10)).isCompatible(FixedWindows.of(Duration.millis(10))));
    assertTrue(
        FixedWindows.of(Duration.millis(10)).isCompatible(FixedWindows.of(Duration.millis(10))));
    assertTrue(
        FixedWindows.of(Duration.millis(10)).isCompatible(FixedWindows.of(Duration.millis(10))));

    assertFalse(
        FixedWindows.of(Duration.millis(10)).isCompatible(FixedWindows.of(Duration.millis(20))));
    assertFalse(
        FixedWindows.of(Duration.millis(10)).isCompatible(FixedWindows.of(Duration.millis(20))));
  }

  @Test
  public void testVerifyCompatibility() throws IncompatibleWindowException {
    FixedWindows.of(Duration.millis(10)).verifyCompatibility(FixedWindows.of(Duration.millis(10)));
    thrown.expect(IncompatibleWindowException.class);
    FixedWindows.of(Duration.millis(10)).verifyCompatibility(FixedWindows.of(Duration.millis(20)));
  }

  @Test
  public void testDisplayData() {
    Duration offset = Duration.standardSeconds(1234);
    Duration size = Duration.standardSeconds(2345);

    FixedWindows fixedWindows = FixedWindows.of(size).withOffset(offset);
    DisplayData displayData = DisplayData.from(fixedWindows);

    assertThat(displayData, hasDisplayItem("size", size));
    assertThat(displayData, hasDisplayItem("offset", offset));
  }
}
