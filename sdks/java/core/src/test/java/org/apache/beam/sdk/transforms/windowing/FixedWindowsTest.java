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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.WindowFnTestUtils;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for FixedWindows WindowFn.
 */
@RunWith(JUnit4.class)
public class FixedWindowsTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSimpleFixedWindow() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(0), new Instant(10)), set(1, 2, 5, 9));
    expected.put(new IntervalWindow(new Instant(10), new Instant(20)), set(10, 11));
    expected.put(new IntervalWindow(new Instant(100), new Instant(110)), set(100));
    assertEquals(
        expected,
        runWindowFn(
            FixedWindows.of(new Duration(10)),
            Arrays.asList(1L, 2L, 5L, 9L, 10L, 11L, 100L)));
  }

  @Test
  public void testFixedOffsetWindow() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(-5), new Instant(5)), set(1, 2));
    expected.put(new IntervalWindow(new Instant(5), new Instant(15)), set(5, 9, 10, 11));
    expected.put(new IntervalWindow(new Instant(95), new Instant(105)), set(100));
    assertEquals(
        expected,
        runWindowFn(
            FixedWindows.of(new Duration(10)).withOffset(new Duration(5)),
            Arrays.asList(1L, 2L, 5L, 9L, 10L, 11L, 100L)));
  }

  @Test
  public void testTimeUnit() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(-5000), new Instant(5000)), set(1, 2, 1000));
    expected.put(new IntervalWindow(new Instant(5000), new Instant(15000)), set(5000, 5001, 10000));
    assertEquals(
        expected,
        runWindowFn(
            FixedWindows.of(Duration.standardSeconds(10)).withOffset(Duration.standardSeconds(5)),
            Arrays.asList(1L, 2L, 1000L, 5000L, 5001L, 10000L)));
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
        windowFn.assignWindow(endOfGlobalWindow.minus(1));

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
      assertThat(e.toString(),
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
    assertTrue(FixedWindows.of(new Duration(10)).isCompatible(FixedWindows.of(new Duration(10))));
    assertTrue(
        FixedWindows.of(new Duration(10)).isCompatible(
            FixedWindows.of(new Duration(10))));
    assertTrue(
        FixedWindows.of(new Duration(10)).isCompatible(
            FixedWindows.of(new Duration(10))));

    assertFalse(FixedWindows.of(new Duration(10)).isCompatible(FixedWindows.of(new Duration(20))));
    assertFalse(FixedWindows.of(new Duration(10)).isCompatible(
        FixedWindows.of(new Duration(20))));
  }

  @Test
  public void testVerifyCompatibility() throws IncompatibleWindowException {
    FixedWindows.of(new Duration(10)).verifyCompatibility(FixedWindows.of(new Duration(10)));
    thrown.expect(IncompatibleWindowException.class);
    FixedWindows.of(new Duration(10)).verifyCompatibility(FixedWindows.of(new Duration(20)));
  }

  @Test
  public void testValidOutputTimes() throws Exception {
    for (long timestamp : Arrays.asList(200, 800, 700)) {
      WindowFnTestUtils.validateGetOutputTimestamp(
          FixedWindows.of(new Duration(500)), timestamp);
    }
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
