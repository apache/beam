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
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
 * Tests for FixedWindows WindowFn.
 */
@RunWith(JUnit4.class)
public class FixedWindowsTest {

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
  public void testValidOutputTimes() throws Exception {
    for (long timestamp : Arrays.asList(200, 800, 700)) {
      WindowFnTestUtils.validateGetOutputTimestamp(
          FixedWindows.of(new Duration(500)), timestamp);
    }
  }
}
