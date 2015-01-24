/*
 * Copyright (C) 2014 Google Inc.
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
import static org.junit.Assert.assertTrue;

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
 * Tests for Sessions WindowFn.
 */
@RunWith(JUnit4.class)
public class SessionsTest {

  @Test
  public void testSimple() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(0), new Instant(10)), set(0));
    expected.put(new IntervalWindow(new Instant(10), new Instant(20)), set(10));
    expected.put(new IntervalWindow(new Instant(101), new Instant(111)), set(101));
    assertEquals(
        expected,
        runWindowFn(
            Sessions.withGapDuration(new Duration(10)),
            Arrays.asList(0L, 10L, 101L)));
  }

  @Test
  public void testConsecutive() throws Exception {
    Map<IntervalWindow, Set<String>> expected = new HashMap<>();
    expected.put(new IntervalWindow(new Instant(1), new Instant(19)), set(1, 2, 5, 9));
    expected.put(new IntervalWindow(new Instant(100), new Instant(111)), set(100, 101));
    assertEquals(
        expected,
        runWindowFn(
            Sessions.withGapDuration(new Duration(10)),
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
            Sessions.withGapDuration(new Duration(10)),
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
        Sessions.withGapDuration(new Duration(10)).isCompatible(
            Sessions.withGapDuration(new Duration(10))));
    assertTrue(
        Sessions.withGapDuration(new Duration(10)).isCompatible(
            Sessions.withGapDuration(new Duration(20))));
  }
}
