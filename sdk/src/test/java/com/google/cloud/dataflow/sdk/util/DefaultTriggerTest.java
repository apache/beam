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

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the {@link DefaultTrigger} in a variety of windowing modes.
 */
@RunWith(JUnit4.class)
public class DefaultTriggerTest {

  @Test
  public void testDefaultTriggerWithFixedWindow() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.of(
        FixedWindows.of(Duration.millis(10)),
        new DefaultTrigger<IntervalWindow>(),
        BufferingWindowSet.<String, Integer, IntervalWindow>factory(VarIntCoder.of()));

    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(9));
    tester.injectElement(3, new Instant(15));
    tester.injectElement(4, new Instant(19));
    tester.injectElement(5, new Instant(30));

    // We're processing the data after it arrived.
    tester.advanceProcessingTime(new Instant(500));
    tester.assertNoMoreOutput();

    // Advance the watermark almost to the end of the first window.
    tester.advanceWatermark(new Instant(9));
    tester.assertNoMoreOutput();

    // Advance the watermark past the first window
    tester.advanceWatermark(new Instant(12));
    tester.assertNextOutput(new Instant(9), Matchers.containsInAnyOrder(1, 2));
    tester.assertNoMoreOutput();

    // Advance the watermark to the end
    tester.advanceWatermark(new Instant(100));
    tester.assertNextOutput(new Instant(19), Matchers.containsInAnyOrder(3, 4));
    tester.assertNextOutput(new Instant(39), Matchers.contains(5));
  }

  @Test
  public void testDefaultTriggerWithSessionWindow() throws Exception {
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.of(
        Sessions.withGapDuration(Duration.millis(10)),
        new DefaultTrigger<IntervalWindow>(),
        BufferingWindowSet.<String, Integer, IntervalWindow>factory(VarIntCoder.of()));

    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(9));

    tester.advanceWatermark(new Instant(10));
    tester.assertNoMoreOutput(); // no output, because we merge 1 into the [9-19) session

    tester.injectElement(3, new Instant(15));
    tester.injectElement(4, new Instant(30));

    // Advance the watermark to the end
    tester.advanceWatermark(new Instant(100));
    tester.assertNextOutput(new Instant(24), Matchers.containsInAnyOrder(1, 2, 3));
    tester.assertNextOutput(new Instant(39), Matchers.contains(4));
  }
}
