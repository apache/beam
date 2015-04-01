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
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
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
 * Tests the {@link DelayAfterFirstInPane}.
 */
@RunWith(JUnit4.class)
public class DelayAfterFirstInPaneTest {
  @Test
  public void testDefaultTriggerWithFixedWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.of(
        FixedWindows.of(windowDuration),
        new DelayAfterFirstInPane<IntervalWindow>(new SerializableFunction<Instant, Instant>() {
          private static final long serialVersionUID = 1L;
          @Override
          public Instant apply(Instant input) {
            return input.plus(Duration.millis(5));
          }
        }),
        BufferingWindowSet.<String, Integer, IntervalWindow>factory(VarIntCoder.of()));

    tester.advanceProcessingTime(new Instant(10));
    tester.injectElement(1, new Instant(1)); // first in window [0, 10), timer set for 15
    tester.advanceProcessingTime(new Instant(11));
    tester.injectElement(2, new Instant(9));

    tester.advanceProcessingTime(new Instant(12));
    tester.assertNoMoreOutput();

    tester.injectElement(3, new Instant(8));
    tester.injectElement(4, new Instant(19)); // timer set for 17
    tester.advanceProcessingTime(new Instant(13));
    tester.injectElement(5, new Instant(30)); // timer set for 18

    tester.advanceProcessingTime(new Instant(16));

    tester.assertNextOutput(
        Matchers.containsInAnyOrder(1, 2, 3),
        new IntervalWindow(new Instant(0), windowDuration));
    tester.assertNoMoreOutput();

    tester.advanceProcessingTime(new Instant(19));
    tester.assertNextOutput(Matchers.containsInAnyOrder(4),
        new IntervalWindow(new Instant(10), windowDuration));
    tester.assertNextOutput(Matchers.containsInAnyOrder(5),
        new IntervalWindow(new Instant(30), windowDuration));
    tester.assertNoMoreOutput();
  }

  @Test
  public void testDefaultTriggerWithMergingWindowAlreadyFired() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.of(
        Sessions.withGapDuration(windowDuration),
        new DelayAfterFirstInPane<IntervalWindow>(new SerializableFunction<Instant, Instant>() {
          private static final long serialVersionUID = 1L;
          @Override
          public Instant apply(Instant input) {
            return input.plus(Duration.millis(5));
          }
        }),
        BufferingWindowSet.<String, Integer, IntervalWindow>factory(VarIntCoder.of()));

    tester.advanceProcessingTime(new Instant(10));
    tester.injectElement(1, new Instant(1)); // in [1, 11), timer for 15
    tester.advanceProcessingTime(new Instant(16));
    tester.assertNextOutput(
        Matchers.containsInAnyOrder(1), new IntervalWindow(new Instant(1), new Instant(11)));
    tester.assertNoMoreOutput();

    // Because we discarded the previous window, we don't have it around to merge with.
    tester.injectElement(2, new Instant(2)); // in [2, 12), timer for 21
    tester.advanceProcessingTime(new Instant(100));

    tester.assertNextOutput(
        Matchers.containsInAnyOrder(2), new IntervalWindow(new Instant(2), new Instant(12)));
    tester.assertNoMoreOutput();
  }
}
