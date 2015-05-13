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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.WindowMatchers;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the {@link AfterProcessingTime}.
 */
@RunWith(JUnit4.class)
public class AfterProcessingTimeTest {
  @Test
  public void testAfterProcessingTimeWithFixedWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(windowDuration),
        AfterProcessingTime
            .<IntervalWindow>pastFirstElementInPane()
            .plusDelayOf(Duration.millis(5)),
        AccumulationMode.DISCARDING_FIRED_PANES);

    tester.advanceProcessingTime(new Instant(10));

    tester.injectElement(1, new Instant(1)); // first in window [0, 10), timer set for 15
    tester.advanceProcessingTime(new Instant(11));
    tester.injectElement(2, new Instant(9));

    tester.advanceProcessingTime(new Instant(12));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    tester.injectElement(3, new Instant(8));
    tester.injectElement(4, new Instant(19));
    tester.injectElement(5, new Instant(30));

    tester.advanceProcessingTime(new Instant(16));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 1, 0, 10)));

    // This element belongs in the window that has already fired. It should not be re-output because
    // that trigger (which was one-time) has already gone off.
    tester.injectElement(6, new Instant(2));

    tester.advanceProcessingTime(new Instant(19));
    assertThat(tester.extractOutput(), Matchers.containsInAnyOrder(
        WindowMatchers.isSingleWindowedValue(Matchers.contains(4), 19, 10, 20),
        WindowMatchers.isSingleWindowedValue(Matchers.contains(5), 30, 30, 40)));
    assertTrue(tester.isDone(new IntervalWindow(new Instant(0), new Instant(10))));
    assertThat(tester.getKeyedStateInUse(), Matchers.containsInAnyOrder(
        tester.finishedSet(new IntervalWindow(new Instant(0), new Instant(10))),
        tester.finishedSet(new IntervalWindow(new Instant(10), new Instant(20))),
        tester.finishedSet(new IntervalWindow(new Instant(30), new Instant(40)))));
  }

  @Test
  public void testAfterProcessingTimeWithMergingWindowAlreadyFired() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(windowDuration),
        AfterProcessingTime
            .<IntervalWindow>pastFirstElementInPane()
            .plusDelayOf(Duration.millis(5)),
        AccumulationMode.DISCARDING_FIRED_PANES);

    tester.advanceProcessingTime(new Instant(10));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    tester.injectElement(1, new Instant(1)); // in [1, 11), timer for 15

    tester.advanceProcessingTime(new Instant(16));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.contains(1), 1, 1, 11)));

    // Because we discarded the previous window, we don't have it around to merge with.
    tester.injectElement(2, new Instant(2)); // in [2, 12), timer for 21


    tester.advanceProcessingTime(new Instant(100));
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.contains(2), 2, 2, 12)));

    assertTrue(tester.isDone(new IntervalWindow(new Instant(2), new Instant(12))));
    assertThat(tester.getKeyedStateInUse(), Matchers.containsInAnyOrder(
        tester.finishedSet(new IntervalWindow(new Instant(1), new Instant(11))),
        tester.finishedSet(new IntervalWindow(new Instant(2), new Instant(12)))));
  }

  @Test
  public void testFireDeadline() throws Exception {
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        AfterProcessingTime.pastFirstElementInPane().getWatermarkCutoff(
            new IntervalWindow(new Instant(0), new Instant(10))));
  }
}
