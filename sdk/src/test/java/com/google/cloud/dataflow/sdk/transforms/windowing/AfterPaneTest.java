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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.WindowMatchers;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.Sum.SumIntegerFn;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link AfterPane}.
 */
@RunWith(JUnit4.class)
public class AfterPaneTest {
  @Test
  public void testAfterPaneWithGlobalWindowsAndCombining() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Integer, IntervalWindow> tester = TriggerTester.combining(
        FixedWindows.of(windowDuration),
        AfterPane.<IntervalWindow>elementCountAtLeast(2),
        AccumulationMode.DISCARDING_FIRED_PANES,
        new SumIntegerFn().<String>asKeyedFn(),
        VarIntCoder.of(),
        Duration.millis(100));

    tester.injectElement(1, new Instant(1));
    tester.injectElement(2, new Instant(9));

    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.equalTo(3), 1, 0, 10)));

    // This element should not be output because that trigger (which was one-time) has already
    // gone off.
    tester.injectElement(6, new Instant(2));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(10))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(10), new Instant(20))));

    tester.assertHasOnlyGlobalAndFinishedSetsFor(
        new IntervalWindow(new Instant(0), new Instant(10)));
  }

  @Test
  public void testAfterPaneWithFixedWindow() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        FixedWindows.of(windowDuration),
        AfterPane.<IntervalWindow>elementCountAtLeast(2),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    tester.injectElement(1, new Instant(1)); // first in window [0, 10)
    tester.injectElement(2, new Instant(9));

    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));

    // This element belongs in the window that has already fired. It should not be re-output because
    // that trigger (which was one-time) has already gone off.
    tester.injectElement(6, new Instant(2));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(0), new Instant(10))));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(10), new Instant(20))));

    tester.assertHasOnlyGlobalAndFinishedSetsFor(
        new IntervalWindow(new Instant(0), new Instant(10)));
  }

  @Test
  public void testAfterPaneWithMerging() throws Exception {
    Duration windowDuration = Duration.millis(10);
    TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester = TriggerTester.nonCombining(
        Sessions.withGapDuration(windowDuration),
        AfterPane.<IntervalWindow>elementCountAtLeast(2),
        AccumulationMode.DISCARDING_FIRED_PANES,
        Duration.millis(100));

    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    tester.injectElement(1, new Instant(1)); // in [1, 11)
    tester.injectElement(2, new Instant(2)); // in [2, 12)
    tester.doMerge();
    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 1, 12)));

    // Because we closed the previous window, we don't have it around to merge with.
    tester.injectElement(3, new Instant(7)); // in [7, 17)
    tester.injectElement(4, new Instant(8)); // in [8, 18)
    tester.doMerge();

    assertThat(tester.extractOutput(), Matchers.contains(
        WindowMatchers.isSingleWindowedValue(Matchers.containsInAnyOrder(3, 4), 7, 7, 18)));

    assertTrue(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(12))));

    tester.assertHasOnlyGlobalAndFinishedSetsFor(
        new IntervalWindow(new Instant(1), new Instant(12)),
        new IntervalWindow(new Instant(7), new Instant(18)));
  }

  @Test
  public void testFireDeadline() throws Exception {
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        AfterPane.elementCountAtLeast(1).getWatermarkThatGuaranteesFiring(
            new IntervalWindow(new Instant(0), new Instant(10))));
  }

  @Test
  public void testContinuation() throws Exception {
    assertEquals(
        AfterPane.elementCountAtLeast(1),
        AfterPane.elementCountAtLeast(100).getContinuationTrigger());
    assertEquals(
        AfterPane.elementCountAtLeast(1),
        AfterPane.elementCountAtLeast(100).getContinuationTrigger().getContinuationTrigger());
  }
}
