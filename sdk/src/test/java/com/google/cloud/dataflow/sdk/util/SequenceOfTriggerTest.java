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

import static com.google.cloud.dataflow.sdk.WindowMatchers.isSingleWindowedValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.WindowMatchers;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.Trigger.TimeDomain;
import com.google.cloud.dataflow.sdk.util.Trigger.TriggerContext;
import com.google.cloud.dataflow.sdk.util.Trigger.TriggerId;
import com.google.cloud.dataflow.sdk.util.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.Trigger.WindowStatus;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

/**
 * Tests for {@link SequenceOfTrigger}.
 */
@RunWith(JUnit4.class)
public class SequenceOfTriggerTest {

  @Mock private Trigger<IntervalWindow> mockTrigger1;
  @Mock private Trigger<IntervalWindow> mockTrigger2;
  private TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester;
  private IntervalWindow firstWindow;

  public void setUp(WindowFn<?, IntervalWindow> windowFn) throws Exception {
    MockitoAnnotations.initMocks(this);
    tester = TriggerTester.of(
        windowFn,
        new SequenceOfTrigger<>(Arrays.asList(mockTrigger1, mockTrigger2)),
        BufferingWindowSet.<String, Integer, IntervalWindow>factory(VarIntCoder.of()));
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  @SuppressWarnings("unchecked")
  private TriggerContext<IntervalWindow> isTriggerContext() {
    return Mockito.isA(TriggerContext.class);
  }

  private void injectElement(int element, TriggerResult result1, TriggerResult result2)
      throws Exception {
    if (result1 != null) {
      when(mockTrigger1.onElement(
          isTriggerContext(), Mockito.eq(element),
          Mockito.any(IntervalWindow.class), Mockito.any(WindowStatus.class)))
          .thenReturn(result1);
    }
    if (result2 != null) {
      when(mockTrigger2.onElement(
          isTriggerContext(), Mockito.eq(element),
          Mockito.any(IntervalWindow.class), Mockito.any(WindowStatus.class)))
          .thenReturn(result2);
    }
    tester.injectElement(element, new Instant(element));
  }

  @Test
  public void testOnElementT1Fires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, null);
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    injectElement(2, TriggerResult.FIRE, null);
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));

    injectElement(3, TriggerResult.FIRE_AND_FINISH, null);
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(3), 3, 0, 10)));

    injectElement(4, null, TriggerResult.FIRE);
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(4), 4, 0, 10)));
    injectElement(5, null, TriggerResult.FINISH);
    assertThat(tester.extractOutput(), Matchers.emptyIterable());
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(tester.rootFinished(firstWindow)));
  }

  @Test
  public void testOnElementT2Fires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.FIRE);
    assertThat(tester.extractOutput(), Matchers.emptyIterable());
    assertFalse(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        // Buffering element 1; Ignored the trigger for T2 since we aren't there yet.
        tester.bufferTag(firstWindow)));
  }

  @Test
  public void testOnElementT1Finishes() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.FINISH, TriggerResult.CONTINUE);
    assertThat(tester.extractOutput(), Matchers.emptyIterable());
    injectElement(2, null, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(tester.rootFinished(firstWindow)));
  }

  @Test
  public void testOnElementBothFinish() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.FINISH, null);
    assertThat(tester.extractOutput(), Matchers.emptyIterable());
    injectElement(2, null, TriggerResult.FINISH);
    assertThat(tester.extractOutput(), Matchers.emptyIterable());
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(tester.rootFinished(firstWindow)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnTimerFire() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, null);

    tester.setTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME, ImmutableList.of(0));
    when(mockTrigger1.onTimer(isTriggerContext(), Mockito.isA(TriggerId.class)))
        .thenReturn(TriggerResult.FIRE);
    tester.advanceWatermark(new Instant(12));

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1), 1, 0, 10)));
    assertFalse("Should still be waiting for the second trigger.", tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.emptyIterable());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnTimerFinish() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, null);

    tester.setTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME, ImmutableList.of(0));
    when(mockTrigger1.onTimer(isTriggerContext(), Mockito.isA(TriggerId.class)))
        .thenReturn(TriggerResult.FINISH);

    tester.advanceWatermark(new Instant(12));
    assertThat(tester.extractOutput(), Matchers.emptyIterable());

    injectElement(2, null, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(tester.rootFinished(firstWindow)));
  }

  @Test
  public void testOnMergeFires() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE, TriggerResult.CONTINUE);
    injectElement(12, TriggerResult.FINISH, TriggerResult.CONTINUE);

    when(mockTrigger2.onMerge(
        isTriggerContext(),
        Mockito.argThat(WindowMatchers.ofWindows(
            WindowMatchers.intervalWindow(1, 11),
            WindowMatchers.intervalWindow(12, 22),
            WindowMatchers.intervalWindow(5, 15))),
        Mockito.isA(IntervalWindow.class))).thenReturn(TriggerResult.FIRE_AND_FINISH);

    // The arrival of this element should trigger merging.
    injectElement(5, TriggerResult.CONTINUE, TriggerResult.CONTINUE);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 5, 12), 1, 1, 22)));
    assertTrue(tester.isDone(new IntervalWindow(new Instant(1), new Instant(22))));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(
        tester.rootFinished(new IntervalWindow(new Instant(1), new Instant(22)))));

    verify(mockTrigger1, Mockito.never())
        .onMerge(
            Mockito.<TriggerContext<IntervalWindow>>any(),
            Mockito.<Iterable<IntervalWindow>>any(),
            Mockito.<IntervalWindow>any());
  }
}
