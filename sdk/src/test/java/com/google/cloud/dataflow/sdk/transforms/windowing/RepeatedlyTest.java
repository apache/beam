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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TriggerTester;
import com.google.cloud.dataflow.sdk.util.TriggerTester.SimpleTriggerTester;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link Repeatedly}.
 */
@RunWith(JUnit4.class)
public class RepeatedlyTest {
  @Mock private Trigger<IntervalWindow> mockRepeated;

  private SimpleTriggerTester<IntervalWindow> tester;
  private IntervalWindow firstWindow;

  public void setUp(WindowFn<?, IntervalWindow> windowFn) throws Exception {
    MockitoAnnotations.initMocks(this);
    Trigger<IntervalWindow> underTest = Repeatedly.forever(mockRepeated);
    tester = TriggerTester.forTrigger(
        underTest, windowFn);
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  private void injectElement(int element, TriggerResult result1)
      throws Exception {
    if (result1 != null) {
      when(mockRepeated.onElement(
          Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
          .thenReturn(result1);
    }

    tester.injectElements(element);
  }

  @Test
  public void testOnElement() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    injectElement(2, TriggerResult.FIRE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    injectElement(3, TriggerResult.FIRE_AND_FINISH);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    injectElement(4, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    assertFalse(tester.isMarkedFinished(firstWindow));
  }

  @Test
  public void testOnElementTimerFires() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));

    injectElement(1, TriggerResult.CONTINUE);

    when(mockRepeated.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(11), TimeDomain.EVENT_TIME);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    injectElement(2, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    when(mockRepeated.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE_AND_FINISH);
    tester.fireTimer(firstWindow, new Instant(12), TimeDomain.EVENT_TIME);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));

    injectElement(3, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    when(mockRepeated.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.fireTimer(firstWindow, new Instant(13), TimeDomain.EVENT_TIME);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    injectElement(4, TriggerResult.CONTINUE);
    assertThat(tester.getLatestResult(), equalTo(TriggerResult.CONTINUE));

    when(mockRepeated.onTimer(Mockito.<Trigger<IntervalWindow>.OnTimerContext>any()))
        .thenReturn(TriggerResult.FIRE);
    tester.fireTimer(firstWindow, new Instant(14), TimeDomain.EVENT_TIME);

    assertThat(tester.getLatestResult(), equalTo(TriggerResult.FIRE));
    assertFalse(tester.isMarkedFinished(firstWindow));
  }

  @Test
  public void testMerge() throws Exception {
    setUp(Sessions.withGapDuration(Duration.millis(10)));

    when(mockRepeated.onElement(Mockito.<Trigger<IntervalWindow>.OnElementContext>any()))
        .thenReturn(TriggerResult.CONTINUE);
    tester.injectElements(1, 5);

    when(mockRepeated.onMerge(Mockito.<Trigger<IntervalWindow>.OnMergeContext>any()))
        .thenReturn(MergeResult.FIRE_AND_FINISH);

    tester.mergeWindows();

    assertThat(tester.getLatestMergeResult(), equalTo(MergeResult.FIRE));
    assertFalse(tester.isMarkedFinished(new IntervalWindow(new Instant(1), new Instant(16))));
  }

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(new Instant(9),
        Repeatedly.forever(AfterWatermark.pastEndOfWindow())
        .getWatermarkThatGuaranteesFiring(window));
    assertEquals(new Instant(9), Repeatedly.forever(AfterWatermark.pastEndOfWindow())
        .orFinally(AfterPane.elementCountAtLeast(1))
        .getWatermarkThatGuaranteesFiring(window));
    assertEquals(new Instant(9), Repeatedly.forever(AfterPane.elementCountAtLeast(1))
        .orFinally(AfterWatermark.pastEndOfWindow())
        .getWatermarkThatGuaranteesFiring(window));
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE,
        Repeatedly.forever(AfterPane.elementCountAtLeast(1))
        .orFinally(AfterPane.elementCountAtLeast(10))
        .getWatermarkThatGuaranteesFiring(window));
  }

  @Test
  public void testContinuation() throws Exception {
    Trigger<IntervalWindow> trigger = AfterProcessingTime.pastFirstElementInPane();
    Trigger<IntervalWindow> repeatedly = Repeatedly.forever(trigger);
    assertEquals(
        Repeatedly.forever(trigger.getContinuationTrigger()),
        repeatedly.getContinuationTrigger());
    assertEquals(
        Repeatedly.forever(trigger.getContinuationTrigger().getContinuationTrigger()),
        repeatedly.getContinuationTrigger().getContinuationTrigger());
  }
}
