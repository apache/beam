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
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnElementEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link TriggerExecutor}.
 */
@RunWith(JUnit4.class)
public class TriggerExecutorTest {

  @Mock private Trigger<IntervalWindow> mockTrigger;
  private TriggerTester<Integer, Iterable<Integer>, IntervalWindow> tester;
  private IntervalWindow firstWindow;

  public void setUpBuffering(
      WindowFn<?, IntervalWindow> windowFn, AccumulationMode mode) throws Exception {
    MockitoAnnotations.initMocks(this);
    tester = TriggerTester.nonCombining(windowFn, mockTrigger, mode);
    firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
  }

  @SuppressWarnings("unchecked")
  private TriggerContext<IntervalWindow> isTriggerContext() {
    return Mockito.isA(TriggerContext.class);
  }

  private void injectElement(int element, TriggerResult result)
      throws Exception {
    when(mockTrigger.onElement(
        isTriggerContext(), Mockito.<OnElementEvent<IntervalWindow>>any()))
        .thenReturn(result);
    tester.injectElement(element, new Instant(element));
  }

  @Test
  public void testOnElementBufferingDiscarding() throws Exception {
    setUpBuffering(FixedWindows.of(Duration.millis(10)), AccumulationMode.DISCARDING_FIRED_PANES);

    injectElement(1, TriggerResult.CONTINUE);
    assertTrue(tester.isWindowActive(firstWindow));

    injectElement(2, TriggerResult.FIRE);
    assertFalse(tester.isWindowActive(firstWindow));

    injectElement(3, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(4, null);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(3), 3, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(tester.finishedSet(firstWindow)));
    assertFalse(tester.isWindowActive(firstWindow));
  }

  @Test
  public void testOnElementBufferingAccumulating() throws Exception {
    setUpBuffering(FixedWindows.of(Duration.millis(10)), AccumulationMode.ACCUMULATING_FIRED_PANES);

    injectElement(1, TriggerResult.CONTINUE);
    assertTrue(tester.isWindowActive(firstWindow));

    injectElement(2, TriggerResult.FIRE);
    assertTrue(tester.isWindowActive(firstWindow));

    injectElement(3, TriggerResult.FIRE_AND_FINISH);

    // This element shouldn't be seen, because the trigger has finished
    injectElement(4, null);

    assertThat(tester.extractOutput(), Matchers.contains(
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2), 1, 0, 10),
        isSingleWindowedValue(Matchers.containsInAnyOrder(1, 2, 3), 3, 0, 10)));
    assertTrue(tester.isDone(firstWindow));
    assertThat(tester.getKeyedStateInUse(), Matchers.contains(tester.finishedSet(firstWindow)));
    assertFalse(tester.isWindowActive(firstWindow));
  }
}
