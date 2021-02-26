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
package org.apache.beam.runners.core.triggers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.core.triggers.TriggerStateMachine.OnMergeContext;
import org.apache.beam.runners.core.triggers.TriggerStateMachineTester.SimpleTriggerStateMachineTester;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests the {@link AfterWatermarkStateMachine} triggers. */
@RunWith(JUnit4.class)
public class AfterWatermarkStateMachineTest {

  @Mock private TriggerStateMachine mockEarly;
  @Mock private TriggerStateMachine mockLate;

  private SimpleTriggerStateMachineTester<IntervalWindow> tester;

  private static TriggerStateMachine.TriggerContext anyTriggerContext() {
    return Mockito.any();
  }

  private static TriggerStateMachine.OnElementContext anyElementContext() {
    return Mockito.any();
  }

  private void injectElements(int... elements) throws Exception {
    for (int element : elements) {
      doNothing().when(mockEarly).onElement(anyElementContext());
      doNothing().when(mockLate).onElement(anyElementContext());
      tester.injectElements(element);
    }
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  public void testRunningAsTrigger(TriggerStateMachine mockTrigger, IntervalWindow window)
      throws Exception {

    // Don't fire due to mock saying no
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(false);
    assertFalse(tester.shouldFire(window)); // not ready

    // Fire due to mock trigger; early trigger is required to be a OnceTrigger
    when(mockTrigger.shouldFire(anyTriggerContext())).thenReturn(true);
    assertTrue(tester.shouldFire(window)); // ready
    tester.fireIfShouldFire(window);
    assertFalse(tester.isMarkedFinished(window));
  }

  @Test
  public void testEarlyAndAtWatermark() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterWatermarkStateMachine.pastEndOfWindow().withEarlyFirings(mockEarly),
            FixedWindows.of(Duration.millis(100)));

    injectElements(1);
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(100));

    testRunningAsTrigger(mockEarly, window);

    // Fire due to watermark
    when(mockEarly.shouldFire(anyTriggerContext())).thenReturn(false);
    tester.advanceInputWatermark(new Instant(100));
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertTrue(tester.isMarkedFinished(window));
  }

  @Test
  public void testTimerForEndOfWindow() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterWatermarkStateMachine.pastEndOfWindow(), FixedWindows.of(Duration.millis(100)));

    assertThat(tester.getNextTimer(TimeDomain.EVENT_TIME), nullValue());
    injectElements(1);
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(100));
    assertThat(tester.getNextTimer(TimeDomain.EVENT_TIME), equalTo(window.maxTimestamp()));
  }

  @Test
  public void testTimerForEndOfWindowCompound() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterWatermarkStateMachine.pastEndOfWindow().withEarlyFirings(NeverStateMachine.ever()),
            FixedWindows.of(Duration.millis(100)));

    assertThat(tester.getNextTimer(TimeDomain.EVENT_TIME), nullValue());
    injectElements(1);
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(100));
    assertThat(tester.getNextTimer(TimeDomain.EVENT_TIME), equalTo(window.maxTimestamp()));
  }

  @Test
  public void testAtWatermarkAndLate() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterWatermarkStateMachine.pastEndOfWindow().withLateFirings(mockLate),
            FixedWindows.of(Duration.millis(100)));

    injectElements(1);
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(100));

    // No early firing, just double checking
    when(mockEarly.shouldFire(anyTriggerContext())).thenReturn(true);
    assertFalse(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.isMarkedFinished(window));

    // Fire due to watermark
    when(mockEarly.shouldFire(anyTriggerContext())).thenReturn(false);
    tester.advanceInputWatermark(new Instant(100));
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.isMarkedFinished(window));

    testRunningAsTrigger(mockLate, window);
  }

  @Test
  public void testEarlyAndAtWatermarkAndLate() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterWatermarkStateMachine.pastEndOfWindow()
                .withEarlyFirings(mockEarly)
                .withLateFirings(mockLate),
            FixedWindows.of(Duration.millis(100)));

    injectElements(1);
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(100));

    testRunningAsTrigger(mockEarly, window);

    // Fire due to watermark
    when(mockEarly.shouldFire(anyTriggerContext())).thenReturn(false);
    tester.advanceInputWatermark(new Instant(100));
    assertTrue(tester.shouldFire(window));
    tester.fireIfShouldFire(window);
    assertFalse(tester.isMarkedFinished(window));

    testRunningAsTrigger(mockLate, window);
  }

  /**
   * Tests that if the EOW is finished in both as well as the merged window, then it is finished in
   * the merged result.
   *
   * <p>Because windows are discarded when a trigger finishes, we need to embed this in a sequence
   * in order to check that it is re-activated. So this test is potentially sensitive to other
   * triggers' correctness.
   */
  @Test
  public void testOnMergeAlreadyFinished() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterEachStateMachine.inOrder(
                AfterWatermarkStateMachine.pastEndOfWindow(),
                RepeatedlyStateMachine.forever(AfterPaneStateMachine.elementCountAtLeast(1))),
            Sessions.withGapDuration(Duration.millis(10)));

    tester.injectElements(1);
    tester.injectElements(5);
    IntervalWindow firstWindow = new IntervalWindow(new Instant(1), new Instant(11));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(5), new Instant(15));
    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(15));

    // Finish the AfterWatermark.pastEndOfWindow() trigger in both windows
    tester.advanceInputWatermark(new Instant(15));
    assertTrue(tester.shouldFire(firstWindow));
    assertTrue(tester.shouldFire(secondWindow));
    tester.fireIfShouldFire(firstWindow);
    tester.fireIfShouldFire(secondWindow);

    // Confirm that we are on the second trigger by probing
    assertFalse(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    tester.injectElements(1);
    tester.injectElements(5);
    assertTrue(tester.shouldFire(firstWindow));
    assertTrue(tester.shouldFire(secondWindow));
    tester.fireIfShouldFire(firstWindow);
    tester.fireIfShouldFire(secondWindow);

    // Merging should leave it finished
    tester.mergeWindows();

    // Confirm that we are on the second trigger by probing
    assertFalse(tester.shouldFire(mergedWindow));
    tester.injectElements(1);
    assertTrue(tester.shouldFire(mergedWindow));
  }

  /**
   * Tests that the trigger rewinds to be non-finished in the merged window.
   *
   * <p>Because windows are discarded when a trigger finishes, we need to embed this in a sequence
   * in order to check that it is re-activated. So this test is potentially sensitive to other
   * triggers' correctness.
   */
  @Test
  public void testOnMergeRewinds() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterEachStateMachine.inOrder(
                AfterWatermarkStateMachine.pastEndOfWindow(),
                RepeatedlyStateMachine.forever(AfterPaneStateMachine.elementCountAtLeast(1))),
            Sessions.withGapDuration(Duration.millis(10)));

    tester.injectElements(1);
    tester.injectElements(5);
    IntervalWindow firstWindow = new IntervalWindow(new Instant(1), new Instant(11));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(5), new Instant(15));
    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(15));

    // Finish the AfterWatermark.pastEndOfWindow() trigger in only the first window
    tester.advanceInputWatermark(new Instant(11));
    assertTrue(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    tester.fireIfShouldFire(firstWindow);

    // Confirm that we are on the second trigger by probing
    assertFalse(tester.shouldFire(firstWindow));
    tester.injectElements(1);
    assertTrue(tester.shouldFire(firstWindow));
    tester.fireIfShouldFire(firstWindow);

    // Merging should re-activate the watermark trigger in the merged window
    tester.mergeWindows();

    // Confirm that we are not on the second trigger by probing
    assertFalse(tester.shouldFire(mergedWindow));
    tester.injectElements(1);
    assertFalse(tester.shouldFire(mergedWindow));

    // And confirm that advancing the watermark fires again
    tester.advanceInputWatermark(new Instant(15));
    assertTrue(tester.shouldFire(mergedWindow));
  }

  /**
   * Tests that if the EOW is finished in both as well as the merged window, then it is finished in
   * the merged result.
   *
   * <p>Because windows are discarded when a trigger finishes, we need to embed this in a sequence
   * in order to check that it is re-activated. So this test is potentially sensitive to other
   * triggers' correctness.
   */
  @Test
  public void testEarlyAndLateOnMergeAlreadyFinished() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterWatermarkStateMachine.pastEndOfWindow()
                .withEarlyFirings(AfterPaneStateMachine.elementCountAtLeast(100))
                .withLateFirings(AfterPaneStateMachine.elementCountAtLeast(1)),
            Sessions.withGapDuration(Duration.millis(10)));

    tester.injectElements(1);
    tester.injectElements(5);
    IntervalWindow firstWindow = new IntervalWindow(new Instant(1), new Instant(11));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(5), new Instant(15));
    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(15));

    // Finish the AfterWatermark.pastEndOfWindow() bit of the trigger in both windows
    tester.advanceInputWatermark(new Instant(15));
    assertTrue(tester.shouldFire(firstWindow));
    assertTrue(tester.shouldFire(secondWindow));
    tester.fireIfShouldFire(firstWindow);
    tester.fireIfShouldFire(secondWindow);

    // Confirm that we are on the late trigger by probing
    assertFalse(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    tester.injectElements(1);
    tester.injectElements(5);
    assertTrue(tester.shouldFire(firstWindow));
    assertTrue(tester.shouldFire(secondWindow));
    tester.fireIfShouldFire(firstWindow);
    tester.fireIfShouldFire(secondWindow);

    // Merging should leave it on the late trigger
    tester.mergeWindows();

    // Confirm that we are on the late trigger by probing
    assertFalse(tester.shouldFire(mergedWindow));
    tester.injectElements(1);
    assertTrue(tester.shouldFire(mergedWindow));
  }

  @Test
  public void testEarlyAndLateOnMergeSubtriggerMerges() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterWatermarkStateMachine.pastEndOfWindow()
                .withEarlyFirings(mockEarly)
                .withLateFirings(mockLate),
            Sessions.withGapDuration(Duration.millis(10)));

    tester.injectElements(1);
    tester.injectElements(5);

    tester.mergeWindows();
    // Merging should re-activate the early trigger in the merged window
    verify(mockEarly).onMerge(Mockito.any(OnMergeContext.class));
    // Merging should merge the late trigger in the merged window so it can be cleared.
    verify(mockLate).onMerge(Mockito.any(OnMergeContext.class));
  }

  /**
   * Tests that the trigger rewinds to be non-finished in the merged window.
   *
   * <p>Because windows are discarded when a trigger finishes, we need to embed this in a sequence
   * in order to check that it is re-activated. So this test is potentially sensitive to other
   * triggers' correctness.
   */
  @Test
  public void testEarlyAndLateOnMergeRewinds() throws Exception {
    tester =
        TriggerStateMachineTester.forTrigger(
            AfterWatermarkStateMachine.pastEndOfWindow()
                .withEarlyFirings(AfterPaneStateMachine.elementCountAtLeast(100))
                .withLateFirings(AfterPaneStateMachine.elementCountAtLeast(1)),
            Sessions.withGapDuration(Duration.millis(10)));

    tester.injectElements(1);
    tester.injectElements(5);
    IntervalWindow firstWindow = new IntervalWindow(new Instant(1), new Instant(11));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(5), new Instant(15));
    IntervalWindow mergedWindow = new IntervalWindow(new Instant(1), new Instant(15));

    // Finish the AfterWatermark.pastEndOfWindow() bit of the trigger in only the first window
    tester.advanceInputWatermark(new Instant(11));
    assertTrue(tester.shouldFire(firstWindow));
    assertFalse(tester.shouldFire(secondWindow));
    tester.fireIfShouldFire(firstWindow);

    // Confirm that we are on the late trigger by probing
    assertFalse(tester.shouldFire(firstWindow));
    tester.injectElements(1);
    assertTrue(tester.shouldFire(firstWindow));
    tester.fireIfShouldFire(firstWindow);

    // Merging should re-activate the early trigger in the merged window
    tester.mergeWindows();

    // Confirm that we are not on the second trigger by probing
    assertFalse(tester.shouldFire(mergedWindow));
    tester.injectElements(1);
    assertFalse(tester.shouldFire(mergedWindow));

    // And confirm that advancing the watermark fires again
    tester.advanceInputWatermark(new Instant(15));
    assertTrue(tester.shouldFire(mergedWindow));
  }

  @Test
  public void testFromEndOfWindowToString() {
    TriggerStateMachine trigger = AfterWatermarkStateMachine.pastEndOfWindow();
    assertEquals("AfterWatermark.pastEndOfWindow()", trigger.toString());
  }

  @Test
  public void testEarlyFiringsToString() {
    TriggerStateMachine trigger =
        AfterWatermarkStateMachine.pastEndOfWindow()
            .withEarlyFirings(StubTriggerStateMachine.named("t1"));

    assertEquals("AfterWatermark.pastEndOfWindow().withEarlyFirings(t1)", trigger.toString());
  }

  @Test
  public void testLateFiringsToString() {
    TriggerStateMachine trigger =
        AfterWatermarkStateMachine.pastEndOfWindow()
            .withLateFirings(StubTriggerStateMachine.named("t1"));

    assertEquals("AfterWatermark.pastEndOfWindow().withLateFirings(t1)", trigger.toString());
  }

  @Test
  public void testEarlyAndLateFiringsToString() {
    TriggerStateMachine trigger =
        AfterWatermarkStateMachine.pastEndOfWindow()
            .withEarlyFirings(StubTriggerStateMachine.named("t1"))
            .withLateFirings(StubTriggerStateMachine.named("t2"));

    assertEquals(
        "AfterWatermark.pastEndOfWindow().withEarlyFirings(t1).withLateFirings(t2)",
        trigger.toString());
  }

  @Test
  public void testToStringExcludesNeverTrigger() {
    TriggerStateMachine trigger =
        AfterWatermarkStateMachine.pastEndOfWindow()
            .withEarlyFirings(NeverStateMachine.ever())
            .withLateFirings(NeverStateMachine.ever());

    assertEquals("AfterWatermark.pastEndOfWindow()", trigger.toString());
  }
}
