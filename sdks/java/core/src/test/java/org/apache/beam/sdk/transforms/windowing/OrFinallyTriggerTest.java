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
package org.apache.beam.sdk.transforms.windowing;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link OrFinallyTrigger}. */
@RunWith(JUnit4.class)
public class OrFinallyTriggerTest {

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(
        new Instant(9),
        Repeatedly.forever(AfterWatermark.pastEndOfWindow())
            .getWatermarkThatGuaranteesFiring(window));
    assertEquals(
        new Instant(9),
        Repeatedly.forever(AfterWatermark.pastEndOfWindow())
            .orFinally(AfterPane.elementCountAtLeast(1))
            .getWatermarkThatGuaranteesFiring(window));
    assertEquals(
        new Instant(9),
        Repeatedly.forever(AfterPane.elementCountAtLeast(1))
            .orFinally(AfterWatermark.pastEndOfWindow())
            .getWatermarkThatGuaranteesFiring(window));
    assertEquals(
        new Instant(9),
        AfterPane.elementCountAtLeast(100)
            .orFinally(AfterWatermark.pastEndOfWindow())
            .getWatermarkThatGuaranteesFiring(window));

    assertEquals(
        BoundedWindow.TIMESTAMP_MAX_VALUE,
        Repeatedly.forever(AfterPane.elementCountAtLeast(1))
            .orFinally(AfterPane.elementCountAtLeast(10))
            .getWatermarkThatGuaranteesFiring(window));
  }

  @Test
  public void testContinuation() throws Exception {
    OnceTrigger triggerA = AfterProcessingTime.pastFirstElementInPane();
    OnceTrigger triggerB = AfterWatermark.pastEndOfWindow();
    Trigger aOrFinallyB = triggerA.orFinally(triggerB);
    Trigger bOrFinallyA = triggerB.orFinally(triggerA);
    assertEquals(
        Repeatedly.forever(
            triggerA.getContinuationTrigger().orFinally(triggerB.getContinuationTrigger())),
        aOrFinallyB.getContinuationTrigger());
    assertEquals(
        Repeatedly.forever(
            triggerB.getContinuationTrigger().orFinally(triggerA.getContinuationTrigger())),
        bOrFinallyA.getContinuationTrigger());
  }

  @Test
  public void testToString() {
    Trigger trigger = StubTrigger.named("t1").orFinally(StubTrigger.named("t2"));
    assertEquals("t1.orFinally(t2)", trigger.toString());
  }
}
