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

/** Tests for {@link AfterEach}. */
@RunWith(JUnit4.class)
public class AfterEachTest {

  @Test
  public void testFireDeadline() throws Exception {
    BoundedWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    assertEquals(
        new Instant(9),
        AfterEach.inOrder(AfterWatermark.pastEndOfWindow(), AfterPane.elementCountAtLeast(4))
            .getWatermarkThatGuaranteesFiring(window));

    assertEquals(
        BoundedWindow.TIMESTAMP_MAX_VALUE,
        AfterEach.inOrder(AfterPane.elementCountAtLeast(2), AfterWatermark.pastEndOfWindow())
            .getWatermarkThatGuaranteesFiring(window));
  }

  @Test
  public void testContinuation() throws Exception {
    OnceTrigger trigger1 = AfterProcessingTime.pastFirstElementInPane();
    OnceTrigger trigger2 = AfterWatermark.pastEndOfWindow();
    Trigger afterEach = AfterEach.inOrder(trigger1, trigger2);
    assertEquals(
        Repeatedly.forever(
            AfterFirst.of(trigger1.getContinuationTrigger(), trigger2.getContinuationTrigger())),
        afterEach.getContinuationTrigger());
  }

  @Test
  public void testToString() {
    Trigger trigger =
        AfterEach.inOrder(
            StubTrigger.named("t1"), StubTrigger.named("t2"), StubTrigger.named("t3"));

    assertEquals("AfterEach.inOrder(t1, t2, t3)", trigger.toString());
  }
}
