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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.core.triggers.TriggerStateMachineTester.SimpleTriggerStateMachineTester;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link NeverStateMachine}. */
@RunWith(JUnit4.class)
public class NeverStateMachineTest {
  private SimpleTriggerStateMachineTester<IntervalWindow> triggerTester;

  @Before
  public void setup() throws Exception {
    triggerTester =
        TriggerStateMachineTester.forTrigger(
            NeverStateMachine.ever(), FixedWindows.of(Duration.standardMinutes(5)));
  }

  @Test
  public void falseAfterEndOfWindow() throws Exception {
    triggerTester.injectElements(TimestampedValue.of(1, new Instant(1)));
    IntervalWindow window =
        new IntervalWindow(new Instant(0), new Instant(0).plus(Duration.standardMinutes(5)));
    assertThat(triggerTester.shouldFire(window), is(false));
    triggerTester.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);
    assertThat(triggerTester.shouldFire(window), is(false));
  }
}
