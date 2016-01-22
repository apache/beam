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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ReshuffleTrigger}.
 */
@RunWith(JUnit4.class)
public class ReshuffleTriggerTest {

  /** Public so that other tests can instantiate ReshufleTrigger. */
  public static <W extends BoundedWindow> ReshuffleTrigger<W> forTest() {
    return new ReshuffleTrigger<>();
  }

  @Test
  public void testShouldFire() throws Exception {
    TriggerTester<Integer, IntervalWindow> tester = TriggerTester.forTrigger(
        new ReshuffleTrigger<IntervalWindow>(), FixedWindows.of(Duration.millis(100)));
    IntervalWindow arbitraryWindow = new IntervalWindow(new Instant(300), new Instant(400));
    assertTrue(tester.shouldFire(arbitraryWindow));
  }

  @Test
  public void testOnTimer() throws Exception {
    TriggerTester<Integer, IntervalWindow> tester = TriggerTester.forTrigger(
        new ReshuffleTrigger<IntervalWindow>(), FixedWindows.of(Duration.millis(100)));
    IntervalWindow arbitraryWindow = new IntervalWindow(new Instant(100), new Instant(200));
    tester.fireIfShouldFire(arbitraryWindow);
    assertFalse(tester.isMarkedFinished(arbitraryWindow));
  }
}
