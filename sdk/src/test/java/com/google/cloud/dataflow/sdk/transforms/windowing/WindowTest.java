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
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;

import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link Window}.
 */
@RunWith(JUnit4.class)
public class WindowTest {

  @Test
  public void testBasicWindowIntoSettings() {
    WindowingStrategy<?, ?> strategy = TestPipeline.create()
      .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
      .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(10))))
      .getWindowingStrategy();
    assertTrue(strategy.getWindowFn() instanceof FixedWindows);
    assertTrue(strategy.getTrigger().getSpec() instanceof DefaultTrigger);
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, strategy.getMode());
  }

  @Test
  public void testWindowIntoTriggersAndAccumulating() {
    WindowingStrategy<?, ?> strategy = TestPipeline.create()
      .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
      .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(10)))
          .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(5)))
          .accumulatingFiredPanes())
      .getWindowingStrategy();

    assertTrue(strategy.getWindowFn() instanceof FixedWindows);
    assertTrue(strategy.getTrigger().getSpec() instanceof Repeatedly);
    assertEquals(AccumulationMode.ACCUMULATING_FIRED_PANES, strategy.getMode());
  }

  @Test
  public void testWindowIntoPropagatesLateness() {
    WindowingStrategy<?, ?> strategy = TestPipeline.create()
        .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(10)))
            .withAllowedLateness(Duration.standardDays(1))
            .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(5)))
            .accumulatingFiredPanes())
        .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(25))))
        .getWindowingStrategy();

    assertEquals(Duration.standardDays(1), strategy.getAllowedLateness());
  }
}
