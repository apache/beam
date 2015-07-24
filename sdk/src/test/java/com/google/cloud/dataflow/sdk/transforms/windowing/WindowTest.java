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
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.Coder.NonDeterministicException;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/**
 * Tests for {@link Window}.
 */
@RunWith(JUnit4.class)
public class WindowTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testWindowIntoSetWindowfn() {
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
    FixedWindows fixed10 = FixedWindows.of(Duration.standardMinutes(10));
    Repeatedly<BoundedWindow> trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));
    WindowingStrategy<?, ?> strategy = TestPipeline.create()
      .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
      .apply(Window.<String>into(fixed10)
          .triggering(trigger)
          .accumulatingFiredPanes()
          .withAllowedLateness(Duration.ZERO))
      .getWindowingStrategy();

    assertEquals(fixed10, strategy.getWindowFn());
    assertEquals(trigger, strategy.getTrigger().getSpec());
    assertEquals(AccumulationMode.ACCUMULATING_FIRED_PANES, strategy.getMode());
  }

  @Test
  public void testWindowPropagatesEachPart() {
    FixedWindows fixed10 = FixedWindows.of(Duration.standardMinutes(10));
    Repeatedly<BoundedWindow> trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));
    WindowingStrategy<?, ?> strategy = TestPipeline.create()
      .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
      .apply("Mode", Window.<String>accumulatingFiredPanes())
      .apply("Lateness", Window.<String>withAllowedLateness(Duration.standardDays(1)))
      .apply("Trigger", Window.<String>triggering(trigger))
      .apply("Window", Window.<String>into(fixed10))
      .getWindowingStrategy();

    assertEquals(fixed10, strategy.getWindowFn());
    assertEquals(trigger, strategy.getTrigger().getSpec());
    assertEquals(AccumulationMode.ACCUMULATING_FIRED_PANES, strategy.getMode());
    assertEquals(Duration.standardDays(1), strategy.getAllowedLateness());
  }

  @Test
  public void testWindowIntoPropagatesLateness() {
    FixedWindows fixed10 = FixedWindows.of(Duration.standardMinutes(10));
    FixedWindows fixed25 = FixedWindows.of(Duration.standardMinutes(25));
    WindowingStrategy<?, ?> strategy = TestPipeline.create()
        .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
        .apply(Window.named("WindowInto10").<String>into(fixed10)
            .withAllowedLateness(Duration.standardDays(1))
            .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(5)))
            .accumulatingFiredPanes())
        .apply(Window.named("WindowInto25").<String>into(fixed25))
        .getWindowingStrategy();

    assertEquals(Duration.standardDays(1), strategy.getAllowedLateness());
    assertEquals(fixed25, strategy.getWindowFn());
  }

  @Test
  public void testWindowGetName() {
    assertEquals("Window.Into()",
        Window.<String>into(FixedWindows.of(Duration.standardMinutes(10))).getName());
  }

  @Test
  public void testNonDeterministicWindowCoder() throws NonDeterministicException {
    FixedWindows mockWindowFn = Mockito.mock(FixedWindows.class);
    @SuppressWarnings({"unchecked", "rawtypes"})
    Class<Coder<IntervalWindow>> coderClazz = (Class) Coder.class;
    Coder<IntervalWindow> mockCoder = Mockito.mock(coderClazz);
    when(mockWindowFn.windowCoder()).thenReturn(mockCoder);
    NonDeterministicException toBeThrown =
        new NonDeterministicException(mockCoder, "Its just not deterministic.");
    Mockito.doThrow(toBeThrown).when(mockCoder).verifyDeterministic();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectCause(Matchers.sameInstance(toBeThrown));
    thrown.expectMessage("Window coders must be deterministic");
    Window.into(mockWindowFn);
  }

  @Test
  public void testMissingMode() {
    FixedWindows fixed10 = FixedWindows.of(Duration.standardMinutes(10));
    Repeatedly<BoundedWindow> trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("requires that the accumulation mode");
    TestPipeline.create()
      .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
      .apply("Window", Window.<String>into(fixed10))
      .apply("Lateness", Window.<String>withAllowedLateness(Duration.standardDays(1)))
      .apply("Trigger", Window.<String>triggering(trigger));
  }

  @Test
  public void testMissingLateness() {
    FixedWindows fixed10 = FixedWindows.of(Duration.standardMinutes(10));
    Repeatedly<BoundedWindow> trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("requires that the allowed lateness");
    TestPipeline.create()
      .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
      .apply("Mode", Window.<String>accumulatingFiredPanes())
      .apply("Window", Window.<String>into(fixed10))
      .apply("Trigger", Window.<String>triggering(trigger));
  }
}
