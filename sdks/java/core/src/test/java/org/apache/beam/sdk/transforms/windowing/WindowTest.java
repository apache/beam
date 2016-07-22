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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasKey;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFrom;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.WindowingStrategy.AccumulationMode;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import java.io.Serializable;

/**
 * Tests for {@link Window}.
 */
@RunWith(JUnit4.class)
public class WindowTest implements Serializable {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

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
    Repeatedly trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));
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
    Repeatedly trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));
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
        .apply("WindowInto10", Window.<String>into(fixed10)
            .withAllowedLateness(Duration.standardDays(1))
            .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(5)))
            .accumulatingFiredPanes())
        .apply("WindowInto25", Window.<String>into(fixed25))
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
    Repeatedly trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));

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
    Repeatedly trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("requires that the allowed lateness");
    TestPipeline.create()
      .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
      .apply("Mode", Window.<String>accumulatingFiredPanes())
      .apply("Window", Window.<String>into(fixed10))
      .apply("Trigger", Window.<String>triggering(trigger));
  }

  /**
   * Tests that when two elements are combined via a GroupByKey their output timestamp agrees
   * with the windowing function default, the end of the window.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testOutputTimeFnDefault() {
    Pipeline pipeline = TestPipeline.create();

    pipeline
        .apply(
            Create.timestamped(
                TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
                TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
        .apply(Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10))))
        .apply(GroupByKey.<Integer, String>create())
        .apply(
            ParDo.of(
                new OldDoFn<KV<Integer, Iterable<String>>, Void>() {
                  @Override
                  public void processElement(ProcessContext c) throws Exception {
                    assertThat(
                        c.timestamp(),
                        equalTo(
                            new IntervalWindow(
                                    new Instant(0),
                                    new Instant(0).plus(Duration.standardMinutes(10)))
                                .maxTimestamp()));
                  }
                }));

    pipeline.run();
  }

  /**
   * Tests that when two elements are combined via a GroupByKey their output timestamp agrees
   * with the windowing function customized to use the end of the window.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testOutputTimeFnEndOfWindow() {
    Pipeline pipeline = TestPipeline.create();

    pipeline.apply(
        Create.timestamped(
            TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
            TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
        .apply(Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
            .withOutputTimeFn(OutputTimeFns.outputAtEndOfWindow()))
        .apply(GroupByKey.<Integer, String>create())
        .apply(ParDo.of(new OldDoFn<KV<Integer, Iterable<String>>, Void>() {
          @Override
          public void processElement(ProcessContext c) throws Exception {
            assertThat(c.timestamp(), equalTo(new Instant(10 * 60 * 1000 - 1)));
          }
        }));

    pipeline.run();
  }

  @Test
  public void testDisplayData() {
    FixedWindows windowFn = FixedWindows.of(Duration.standardHours(5));
    AfterWatermark.FromEndOfWindow triggerBuilder = AfterWatermark.pastEndOfWindow();
    Duration allowedLateness = Duration.standardMinutes(10);
    Window.ClosingBehavior closingBehavior = Window.ClosingBehavior.FIRE_IF_NON_EMPTY;
    OutputTimeFn<BoundedWindow> outputTimeFn = OutputTimeFns.outputAtEndOfWindow();

    Window.Bound<?> window = Window
        .into(windowFn)
        .triggering(triggerBuilder)
        .accumulatingFiredPanes()
        .withAllowedLateness(allowedLateness, closingBehavior)
        .withOutputTimeFn(outputTimeFn);

    DisplayData displayData = DisplayData.from(window);

    assertThat(displayData, hasDisplayItem("windowFn", windowFn.getClass()));
    assertThat(displayData, includesDisplayDataFrom(windowFn));

    assertThat(displayData, hasDisplayItem("trigger", triggerBuilder.toString()));
    assertThat(displayData,
        hasDisplayItem("accumulationMode", AccumulationMode.ACCUMULATING_FIRED_PANES.toString()));
    assertThat(displayData,
        hasDisplayItem("allowedLateness", allowedLateness));
    assertThat(displayData, hasDisplayItem("closingBehavior", closingBehavior.toString()));
    assertThat(displayData, hasDisplayItem("outputTimeFn", outputTimeFn.getClass()));
  }

  @Test
  public void testDisplayDataExcludesUnspecifiedProperties() {
    Window.Bound<?> onlyHasAccumulationMode = Window.discardingFiredPanes();
    assertThat(DisplayData.from(onlyHasAccumulationMode), not(hasDisplayItem(hasKey(isOneOf(
        "windowFn",
        "trigger",
        "outputTimeFn",
        "allowedLateness",
        "closingBehavior")))));

    Window.Bound<?> noAccumulationMode = Window.into(new GlobalWindows());
    assertThat(DisplayData.from(noAccumulationMode),
        not(hasDisplayItem(hasKey("accumulationMode"))));
  }

  @Test
  public void testDisplayDataExcludesDefaults() {
    Window.Bound<?> window = Window.into(new GlobalWindows())
        .triggering(DefaultTrigger.of())
        .withAllowedLateness(Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));

    DisplayData data = DisplayData.from(window);
    assertThat(data, not(hasDisplayItem("trigger")));
    assertThat(data, not(hasDisplayItem("allowedLateness")));
  }
}
