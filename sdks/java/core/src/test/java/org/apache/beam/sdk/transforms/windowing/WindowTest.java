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
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFor;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.WindowingStrategy.AccumulationMode;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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

/**
 * Tests for {@link Window}.
 */
@RunWith(JUnit4.class)
public class WindowTest implements Serializable {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create()
                                                             .enableAbandonedNodeEnforcement(false);

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testWindowIntoSetWindowfn() {
    WindowingStrategy<?, ?> strategy = pipeline
      .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
      .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(10))))
      .getWindowingStrategy();
    assertTrue(strategy.getWindowFn() instanceof FixedWindows);
    assertTrue(strategy.getTrigger() instanceof DefaultTrigger);
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, strategy.getMode());
  }

  @Test
  public void testWindowIntoTriggersAndAccumulating() {
    FixedWindows fixed10 = FixedWindows.of(Duration.standardMinutes(10));
    Repeatedly trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));
    WindowingStrategy<?, ?> strategy = pipeline
      .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
      .apply(Window.<String>into(fixed10)
          .triggering(trigger)
          .accumulatingFiredPanes()
          .withAllowedLateness(Duration.ZERO))
      .getWindowingStrategy();

    assertEquals(fixed10, strategy.getWindowFn());
    assertEquals(trigger, strategy.getTrigger());
    assertEquals(AccumulationMode.ACCUMULATING_FIRED_PANES, strategy.getMode());
  }

  @Test
  public void testWindowIntoAccumulatingLatenessNoTrigger() {
    FixedWindows fixed = FixedWindows.of(Duration.standardMinutes(10));
    WindowingStrategy<?, ?> strategy =
        pipeline
            .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
            .apply(
                "Lateness",
                Window.<String>into(fixed)
                    .withAllowedLateness(Duration.standardDays(1))
                    .accumulatingFiredPanes())
            .getWindowingStrategy();

    assertThat(strategy.isTriggerSpecified(), is(false));
    assertThat(strategy.isModeSpecified(), is(true));
    assertThat(strategy.isAllowedLatenessSpecified(), is(true));
    assertThat(strategy.getMode(), equalTo(AccumulationMode.ACCUMULATING_FIRED_PANES));
    assertThat(strategy.getAllowedLateness(), equalTo(Duration.standardDays(1)));
  }

  @Test
  public void testWindowPropagatesEachPart() {
    FixedWindows fixed10 = FixedWindows.of(Duration.standardMinutes(10));
    Repeatedly trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));
    WindowingStrategy<?, ?> strategy = pipeline
      .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
      .apply("Mode", Window.<String>configure().accumulatingFiredPanes())
      .apply("Lateness", Window.<String>configure().withAllowedLateness(Duration.standardDays(1)))
      .apply("Trigger", Window.<String>configure().triggering(trigger))
      .apply("Window", Window.<String>into(fixed10))
      .getWindowingStrategy();

    assertEquals(fixed10, strategy.getWindowFn());
    assertEquals(trigger, strategy.getTrigger());
    assertEquals(AccumulationMode.ACCUMULATING_FIRED_PANES, strategy.getMode());
    assertEquals(Duration.standardDays(1), strategy.getAllowedLateness());
  }

  @Test
  public void testWindowIntoPropagatesLateness() {

    FixedWindows fixed10 = FixedWindows.of(Duration.standardMinutes(10));
    FixedWindows fixed25 = FixedWindows.of(Duration.standardMinutes(25));
    WindowingStrategy<?, ?> strategy = pipeline
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

  /**
   * With {@link #testWindowIntoNullWindowFnNoAssign()}, demonstrates that the expansions of the
   * {@link Window} transform depends on if it actually assigns elements to windows.
   */
  @Test
  public void testWindowIntoWindowFnAssign() {
    pipeline
        .apply(Create.of(1, 2, 3))
        .apply(
            Window.<Integer>into(
                FixedWindows.of(Duration.standardMinutes(11L).plus(Duration.millis(1L)))));

    final AtomicBoolean foundAssign = new AtomicBoolean(false);
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            if (node.getTransform() instanceof Window.Assign) {
              foundAssign.set(true);
            }
          }
        });
    assertThat(foundAssign.get(), is(true));
  }

  /**
   * With {@link #testWindowIntoWindowFnAssign()}, demonstrates that the expansions of the
   * {@link Window} transform depends on if it actually assigns elements to windows.
   */
  @Test
  public void testWindowIntoNullWindowFnNoAssign() {
    pipeline
        .apply(Create.of(1, 2, 3))
        .apply(
            Window.<Integer>configure().triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO)
                .accumulatingFiredPanes());

    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            assertThat(node.getTransform(), not(instanceOf(Window.Assign.class)));
          }
        });
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

    PCollection<String> input =
        pipeline
            .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
            .apply("Window", Window.<String>into(fixed10));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("requires that the accumulation mode");
    input.apply(
        "Triggering",
        Window.<String>configure()
            .withAllowedLateness(Duration.standardDays(1))
            .triggering(trigger));
  }

  @Test
  public void testMissingModeViaLateness() {
    FixedWindows fixed = FixedWindows.of(Duration.standardMinutes(10));
    PCollection<String> input =
        pipeline
            .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
            .apply("Window", Window.<String>into(fixed));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("allowed lateness");
    thrown.expectMessage("accumulation mode be specified");
    input.apply(
        "Lateness", Window.<String>configure().withAllowedLateness(Duration.standardDays(1)));
  }

  @Test
  public void testMissingLateness() {
    FixedWindows fixed10 = FixedWindows.of(Duration.standardMinutes(10));
    Repeatedly trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("requires that the allowed lateness");
    pipeline
      .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
      .apply("Mode", Window.<String>configure().accumulatingFiredPanes())
      .apply("Window", Window.<String>into(fixed10))
      .apply("Trigger", Window.<String>configure().triggering(trigger));
  }

  private static class WindowOddEvenBuckets extends NonMergingWindowFn<Long, IntervalWindow> {
    private static final IntervalWindow EVEN_WINDOW =
        new IntervalWindow(
            BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE.maxTimestamp());
    private static final IntervalWindow ODD_WINDOW =
        new IntervalWindow(
            BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE.maxTimestamp().minus(1));

    @Override
    public Collection<IntervalWindow> assignWindows(AssignContext c) throws Exception {
      if (c.element() % 2 == 0) {
        return Collections.singleton(EVEN_WINDOW);
      }
      return Collections.singleton(ODD_WINDOW);
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return other instanceof WindowOddEvenBuckets;
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
      return new IntervalWindow.IntervalWindowCoder();
    }

    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
      throw new UnsupportedOperationException(
          String.format("Can't use %s for side inputs", getClass().getSimpleName()));
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testNoWindowFnDoesNotReassignWindows() {
    pipeline.enableAbandonedNodeEnforcement(true);

    final PCollection<Long> initialWindows =
        pipeline
            .apply(CountingInput.upTo(10L))
            .apply("AssignWindows", Window.into(new WindowOddEvenBuckets()));

    // Sanity check the window assignment to demonstrate the baseline
    PAssert.that(initialWindows)
        .inWindow(WindowOddEvenBuckets.EVEN_WINDOW)
        .containsInAnyOrder(0L, 2L, 4L, 6L, 8L);
    PAssert.that(initialWindows)
        .inWindow(WindowOddEvenBuckets.ODD_WINDOW)
        .containsInAnyOrder(1L, 3L, 5L, 7L, 9L);

    PCollection<Boolean> upOne =
        initialWindows.apply(
            "ModifyTypes",
            MapElements.<Long, Boolean>via(
                new SimpleFunction<Long, Boolean>() {
                  @Override
                  public Boolean apply(Long input) {
                    return input % 2 == 0;
                  }
                }));
    PAssert.that(upOne)
        .inWindow(WindowOddEvenBuckets.EVEN_WINDOW)
        .containsInAnyOrder(true, true, true, true, true);
    PAssert.that(upOne)
        .inWindow(WindowOddEvenBuckets.ODD_WINDOW)
        .containsInAnyOrder(false, false, false, false, false);

    // The elements should be in the same windows, even though they would not be assigned to the
    // same windows with the updated timestamps. If we try to apply the original WindowFn, the type
    // will not be appropriate and the runner should crash, as a Boolean cannot be converted into
    // a long.
    PCollection<Boolean> updatedTrigger =
        upOne.apply(
            "UpdateWindowingStrategy",
            Window.<Boolean>configure().triggering(Never.ever())
                .withAllowedLateness(Duration.ZERO)
                .accumulatingFiredPanes());
    pipeline.run();
  }

  /**
   * Tests that when two elements are combined via a GroupByKey their output timestamp agrees
   * with the windowing function default, the end of the window.
   */
  @Test
  @Category(ValidatesRunner.class)
  public void testOutputTimeFnDefault() {
    pipeline.enableAbandonedNodeEnforcement(true);

    pipeline
        .apply(
            Create.timestamped(
                TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
                TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
        .apply(Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10))))
        .apply(GroupByKey.<Integer, String>create())
        .apply(
            ParDo.of(
                new DoFn<KV<Integer, Iterable<String>>, Void>() {
                  @ProcessElement
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
  @Category(ValidatesRunner.class)
  public void testOutputTimeFnEndOfWindow() {
    pipeline.enableAbandonedNodeEnforcement(true);

    pipeline.apply(
        Create.timestamped(
            TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
            TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
        .apply(Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
            .withOutputTimeFn(OutputTimeFns.outputAtEndOfWindow()))
        .apply(GroupByKey.<Integer, String>create())
        .apply(ParDo.of(new DoFn<KV<Integer, Iterable<String>>, Void>() {
          @ProcessElement
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

    Window<?> window = Window
        .into(windowFn)
        .triggering(triggerBuilder)
        .accumulatingFiredPanes()
        .withAllowedLateness(allowedLateness, closingBehavior)
        .withOutputTimeFn(outputTimeFn);

    DisplayData displayData = DisplayData.from(window);

    assertThat(displayData, hasDisplayItem("windowFn", windowFn.getClass()));
    assertThat(displayData, includesDisplayDataFor("windowFn", windowFn));

    assertThat(displayData, hasDisplayItem("trigger", triggerBuilder.toString()));
    assertThat(displayData,
        hasDisplayItem("accumulationMode", AccumulationMode.ACCUMULATING_FIRED_PANES.toString()));
    assertThat(displayData,
        hasDisplayItem("allowedLateness", allowedLateness));
    assertThat(displayData, hasDisplayItem("closingBehavior", closingBehavior.toString()));
    assertThat(displayData, hasDisplayItem("outputTimeFn", outputTimeFn.getClass()));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testPrimitiveDisplayData() {
    FixedWindows windowFn = FixedWindows.of(Duration.standardHours(5));
    AfterWatermark.FromEndOfWindow triggerBuilder = AfterWatermark.pastEndOfWindow();
    Duration allowedLateness = Duration.standardMinutes(10);
    Window.ClosingBehavior closingBehavior = Window.ClosingBehavior.FIRE_IF_NON_EMPTY;
    OutputTimeFn<BoundedWindow> outputTimeFn = OutputTimeFns.outputAtEndOfWindow();

    Window<?> window = Window
        .into(windowFn)
        .triggering(triggerBuilder)
        .accumulatingFiredPanes()
        .withAllowedLateness(allowedLateness, closingBehavior)
        .withOutputTimeFn(outputTimeFn);

    DisplayData primitiveDisplayData =
        Iterables.getOnlyElement(
            DisplayDataEvaluator.create().displayDataForPrimitiveTransforms(window));

    assertThat(primitiveDisplayData, hasDisplayItem("windowFn", windowFn.getClass()));
    assertThat(primitiveDisplayData, includesDisplayDataFor("windowFn", windowFn));

    assertThat(primitiveDisplayData, hasDisplayItem("trigger", triggerBuilder.toString()));
    assertThat(primitiveDisplayData,
        hasDisplayItem("accumulationMode", AccumulationMode.ACCUMULATING_FIRED_PANES.toString()));
    assertThat(primitiveDisplayData,
        hasDisplayItem("allowedLateness", allowedLateness));
    assertThat(primitiveDisplayData, hasDisplayItem("closingBehavior", closingBehavior.toString()));
    assertThat(primitiveDisplayData, hasDisplayItem("outputTimeFn", outputTimeFn.getClass()));
  }

  @Test
  public void testAssignDisplayDataUnchanged() {
    FixedWindows windowFn = FixedWindows.of(Duration.standardHours(5));

    Window<Object> original = Window.into(windowFn);
    WindowingStrategy<?, ?> updated = WindowingStrategy.globalDefault().withWindowFn(windowFn);

    DisplayData displayData = DisplayData.from(new Window.Assign<>(original, updated));

    assertThat(displayData, hasDisplayItem("windowFn", windowFn.getClass()));
    assertThat(displayData, includesDisplayDataFor("windowFn", windowFn));

    assertThat(displayData, not(hasDisplayItem("trigger")));
    assertThat(displayData, not(hasDisplayItem("accumulationMode")));
    assertThat(displayData, not(hasDisplayItem("allowedLateness")));
    assertThat(displayData, not(hasDisplayItem("closingBehavior")));
    assertThat(displayData, not(hasDisplayItem("outputTimeFn")));
  }

  @Test
  public void testDisplayDataExcludesUnspecifiedProperties() {
    Window<?> onlyHasAccumulationMode = Window.<Object>configure().discardingFiredPanes();
    assertThat(DisplayData.from(onlyHasAccumulationMode), not(hasDisplayItem(hasKey(isOneOf(
        "windowFn",
        "trigger",
        "outputTimeFn",
        "allowedLateness",
        "closingBehavior")))));

    Window<?> noAccumulationMode = Window.into(new GlobalWindows());
    assertThat(DisplayData.from(noAccumulationMode),
        not(hasDisplayItem(hasKey("accumulationMode"))));
  }

  @Test
  public void testDisplayDataExcludesDefaults() {
    Window<?> window = Window.into(new GlobalWindows())
        .triggering(DefaultTrigger.of())
        .withAllowedLateness(Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));

    DisplayData data = DisplayData.from(window);
    assertThat(data, not(hasDisplayItem("trigger")));
    assertThat(data, not(hasDisplayItem("allowedLateness")));
  }
}
