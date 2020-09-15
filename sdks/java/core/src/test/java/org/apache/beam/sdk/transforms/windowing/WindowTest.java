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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesCustomWindowMerging;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
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

/** Tests for {@link Window}. */
@RunWith(JUnit4.class)
public class WindowTest implements Serializable {

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testWindowIntoSetWindowfn() {
    WindowingStrategy<?, ?> strategy =
        pipeline
            .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(10))))
            .getWindowingStrategy();
    assertTrue(strategy.getWindowFn() instanceof FixedWindows);
    assertTrue(strategy.getTrigger() instanceof DefaultTrigger);
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, strategy.getMode());
  }

  @Test
  public void testWindowIntoTriggersAndAccumulating() {
    FixedWindows fixed10 = FixedWindows.of(Duration.standardMinutes(10));
    Repeatedly trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5));
    WindowingStrategy<?, ?> strategy =
        pipeline
            .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
            .apply(
                Window.<String>into(fixed10)
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
    WindowingStrategy<?, ?> strategy =
        pipeline
            .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
            .apply("Mode", Window.<String>configure().accumulatingFiredPanes())
            .apply(
                "Lateness",
                Window.<String>configure().withAllowedLateness(Duration.standardDays(1)))
            .apply("Trigger", Window.<String>configure().triggering(trigger))
            .apply("Window", Window.into(fixed10))
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
    WindowingStrategy<?, ?> strategy =
        pipeline
            .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
            .apply(
                "WindowInto10",
                Window.<String>into(fixed10)
                    .withAllowedLateness(Duration.standardDays(1))
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(5)))
                    .accumulatingFiredPanes())
            .apply("WindowInto25", Window.into(fixed25))
            .getWindowingStrategy();

    assertEquals(Duration.standardDays(1), strategy.getAllowedLateness());
    assertEquals(fixed25, strategy.getWindowFn());
  }

  @Test
  public void testWindowIntoAssignesLongerAllowedLateness() {

    FixedWindows fixed10 = FixedWindows.of(Duration.standardMinutes(10));
    FixedWindows fixed25 = FixedWindows.of(Duration.standardMinutes(25));

    PCollection<String> notChanged =
        pipeline
            .apply(Create.of("hello", "world").withCoder(StringUtf8Coder.of()))
            .apply(
                "WindowInto25",
                Window.<String>into(fixed25)
                    .withAllowedLateness(Duration.standardDays(1))
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(5)))
                    .accumulatingFiredPanes())
            .apply(
                "WindowInto10",
                Window.<String>into(fixed10).withAllowedLateness(Duration.standardDays(2)));

    assertEquals(Duration.standardDays(2), notChanged.getWindowingStrategy().getAllowedLateness());

    PCollection<String> data =
        pipeline.apply(
            "createChanged", Create.of("hello", "world").withCoder(StringUtf8Coder.of()));

    PCollection<String> longWindow =
        data.apply(
            "WindowInto25c",
            Window.<String>into(fixed25)
                .withAllowedLateness(Duration.standardDays(1))
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(5)))
                .accumulatingFiredPanes());

    assertEquals(Duration.standardDays(1), longWindow.getWindowingStrategy().getAllowedLateness());

    PCollection<String> autoCorrectedWindow =
        longWindow.apply(
            "WindowInto10c",
            Window.<String>into(fixed10).withAllowedLateness(Duration.standardHours(1)));

    assertEquals(
        Duration.standardDays(1), autoCorrectedWindow.getWindowingStrategy().getAllowedLateness());
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
            Window.into(FixedWindows.of(Duration.standardMinutes(11L).plus(Duration.millis(1L)))));

    final AtomicBoolean foundAssign = new AtomicBoolean(false);
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            if (node.getTransform() instanceof Window.Assign) {
              foundAssign.set(true);
            }
          }
        });
    assertThat(foundAssign.get(), is(true));
  }

  /**
   * With {@link #testWindowIntoWindowFnAssign()}, demonstrates that the expansions of the {@link
   * Window} transform depends on if it actually assigns elements to windows.
   */
  @Test
  public void testWindowIntoNullWindowFnNoAssign() {
    pipeline
        .apply(Create.of(1, 2, 3))
        .apply(
            Window.<Integer>configure()
                .triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO)
                .accumulatingFiredPanes());

    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            assertThat(node.getTransform(), not(instanceOf(Window.Assign.class)));
          }
        });
  }

  @Test
  public void testWindowGetName() {
    assertEquals(
        "Window.Into()",
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
            .apply("Window", Window.into(fixed10));
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
            .apply("Window", Window.into(fixed));
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
        .apply("Window", Window.into(fixed10))
        .apply("Trigger", Window.<String>configure().triggering(trigger));
  }

  private static class WindowOddEvenBuckets extends NonMergingWindowFn<Long, IntervalWindow> {
    private static final IntervalWindow EVEN_WINDOW =
        new IntervalWindow(BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE.maxTimestamp());
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
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (!this.isCompatible(other)) {
        throw new IncompatibleWindowException(
            other, "WindowOddEvenBuckets is only compatible with WindowOddEvenBuckets.");
      }
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
  @Category({ValidatesRunner.class})
  public void testNoWindowFnDoesNotReassignWindows() {
    pipeline.enableAbandonedNodeEnforcement(true);

    final PCollection<Long> initialWindows =
        pipeline
            .apply(GenerateSequence.from(0).to(10))
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
            MapElements.via(
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
    upOne.apply(
        "UpdateWindowingStrategy",
        Window.<Boolean>configure()
            .triggering(Never.ever())
            .withAllowedLateness(Duration.ZERO)
            .accumulatingFiredPanes());
    pipeline.run();
  }

  /**
   * Tests that when two elements are combined via a GroupByKey their output timestamp agrees with
   * the windowing function default, the end of the window.
   */
  @Test
  @Category(ValidatesRunner.class)
  public void testTimestampCombinerDefault() {
    pipeline.enableAbandonedNodeEnforcement(true);

    pipeline
        .apply(
            Create.timestamped(
                TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
                TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(10))))
        .apply(GroupByKey.create())
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
   * Tests that when two elements are combined via a GroupByKey their output timestamp agrees with
   * the windowing function customized to use the end of the window.
   */
  @Test
  @Category(ValidatesRunner.class)
  public void testTimestampCombinerEndOfWindow() {
    pipeline.enableAbandonedNodeEnforcement(true);

    pipeline
        .apply(
            Create.timestamped(
                TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
                TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
        .apply(
            Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
                .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW))
        .apply(GroupByKey.create())
        .apply(
            ParDo.of(
                new DoFn<KV<Integer, Iterable<String>>, Void>() {
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
    TimestampCombiner timestampCombiner = TimestampCombiner.END_OF_WINDOW;

    Window<?> window =
        Window.into(windowFn)
            .triggering(triggerBuilder)
            .accumulatingFiredPanes()
            .withAllowedLateness(allowedLateness, closingBehavior)
            .withTimestampCombiner(timestampCombiner);

    DisplayData displayData = DisplayData.from(window);

    assertThat(displayData, hasDisplayItem("windowFn", windowFn.getClass()));
    assertThat(displayData, includesDisplayDataFor("windowFn", windowFn));

    assertThat(displayData, hasDisplayItem("trigger", triggerBuilder.toString()));
    assertThat(
        displayData,
        hasDisplayItem("accumulationMode", AccumulationMode.ACCUMULATING_FIRED_PANES.toString()));
    assertThat(displayData, hasDisplayItem("allowedLateness", allowedLateness));
    assertThat(displayData, hasDisplayItem("closingBehavior", closingBehavior.toString()));
    assertThat(displayData, hasDisplayItem("timestampCombiner", timestampCombiner.toString()));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testPrimitiveDisplayData() {
    FixedWindows windowFn = FixedWindows.of(Duration.standardHours(5));
    AfterWatermark.FromEndOfWindow triggerBuilder = AfterWatermark.pastEndOfWindow();
    Duration allowedLateness = Duration.standardMinutes(10);
    Window.ClosingBehavior closingBehavior = Window.ClosingBehavior.FIRE_IF_NON_EMPTY;
    TimestampCombiner timestampCombiner = TimestampCombiner.END_OF_WINDOW;

    Window<?> window =
        Window.into(windowFn)
            .triggering(triggerBuilder)
            .accumulatingFiredPanes()
            .withAllowedLateness(allowedLateness, closingBehavior)
            .withTimestampCombiner(timestampCombiner);

    DisplayData primitiveDisplayData =
        Iterables.getOnlyElement(
            DisplayDataEvaluator.create().displayDataForPrimitiveTransforms(window));

    assertThat(primitiveDisplayData, hasDisplayItem("windowFn", windowFn.getClass()));
    assertThat(primitiveDisplayData, includesDisplayDataFor("windowFn", windowFn));

    assertThat(primitiveDisplayData, hasDisplayItem("trigger", triggerBuilder.toString()));
    assertThat(
        primitiveDisplayData,
        hasDisplayItem("accumulationMode", AccumulationMode.ACCUMULATING_FIRED_PANES.toString()));
    assertThat(primitiveDisplayData, hasDisplayItem("allowedLateness", allowedLateness));
    assertThat(primitiveDisplayData, hasDisplayItem("closingBehavior", closingBehavior.toString()));
    assertThat(
        primitiveDisplayData, hasDisplayItem("timestampCombiner", timestampCombiner.toString()));
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
    assertThat(displayData, not(hasDisplayItem("timestampCombiner")));
  }

  @Test
  public void testDisplayDataExcludesUnspecifiedProperties() {
    Window<?> onlyHasAccumulationMode = Window.configure().discardingFiredPanes();
    assertThat(
        DisplayData.from(onlyHasAccumulationMode),
        not(
            hasDisplayItem(
                hasKey(
                    isOneOf(
                        "windowFn",
                        "trigger",
                        "timestampCombiner",
                        "allowedLateness",
                        "closingBehavior")))));

    Window<?> noAccumulationMode = Window.into(new GlobalWindows());
    assertThat(
        DisplayData.from(noAccumulationMode), not(hasDisplayItem(hasKey("accumulationMode"))));
  }

  @Test
  public void testDisplayDataExcludesDefaults() {
    Window<?> window =
        Window.into(new GlobalWindows())
            .triggering(DefaultTrigger.of())
            .withAllowedLateness(Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));

    DisplayData data = DisplayData.from(window);
    assertThat(data, not(hasDisplayItem("trigger")));
    assertThat(data, not(hasDisplayItem("allowedLateness")));
  }

  @Test
  @Category({ValidatesRunner.class, UsesCustomWindowMerging.class})
  public void testMergingCustomWindows() {
    Instant startInstant = new Instant(0L);
    PCollection<String> inputCollection =
        pipeline.apply(
            Create.timestamped(
                TimestampedValue.of("big", startInstant.plus(Duration.standardSeconds(10))),
                TimestampedValue.of("small1", startInstant.plus(Duration.standardSeconds(20))),
                // This one will be outside of bigWindow thus not merged
                TimestampedValue.of("small2", startInstant.plus(Duration.standardSeconds(39)))));
    PCollection<String> windowedCollection =
        inputCollection.apply(Window.into(new CustomWindowFn<>()));
    PCollection<Long> count =
        windowedCollection.apply(Combine.globally(Count.<String>combineFn()).withoutDefaults());
    // "small1" and "big" elements merged into bigWindow "small2" not merged
    // because timestamp is not in bigWindow
    PAssert.that("Wrong number of elements in output collection", count).containsInAnyOrder(2L, 1L);
    pipeline.run();
  }

  //  This test is usefull because some runners have a special merge implementation
  // for keyed collections
  @Test
  @Category({ValidatesRunner.class, UsesCustomWindowMerging.class})
  public void testMergingCustomWindowsKeyedCollection() {
    Instant startInstant = new Instant(0L);
    PCollection<KV<Integer, String>> inputCollection =
        pipeline.apply(
            Create.timestamped(
                TimestampedValue.of(
                    KV.of(0, "big"), startInstant.plus(Duration.standardSeconds(10))),
                TimestampedValue.of(
                    KV.of(1, "small1"), startInstant.plus(Duration.standardSeconds(20))),
                // This element is not contained within the bigWindow and not merged
                TimestampedValue.of(
                    KV.of(2, "small2"), startInstant.plus(Duration.standardSeconds(39)))));
    PCollection<KV<Integer, String>> windowedCollection =
        inputCollection.apply(Window.into(new CustomWindowFn<>()));
    PCollection<Long> count =
        windowedCollection.apply(
            Combine.globally(Count.<KV<Integer, String>>combineFn()).withoutDefaults());
    // "small1" and "big" elements merged into bigWindow "small2" not merged
    // because it is not contained in bigWindow
    PAssert.that("Wrong number of elements in output collection", count).containsInAnyOrder(2L, 1L);
    pipeline.run();
  }

  private static class CustomWindow extends IntervalWindow {
    private boolean isBig;

    CustomWindow(Instant start, Instant end, boolean isBig) {
      super(start, end);
      this.isBig = isBig;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CustomWindow that = (CustomWindow) o;
      return super.equals(o) && this.isBig == that.isBig;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), isBig);
    }
  }

  private static class CustomWindowCoder extends CustomCoder<CustomWindow> {

    private static final CustomWindowCoder INSTANCE = new CustomWindowCoder();
    private static final Coder<IntervalWindow> INTERVAL_WINDOW_CODER = IntervalWindow.getCoder();
    private static final VarIntCoder VAR_INT_CODER = VarIntCoder.of();

    public static CustomWindowCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(CustomWindow window, OutputStream outStream) throws IOException {
      INTERVAL_WINDOW_CODER.encode(window, outStream);
      VAR_INT_CODER.encode(window.isBig ? 1 : 0, outStream);
    }

    @Override
    public CustomWindow decode(InputStream inStream) throws IOException {
      IntervalWindow superWindow = INTERVAL_WINDOW_CODER.decode(inStream);
      boolean isBig = VAR_INT_CODER.decode(inStream) != 0;
      return new CustomWindow(superWindow.start(), superWindow.end(), isBig);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      INTERVAL_WINDOW_CODER.verifyDeterministic();
      VAR_INT_CODER.verifyDeterministic();
    }
  }

  private static class CustomWindowFn<T> extends WindowFn<T, CustomWindow> {
    @Override
    public Collection<CustomWindow> assignWindows(AssignContext c) throws Exception {
      String element;
      // It loses genericity of type T but this is not a big deal for a test.
      // And it allows to avoid duplicating CustomWindowFn to support PCollection<KV>
      if (c.element() instanceof KV) {
        element = ((KV<Integer, String>) c.element()).getValue();
      } else {
        element = (String) c.element();
      }
      // put big elements in windows of 30s and small ones in windows of 5s
      if ("big".equals(element)) {
        return Collections.singletonList(
            new CustomWindow(
                c.timestamp(), c.timestamp().plus(Duration.standardSeconds(30)), true));
      } else {
        return Collections.singletonList(
            new CustomWindow(
                c.timestamp(), c.timestamp().plus(Duration.standardSeconds(5)), false));
      }
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {
      Map<CustomWindow, Set<CustomWindow>> windowsToMerge = new HashMap<>();
      for (CustomWindow window : c.windows()) {
        if (window.isBig) {
          HashSet<CustomWindow> windows = new HashSet<>();
          windows.add(window);
          windowsToMerge.put(window, windows);
        }
      }
      for (CustomWindow window : c.windows()) {
        for (Map.Entry<CustomWindow, Set<CustomWindow>> bigWindow : windowsToMerge.entrySet()) {
          if (bigWindow.getKey().contains(window)) {
            bigWindow.getValue().add(window);
          }
        }
      }
      for (Map.Entry<CustomWindow, Set<CustomWindow>> mergeEntry : windowsToMerge.entrySet()) {
        c.merge(mergeEntry.getValue(), mergeEntry.getKey());
      }
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return other instanceof CustomWindowFn;
    }

    @Override
    public Coder<CustomWindow> windowCoder() {
      return CustomWindowCoder.of();
    }

    @Override
    public WindowMappingFn<CustomWindow> getDefaultWindowMappingFn() {
      throw new UnsupportedOperationException("side inputs not supported");
    }
  }
}
