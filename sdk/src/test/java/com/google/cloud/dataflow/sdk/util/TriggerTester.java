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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;
import com.google.cloud.dataflow.sdk.util.state.InMemoryStateInternals;
import com.google.cloud.dataflow.sdk.util.state.State;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.WatermarkStateInternal;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * Test utility that runs a {@link WindowFn}, {@link Trigger} using in-memory stub implementations
 * to provide the {@link TimerInternals} and {@link WindowingInternals} needed to run
 * {@code Trigger}s and {@code ReduceFn}s.
 *
 * <p>To have all interactions between the trigger and underlying components logged, call
 * {@link #logInteractions(boolean)}.
 *
 * @param <InputT> The element types.
 * @param <OutputT> The final type for elements in the window (for instance,
 *     {@code Iterable<InputT>})
 * @param <W> The type of windows being used.
 */
public class TriggerTester<InputT, OutputT, W extends BoundedWindow> {

  private static final Logger LOGGER = Logger.getLogger(TriggerTester.class.getName());

  private Instant watermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private Instant processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private final BatchTimerInternals timerInternals = new BatchTimerInternals(processingTime);
  private final ReduceFnRunner<String, InputT, OutputT, W> runner;
  private final WindowFn<Object, W> windowFn;
  private final StubContexts stubContexts;
  private final Coder<OutputT> outputCoder;

  private static final String KEY = "TEST_KEY";
  private boolean logInteractions = false;
  private ExecutableTrigger<W> executableTrigger;

  private final InMemoryLongSumAggregator droppedDueToClosedWindow =
      new InMemoryLongSumAggregator(ReduceFnRunner.DROPPED_DUE_TO_CLOSED_WINDOW_COUNTER);
  private final InMemoryLongSumAggregator droppedDueToLateness =
      new InMemoryLongSumAggregator(ReduceFnRunner.DROPPED_DUE_TO_LATENESS_COUNTER);

  private void logInteraction(String fmt, Object... args) {
    if (logInteractions) {
      LOGGER.warning("Trigger Interaction: " + String.format(fmt, args));
    }
  }

  public static <W extends BoundedWindow> TriggerTester<Integer, Iterable<Integer>, W> nonCombining(
      WindowingStrategy<?, W> windowingStrategy) throws Exception {
    return new TriggerTester<Integer, Iterable<Integer>, W>(
        windowingStrategy,
        SystemReduceFn.<String, Integer, W>buffering(VarIntCoder.of()).create(KEY),
        IterableCoder.of(VarIntCoder.of()));
  }

  public static <W extends BoundedWindow> TriggerTester<Integer, Iterable<Integer>, W> nonCombining(
      WindowFn<?, W> windowFn, Trigger<W> trigger, AccumulationMode mode,
      Duration allowedDataLateness) throws Exception {

    WindowingStrategy<?, W> strategy = WindowingStrategy.of(windowFn)
        .withTrigger(trigger)
        .withMode(mode)
        .withAllowedLateness(allowedDataLateness);
    return nonCombining(strategy);
  }

  public static <W extends BoundedWindow, AccumT, OutputT>
      TriggerTester<Integer, OutputT, W> combining(
          WindowFn<?, W> windowFn, Trigger<W> trigger, AccumulationMode mode,
          KeyedCombineFn<String, Integer, AccumT, OutputT> combineFn,
          Coder<OutputT> outputCoder,
          Duration allowedDataLateness) throws Exception {

    WindowingStrategy<?, W> strategy = WindowingStrategy.of(windowFn)
        .withTrigger(trigger)
        .withMode(mode)
        .withAllowedLateness(allowedDataLateness);

    CoderRegistry registry = new CoderRegistry();
    registry.registerStandardCoders();
    AppliedCombineFn<String, Integer, AccumT, OutputT> fn =
        AppliedCombineFn.<String, Integer, AccumT, OutputT>withInputCoder(
            combineFn, registry, KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

    return new TriggerTester<Integer, OutputT, W>(
        strategy,
        SystemReduceFn.<String, Integer, AccumT, OutputT, W>combining(
            StringUtf8Coder.of(), fn).create(KEY),
        outputCoder);
  }

  private TriggerTester(
      WindowingStrategy<?, W> wildcardStrategy,
      ReduceFn<String, InputT, OutputT, W> reduceFn,
      Coder<OutputT> outputCoder) throws Exception {
    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> objectStrategy = (WindowingStrategy<Object, W>) wildcardStrategy;

    this.windowFn = objectStrategy.getWindowFn();
    this.stubContexts = new StubContexts();
    this.outputCoder = outputCoder;
    executableTrigger = wildcardStrategy.getTrigger();

    this.runner = new ReduceFnRunner<>(
        KEY, objectStrategy, timerInternals, stubContexts,
        droppedDueToClosedWindow, droppedDueToLateness, reduceFn);
  }

  public ExecutableTrigger<W> getTrigger() {
    return executableTrigger;
  }

  public void logInteractions(boolean logInteractions) {
    this.logInteractions = logInteractions;
  }

  public boolean isMarkedFinished(W window) {
    return runner.isFinished(window);
  }

  @SafeVarargs
  public final void assertHasOnlyGlobalAndFinishedSetsFor(W... expectedWindows) {
    assertHasOnlyGlobalAndAllowedTags(
        ImmutableSet.copyOf(expectedWindows),
        ImmutableSet.<StateTag<?>>of(TriggerRunner.FINISHED_BITS_TAG));
  }

  @SafeVarargs
  public final void assertHasOnlyGlobalAndFinishedSetsAndPaneInfoFor(W... expectedWindows) {
    assertHasOnlyGlobalAndAllowedTags(
        ImmutableSet.copyOf(expectedWindows),
        ImmutableSet.<StateTag<?>>of(
            TriggerRunner.FINISHED_BITS_TAG,
            PaneInfoTracker.PANE_INFO_TAG,
            WatermarkHold.DATA_HOLD_TAG));
  }

  public final void assertHasOnlyGlobalState() {
    assertHasOnlyGlobalAndAllowedTags(
        Collections.<W>emptySet(), Collections.<StateTag<?>>emptySet());
  }

  @SafeVarargs
  public final void assertHasOnlyGlobalAndPaneInfoFor(W... expectedWindows) {
    assertHasOnlyGlobalAndAllowedTags(
        ImmutableSet.copyOf(expectedWindows),
        ImmutableSet.<StateTag<?>>of(PaneInfoTracker.PANE_INFO_TAG, WatermarkHold.DATA_HOLD_TAG));
  }

  /**
   * Verifies that the the set of windows that have any state stored is exactly
   * {@code expectedWindows} and that each of these windows has only tags from {@code allowedTags}.
   */
  private void assertHasOnlyGlobalAndAllowedTags(
      Set<W> expectedWindows, Set<StateTag<?>> allowedTags) {
    runner.persist();

    Set<StateNamespace> expectedWindowsSet = new HashSet<>();
    for (W expectedWindow : expectedWindows) {
      expectedWindowsSet.add(windowNamespace(expectedWindow));
    }
    Set<StateNamespace> actualWindows = new HashSet<>();

    for (StateNamespace namespace : stubContexts.state.getNamespacesInUse()) {
      if (namespace instanceof StateNamespaces.GlobalNamespace) {
        continue;
      } else if (namespace instanceof StateNamespaces.WindowNamespace) {
        Set<StateTag<?>> tagsInUse = stubContexts.state.getTagsInUse(namespace);
        if (tagsInUse.isEmpty()) {
          continue;
        }
        actualWindows.add(namespace);
        Set<StateTag<?>> unexpected = Sets.difference(tagsInUse, allowedTags);
        if (unexpected.isEmpty()) {
          continue;
        } else {
          fail(namespace + " has unexpected states: " + tagsInUse);
        }
      } else if (namespace instanceof StateNamespaces.WindowAndTriggerNamespace) {
        Set<StateTag<?>> tagsInUse = stubContexts.state.getTagsInUse(namespace);
        assertTrue(namespace + " contains " + tagsInUse, tagsInUse.isEmpty());
      } else {
        fail("Unrecognized namespace " + namespace);
      }
    }

    assertEquals(expectedWindowsSet, actualWindows);
  }

  private StateNamespace windowNamespace(W window) {
    return StateNamespaces.window(windowFn.windowCoder(), window);
  }

  public Instant getWatermarkHold() {
    runner.persist();
    return stubContexts.state.minimumWatermarkHold();
  }

  public long getElementsDroppedDueToClosedWindow() {
    return droppedDueToClosedWindow.getSum();
  }

  public long getElementsDroppedDueToLateness() {
    return droppedDueToLateness.getSum();
  }

  /**
   * Retrieve the values that have been output to this time, and clear out the output accumulator.
   */
  public List<WindowedValue<OutputT>> extractOutput() {
    ImmutableList<WindowedValue<OutputT>> result = FluentIterable.from(stubContexts.outputs)
        .transform(new Function<WindowedValue<KV<String, OutputT>>, WindowedValue<OutputT>>() {
          @Override
          @Nullable
          public WindowedValue<OutputT> apply(@Nullable WindowedValue<KV<String, OutputT>> input) {
            return input.withValue(input.getValue().getValue());
          }
        })
        .toList();
    stubContexts.outputs.clear();
    return result;
  }

  /** Advance the watermark to the specified time, firing any timers that should fire. */
  public void advanceWatermark(Instant newWatermark) throws Exception {
    Preconditions.checkState(!newWatermark.isBefore(watermark),
        "Cannot move watermark time backwards from %s to %s",
        watermark.getMillis(), newWatermark.getMillis());
    logInteraction("Advancing watermark to %d", newWatermark.getMillis());
    watermark = newWatermark;
    timerInternals.advanceWatermark(runner, newWatermark);
  }

  /** Advance the processing time to the specified time, firing any timers that should fire. */
  public void advanceProcessingTime(
      Instant newProcessingTime) throws Exception {
    Preconditions.checkState(!newProcessingTime.isBefore(processingTime),
        "Cannot move processing time backwards from %s to %s",
        processingTime.getMillis(), newProcessingTime.getMillis());
    logInteraction("Advancing processing time to %d", newProcessingTime.getMillis());
    processingTime = newProcessingTime;
    timerInternals.advanceProcessingTime(runner, newProcessingTime);
  }

  public void injectElement(InputT value, Instant timestamp) throws Exception {
    Collection<W> windows = windowFn.assignWindows(new TriggerTester.StubAssignContext<W>(
        windowFn, value, timestamp, Arrays.asList(GlobalWindow.INSTANCE)));
    logInteraction("Element %s at time %d put in windows %s",
        value, timestamp.getMillis(), windows);
    runner.processElement(WindowedValue.of(value, timestamp, windows, PaneInfo.NO_FIRING));
  }

  public void doMerge() throws Exception {
    runner.merge();
  }

  public void fireTimer(W window, Instant timestamp, TimeDomain domain) {
    runner.onTimer(TimerData.of(
        StateNamespaces.window(windowFn.windowCoder(), window), timestamp, domain));
  }

  private static class TestingInMemoryStateInternals extends InMemoryStateInternals {

    protected Set<StateTag<?>> getTagsInUse(StateNamespace namespace) {
      Set<StateTag<?>> inUse = new HashSet<>();
      for (Map.Entry<StateTag<?>, State> entry : inMemoryState.getTagsInUse(namespace).entrySet()) {
        if (!isEmptyForTesting(entry.getValue())) {
          inUse.add(entry.getKey());
        }
      }
      return inUse;
    }

    public Set<StateNamespace> getNamespacesInUse() {
      return inMemoryState.getNamespacesInUse();
    }

    public Instant minimumWatermarkHold() {
      Instant minimum = null;
      for (State storage : inMemoryState.values()) {
        if (storage instanceof WatermarkStateInternal) {
          Instant hold = ((WatermarkStateInternal) storage).get().read();
          if (minimum == null || (hold != null && hold.isBefore(minimum))) {
            minimum = hold;
          }
        }
      }
      return minimum;
    }
  }

  private class StubContexts implements WindowingInternals<InputT, KV<String, OutputT>> {

    private TestingInMemoryStateInternals state = new TestingInMemoryStateInternals();

    private List<WindowedValue<KV<String, OutputT>>> outputs = new ArrayList<>();

    @Override
    public void outputWindowedValue(KV<String, OutputT> output, Instant timestamp,
        Collection<? extends BoundedWindow> windows, PaneInfo pane) {
      // Copy the output value (using coders) before capturing it.
      KV<String, OutputT> copy = SerializableUtils.<KV<String, OutputT>>ensureSerializableByCoder(
          KvCoder.of(StringUtf8Coder.of(), outputCoder), output, "outputForWindow");
      WindowedValue<KV<String, OutputT>> value = WindowedValue.of(copy, timestamp, windows, pane);
      logInteraction("Outputting: %s", value);
      outputs.add(value);
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException(
          "getTimerInternals() should not be called on StubContexts.");
    }

    @Override
    public Collection<? extends BoundedWindow> windows() {
      throw new UnsupportedOperationException(
          "Testing triggers should not use windows from WindowingInternals.");
    }

    @Override
    public PaneInfo pane() {
      throw new UnsupportedOperationException(
          "Testing triggers should not use pane from WindowingInternals.");
    }
    @Override
    public <T> void writePCollectionViewData(TupleTag<?> tag, Iterable<WindowedValue<T>> data,
        Coder<T> elemCoder) throws IOException {
      throw new UnsupportedOperationException(
          "Testing triggers should not use writePCollectionViewData from WindowingInternals.");
    }

    @Override
    public StateInternals stateInternals() {
      return state;
    }
  }

  private static class StubAssignContext<W extends BoundedWindow>
      extends WindowFn<Object, W>.AssignContext {
    private Object element;
    private Instant timestamp;
    private Collection<? extends BoundedWindow> windows;

    public StubAssignContext(WindowFn<Object, W> windowFn,
        Object element, Instant timestamp, Collection<? extends BoundedWindow> windows) {
      windowFn.super();
      this.element = element;
      this.timestamp = timestamp;
      this.windows = windows;
    }

    @Override
    public Object element() {
      return element;
    }

    @Override
    public Instant timestamp() {
      return timestamp;
    }

    @Override
    public Collection<? extends BoundedWindow> windows() {
      return windows;
    }
  }

  private static class InMemoryLongSumAggregator implements Aggregator<Long, Long> {

    private final String name;
    private long sum = 0;

    public InMemoryLongSumAggregator(String name) {
      this.name = name;
    }

    @Override
    public void addValue(Long value) {
      sum += value;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public CombineFn<Long, ?, Long> getCombineFn() {
      return new Sum.SumLongFn();
    }

    public long getSum() {
      return sum;
    }
  }
}
