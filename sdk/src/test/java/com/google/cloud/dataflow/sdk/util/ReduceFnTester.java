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
import com.google.cloud.dataflow.sdk.transforms.windowing.TriggerBuilder;
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
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Test utility that runs a {@link ReduceFn}, {@link WindowFn}, {@link Trigger} using in-memory stub
 * implementations to provide the {@link TimerInternals} and {@link WindowingInternals} needed to
 * run {@code Trigger}s and {@code ReduceFn}s.
 *
 * @param <InputT> The element types.
 * @param <OutputT> The final type for elements in the window (for instance,
 *     {@code Iterable<InputT>})
 * @param <W> The type of windows being used.
 */
public class ReduceFnTester<InputT, OutputT, W extends BoundedWindow> {
  private final TestInMemoryStateInternals stateInternals = new TestInMemoryStateInternals();
  private final TestTimerInternals timerInternals = new TestTimerInternals();

  private final WindowFn<Object, W> windowFn;
  private final TestWindowingInternals windowingInternals;
  private final Coder<OutputT> outputCoder;
  private final WindowingStrategy<Object, W> objectStrategy;
  private final ReduceFn<String, InputT, OutputT, W> reduceFn;

  private static final String KEY = "TEST_KEY";
  private ExecutableTrigger<W> executableTrigger;

  private final InMemoryLongSumAggregator droppedDueToClosedWindow =
      new InMemoryLongSumAggregator(GroupAlsoByWindowsDoFn.DROPPED_DUE_TO_CLOSED_WINDOW_COUNTER);

  public static <W extends BoundedWindow> ReduceFnTester<Integer, Iterable<Integer>, W>
      nonCombining(WindowingStrategy<?, W> windowingStrategy) throws Exception {
    return new ReduceFnTester<Integer, Iterable<Integer>, W>(
        windowingStrategy,
        SystemReduceFn.<String, Integer, W>buffering(VarIntCoder.of()).create(KEY),
        IterableCoder.of(VarIntCoder.of()));
  }

  public static <W extends BoundedWindow> ReduceFnTester<Integer, Iterable<Integer>, W>
      nonCombining(WindowFn<?, W> windowFn, TriggerBuilder<W> trigger, AccumulationMode mode,
          Duration allowedDataLateness) throws Exception {
    WindowingStrategy<?, W> strategy =
        WindowingStrategy.of(windowFn)
            .withTrigger(trigger.buildTrigger())
            .withMode(mode)
            .withAllowedLateness(allowedDataLateness);
    return nonCombining(strategy);
  }

  public static <W extends BoundedWindow, AccumT, OutputT> ReduceFnTester<Integer, OutputT, W>
      combining(WindowingStrategy<?, W> strategy,
          KeyedCombineFn<String, Integer, AccumT, OutputT> combineFn,
          Coder<OutputT> outputCoder) throws Exception {

    CoderRegistry registry = new CoderRegistry();
    registry.registerStandardCoders();
    AppliedCombineFn<String, Integer, AccumT, OutputT> fn =
        AppliedCombineFn.<String, Integer, AccumT, OutputT>withInputCoder(
            combineFn, registry, KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

    return new ReduceFnTester<Integer, OutputT, W>(
        strategy,
        SystemReduceFn.<String, Integer, AccumT, OutputT, W>combining(StringUtf8Coder.of(), fn)
            .create(KEY),
        outputCoder);
  }

  public static <W extends BoundedWindow, AccumT, OutputT> ReduceFnTester<Integer, OutputT, W>
      combining(WindowFn<?, W> windowFn, Trigger<W> trigger, AccumulationMode mode,
          KeyedCombineFn<String, Integer, AccumT, OutputT> combineFn, Coder<OutputT> outputCoder,
          Duration allowedDataLateness) throws Exception {

    WindowingStrategy<?, W> strategy =
        WindowingStrategy.of(windowFn).withTrigger(trigger).withMode(mode).withAllowedLateness(
            allowedDataLateness);

    return combining(strategy, combineFn, outputCoder);
  }

  private ReduceFnTester(WindowingStrategy<?, W> wildcardStrategy,
      ReduceFn<String, InputT, OutputT, W> reduceFn, Coder<OutputT> outputCoder) throws Exception {
    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> objectStrategy = (WindowingStrategy<Object, W>) wildcardStrategy;

    this.objectStrategy = objectStrategy;
    this.reduceFn = reduceFn;
    this.windowFn = objectStrategy.getWindowFn();
    this.windowingInternals = new TestWindowingInternals();
    this.outputCoder = outputCoder;
    executableTrigger = wildcardStrategy.getTrigger();
  }

  ReduceFnRunner<String, InputT, OutputT, W> createRunner() {
    return new ReduceFnRunner<>(KEY, objectStrategy, timerInternals, windowingInternals,
        droppedDueToClosedWindow, reduceFn);
  }

  public ExecutableTrigger<W> getTrigger() {
    return executableTrigger;
  }

  public boolean isMarkedFinished(W window) {
    return createRunner().isFinished(window);
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
        ImmutableSet.<StateTag<?>>of(TriggerRunner.FINISHED_BITS_TAG, PaneInfoTracker.PANE_INFO_TAG,
            WatermarkHold.watermarkHoldTagForOutputTimeFn(objectStrategy.getOutputTimeFn()),
            WatermarkHold.EXTRA_HOLD_TAG));
  }

  public final void assertHasOnlyGlobalState() {
    assertHasOnlyGlobalAndAllowedTags(
        Collections.<W>emptySet(), Collections.<StateTag<?>>emptySet());
  }

  @SafeVarargs
  public final void assertHasOnlyGlobalAndPaneInfoFor(W... expectedWindows) {
    assertHasOnlyGlobalAndAllowedTags(
        ImmutableSet.copyOf(expectedWindows),
        ImmutableSet.<StateTag<?>>of(
            PaneInfoTracker.PANE_INFO_TAG,
            WatermarkHold.watermarkHoldTagForOutputTimeFn(objectStrategy.getOutputTimeFn()),
            WatermarkHold.EXTRA_HOLD_TAG));
  }

  /**
   * Verifies that the the set of windows that have any state stored is exactly
   * {@code expectedWindows} and that each of these windows has only tags from {@code allowedTags}.
   */
  private void assertHasOnlyGlobalAndAllowedTags(
      Set<W> expectedWindows, Set<StateTag<?>> allowedTags) {
    Set<StateNamespace> expectedWindowsSet = new HashSet<>();
    for (W expectedWindow : expectedWindows) {
      expectedWindowsSet.add(windowNamespace(expectedWindow));
    }
    Map<StateNamespace, Set<StateTag<?>>> actualWindows = new HashMap<>();

    for (StateNamespace namespace : stateInternals.getNamespacesInUse()) {
      if (namespace instanceof StateNamespaces.GlobalNamespace) {
        continue;
      } else if (namespace instanceof StateNamespaces.WindowNamespace) {
        Set<StateTag<?>> tagsInUse = stateInternals.getTagsInUse(namespace);
        if (tagsInUse.isEmpty()) {
          continue;
        }
        actualWindows.put(namespace, tagsInUse);
        Set<StateTag<?>> unexpected = Sets.difference(tagsInUse, allowedTags);
        if (unexpected.isEmpty()) {
          continue;
        } else {
          fail(namespace + " has unexpected states: " + tagsInUse);
        }
      } else if (namespace instanceof StateNamespaces.WindowAndTriggerNamespace) {
        Set<StateTag<?>> tagsInUse = stateInternals.getTagsInUse(namespace);
        assertTrue(namespace + " contains " + tagsInUse, tagsInUse.isEmpty());
      } else {
        fail("Unrecognized namespace " + namespace);
      }
    }

    assertEquals("Still in use: " + actualWindows.toString(), expectedWindowsSet,
        actualWindows.keySet());
  }

  private StateNamespace windowNamespace(W window) {
    return StateNamespaces.window(windowFn.windowCoder(), window);
  }

  public Instant getWatermarkHold() {
    return stateInternals.earliestWatermarkHold();
  }

  public Instant getOutputWatermark() {
    return timerInternals.currentOutputWatermarkTime();
  }

  public long getElementsDroppedDueToClosedWindow() {
    return droppedDueToClosedWindow.getSum();
  }

  /**
   * How many panes do we have in the output?
   */
  public int getOutputSize() {
    return windowingInternals.outputs.size();
  }

  /**
   * Retrieve the values that have been output to this time, and clear out the output accumulator.
   */
  public List<WindowedValue<OutputT>> extractOutput() {
    ImmutableList<WindowedValue<OutputT>> result =
        FluentIterable.from(windowingInternals.outputs)
            .transform(new Function<WindowedValue<KV<String, OutputT>>, WindowedValue<OutputT>>() {
              @Override
              public WindowedValue<OutputT> apply(WindowedValue<KV<String, OutputT>> input) {
                return input.withValue(input.getValue().getValue());
              }
            })
            .toList();
    windowingInternals.outputs.clear();
    return result;
  }

  /**
   * Advance the input watermark to the specified time, firing any timers that should
   * fire. Then advance the output watermark as far as possible.
   */
  public void advanceInputWatermark(Instant newInputWatermark) throws Exception {
    ReduceFnRunner<String, InputT, OutputT, W> runner = createRunner();
    timerInternals.advanceInputWatermark(runner, newInputWatermark);
    runner.persist();
  }

  /** Advance the processing time to the specified time, firing any timers that should fire. */
  public void advanceProcessingTime(Instant newProcessingTime) throws Exception {
    ReduceFnRunner<String, InputT, OutputT, W> runner = createRunner();
    timerInternals.advanceProcessingTime(runner, newProcessingTime);
    runner.persist();
  }

  /**
   * Advance the synchronized processing time to the specified time,
   * firing any timers that should fire.
   */
  public void advanceSynchronizedProcessingTime(Instant newProcessingTime) throws Exception {
    ReduceFnRunner<String, InputT, OutputT, W> runner = createRunner();
    timerInternals.advanceSynchronizedProcessingTime(runner, newProcessingTime);
    runner.persist();
  }

  /**
   * Inject all the timestamped values (after passing through the window function) as if they
   * arrived in a single chunk of a bundle (or work-unit).
   */
  @SafeVarargs
  public final void injectElements(TimestampedValue<InputT>... values) throws Exception {
    for (TimestampedValue<InputT> value : values) {
      WindowTracing.trace("TriggerTester.injectElements: {}", value);
    }
    ReduceFnRunner<String, InputT, OutputT, W> runner = createRunner();
    runner.processElements(Iterables.transform(
        Arrays.asList(values), new Function<TimestampedValue<InputT>, WindowedValue<InputT>>() {
          @Override
          public WindowedValue<InputT> apply(TimestampedValue<InputT> input) {
            try {
              InputT value = input.getValue();
              Instant timestamp = input.getTimestamp();
              Collection<W> windows = windowFn.assignWindows(new TestAssignContext<W>(
                  windowFn, value, timestamp, Arrays.asList(GlobalWindow.INSTANCE)));
              return WindowedValue.of(value, timestamp, windows, PaneInfo.NO_FIRING);
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }));

    // Persist after each bundle.
    runner.persist();
  }

  public void fireTimer(W window, Instant timestamp, TimeDomain domain) {
    ReduceFnRunner<String, InputT, OutputT, W> runner = createRunner();
    runner.onTimer(
        TimerData.of(StateNamespaces.window(windowFn.windowCoder(), window), timestamp, domain));
    runner.persist();
  }

  /**
   * Simulate state.
   */
  private static class TestInMemoryStateInternals extends InMemoryStateInternals {
    public Set<StateTag<?>> getTagsInUse(StateNamespace namespace) {
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

    /** Return the earliest output watermark hold in state, or null if none. */
    public Instant earliestWatermarkHold() {
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

  /**
   * Convey the simulated state and implement {@link #outputWindowedValue} to capture all output
   * elements.
   */
  private class TestWindowingInternals implements WindowingInternals<InputT, KV<String, OutputT>> {
    private List<WindowedValue<KV<String, OutputT>>> outputs = new ArrayList<>();

    @Override
    public void outputWindowedValue(KV<String, OutputT> output, Instant timestamp,
        Collection<? extends BoundedWindow> windows, PaneInfo pane) {
      // Copy the output value (using coders) before capturing it.
      KV<String, OutputT> copy = SerializableUtils.<KV<String, OutputT>>ensureSerializableByCoder(
          KvCoder.of(StringUtf8Coder.of(), outputCoder), output, "outputForWindow");
      WindowedValue<KV<String, OutputT>> value = WindowedValue.of(copy, timestamp, windows, pane);
      outputs.add(value);
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException(
          "Testing triggers should not use timers from WindowingInternals.");
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
    public <T> void writePCollectionViewData(
        TupleTag<?> tag, Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException {
      throw new UnsupportedOperationException(
          "Testing triggers should not use writePCollectionViewData from WindowingInternals.");
    }

    @Override
    public StateInternals stateInternals() {
      return stateInternals;
    }
  }

  private static class TestAssignContext<W extends BoundedWindow>
      extends WindowFn<Object, W>.AssignContext {
    private Object element;
    private Instant timestamp;
    private Collection<? extends BoundedWindow> windows;

    public TestAssignContext(WindowFn<Object, W> windowFn, Object element, Instant timestamp,
        Collection<? extends BoundedWindow> windows) {
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

  /**
   * Simulate the firing of timers and progression of input and output watermarks for a
   * single computation and key in a Windmill-like streaming environment. Similar to
   * {@link BatchTimerInternals}, but also tracks the output watermark.
   */
  private class TestTimerInternals implements TimerInternals {
    /** At most one timer per timestamp is kept. */
    private Set<TimerData> existingTimers = new HashSet<>();

    /** Pending input watermark timers, in timestamp order. */
    private PriorityQueue<TimerData> watermarkTimers = new PriorityQueue<>(11);

    /** Pending processing time timers, in timestamp order. */
    private PriorityQueue<TimerData> processingTimers = new PriorityQueue<>(11);

    /** Current input watermark. */
    @Nullable
    private Instant inputWatermarkTime = null;

    /** Current output watermark. */
    @Nullable
    private Instant outputWatermarkTime = null;

    /** Current processing time. */
    private Instant processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

    /** Current synchronized processing time. */
    @Nullable
    private Instant synchronizedProcessingTime = null;

    private PriorityQueue<TimerData> queue(TimeDomain domain) {
      return TimeDomain.EVENT_TIME.equals(domain) ? watermarkTimers : processingTimers;
    }

    @Override
    public void setTimer(TimerData timer) {
      WindowTracing.trace("TestTimerInternals.setTimer: {}", timer);
      if (existingTimers.add(timer)) {
        queue(timer.getDomain()).add(timer);
      }
    }

    @Override
    public void deleteTimer(TimerData timer) {
      WindowTracing.trace("TestTimerInternals.deleteTimer: {}", timer);
      existingTimers.remove(timer);
      queue(timer.getDomain()).remove(timer);
    }

    @Override
    public Instant currentProcessingTime() {
      return processingTime;
    }

    @Override
    @Nullable
    public Instant currentSynchronizedProcessingTime() {
      return synchronizedProcessingTime;
    }

    @Override
    @Nullable
    public Instant currentInputWatermarkTime() {
      return inputWatermarkTime;
    }

    @Override
    @Nullable
    public Instant currentOutputWatermarkTime() {
      return outputWatermarkTime;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("watermarkTimers", watermarkTimers)
          .add("processingTimers", processingTimers)
          .add("inputWatermarkTime", inputWatermarkTime)
          .add("outputWatermarkTime", outputWatermarkTime)
          .add("processingTime", processingTime)
          .toString();
    }

    public void advanceInputWatermark(
        ReduceFnRunner<?, ?, ?, ?> runner, Instant newInputWatermark) {
      Preconditions.checkNotNull(newInputWatermark);
      Preconditions.checkState(
          inputWatermarkTime == null || !newInputWatermark.isBefore(inputWatermarkTime),
          "Cannot move input watermark time backwards from %s to %s", inputWatermarkTime,
          newInputWatermark);
      WindowTracing.trace("TestTimerInternals.advanceInputWatermark: from {} to {}",
          inputWatermarkTime, newInputWatermark);
      inputWatermarkTime = newInputWatermark;
      advanceAndFire(runner, newInputWatermark, TimeDomain.EVENT_TIME);

      Instant hold = stateInternals.earliestWatermarkHold();
      if (hold == null) {
        WindowTracing.trace("TestTimerInternals.advanceInputWatermark: no holds, "
            + "so output watermark = input watermark");
        hold = inputWatermarkTime;
      }
      advanceOutputWatermark(hold);
    }

    private void advanceOutputWatermark(Instant newOutputWatermark) {
      Preconditions.checkNotNull(newOutputWatermark);
      Preconditions.checkNotNull(inputWatermarkTime);
      if (newOutputWatermark.isAfter(inputWatermarkTime)) {
        WindowTracing.trace(
            "TestTimerInternals.advanceOutputWatermark: clipping output watermark from {} to {}",
            newOutputWatermark, inputWatermarkTime);
        newOutputWatermark = inputWatermarkTime;
      }
      Preconditions.checkState(
          outputWatermarkTime == null || !newOutputWatermark.isBefore(outputWatermarkTime),
          "Cannot move output watermark time backwards from %s to %s", outputWatermarkTime,
          newOutputWatermark);
      WindowTracing.trace("TestTimerInternals.advanceOutputWatermark: from {} to {}",
          outputWatermarkTime, newOutputWatermark);
      outputWatermarkTime = newOutputWatermark;
    }

    public void advanceProcessingTime(
        ReduceFnRunner<?, ?, ?, ?> runner, Instant newProcessingTime) {
      Preconditions.checkState(!newProcessingTime.isBefore(processingTime),
          "Cannot move processing time backwards from %s to %s", processingTime, newProcessingTime);
      WindowTracing.trace("TestTimerInternals.advanceProcessingTime: from {} to {}", processingTime,
          newProcessingTime);
      processingTime = newProcessingTime;
      advanceAndFire(runner, newProcessingTime, TimeDomain.PROCESSING_TIME);
    }

    public void advanceSynchronizedProcessingTime(
        ReduceFnRunner<?, ?, ?, ?> runner, Instant newSynchronizedProcessingTime) {
      Preconditions.checkState(!newSynchronizedProcessingTime.isBefore(synchronizedProcessingTime),
          "Cannot move processing time backwards from %s to %s", processingTime,
          newSynchronizedProcessingTime);
      WindowTracing.trace("TestTimerInternals.advanceProcessingTime: from {} to {}",
          synchronizedProcessingTime, newSynchronizedProcessingTime);
      synchronizedProcessingTime = newSynchronizedProcessingTime;
      advanceAndFire(
          runner, newSynchronizedProcessingTime, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    }

    private void advanceAndFire(
        ReduceFnRunner<?, ?, ?, ?> runner, Instant currentTime, TimeDomain domain) {
      PriorityQueue<TimerData> queue = queue(domain);
      boolean shouldFire = false;

      do {
        TimerData timer = queue.peek();
        // Timers fire when the current time progresses past the timer time.
        shouldFire = timer != null && currentTime.isAfter(timer.getTimestamp());
        if (shouldFire) {
          WindowTracing.trace(
              "TestTimerInternals.advanceAndFire: firing {} at {}", timer, currentTime);
          // Remove before firing, so that if the trigger adds another identical
          // timer we don't remove it.
          queue.remove();

          runner.onTimer(timer);
        }
      } while (shouldFire);
    }
  }
}
