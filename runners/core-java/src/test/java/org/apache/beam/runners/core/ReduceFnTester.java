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
package org.apache.beam.runners.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachineRunner;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.NullSideInputReader;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.WindowingStrategy.AccumulationMode;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;

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
  private static final String KEY = "TEST_KEY";

  private final TestInMemoryStateInternals<String> stateInternals =
      new TestInMemoryStateInternals<>(KEY);
  private final InMemoryTimerInternals timerInternals = new InMemoryTimerInternals();

  private final WindowFn<Object, W> windowFn;
  private final TestOutputWindowedValue testOutputter;
  private final SideInputReader sideInputReader;
  private final Coder<OutputT> outputCoder;
  private final WindowingStrategy<Object, W> objectStrategy;
  private final ExecutableTriggerStateMachine executableTriggerStateMachine;
  private final ReduceFn<String, InputT, OutputT, W> reduceFn;
  private final PipelineOptions options;

  /**
   * If true, the output watermark is automatically advanced to the latest possible
   * point when the input watermark is advanced. This is the default for most tests.
   * If false, the output watermark must be explicitly advanced by the test, which can
   * be used to exercise some of the more subtle behavior of WatermarkHold.
   */
  private boolean autoAdvanceOutputWatermark = true;

  private final InMemoryLongSumAggregator droppedDueToClosedWindow =
      new InMemoryLongSumAggregator(GroupAlsoByWindowsDoFn.DROPPED_DUE_TO_CLOSED_WINDOW_COUNTER);

  /**
   * Creates a {@link ReduceFnTester} for the given {@link WindowingStrategy}, creating
   * a {@link TriggerStateMachine} from its {@link Trigger}.
   */
  public static <W extends BoundedWindow> ReduceFnTester<Integer, Iterable<Integer>, W>
      nonCombining(WindowingStrategy<?, W> windowingStrategy) throws Exception {
    return new ReduceFnTester<Integer, Iterable<Integer>, W>(
        windowingStrategy,
        TriggerStateMachines.stateMachineForTrigger(windowingStrategy.getTrigger()),
        SystemReduceFn.<String, Integer, W>buffering(VarIntCoder.of()),
        IterableCoder.of(VarIntCoder.of()),
        PipelineOptionsFactory.create(),
        NullSideInputReader.empty());
  }

  /**
   * Creates a {@link ReduceFnTester} for the given {@link WindowingStrategy} and {@link
   * TriggerStateMachine}, for mocking the interactions between {@link ReduceFnRunner} and the
   * {@link TriggerStateMachine}.
   *
   * <p>Ignores the {@link Trigger} on the {@link WindowingStrategy}.
   */
  public static <W extends BoundedWindow>
      ReduceFnTester<Integer, Iterable<Integer>, W> nonCombining(
          WindowingStrategy<?, W> windowingStrategy, TriggerStateMachine triggerStateMachine)
          throws Exception {
    return new ReduceFnTester<>(
        windowingStrategy,
        triggerStateMachine,
        SystemReduceFn.<String, Integer, W>buffering(VarIntCoder.of()),
        IterableCoder.of(VarIntCoder.of()),
        PipelineOptionsFactory.create(),
        NullSideInputReader.empty());
  }

  public static <W extends BoundedWindow>
      ReduceFnTester<Integer, Iterable<Integer>, W> nonCombining(
          WindowFn<?, W> windowFn,
          TriggerStateMachine triggerStateMachine,
          AccumulationMode mode,
          Duration allowedDataLateness,
          ClosingBehavior closingBehavior)
          throws Exception {
    WindowingStrategy<?, W> strategy =
        WindowingStrategy.of(windowFn)
            .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp())
            .withMode(mode)
            .withAllowedLateness(allowedDataLateness)
            .withClosingBehavior(closingBehavior);
    return nonCombining(strategy, triggerStateMachine);
  }

  /**
   * Creates a {@link ReduceFnTester} for the given {@link WindowingStrategy} and
   * {@link KeyedCombineFn}, creating a {@link TriggerStateMachine} from the
   * {@link Trigger} in the {@link WindowingStrategy}.
   */
  public static <W extends BoundedWindow, AccumT, OutputT>
      ReduceFnTester<Integer, OutputT, W> combining(
          WindowingStrategy<?, W> strategy,
          KeyedCombineFn<String, Integer, AccumT, OutputT> combineFn,
          Coder<OutputT> outputCoder)
          throws Exception {

    CoderRegistry registry = new CoderRegistry();
    registry.registerStandardCoders();
    AppliedCombineFn<String, Integer, AccumT, OutputT> fn =
        AppliedCombineFn.<String, Integer, AccumT, OutputT>withInputCoder(
            combineFn, registry, KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

    return combining(
        strategy,
        TriggerStateMachines.stateMachineForTrigger(strategy.getTrigger()),
        combineFn,
        outputCoder);
  }

  /**
   * Creates a {@link ReduceFnTester} for the given {@link WindowingStrategy},
   * {@link KeyedCombineFn}, and {@link TriggerStateMachine}, for mocking the interaction
   * between {@link ReduceFnRunner} and the {@link TriggerStateMachine}.
   * Ignores the {@link Trigger} in the {@link WindowingStrategy}.
   */
  public static <W extends BoundedWindow, AccumT, OutputT>
  ReduceFnTester<Integer, OutputT, W> combining(
      WindowingStrategy<?, W> strategy,
      TriggerStateMachine triggerStateMachine,
      KeyedCombineFn<String, Integer, AccumT, OutputT> combineFn,
      Coder<OutputT> outputCoder)
      throws Exception {

    CoderRegistry registry = new CoderRegistry();
    registry.registerStandardCoders();
    AppliedCombineFn<String, Integer, AccumT, OutputT> fn =
        AppliedCombineFn.<String, Integer, AccumT, OutputT>withInputCoder(
            combineFn, registry, KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

    return new ReduceFnTester<Integer, OutputT, W>(
        strategy,
        triggerStateMachine,
        SystemReduceFn.<String, Integer, AccumT, OutputT, W>combining(StringUtf8Coder.of(), fn),
        outputCoder,
        PipelineOptionsFactory.create(),
        NullSideInputReader.empty());
  }

  public static <W extends BoundedWindow, AccumT, OutputT>
      ReduceFnTester<Integer, OutputT, W> combining(
          WindowingStrategy<?, W> strategy,
          KeyedCombineFnWithContext<String, Integer, AccumT, OutputT> combineFn,
          Coder<OutputT> outputCoder,
          PipelineOptions options,
          SideInputReader sideInputReader)
          throws Exception {
    CoderRegistry registry = new CoderRegistry();
    registry.registerStandardCoders();
    AppliedCombineFn<String, Integer, AccumT, OutputT> fn =
        AppliedCombineFn.<String, Integer, AccumT, OutputT>withInputCoder(
            combineFn, registry, KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

    return combining(
        strategy,
        TriggerStateMachines.stateMachineForTrigger(strategy.getTrigger()),
        combineFn,
        outputCoder,
        options,
        sideInputReader);
  }

  public static <W extends BoundedWindow, AccumT, OutputT>
  ReduceFnTester<Integer, OutputT, W> combining(
      WindowingStrategy<?, W> strategy,
      TriggerStateMachine triggerStateMachine,
      KeyedCombineFnWithContext<String, Integer, AccumT, OutputT> combineFn,
      Coder<OutputT> outputCoder,
      PipelineOptions options,
      SideInputReader sideInputReader)
      throws Exception {
    CoderRegistry registry = new CoderRegistry();
    registry.registerStandardCoders();
    AppliedCombineFn<String, Integer, AccumT, OutputT> fn =
        AppliedCombineFn.<String, Integer, AccumT, OutputT>withInputCoder(
            combineFn, registry, KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

    return new ReduceFnTester<Integer, OutputT, W>(
        strategy,
        triggerStateMachine,
        SystemReduceFn.<String, Integer, AccumT, OutputT, W>combining(StringUtf8Coder.of(), fn),
        outputCoder,
        options,
        sideInputReader);
  }

  private ReduceFnTester(
      WindowingStrategy<?, W> wildcardStrategy,
      TriggerStateMachine triggerStateMachine,
      ReduceFn<String, InputT, OutputT, W> reduceFn,
      Coder<OutputT> outputCoder,
      PipelineOptions options,
      SideInputReader sideInputReader)
      throws Exception {
    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> objectStrategy = (WindowingStrategy<Object, W>) wildcardStrategy;

    this.objectStrategy = objectStrategy;
    this.reduceFn = reduceFn;
    this.windowFn = objectStrategy.getWindowFn();
    this.testOutputter = new TestOutputWindowedValue();
    this.sideInputReader = sideInputReader;
    this.executableTriggerStateMachine = ExecutableTriggerStateMachine.create(triggerStateMachine);
    this.outputCoder = outputCoder;
    this.options = options;
  }

  public void setAutoAdvanceOutputWatermark(boolean autoAdvanceOutputWatermark) {
    this.autoAdvanceOutputWatermark = autoAdvanceOutputWatermark;
  }

  @Nullable
  public Instant getNextTimer(TimeDomain domain) {
    return timerInternals.getNextTimer(domain);
  }

  ReduceFnRunner<String, InputT, OutputT, W> createRunner() {
    return new ReduceFnRunner<>(
        KEY,
        objectStrategy,
        executableTriggerStateMachine,
        stateInternals,
        timerInternals,
        testOutputter,
        sideInputReader,
        droppedDueToClosedWindow,
        reduceFn,
        options);
  }

  public boolean isMarkedFinished(W window) {
    return createRunner().isFinished(window);
  }

  public boolean hasNoActiveWindows() {
    return createRunner().hasNoActiveWindows();
  }

  @SafeVarargs
  public final void assertHasOnlyGlobalAndFinishedSetsFor(W... expectedWindows) {
    assertHasOnlyGlobalAndAllowedTags(
        ImmutableSet.copyOf(expectedWindows),
        ImmutableSet.<StateTag<? super String, ?>>of(TriggerStateMachineRunner.FINISHED_BITS_TAG));
  }

  @SafeVarargs
  public final void assertHasOnlyGlobalAndFinishedSetsAndPaneInfoFor(W... expectedWindows) {
    assertHasOnlyGlobalAndAllowedTags(
        ImmutableSet.copyOf(expectedWindows),
        ImmutableSet.<StateTag<? super String, ?>>of(
            TriggerStateMachineRunner.FINISHED_BITS_TAG, PaneInfoTracker.PANE_INFO_TAG,
            WatermarkHold.watermarkHoldTagForOutputTimeFn(objectStrategy.getOutputTimeFn()),
            WatermarkHold.EXTRA_HOLD_TAG));
  }

  public final void assertHasOnlyGlobalState() {
    assertHasOnlyGlobalAndAllowedTags(
        Collections.<W>emptySet(), Collections.<StateTag<? super String, ?>>emptySet());
  }

  @SafeVarargs
  public final void assertHasOnlyGlobalAndPaneInfoFor(W... expectedWindows) {
    assertHasOnlyGlobalAndAllowedTags(
        ImmutableSet.copyOf(expectedWindows),
        ImmutableSet.<StateTag<? super String, ?>>of(
            PaneInfoTracker.PANE_INFO_TAG,
            WatermarkHold.watermarkHoldTagForOutputTimeFn(objectStrategy.getOutputTimeFn()),
            WatermarkHold.EXTRA_HOLD_TAG));
  }

  /**
   * Verifies that the the set of windows that have any state stored is exactly
   * {@code expectedWindows} and that each of these windows has only tags from {@code allowedTags}.
   */
  private void assertHasOnlyGlobalAndAllowedTags(
      Set<W> expectedWindows, Set<StateTag<? super String, ?>> allowedTags) {
    Set<StateNamespace> expectedWindowsSet = new HashSet<>();
    for (W expectedWindow : expectedWindows) {
      expectedWindowsSet.add(windowNamespace(expectedWindow));
    }
    Map<StateNamespace, Set<StateTag<? super String, ?>>> actualWindows = new HashMap<>();

    for (StateNamespace namespace : stateInternals.getNamespacesInUse()) {
      if (namespace instanceof StateNamespaces.GlobalNamespace) {
        continue;
      } else if (namespace instanceof StateNamespaces.WindowNamespace) {
        Set<StateTag<? super String, ?>> tagsInUse = stateInternals.getTagsInUse(namespace);
        if (tagsInUse.isEmpty()) {
          continue;
        }
        actualWindows.put(namespace, tagsInUse);
        Set<StateTag<? super String, ?>> unexpected = Sets.difference(tagsInUse, allowedTags);
        if (unexpected.isEmpty()) {
          continue;
        } else {
          fail(namespace + " has unexpected states: " + tagsInUse);
        }
      } else if (namespace instanceof StateNamespaces.WindowAndTriggerNamespace) {
        Set<StateTag<? super String, ?>> tagsInUse = stateInternals.getTagsInUse(namespace);
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
    return testOutputter.outputs.size();
  }

  /**
   * Retrieve the values that have been output to this time, and clear out the output accumulator.
   */
  public List<WindowedValue<OutputT>> extractOutput() {
    ImmutableList<WindowedValue<OutputT>> result =
        FluentIterable.from(testOutputter.outputs)
            .transform(new Function<WindowedValue<KV<String, OutputT>>, WindowedValue<OutputT>>() {
              @Override
              public WindowedValue<OutputT> apply(WindowedValue<KV<String, OutputT>> input) {
                return input.withValue(input.getValue().getValue());
              }
            })
            .toList();
    testOutputter.outputs.clear();
    return result;
  }

  /**
   * Advance the input watermark to the specified time, firing any timers that should
   * fire. Then advance the output watermark as far as possible.
   */
  public void advanceInputWatermark(Instant newInputWatermark) throws Exception {
    timerInternals.advanceInputWatermark(newInputWatermark);
    ReduceFnRunner<String, InputT, OutputT, W> runner = createRunner();
    while (true) {
      TimerData timer;
      List<TimerInternals.TimerData> timers = new ArrayList<>();
      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        timers.add(timer);
      }
      if (timers.isEmpty()) {
        break;
      }
      runner.onTimers(timers);
    }
    if (autoAdvanceOutputWatermark) {
      Instant hold = stateInternals.earliestWatermarkHold();
      if (hold == null) {
        WindowTracing.trace(
            "TestInMemoryTimerInternals.advanceInputWatermark: no holds, "
                + "so output watermark = input watermark");
        hold = timerInternals.currentInputWatermarkTime();
      }
      advanceOutputWatermark(hold);
    }
    runner.persist();
  }

  /**
   * If {@link #autoAdvanceOutputWatermark} is {@literal false}, advance the output watermark
   * to the given value. Otherwise throw.
   */
  public void advanceOutputWatermark(Instant newOutputWatermark) throws Exception {
    timerInternals.advanceOutputWatermark(newOutputWatermark);
  }

  /** Advance the processing time to the specified time, firing any timers that should fire. */
  public void advanceProcessingTime(Instant newProcessingTime) throws Exception {
    timerInternals.advanceProcessingTime(newProcessingTime);
    ReduceFnRunner<String, InputT, OutputT, W> runner = createRunner();
    while (true) {
      TimerData timer;
      List<TimerInternals.TimerData> timers = new ArrayList<>();
      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        timers.add(timer);
      }
      if (timers.isEmpty()) {
        break;
      }
      runner.onTimers(timers);
    }
    runner.persist();
  }

  /**
   * Advance the synchronized processing time to the specified time,
   * firing any timers that should fire.
   */
  public void advanceSynchronizedProcessingTime(
      Instant newSynchronizedProcessingTime) throws Exception {
    timerInternals.advanceSynchronizedProcessingTime(newSynchronizedProcessingTime);
    ReduceFnRunner<String, InputT, OutputT, W> runner = createRunner();
    while (true) {
      TimerData timer;
      List<TimerInternals.TimerData> timers = new ArrayList<>();
      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
        timers.add(timer);
      }
      if (timers.isEmpty()) {
        break;
      }
      runner.onTimers(timers);
    }
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
    runner.processElements(
        Iterables.transform(
            Arrays.asList(values),
            new Function<TimestampedValue<InputT>, WindowedValue<InputT>>() {
              @Override
              public WindowedValue<InputT> apply(TimestampedValue<InputT> input) {
                try {
                  InputT value = input.getValue();
                  Instant timestamp = input.getTimestamp();
                  Collection<W> windows =
                      windowFn.assignWindows(
                          new TestAssignContext<W>(
                              windowFn, value, timestamp, GlobalWindow.INSTANCE));
                  return WindowedValue.of(value, timestamp, windows, PaneInfo.NO_FIRING);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            }));

    // Persist after each bundle.
    runner.persist();
  }

  public void fireTimer(W window, Instant timestamp, TimeDomain domain) throws Exception {
    ReduceFnRunner<String, InputT, OutputT, W> runner = createRunner();
    ArrayList timers = new ArrayList(1);
    timers.add(
        TimerData.of(StateNamespaces.window(windowFn.windowCoder(), window), timestamp, domain));
    runner.onTimers(timers);
    runner.persist();
  }

  /**
   * Convey the simulated state and implement {@link #outputWindowedValue} to capture all output
   * elements.
   */private class TestOutputWindowedValue implements OutputWindowedValue<KV<String, OutputT>> {
    private List<WindowedValue<KV<String, OutputT>>> outputs = new ArrayList<>();

    @Override
    public void outputWindowedValue(
        KV<String, OutputT> output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      // Copy the output value (using coders) before capturing it.
      KV<String, OutputT> copy = SerializableUtils.<KV<String, OutputT>>ensureSerializableByCoder(
          KvCoder.of(StringUtf8Coder.of(), outputCoder), output, "outputForWindow");
      WindowedValue<KV<String, OutputT>> value = WindowedValue.of(copy, timestamp, windows, pane);
      outputs.add(value);
    }

    @Override
    public <SideOutputT> void sideOutputWindowedValue(
        TupleTag<SideOutputT> tag,
        SideOutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      throw new UnsupportedOperationException("GroupAlsoByWindow should not use side outputs");
    }
  }

  private static class TestAssignContext<W extends BoundedWindow>
      extends WindowFn<Object, W>.AssignContext {
    private Object element;
    private Instant timestamp;
    private BoundedWindow window;

    public TestAssignContext(
        WindowFn<Object, W> windowFn, Object element, Instant timestamp, BoundedWindow window) {
      windowFn.super();
      this.element = element;
      this.timestamp = timestamp;
      this.window = window;
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
    public BoundedWindow window() {
      return window;
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
      return Sum.ofLongs();
    }

    public long getSum() {
      return sum;
    }
  }
}
