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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.ActiveWindowSet;
import org.apache.beam.runners.core.ActiveWindowSet.MergeCallback;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.MergingActiveWindowSet;
import org.apache.beam.runners.core.NonMergingActiveWindowSet;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateNamespaces.WindowAndTriggerNamespace;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.TestInMemoryStateInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Test utility that runs a {@link TriggerStateMachine}, using in-memory stub implementation to
 * provide the {@link StateInternals}.
 *
 * @param <W> The type of windows being used.
 */
public class TriggerStateMachineTester<InputT, W extends BoundedWindow> {

  /**
   * A {@link TriggerStateMachineTester} specialized to {@link Integer} values, so elements and
   * timestamps can be conflated. Today, triggers should not observed the element type, so this is
   * the only trigger tester that needs to be used.
   */
  public static class SimpleTriggerStateMachineTester<W extends BoundedWindow>
      extends TriggerStateMachineTester<Integer, W> {

    private SimpleTriggerStateMachineTester(
        ExecutableTriggerStateMachine executableTriggerStateMachine,
        WindowFn<Object, W> windowFn,
        Duration allowedLateness)
        throws Exception {
      super(executableTriggerStateMachine, windowFn, allowedLateness);
    }

    public void injectElements(int... values) throws Exception {
      List<TimestampedValue<Integer>> timestampedValues =
          Lists.newArrayListWithCapacity(values.length);
      for (int value : values) {
        timestampedValues.add(TimestampedValue.of(value, new Instant(value)));
      }
      injectElements(timestampedValues);
    }

    public SimpleTriggerStateMachineTester<W> withAllowedLateness(Duration allowedLateness)
        throws Exception {
      return new SimpleTriggerStateMachineTester<>(executableTrigger, windowFn, allowedLateness);
    }
  }

  private final TestInMemoryStateInternals<?> stateInternals =
      new TestInMemoryStateInternals<>(null /* key */);
  private final InMemoryTimerInternals timerInternals = new InMemoryTimerInternals();
  private final TriggerStateMachineContextFactory<W> contextFactory;
  protected final WindowFn<Object, W> windowFn;
  private final ActiveWindowSet<W> activeWindows;
  private final Map<W, W> windowToMergeResult;

  /** An {@link ExecutableTriggerStateMachine} under test. */
  protected final ExecutableTriggerStateMachine executableTrigger;

  /** A map from a window and trigger to whether that trigger is finished for the window. */
  private final Map<W, FinishedTriggers> finishedSets;

  public static <W extends BoundedWindow> SimpleTriggerStateMachineTester<W> forTrigger(
      TriggerStateMachine stateMachine, WindowFn<Object, W> windowFn) throws Exception {

    ExecutableTriggerStateMachine executableTriggerStateMachine =
        ExecutableTriggerStateMachine.create(stateMachine);

    // Merging requires accumulation mode or early firings can break up a session.
    // Not currently an issue with the tester (because we never GC) but we don't want
    // mystery failures due to violating this need.
    AccumulationMode mode =
        windowFn.isNonMerging()
            ? AccumulationMode.DISCARDING_FIRED_PANES
            : AccumulationMode.ACCUMULATING_FIRED_PANES;

    return new SimpleTriggerStateMachineTester<>(
        executableTriggerStateMachine, windowFn, Duration.ZERO);
  }

  public static <InputT, W extends BoundedWindow>
      TriggerStateMachineTester<InputT, W> forAdvancedTrigger(
          TriggerStateMachine stateMachine, WindowFn<Object, W> windowFn) throws Exception {
    ExecutableTriggerStateMachine executableTriggerStateMachine =
        ExecutableTriggerStateMachine.create(stateMachine);

    // Merging requires accumulation mode or early firings can break up a session.
    // Not currently an issue with the tester (because we never GC) but we don't want
    // mystery failures due to violating this need.
    AccumulationMode mode =
        windowFn.isNonMerging()
            ? AccumulationMode.DISCARDING_FIRED_PANES
            : AccumulationMode.ACCUMULATING_FIRED_PANES;

    return new TriggerStateMachineTester<>(executableTriggerStateMachine, windowFn, Duration.ZERO);
  }

  protected TriggerStateMachineTester(
      ExecutableTriggerStateMachine executableTriggerStateMachine,
      WindowFn<Object, W> windowFn,
      Duration allowedLateness)
      throws Exception {
    this.windowFn = windowFn;
    this.executableTrigger = executableTriggerStateMachine;
    this.finishedSets = new HashMap<>();

    this.activeWindows =
        windowFn.isNonMerging()
            ? new NonMergingActiveWindowSet<>()
            : new MergingActiveWindowSet<>(windowFn, stateInternals);
    this.windowToMergeResult = new HashMap<>();

    this.contextFactory =
        new TriggerStateMachineContextFactory<>(windowFn, stateInternals, activeWindows);
  }

  /** Instructs the trigger to clear its state for the given window. */
  public void clearState(W window) throws Exception {
    executableTrigger.invokeClear(
        contextFactory.base(
            window,
            new TestTimers(windowNamespace(window)),
            executableTrigger,
            getFinishedSet(window)));
  }

  /**
   * Asserts that the trigger has actually cleared all of its state for the given window. Since the
   * trigger under test is the root, this makes the assert for all triggers regardless of their
   * position in the trigger tree.
   */
  public void assertCleared(W window) {
    for (StateNamespace untypedNamespace : stateInternals.getNamespacesInUse()) {
      if (untypedNamespace instanceof WindowAndTriggerNamespace) {
        @SuppressWarnings("unchecked")
        WindowAndTriggerNamespace<W> namespace = (WindowAndTriggerNamespace<W>) untypedNamespace;
        if (namespace.getWindow().equals(window)) {
          Set<?> tagsInUse = stateInternals.getTagsInUse(namespace);
          assertTrue("Trigger has not cleared tags: " + tagsInUse, tagsInUse.isEmpty());
        }
      }
    }
  }

  /** Retrieves the next timer for this time domain, if any, for use in assertions. */
  public @Nullable Instant getNextTimer(TimeDomain domain) {
    return timerInternals.getNextTimer(domain);
  }

  /**
   * Returns {@code true} if the {@link TriggerStateMachine} under test is finished for the given
   * window.
   */
  public boolean isMarkedFinished(W window) {
    FinishedTriggers finishedSet = finishedSets.get(window);
    if (finishedSet == null) {
      return false;
    }

    return finishedSet.isFinished(executableTrigger);
  }

  private StateNamespace windowNamespace(W window) {
    return StateNamespaces.window(windowFn.windowCoder(), checkNotNull(window));
  }

  /**
   * Advance the input watermark to the specified time, then advance the output watermark as far as
   * possible.
   */
  public void advanceInputWatermark(Instant newInputWatermark) throws Exception {
    timerInternals.advanceInputWatermark(newInputWatermark);
    while (timerInternals.removeNextEventTimer() != null) {
      // TODO: Should test timer firings: see https://issues.apache.org/jira/browse/BEAM-694
    }
  }

  /** Advance the processing time to the specified time. */
  public void advanceProcessingTime(Instant newProcessingTime) throws Exception {
    timerInternals.advanceProcessingTime(newProcessingTime);
    while (timerInternals.removeNextProcessingTimer() != null) {
      // TODO: Should test timer firings: see https://issues.apache.org/jira/browse/BEAM-694
    }
    timerInternals.advanceSynchronizedProcessingTime(newProcessingTime);
    while (timerInternals.removeNextSynchronizedProcessingTimer() != null) {
      // TODO: Should test timer firings: see https://issues.apache.org/jira/browse/BEAM-694
    }
  }

  /**
   * Inject all the timestamped values (after passing through the window function) as if they
   * arrived in a single chunk of a bundle (or work-unit).
   */
  @SafeVarargs
  public final void injectElements(TimestampedValue<InputT>... values) throws Exception {
    injectElements(Arrays.asList(values));
  }

  public final void injectElements(Collection<TimestampedValue<InputT>> values) throws Exception {
    for (TimestampedValue<InputT> value : values) {
      WindowTracing.trace("TriggerTester.injectElements: {}", value);
    }

    List<WindowedValue<InputT>> windowedValues = Lists.newArrayListWithCapacity(values.size());

    for (TimestampedValue<InputT> input : values) {
      try {
        InputT value = input.getValue();
        Instant timestamp = input.getTimestamp();
        Collection<W> assignedWindows =
            windowFn.assignWindows(
                new TestAssignContext<W>(windowFn, value, timestamp, GlobalWindow.INSTANCE));

        for (W window : assignedWindows) {
          activeWindows.addActiveForTesting(window);
        }

        windowedValues.add(WindowedValue.of(value, timestamp, assignedWindows, PaneInfo.NO_FIRING));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    for (WindowedValue<InputT> windowedValue : windowedValues) {
      for (BoundedWindow untypedWindow : windowedValue.getWindows()) {
        // SDK is responsible for type safety
        @SuppressWarnings("unchecked")
        W window = mergeResult((W) untypedWindow);

        TriggerStateMachine.OnElementContext context =
            contextFactory.createOnElementContext(
                window,
                new TestTimers(windowNamespace(window)),
                windowedValue.getTimestamp(),
                executableTrigger,
                getFinishedSet(window));

        if (!context.trigger().isFinished()) {
          executableTrigger.invokeOnElement(context);
        }
      }
    }
  }

  public boolean shouldFire(W window) throws Exception {
    TriggerStateMachine.TriggerContext context =
        contextFactory.base(
            window,
            new TestTimers(windowNamespace(window)),
            executableTrigger,
            getFinishedSet(window));
    executableTrigger.getSpec().prefetchShouldFire(context.state());
    return executableTrigger.invokeShouldFire(context);
  }

  public void fireIfShouldFire(W window) throws Exception {
    TriggerStateMachine.TriggerContext context =
        contextFactory.base(
            window,
            new TestTimers(windowNamespace(window)),
            executableTrigger,
            getFinishedSet(window));

    executableTrigger.getSpec().prefetchShouldFire(context.state());
    if (executableTrigger.invokeShouldFire(context)) {
      executableTrigger.getSpec().prefetchOnFire(context.state());
      executableTrigger.invokeOnFire(context);
      if (context.trigger().isFinished()) {
        activeWindows.remove(window);
        executableTrigger.invokeClear(context);
      }
    }
  }

  public void setSubTriggerFinishedForWindow(int subTriggerIndex, W window, boolean value) {
    getFinishedSet(window).setFinished(executableTrigger.subTriggers().get(subTriggerIndex), value);
  }

  /**
   * Invokes merge from the {@link WindowFn} a single time and passes the resulting merge events on
   * to the trigger under test. Does not persist the fact that merging happened, since it is just to
   * test the trigger's {@code OnMerge} method.
   */
  public final void mergeWindows() throws Exception {
    windowToMergeResult.clear();
    activeWindows.merge(
        new MergeCallback<W>() {
          @Override
          public void prefetchOnMerge(Collection<W> toBeMerged, W mergeResult) throws Exception {}

          @Override
          public void onMerge(Collection<W> toBeMerged, W mergeResult) throws Exception {
            List<W> activeToBeMerged = new ArrayList<>();
            for (W window : toBeMerged) {
              windowToMergeResult.put(window, mergeResult);
              if (activeWindows.isActive(window)) {
                activeToBeMerged.add(window);
              }
            }
            Map<W, FinishedTriggers> mergingFinishedSets =
                Maps.newHashMapWithExpectedSize(activeToBeMerged.size());
            for (W oldWindow : activeToBeMerged) {
              mergingFinishedSets.put(oldWindow, getFinishedSet(oldWindow));
            }
            executableTrigger.invokeOnMerge(
                contextFactory.createOnMergeContext(
                    mergeResult,
                    new TestTimers(windowNamespace(mergeResult)),
                    executableTrigger,
                    getFinishedSet(mergeResult),
                    mergingFinishedSets));
          }
        });
  }

  public W mergeResult(W window) {
    W result = windowToMergeResult.get(window);
    return result == null ? window : result;
  }

  private FinishedTriggers getFinishedSet(W window) {
    return finishedSets.computeIfAbsent(window, k -> FinishedTriggersSet.fromSet(new HashSet<>()));
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

  private class TestTimers implements Timers {
    private final StateNamespace namespace;

    public TestTimers(StateNamespace namespace) {
      checkArgument(namespace instanceof WindowNamespace);
      this.namespace = namespace;
    }

    @Override
    public void setTimer(Instant timestamp, TimeDomain timeDomain) {
      timerInternals.setTimer(TimerData.of(namespace, timestamp, timestamp, timeDomain));
    }

    @Override
    public void setTimer(Instant timestamp, Instant outputTimestamp, TimeDomain timeDomain) {
      timerInternals.setTimer(TimerData.of(namespace, timestamp, outputTimestamp, timeDomain));
    }

    @Override
    public void deleteTimer(Instant timestamp, TimeDomain timeDomain) {
      timerInternals.deleteTimer(TimerData.of(namespace, timestamp, timestamp, timeDomain));
    }

    @Override
    public Instant currentProcessingTime() {
      return timerInternals.currentProcessingTime();
    }

    @Override
    public @Nullable Instant currentSynchronizedProcessingTime() {
      return timerInternals.currentSynchronizedProcessingTime();
    }

    @Override
    public Instant currentEventTime() {
      return timerInternals.currentInputWatermarkTime();
    }
  }
}
