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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.ActiveWindowSet.MergeCallback;
import org.apache.beam.sdk.util.TimerInternals.TimerData;
import org.apache.beam.sdk.util.WindowingStrategy.AccumulationMode;
import org.apache.beam.sdk.util.state.InMemoryTimerInternals;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateNamespaces;
import org.apache.beam.sdk.util.state.StateNamespaces.WindowAndTriggerNamespace;
import org.apache.beam.sdk.util.state.StateNamespaces.WindowNamespace;
import org.apache.beam.sdk.util.state.TestInMemoryStateInternals;
import org.apache.beam.sdk.util.state.TimerCallback;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Test utility that runs a {@link Trigger}, using in-memory stub implementation to provide
 * the {@link StateInternals}.
 *
 * @param <W> The type of windows being used.
 */
public class TriggerTester<InputT, W extends BoundedWindow> {

  /**
   * A {@link TriggerTester} specialized to {@link Integer} values, so elements and timestamps
   * can be conflated. Today, triggers should not observed the element type, so this is the
   * only trigger tester that needs to be used.
   */
  public static class SimpleTriggerTester<W extends BoundedWindow>
      extends TriggerTester<Integer, W> {

    private SimpleTriggerTester(WindowingStrategy<Object, W> windowingStrategy) throws Exception {
      super(windowingStrategy);
    }

    public void injectElements(int... values) throws Exception {
      List<TimestampedValue<Integer>> timestampedValues =
          Lists.newArrayListWithCapacity(values.length);
      for (int value : values) {
        timestampedValues.add(TimestampedValue.of(value, new Instant(value)));
      }
      injectElements(timestampedValues);
    }

    public SimpleTriggerTester<W> withAllowedLateness(Duration allowedLateness) throws Exception {
      return new SimpleTriggerTester<>(
          windowingStrategy.withAllowedLateness(allowedLateness));
    }
  }

  protected final WindowingStrategy<Object, W> windowingStrategy;

  private final TestInMemoryStateInternals<?> stateInternals =
      new TestInMemoryStateInternals<Object>(null /* key */);
  private final InMemoryTimerInternals timerInternals = new InMemoryTimerInternals();
  private final TriggerContextFactory<W> contextFactory;
  private final WindowFn<Object, W> windowFn;
  private final ActiveWindowSet<W> activeWindows;
  private final Map<W, W> windowToMergeResult;

  /**
   * An {@link ExecutableTrigger} built from the {@link Trigger} or {@link Trigger}
   * under test.
   */
  private final ExecutableTrigger executableTrigger;

  /**
   * A map from a window and trigger to whether that trigger is finished for the window.
   */
  private final Map<W, FinishedTriggers> finishedSets;

  public static <W extends BoundedWindow> SimpleTriggerTester<W> forTrigger(
      Trigger trigger, WindowFn<Object, W> windowFn)
          throws Exception {
    WindowingStrategy<Object, W> windowingStrategy =
        WindowingStrategy.of(windowFn).withTrigger(trigger)
        // Merging requires accumulation mode or early firings can break up a session.
        // Not currently an issue with the tester (because we never GC) but we don't want
        // mystery failures due to violating this need.
        .withMode(windowFn.isNonMerging()
            ? AccumulationMode.DISCARDING_FIRED_PANES
            : AccumulationMode.ACCUMULATING_FIRED_PANES);

    return new SimpleTriggerTester<>(windowingStrategy);
  }

  public static <InputT, W extends BoundedWindow> TriggerTester<InputT, W> forAdvancedTrigger(
      Trigger trigger, WindowFn<Object, W> windowFn) throws Exception {
    WindowingStrategy<Object, W> strategy =
        WindowingStrategy.of(windowFn).withTrigger(trigger)
        // Merging requires accumulation mode or early firings can break up a session.
        // Not currently an issue with the tester (because we never GC) but we don't want
        // mystery failures due to violating this need.
        .withMode(windowFn.isNonMerging()
            ? AccumulationMode.DISCARDING_FIRED_PANES
            : AccumulationMode.ACCUMULATING_FIRED_PANES);

    return new TriggerTester<>(strategy);
  }

  protected TriggerTester(WindowingStrategy<Object, W> windowingStrategy) throws Exception {
    this.windowingStrategy = windowingStrategy;
    this.windowFn = windowingStrategy.getWindowFn();
    this.executableTrigger = windowingStrategy.getTrigger();
    this.finishedSets = new HashMap<>();

    this.activeWindows =
        windowFn.isNonMerging()
            ? new NonMergingActiveWindowSet<W>()
            : new MergingActiveWindowSet<W>(windowFn, stateInternals);
    this.windowToMergeResult = new HashMap<>();

    this.contextFactory =
        new TriggerContextFactory<>(windowingStrategy.getWindowFn(), stateInternals, activeWindows);
  }

  /**
   * Instructs the trigger to clear its state for the given window.
   */
  public void clearState(W window) throws Exception {
    executableTrigger.invokeClear(contextFactory.base(window,
        new TestTimers(windowNamespace(window)), executableTrigger, getFinishedSet(window)));
  }

  /**
   * Asserts that the trigger has actually cleared all of its state for the given window. Since
   * the trigger under test is the root, this makes the assert for all triggers regardless
   * of their position in the trigger tree.
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

  /**
   * Returns {@code true} if the {@link Trigger} under test is finished for the given window.
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
    // TODO: Should test timer firings: see https://issues.apache.org/jira/browse/BEAM-694
    timerInternals.advanceInputWatermark(TimerCallback.NO_OP, newInputWatermark);
  }

  /** Advance the processing time to the specified time. */
  public void advanceProcessingTime(Instant newProcessingTime) throws Exception {
    // TODO: Should test timer firings: see https://issues.apache.org/jira/browse/BEAM-694
    timerInternals.advanceProcessingTime(TimerCallback.NO_OP, newProcessingTime);
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
        Collection<W> assignedWindows = windowFn.assignWindows(new TestAssignContext<W>(
            windowFn, value, timestamp, GlobalWindow.INSTANCE));

        for (W window : assignedWindows) {
          activeWindows.addActiveForTesting(window);

          // Today, triggers assume onTimer firing at the watermark time, whether or not they
          // explicitly set the timer themselves. So this tester must set it.
          timerInternals.setTimer(
              TimerData.of(windowNamespace(window), window.maxTimestamp(), TimeDomain.EVENT_TIME));
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

        Trigger.OnElementContext context = contextFactory.createOnElementContext(window,
            new TestTimers(windowNamespace(window)), windowedValue.getTimestamp(),
            executableTrigger, getFinishedSet(window));

        if (!context.trigger().isFinished()) {
          executableTrigger.invokeOnElement(context);
        }
      }
    }
  }

  public boolean shouldFire(W window) throws Exception {
    Trigger.TriggerContext context = contextFactory.base(
        window,
        new TestTimers(windowNamespace(window)),
        executableTrigger, getFinishedSet(window));
    executableTrigger.getSpec().prefetchShouldFire(context.state());
    return executableTrigger.invokeShouldFire(context);
  }

  public void fireIfShouldFire(W window) throws Exception {
    Trigger.TriggerContext context = contextFactory.base(
        window,
        new TestTimers(windowNamespace(window)),
        executableTrigger, getFinishedSet(window));

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
   * Invokes merge from the {@link WindowFn} a single time and passes the resulting merge
   * events on to the trigger under test. Does not persist the fact that merging happened,
   * since it is just to test the trigger's {@code OnMerge} method.
   */
  public final void mergeWindows() throws Exception {
    windowToMergeResult.clear();
    activeWindows.merge(new MergeCallback<W>() {
      @Override
      public void prefetchOnMerge(Collection<W> toBeMerged, W mergeResult) throws Exception {}

      @Override
      public void onMerge(Collection<W> toBeMerged, W mergeResult) throws Exception {
        List<W> activeToBeMerged = new ArrayList<W>();
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
        executableTrigger.invokeOnMerge(contextFactory.createOnMergeContext(mergeResult,
            new TestTimers(windowNamespace(mergeResult)), executableTrigger,
            getFinishedSet(mergeResult), mergingFinishedSets));
        timerInternals.setTimer(TimerData.of(
            windowNamespace(mergeResult), mergeResult.maxTimestamp(), TimeDomain.EVENT_TIME));
      }
    });
  }

  public  W mergeResult(W window) {
    W result = windowToMergeResult.get(window);
    return result == null ? window : result;
  }

  private FinishedTriggers getFinishedSet(W window) {
    FinishedTriggers finishedSet = finishedSets.get(window);
    if (finishedSet == null) {
      finishedSet = FinishedTriggersSet.fromSet(new HashSet<ExecutableTrigger>());
      finishedSets.put(window, finishedSet);
    }
    return finishedSet;
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
      timerInternals.setTimer(TimerData.of(namespace, timestamp, timeDomain));
    }

    @Override
    public void deleteTimer(Instant timestamp, TimeDomain timeDomain) {
      timerInternals.deleteTimer(TimerData.of(namespace, timestamp, timeDomain));
    }

    @Override
    public Instant currentProcessingTime() {
      return timerInternals.currentProcessingTime();
    }

    @Override
    @Nullable
    public Instant currentSynchronizedProcessingTime() {
      return timerInternals.currentSynchronizedProcessingTime();
    }

    @Override
    public Instant currentEventTime() {
      return timerInternals.currentInputWatermarkTime();
    }
  }
}
