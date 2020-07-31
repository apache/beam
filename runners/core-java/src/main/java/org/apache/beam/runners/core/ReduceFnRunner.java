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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.runners.core.ReduceFnContextFactory.StateStyle;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachineContextFactory;
import org.apache.beam.runners.core.triggers.TriggerStateMachineRunner;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Manages the execution of a {@link ReduceFn} after a {@link GroupByKeyOnly} has partitioned the
 * {@link PCollection} by key.
 *
 * <p>The {@link #onTrigger} relies on a {@link TriggerStateMachineRunner} to manage the execution
 * of the triggering logic. The {@code ReduceFnRunner}s responsibilities are:
 *
 * <ul>
 *   <li>Tracking the windows that are active (have buffered data) as elements arrive and triggers
 *       are fired.
 *   <li>Holding the watermark based on the timestamps of elements in a pane and releasing it when
 *       the trigger fires.
 *   <li>Calling the appropriate callbacks on {@link ReduceFn} based on trigger execution, timer
 *       firings, etc, and providing appropriate contexts to the {@link ReduceFn} for actions such
 *       as output.
 *   <li>Scheduling garbage collection of state associated with a specific window, and making that
 *       happen when the appropriate timer fires.
 * </ul>
 *
 * @param <K> The type of key being processed.
 * @param <InputT> The type of values associated with the key.
 * @param <OutputT> The output type that will be produced for each key.
 * @param <W> The type of windows this operates on.
 */
public class ReduceFnRunner<K, InputT, OutputT, W extends BoundedWindow> {

  /**
   * The {@link ReduceFnRunner} depends on most aspects of the {@link WindowingStrategy}.
   *
   * <ul>
   *   <li>It runs the trigger from the {@link WindowingStrategy}.
   *   <li>It merges windows according to the {@link WindowingStrategy}.
   *   <li>It chooses how to track active windows and clear out expired windows according to the
   *       {@link WindowingStrategy}, based on the allowed lateness and whether windows can merge.
   *   <li>It decides whether to emit empty final panes according to whether the {@link
   *       WindowingStrategy} requires it.
   *   <li>
   *   <li>It uses discarding or accumulation mode according to the {@link WindowingStrategy}.
   * </ul>
   */
  private final WindowingStrategy<Object, W> windowingStrategy;

  private final OutputWindowedValue<KV<K, OutputT>> outputter;

  private final StateInternals stateInternals;

  private final Counter droppedDueToClosedWindow;

  public static final String DROPPED_DUE_TO_CLOSED_WINDOW = "droppedDueToClosedWindow";

  private final K key;

  /**
   * Track which windows are still active and the 'state address' windows which hold their state.
   *
   * <ul>
   *   <li>State: Global map for all active windows for this computation and key.
   *   <li>Lifetime: Cleared when no active windows need to be tracked. A window lives within the
   *       active window set until its trigger is closed or the window is garbage collected.
   * </ul>
   */
  private final ActiveWindowSet<W> activeWindows;

  /**
   * Always a {@link SystemReduceFn}.
   *
   * <ul>
   *   <li>State: A bag of accumulated values, or the intermediate result of a combiner.
   *   <li>State style: RENAMED
   *   <li>Merging: Concatenate or otherwise combine the state from each merged window.
   *   <li>Lifetime: Cleared when a pane fires if DISCARDING_FIRED_PANES. Otherwise cleared when
   *       trigger is finished or when the window is garbage collected.
   * </ul>
   */
  private final ReduceFn<K, InputT, OutputT, W> reduceFn;

  /**
   * Manage the setting and firing of timer events.
   *
   * <ul>
   *   <li>Merging: End-of-window and garbage collection timers are cancelled when windows are
   *       merged away. Timers created by triggers are never garbage collected and are left to fire
   *       and be ignored.
   *   <li>Lifetime: Timers automatically disappear after they fire.
   * </ul>
   */
  private final TimerInternals timerInternals;

  /**
   * Manage the execution and state for triggers.
   *
   * <ul>
   *   <li>State: Tracks which sub-triggers have finished, and any additional state needed to
   *       determine when the trigger should fire.
   *   <li>State style: DIRECT
   *   <li>Merging: Finished bits are explicitly managed. Other state is eagerly merged as needed.
   *   <li>Lifetime: Most trigger state is cleared when the final pane is emitted. However the
   *       finished bits are left behind and must be cleared when the window is garbage collected.
   * </ul>
   */
  private final TriggerStateMachineRunner<W> triggerRunner;

  /**
   * Store the output watermark holds for each window.
   *
   * <ul>
   *   <li>State: Bag of hold timestamps.
   *   <li>State style: RENAMED
   *   <li>Merging: Depending on {@link TimestampCombiner}, may need to be recalculated on merging.
   *       When a pane fires it may be necessary to add (back) an end-of-window or garbage
   *       collection hold.
   *   <li>Lifetime: Cleared when a pane fires or when the window is garbage collected.
   * </ul>
   */
  private final WatermarkHold<W> watermarkHold;

  private final ReduceFnContextFactory<K, InputT, OutputT, W> contextFactory;

  /**
   * Store the previously emitted pane (if any) for each window.
   *
   * <ul>
   *   <li>State: The previous {@link PaneInfo} passed to the user's {@code DoFn.ProcessElement}
   *       method, if any.
   *   <li>Style style: DIRECT
   *   <li>Merging: Always keyed by actual window, so does not depend on {@link #activeWindows}.
   *       Cleared when window is merged away.
   *   <li>Lifetime: Cleared when trigger is closed or window is garbage collected.
   * </ul>
   */
  private final PaneInfoTracker paneInfoTracker;

  /**
   * Store whether we've seen any elements for a window since the last pane was emitted.
   *
   * <ul>
   *   <li>State: Unless DISCARDING_FIRED_PANES, a count of number of elements added so far.
   *   <li>State style: RENAMED.
   *   <li>Merging: Counts are summed when windows are merged.
   *   <li>Lifetime: Cleared when pane fires or window is garbage collected.
   * </ul>
   */
  private final NonEmptyPanes<K, W> nonEmptyPanes;

  public ReduceFnRunner(
      K key,
      WindowingStrategy<?, W> windowingStrategy,
      ExecutableTriggerStateMachine triggerStateMachine,
      StateInternals stateInternals,
      TimerInternals timerInternals,
      OutputWindowedValue<KV<K, OutputT>> outputter,
      @Nullable SideInputReader sideInputReader,
      ReduceFn<K, InputT, OutputT, W> reduceFn,
      @Nullable PipelineOptions options) {
    this.key = key;
    this.timerInternals = timerInternals;
    this.paneInfoTracker = new PaneInfoTracker(timerInternals);
    this.stateInternals = stateInternals;
    this.outputter = outputter;
    this.reduceFn = reduceFn;
    this.droppedDueToClosedWindow =
        Metrics.counter(ReduceFnRunner.class, DROPPED_DUE_TO_CLOSED_WINDOW);

    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> objectWindowingStrategy =
        (WindowingStrategy<Object, W>) windowingStrategy;
    this.windowingStrategy = objectWindowingStrategy;

    this.nonEmptyPanes = NonEmptyPanes.create(this.windowingStrategy, this.reduceFn);

    // Note this may incur I/O to load persisted window set data.
    this.activeWindows = createActiveWindowSet();

    this.contextFactory =
        new ReduceFnContextFactory<>(
            key,
            reduceFn,
            this.windowingStrategy,
            stateInternals,
            this.activeWindows,
            timerInternals,
            sideInputReader,
            options);

    this.watermarkHold = new WatermarkHold<>(timerInternals, windowingStrategy);
    this.triggerRunner =
        new TriggerStateMachineRunner<>(
            triggerStateMachine,
            new TriggerStateMachineContextFactory<>(
                windowingStrategy.getWindowFn(), stateInternals, activeWindows));
  }

  private ActiveWindowSet<W> createActiveWindowSet() {
    return windowingStrategy.getWindowFn().isNonMerging()
        ? new NonMergingActiveWindowSet<>()
        : new MergingActiveWindowSet<>(windowingStrategy.getWindowFn(), stateInternals);
  }

  @VisibleForTesting
  boolean isFinished(W window) {
    return triggerRunner.isClosed(contextFactory.base(window, StateStyle.DIRECT).state());
  }

  @VisibleForTesting
  boolean hasNoActiveWindows() {
    return activeWindows.getActiveAndNewWindows().isEmpty();
  }

  private Set<W> windowsThatAreOpen(Collection<W> windows) {
    Set<W> result = new HashSet<>();
    for (W window : windows) {
      ReduceFn<K, InputT, OutputT, W>.Context directContext =
          contextFactory.base(window, StateStyle.DIRECT);
      if (!triggerRunner.isClosed(directContext.state())) {
        result.add(window);
      }
    }
    return result;
  }

  private Collection<W> windowsThatShouldFire(Set<W> windows) throws Exception {
    Collection<W> result = new ArrayList<>();
    // Filter out timers that didn't trigger.
    for (W window : windows) {
      ReduceFn<K, InputT, OutputT, W>.Context directContext =
          contextFactory.base(window, StateStyle.DIRECT);
      if (triggerRunner.shouldFire(
          directContext.window(), directContext.timers(), directContext.state())) {
        result.add(window);
      }
    }
    return result;
  }

  /**
   * Incorporate {@code values} into the underlying reduce function, and manage holds, timers,
   * triggers, and window merging.
   *
   * <p>The general strategy is:
   *
   * <ol>
   *   <li>Use {@link WindowedValue#getWindows} (itself determined using {@link
   *       WindowFn#assignWindows}) to determine which windows each element belongs to. Some of
   *       those windows will already have state associated with them. The rest are considered NEW.
   *   <li>Use {@link WindowFn#mergeWindows} to attempt to merge currently ACTIVE and NEW windows.
   *       Each NEW window will become either ACTIVE or be discardedL. (See {@link ActiveWindowSet}
   *       for definitions of these terms.)
   *   <li>If at all possible, eagerly substitute NEW windows with their ACTIVE state address
   *       windows before any state is associated with the NEW window. In the common case that
   *       windows for new elements are merged into existing ACTIVE windows then no additional
   *       storage or merging overhead will be incurred.
   *   <li>Otherwise, keep track of the state address windows for ACTIVE windows so that their
   *       states can be merged on-demand when a pane fires.
   *   <li>Process the element for each of the windows it's windows have been merged into according
   *       to {@link ActiveWindowSet}. Processing may require running triggers, setting timers,
   *       setting holds, and invoking {@link ReduceFn#onTrigger}.
   * </ol>
   */
  public void processElements(Iterable<WindowedValue<InputT>> values) throws Exception {
    if (!values.iterator().hasNext()) {
      return;
    }

    // Determine all the windows for elements.
    Set<W> windows = collectWindows(values);
    // If an incoming element introduces a new window, attempt to merge it into an existing
    // window eagerly.
    Map<W, W> windowToMergeResult = mergeWindows(windows);
    if (!windowToMergeResult.isEmpty()) {
      // Update windows by removing all windows that were merged away and adding
      // the windows they were merged to. We add after completing all the
      // removals to avoid removing a window that was also added.
      List<W> addedWindows = new ArrayList<>(windowToMergeResult.size());
      for (Map.Entry<W, W> entry : windowToMergeResult.entrySet()) {
        windows.remove(entry.getKey());
        addedWindows.add(entry.getValue());
      }
      windows.addAll(addedWindows);
    }

    prefetchWindowsForValues(windows);

    // All windows that are open before element processing may need to fire.
    Set<W> windowsToConsider = windowsThatAreOpen(windows);

    // Process each element, using the updated activeWindows determined by mergeWindows.
    for (WindowedValue<InputT> value : values) {
      processElement(windowToMergeResult, value);
    }

    // Now that we've processed the elements, see if any of the windows need to fire.
    // Prefetch state necessary to determine if the triggers should fire.
    for (W mergedWindow : windowsToConsider) {
      triggerRunner.prefetchShouldFire(
          mergedWindow, contextFactory.base(mergedWindow, StateStyle.DIRECT).state());
    }
    // Filter to windows that are firing.
    Collection<W> windowsToFire = windowsThatShouldFire(windowsToConsider);
    // Prefetch windows that are firing.
    for (W window : windowsToFire) {
      prefetchEmit(
          contextFactory.base(window, StateStyle.DIRECT),
          contextFactory.base(window, StateStyle.RENAMED));
    }
    // Trigger output from firing windows.
    for (W window : windowsToFire) {
      emit(
          contextFactory.base(window, StateStyle.DIRECT),
          contextFactory.base(window, StateStyle.RENAMED));
    }

    // We're all done with merging and emitting elements so can compress the activeWindow state.
    // Any windows which are still NEW must have come in on a new element which was then discarded
    // due to the window's trigger being closed. We can thus delete them.
    activeWindows.cleanupTemporaryWindows();
  }

  public void persist() {
    activeWindows.persist();
  }

  /** Extract the windows associated with the values. */
  private Set<W> collectWindows(Iterable<WindowedValue<InputT>> values) throws Exception {
    Set<W> windows = new HashSet<>();
    for (WindowedValue<?> value : values) {
      for (BoundedWindow untypedWindow : value.getWindows()) {
        @SuppressWarnings("unchecked")
        W window = (W) untypedWindow;
        windows.add(window);
      }
    }
    return windows;
  }

  /**
   * Invoke merge for the given windows and return a map from windows to the merge result window.
   * Windows that were not merged are not present in the map.
   */
  private Map<W, W> mergeWindows(Set<W> windows) throws Exception {
    if (windowingStrategy.getWindowFn().isNonMerging()) {
      // Return an empty map, indicating that every window is not merged.
      return Collections.emptyMap();
    }

    Map<W, W> windowToMergeResult = new HashMap<>();
    // Collect the windows from all elements (except those which are too late) and
    // make sure they are already in the active window set or are added as NEW windows.
    for (W window : windows) {
      // For backwards compat with pre 1.4 only.
      // We may still have ACTIVE windows with multiple state addresses, representing
      // a window who's state has not yet been eagerly merged.
      // We'll go ahead and merge that state now so that we don't have to worry about
      // this legacy case anywhere else.
      if (activeWindows.isActive(window)) {
        Set<W> stateAddressWindows = activeWindows.readStateAddresses(window);
        if (stateAddressWindows.size() > 1) {
          // This is a legacy window who's state has not been eagerly merged.
          // Do that now.
          ReduceFn<K, InputT, OutputT, W>.OnMergeContext premergeContext =
              contextFactory.forPremerge(window);
          reduceFn.onMerge(premergeContext);
          watermarkHold.onMerge(premergeContext);
          activeWindows.merged(window);
        }
      }

      // Add this window as NEW if it is not currently ACTIVE.
      // If we had already seen this window and closed its trigger, then the
      // window will not be currently ACTIVE. It will then be added as NEW here,
      // and fall into the merging logic as usual.
      activeWindows.ensureWindowExists(window);
    }

    // Merge all of the active windows and retain a mapping from source windows to result windows.
    activeWindows.merge(new OnMergeCallback(windowToMergeResult));
    return windowToMergeResult;
  }

  private class OnMergeCallback implements ActiveWindowSet.MergeCallback<W> {
    private final Map<W, W> windowToMergeResult;

    OnMergeCallback(Map<W, W> windowToMergeResult) {
      this.windowToMergeResult = windowToMergeResult;
    }

    /**
     * Return the subset of {@code windows} which are currently ACTIVE. We only need to worry about
     * merging state from ACTIVE windows. NEW windows by definition have no existing state.
     */
    private List<W> activeWindows(Iterable<W> windows) {
      List<W> active = new ArrayList<>();
      for (W window : windows) {
        if (activeWindows.isActive(window)) {
          active.add(window);
        }
      }
      return active;
    }

    /**
     * Called from the active window set to indicate {@code toBeMerged} (of which only {@code
     * activeToBeMerged} are ACTIVE and thus have state associated with them) will later be merged
     * into {@code mergeResult}.
     */
    @Override
    public void prefetchOnMerge(Collection<W> toBeMerged, W mergeResult) throws Exception {
      List<W> activeToBeMerged = activeWindows(toBeMerged);
      ReduceFn<K, InputT, OutputT, W>.OnMergeContext directMergeContext =
          contextFactory.forMerge(activeToBeMerged, mergeResult, StateStyle.DIRECT);
      ReduceFn<K, InputT, OutputT, W>.OnMergeContext renamedMergeContext =
          contextFactory.forMerge(activeToBeMerged, mergeResult, StateStyle.RENAMED);

      // Prefetch various state.
      triggerRunner.prefetchForMerge(mergeResult, activeToBeMerged, directMergeContext.state());
      reduceFn.prefetchOnMerge(renamedMergeContext.state());
      watermarkHold.prefetchOnMerge(renamedMergeContext.state());
      nonEmptyPanes.prefetchOnMerge(renamedMergeContext.state());
    }

    /**
     * Called from the active window set to indicate {@code toBeMerged} (of which only {@code
     * activeToBeMerged} are ACTIVE and thus have state associated with them) are about to be merged
     * into {@code mergeResult}.
     */
    @Override
    public void onMerge(Collection<W> toBeMerged, W mergeResult) throws Exception {
      // Remember we have merged these windows.
      for (W window : toBeMerged) {
        windowToMergeResult.put(window, mergeResult);
      }

      // At this point activeWindows has NOT incorporated the results of the merge.
      List<W> activeToBeMerged = activeWindows(toBeMerged);
      ReduceFn<K, InputT, OutputT, W>.OnMergeContext directMergeContext =
          contextFactory.forMerge(activeToBeMerged, mergeResult, StateStyle.DIRECT);
      ReduceFn<K, InputT, OutputT, W>.OnMergeContext renamedMergeContext =
          contextFactory.forMerge(activeToBeMerged, mergeResult, StateStyle.RENAMED);

      // Run the reduceFn to perform any needed merging.
      reduceFn.onMerge(renamedMergeContext);

      // Merge the watermark holds.
      watermarkHold.onMerge(renamedMergeContext);

      // Merge non-empty pane state.
      nonEmptyPanes.onMerge(renamedMergeContext.state());

      // Have the trigger merge state as needed.
      triggerRunner.onMerge(
          directMergeContext.window(), directMergeContext.timers(), directMergeContext.state());

      for (W active : activeToBeMerged) {
        if (active.equals(mergeResult)) {
          // Not merged away.
          continue;
        }
        // Cleanup flavor A: Currently ACTIVE window is about to be merged away.
        // Clear any state not already cleared by the onMerge calls above.
        WindowTracing.debug("ReduceFnRunner.onMerge: Merging {} into {}", active, mergeResult);
        ReduceFn<K, InputT, OutputT, W>.Context directClearContext =
            contextFactory.base(active, StateStyle.DIRECT);
        // No need for the end-of-window or garbage collection timers.
        // We will establish a new end-of-window or garbage collection timer for the mergeResult
        // window in processElement below. There must be at least one element for the mergeResult
        // window since a new element with a new window must have triggered this onMerge.
        cancelEndOfWindowAndGarbageCollectionTimers(directClearContext);
        // We no longer care about any previous panes of merged away windows. The
        // merge result window gets to start fresh if it is new.
        paneInfoTracker.clear(directClearContext.state());
      }
    }
  }

  /**
   * Redirect element windows to the ACTIVE windows they have been merged into. The compressed
   * representation (value, {window1, window2, ...}) actually represents distinct elements (value,
   * window1), (value, window2), ... so if window1 and window2 merge, the resulting window will
   * contain both copies of the value.
   */
  private ImmutableSet<W> toMergedWindows(
      final Map<W, W> windowToMergeResult, final Collection<? extends BoundedWindow> windows) {
    return ImmutableSet.copyOf(
        FluentIterable.from(windows)
            .transform(
                untypedWindow -> {
                  @SuppressWarnings("unchecked")
                  W window = (W) untypedWindow;
                  W mergedWindow = windowToMergeResult.get(window);
                  // If the element is not present in the map, the window is unmerged.
                  return (mergedWindow == null) ? window : mergedWindow;
                }));
  }

  private void prefetchWindowsForValues(Collection<W> windows) {
    // Prefetch in each of the windows if we're going to need to process triggers
    for (W window : windows) {
      ReduceFn<K, InputT, OutputT, W>.Context directContext =
          contextFactory.base(window, StateStyle.DIRECT);
      triggerRunner.prefetchForValue(window, directContext.state());
    }
  }

  /**
   * Process an element.
   *
   * @param windowToMergeResult map of windows to merged windows. If a window is not present it is
   *     unmerged.
   * @param value the value being processed
   */
  private void processElement(Map<W, W> windowToMergeResult, WindowedValue<InputT> value)
      throws Exception {
    ImmutableSet<W> windows = toMergedWindows(windowToMergeResult, value.getWindows());

    // Process the element for each (mergeResultWindow, not closed) window it belongs to.
    for (W window : windows) {
      ReduceFn<K, InputT, OutputT, W>.ProcessValueContext directContext =
          contextFactory.forValue(
              window, value.getValue(), value.getTimestamp(), StateStyle.DIRECT);
      if (triggerRunner.isClosed(directContext.state())) {
        // This window has already been closed.
        droppedDueToClosedWindow.inc();
        WindowTracing.debug(
            "ReduceFnRunner.processElement: Dropping element at {} for key:{}; window:{} "
                + "since window is no longer active at inputWatermark:{}; outputWatermark:{}",
            value.getTimestamp(),
            key,
            window,
            timerInternals.currentInputWatermarkTime(),
            timerInternals.currentOutputWatermarkTime());
        continue;
      }

      activeWindows.ensureWindowIsActive(window);
      ReduceFn<K, InputT, OutputT, W>.ProcessValueContext renamedContext =
          contextFactory.forValue(
              window, value.getValue(), value.getTimestamp(), StateStyle.RENAMED);

      nonEmptyPanes.recordContent(renamedContext.state());
      scheduleGarbageCollectionTimer(directContext);

      // Hold back progress of the output watermark until we have processed the pane this
      // element will be included within. If the element is later than the output watermark, the
      // hold will be at GC time.
      watermarkHold.addHolds(renamedContext);

      // Execute the reduceFn, which will buffer the value as appropriate
      reduceFn.processValue(renamedContext);

      // Run the trigger to update its state
      triggerRunner.processValue(
          directContext.window(),
          directContext.timestamp(),
          directContext.timers(),
          directContext.state());

      // At this point, if triggerRunner.shouldFire before the processValue then
      // triggerRunner.shouldFire after the processValue. In other words adding values
      // cannot take a trigger state from firing to non-firing.
      // (We don't actually assert this since it is too slow.)
    }
  }

  /** A descriptor of the activation for a window based on a timer. */
  private class WindowActivation {
    public final ReduceFn<K, InputT, OutputT, W>.Context directContext;
    public final ReduceFn<K, InputT, OutputT, W>.Context renamedContext;
    // If this is an end-of-window timer then we may need to set a garbage collection timer
    // if allowed lateness is non-zero.
    public final boolean isEndOfWindow;
    // If this is a garbage collection timer then we should trigger and
    // garbage collect the window.  We'll consider any timer at or after the
    // end-of-window time to be a signal to garbage collect.
    public final boolean isGarbageCollection;

    WindowActivation(
        ReduceFn<K, InputT, OutputT, W>.Context directContext,
        ReduceFn<K, InputT, OutputT, W>.Context renamedContext) {
      this.directContext = directContext;
      this.renamedContext = renamedContext;
      W window = directContext.window();

      // The output watermark is before the end of the window if it is either unknown
      // or it is known to be before it. If it is unknown, that means that there hasn't been
      // enough data to advance it.
      boolean outputWatermarkBeforeEOW =
          timerInternals.currentOutputWatermarkTime() == null
              || !timerInternals.currentOutputWatermarkTime().isAfter(window.maxTimestamp());

      // The "end of the window" is reached when the local input watermark (for this key) surpasses
      // it but the local output watermark (also for this key) has not. After data is emitted and
      // the output watermark hold is released, the output watermark on this key will immediately
      // exceed the end of the window (otherwise we could see multiple ON_TIME outputs)
      this.isEndOfWindow =
          timerInternals.currentInputWatermarkTime().isAfter(window.maxTimestamp())
              && outputWatermarkBeforeEOW;

      // The "GC time" is reached when the input watermark surpasses the end of the window
      // plus allowed lateness. After this, the window is expired and expunged.
      this.isGarbageCollection =
          timerInternals
              .currentInputWatermarkTime()
              .isAfter(LateDataUtils.garbageCollectionTime(window, windowingStrategy));
    }

    // Has this window had its trigger finish?
    // - The trigger may implement isClosed as constant false.
    // - If the window function does not support windowing then all windows will be considered
    // active.
    // So we must take conjunction of activeWindows and triggerRunner state.
    public boolean windowIsActiveAndOpen() {
      return activeWindows.isActive(directContext.window())
          && !triggerRunner.isClosed(directContext.state());
    }
  }

  public void onTimers(Iterable<TimerData> timers) throws Exception {
    if (!timers.iterator().hasNext()) {
      return;
    }

    // Create a reusable context for each window and begin prefetching necessary
    // state.
    Map<BoundedWindow, WindowActivation> windowActivations = new HashMap();

    for (TimerData timer : timers) {
      checkArgument(
          timer.getNamespace() instanceof WindowNamespace,
          "Expected timer to be in WindowNamespace, but was in %s",
          timer.getNamespace());
      @SuppressWarnings("unchecked")
      WindowNamespace<W> windowNamespace = (WindowNamespace<W>) timer.getNamespace();
      W window = windowNamespace.getWindow();

      WindowTracing.debug(
          "{}: Received timer key:{}; window:{}; data:{} with "
              + "inputWatermark:{}; outputWatermark:{}",
          ReduceFnRunner.class.getSimpleName(),
          key,
          window,
          timer,
          timerInternals.currentInputWatermarkTime(),
          timerInternals.currentOutputWatermarkTime());

      // Processing time timers for an expired window are ignored, just like elements
      // that show up too late. Window GC is management by an event time timer
      if (TimeDomain.EVENT_TIME != timer.getDomain() && windowIsExpired(window)) {
        continue;
      }

      // How a window is processed is a function only of the current state, not the details
      // of the timer. This makes us robust to large leaps in processing time and watermark
      // time, where both EOW and GC timers come in together and we need to GC and emit
      // the final pane.
      if (windowActivations.containsKey(window)) {
        continue;
      }

      ReduceFn<K, InputT, OutputT, W>.Context directContext =
          contextFactory.base(window, StateStyle.DIRECT);
      ReduceFn<K, InputT, OutputT, W>.Context renamedContext =
          contextFactory.base(window, StateStyle.RENAMED);
      WindowActivation windowActivation = new WindowActivation(directContext, renamedContext);
      windowActivations.put(window, windowActivation);

      // Perform prefetching of state to determine if the trigger should fire.
      if (windowActivation.isGarbageCollection) {
        triggerRunner.prefetchIsClosed(directContext.state());
      } else {
        triggerRunner.prefetchShouldFire(directContext.window(), directContext.state());
      }
    }

    // For those windows that are active and open, prefetch the triggering or emitting state.
    for (WindowActivation timer : windowActivations.values()) {
      if (timer.windowIsActiveAndOpen()) {
        ReduceFn<K, InputT, OutputT, W>.Context directContext = timer.directContext;
        if (timer.isGarbageCollection) {
          prefetchOnTrigger(directContext, timer.renamedContext);
        } else if (triggerRunner.shouldFire(
            directContext.window(), directContext.timers(), directContext.state())) {
          prefetchEmit(directContext, timer.renamedContext);
        }
      }
    }

    // Perform processing now that everything is prefetched.
    for (WindowActivation windowActivation : windowActivations.values()) {
      ReduceFn<K, InputT, OutputT, W>.Context directContext = windowActivation.directContext;
      ReduceFn<K, InputT, OutputT, W>.Context renamedContext = windowActivation.renamedContext;

      if (windowActivation.isGarbageCollection) {
        WindowTracing.debug(
            "{}: Cleaning up for key:{}; window:{} with inputWatermark:{}; outputWatermark:{}",
            ReduceFnRunner.class.getSimpleName(),
            key,
            directContext.window(),
            timerInternals.currentInputWatermarkTime(),
            timerInternals.currentOutputWatermarkTime());

        boolean windowIsActiveAndOpen = windowActivation.windowIsActiveAndOpen();
        if (windowIsActiveAndOpen) {
          // We need to call onTrigger to emit the final pane if required.
          // The final pane *may* be ON_TIME if no prior ON_TIME pane has been emitted,
          // and the watermark has passed the end of the window.
          @Nullable
          Instant newHold =
              onTrigger(
                  directContext,
                  renamedContext,
                  true /* isFinished */,
                  windowActivation.isEndOfWindow);
          checkState(newHold == null, "Hold placed at %s despite isFinished being true.", newHold);
        }

        // Cleanup flavor B: Clear all the remaining state for this window since we'll never
        // see elements for it again.
        clearAllState(directContext, renamedContext, windowIsActiveAndOpen);
      } else {
        WindowTracing.debug(
            "{}.onTimers: Triggering for key:{}; window:{} at {} with "
                + "inputWatermark:{}; outputWatermark:{}",
            key,
            directContext.window(),
            timerInternals.currentInputWatermarkTime(),
            timerInternals.currentOutputWatermarkTime());
        if (windowActivation.windowIsActiveAndOpen()
            && triggerRunner.shouldFire(
                directContext.window(), directContext.timers(), directContext.state())) {
          emit(directContext, renamedContext);
        }

        if (windowActivation.isEndOfWindow) {
          // If the window strategy trigger includes a watermark trigger then at this point
          // there should be no data holds, either because we'd already cleared them on an
          // earlier onTrigger, or because we just cleared them on the above emit.
          // We could assert this but it is very expensive.

          // Since we are processing an on-time firing we should schedule the garbage collection
          // timer. (If getAllowedLateness is zero then the timer event will be considered a
          // cleanup event and handled by the above).
          // Note we must do this even if the trigger is finished so that we are sure to cleanup
          // any final trigger finished bits.
          checkState(
              windowingStrategy.getAllowedLateness().isLongerThan(Duration.ZERO),
              "Unexpected zero getAllowedLateness");
          Instant cleanupTime =
              LateDataUtils.garbageCollectionTime(directContext.window(), windowingStrategy);
          WindowTracing.debug(
              "ReduceFnRunner.onTimer: Scheduling cleanup timer for key:{}; window:{} at {} with "
                  + "inputWatermark:{}; outputWatermark:{}",
              key,
              directContext.window(),
              cleanupTime,
              timerInternals.currentInputWatermarkTime(),
              timerInternals.currentOutputWatermarkTime());
          checkState(
              !cleanupTime.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE),
              "Cleanup time %s is beyond end-of-time",
              cleanupTime);
          directContext.timers().setTimer(cleanupTime, TimeDomain.EVENT_TIME);
        }
      }
    }
  }

  /**
   * Clear all the state associated with {@code context}'s window. Should only be invoked if we know
   * all future elements for this window will be considered beyond allowed lateness. This is a
   * superset of the clearing done by {@link #emit} below since:
   *
   * <ol>
   *   <li>We can clear the trigger finished bits since we'll never need to ask if the trigger is
   *       closed again.
   *   <li>We can clear any remaining garbage collection hold.
   * </ol>
   */
  private void clearAllState(
      ReduceFn<K, InputT, OutputT, W>.Context directContext,
      ReduceFn<K, InputT, OutputT, W>.Context renamedContext,
      boolean windowIsActiveAndOpen)
      throws Exception {
    if (windowIsActiveAndOpen) {
      // Since both the window is in the active window set AND the trigger was not yet closed,
      // it is possible we still have state.
      reduceFn.clearState(renamedContext);
      watermarkHold.clearHolds(renamedContext);
      nonEmptyPanes.clearPane(renamedContext.state());
      // These calls work irrespective of whether the window is active or not, but
      // are unnecessary if the window is not active.
      triggerRunner.clearState(
          directContext.window(), directContext.timers(), directContext.state());
      paneInfoTracker.clear(directContext.state());
    } else {
      // If !windowIsActiveAndOpen then !activeWindows.isActive (1) or triggerRunner.isClosed (2).
      // For (1), if !activeWindows.isActive then the window must be merging and has been
      // explicitly removed by emit. But in that case the trigger must have fired
      // and been closed, so this case reduces to (2).
      // For (2), if triggerRunner.isClosed then the trigger was fired and entered the
      // closed state. In that case emit will have cleared all state in
      // reduceFn, triggerRunner (except for finished bits), paneInfoTracker and activeWindows.
      // We also know nonEmptyPanes must have been unconditionally cleared by the trigger.
      // Since the trigger fired the existing watermark holds must have been cleared, and since
      // the trigger closed no new end of window or garbage collection hold will have been
      // placed by WatermarkHold.extractAndRelease.
      // Thus all the state clearing above is unnecessary.
      //
      // But(!) for backwards compatibility we must allow a pipeline to be updated from
      // an sdk version <= 1.3. In that case it is possible we have an end-of-window or
      // garbage collection hold keyed by the current window (reached via directContext) rather
      // than the state address window (reached via renamedContext).
      // However this can only happen if:
      // - We have merging windows.
      // - We are DISCARDING_FIRED_PANES.
      // - A pane has fired.
      // - But the trigger is not (yet) closed.
      if (windowingStrategy.getMode() == AccumulationMode.DISCARDING_FIRED_PANES
          && !windowingStrategy.getWindowFn().isNonMerging()) {
        watermarkHold.clearHolds(directContext);
      }
    }

    // Don't need to track address state windows anymore.
    activeWindows.remove(directContext.window());
    // We'll never need to test for the trigger being closed again.
    triggerRunner.clearFinished(directContext.state());
  }

  /** Should the reduce function state be cleared? */
  private boolean shouldDiscardAfterFiring(boolean isFinished) {
    if (isFinished) {
      // This is the last firing for trigger.
      return true;
    }
    if (windowingStrategy.getMode() == AccumulationMode.DISCARDING_FIRED_PANES) {
      // Nothing should be accumulated between panes.
      return true;
    }
    return false;
  }

  private void prefetchEmit(
      ReduceFn<K, InputT, OutputT, W>.Context directContext,
      ReduceFn<K, InputT, OutputT, W>.Context renamedContext) {
    triggerRunner.prefetchShouldFire(directContext.window(), directContext.state());
    triggerRunner.prefetchOnFire(directContext.window(), directContext.state());
    triggerRunner.prefetchIsClosed(directContext.state());
    prefetchOnTrigger(directContext, renamedContext);
  }

  /** Emit if a trigger is ready to fire or timers require it, and cleanup state. */
  private void emit(
      ReduceFn<K, InputT, OutputT, W>.Context directContext,
      ReduceFn<K, InputT, OutputT, W>.Context renamedContext)
      throws Exception {
    checkState(
        triggerRunner.shouldFire(
            directContext.window(), directContext.timers(), directContext.state()));

    // Inform the trigger of the transition to see if it is finished
    triggerRunner.onFire(directContext.window(), directContext.timers(), directContext.state());
    boolean isFinished = triggerRunner.isClosed(directContext.state());

    // Will be able to clear all element state after triggering?
    boolean shouldDiscard = shouldDiscardAfterFiring(isFinished);

    // Run onTrigger to produce the actual pane contents.
    // As a side effect it will clear all element holds, but not necessarily any
    // end-of-window or garbage collection holds.
    onTrigger(directContext, renamedContext, isFinished, false /*isEndOfWindow*/);

    // Now that we've triggered, the pane is empty.
    nonEmptyPanes.clearPane(renamedContext.state());

    // Cleanup buffered data if appropriate
    if (shouldDiscard) {
      // Cleanup flavor C: The user does not want any buffered data to persist between panes.
      reduceFn.clearState(renamedContext);
    }

    if (isFinished) {
      // Cleanup flavor D: If trigger is closed we will ignore all new incoming elements.
      // Clear state not otherwise cleared by onTrigger and clearPane above.
      // Remember the trigger is, indeed, closed until the window is garbage collected.
      triggerRunner.clearState(
          directContext.window(), directContext.timers(), directContext.state());
      paneInfoTracker.clear(directContext.state());
      activeWindows.remove(directContext.window());
    }
  }

  /** Do we need to emit? */
  private boolean needToEmit(boolean isEmpty, boolean isFinished, PaneInfo.Timing timing) {
    if (!isEmpty) {
      // The pane has elements.
      return true;
    }
    if (timing == Timing.ON_TIME
        && windowingStrategy.getOnTimeBehavior() == Window.OnTimeBehavior.FIRE_ALWAYS) {
      // This is an empty ON_TIME pane.
      return true;
    }
    if (isFinished && windowingStrategy.getClosingBehavior() == ClosingBehavior.FIRE_ALWAYS) {
      // This is known to be the final pane, and the user has requested it even when empty.
      return true;
    }
    return false;
  }

  private void prefetchOnTrigger(
      final ReduceFn<K, InputT, OutputT, W>.Context directContext,
      ReduceFn<K, InputT, OutputT, W>.Context renamedContext) {
    paneInfoTracker.prefetchPaneInfo(directContext);
    watermarkHold.prefetchExtract(renamedContext);
    nonEmptyPanes.isEmpty(renamedContext.state()).readLater();
    reduceFn.prefetchOnTrigger(directContext.state());
  }

  /**
   * Run the {@link ReduceFn#onTrigger} method and produce any necessary output.
   *
   * @return output watermark hold added, or {@literal null} if none.
   */
  private @Nullable Instant onTrigger(
      final ReduceFn<K, InputT, OutputT, W>.Context directContext,
      ReduceFn<K, InputT, OutputT, W>.Context renamedContext,
      final boolean isFinished,
      boolean isEndOfWindow)
      throws Exception {
    // Extract the window hold, and as a side effect clear it.
    final WatermarkHold.OldAndNewHolds pair =
        watermarkHold.extractAndRelease(renamedContext, isFinished).read();
    // TODO: This isn't accurate if the elements are late. See BEAM-2262
    final Instant outputTimestamp = pair.oldHold;
    @Nullable Instant newHold = pair.newHold;

    final boolean isEmpty = nonEmptyPanes.isEmpty(renamedContext.state()).read();
    if (isEmpty
        && windowingStrategy.getClosingBehavior() == ClosingBehavior.FIRE_IF_NON_EMPTY
        && windowingStrategy.getOnTimeBehavior() == Window.OnTimeBehavior.FIRE_IF_NON_EMPTY) {
      return newHold;
    }

    Instant inputWM = timerInternals.currentInputWatermarkTime();
    if (newHold != null) {
      // We can't be finished yet.
      checkState(!isFinished, "new hold at %s but finished %s", newHold, directContext.window());
      // The hold cannot be behind the input watermark.
      checkState(
          !newHold.isBefore(inputWM), "new hold %s is before input watermark %s", newHold, inputWM);
      if (newHold.isAfter(directContext.window().maxTimestamp())) {
        // The hold must be for garbage collection, which can't have happened yet.
        checkState(
            newHold.isEqual(
                LateDataUtils.garbageCollectionTime(directContext.window(), windowingStrategy)),
            "new hold %s should be at garbage collection for window %s plus %s",
            newHold,
            directContext.window(),
            windowingStrategy.getAllowedLateness());
      } else {
        // The hold must be for the end-of-window, which can't have happened yet.
        checkState(
            newHold.isEqual(directContext.window().maxTimestamp()),
            "new hold %s should be at end of window %s",
            newHold,
            directContext.window());
        checkState(
            !isEndOfWindow,
            "new hold at %s for %s but this is the watermark trigger",
            newHold,
            directContext.window());
      }
    }

    // Calculate the pane info.
    final PaneInfo pane = paneInfoTracker.getNextPaneInfo(directContext, isFinished).read();

    // Only emit a pane if it has data or empty panes are observable.
    if (needToEmit(isEmpty, isFinished, pane.getTiming())) {
      // Run reduceFn.onTrigger method.
      final List<W> windows = Collections.singletonList(directContext.window());
      ReduceFn<K, InputT, OutputT, W>.OnTriggerContext renamedTriggerContext =
          contextFactory.forTrigger(
              directContext.window(),
              pane,
              StateStyle.RENAMED,
              toOutput -> {
                // We're going to output panes, so commit the (now used) PaneInfo.
                // This is unnecessary if the trigger isFinished since the saved
                // state will be immediately deleted.
                if (!isFinished) {
                  paneInfoTracker.storeCurrentPaneInfo(directContext, pane);
                }

                // Output the actual value.
                outputter.outputWindowedValue(KV.of(key, toOutput), outputTimestamp, windows, pane);
              });

      reduceFn.onTrigger(renamedTriggerContext);
    }

    return newHold;
  }

  /**
   * Schedule a timer to garbage collect the window.
   *
   * <p>The timer:
   *
   * <ul>
   *   <li>...must be fired strictly after the expiration of the window.
   *   <li>...should be as close to the expiration as possible, to have a timely output of remaining
   *       buffered data, and GC.
   * </ul>
   */
  private void scheduleGarbageCollectionTimer(ReduceFn<?, ?, ?, W>.Context directContext) {
    Instant inputWM = timerInternals.currentInputWatermarkTime();
    Instant gcTime = LateDataUtils.garbageCollectionTime(directContext.window(), windowingStrategy);
    WindowTracing.trace(
        "ReduceFnRunner.scheduleGarbageCollectionTimer: Scheduling at {} for "
            + "key:{}; window:{} where inputWatermark:{}; outputWatermark:{}",
        gcTime,
        key,
        directContext.window(),
        inputWM,
        timerInternals.currentOutputWatermarkTime());
    checkState(
        !gcTime.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE),
        "Timer %s is beyond end-of-time",
        gcTime);
    directContext.timers().setTimer(gcTime, TimeDomain.EVENT_TIME);
  }

  private void cancelEndOfWindowAndGarbageCollectionTimers(
      ReduceFn<?, ?, ?, W>.Context directContext) {
    WindowTracing.debug(
        "ReduceFnRunner.cancelEndOfWindowAndGarbageCollectionTimers: Deleting timers for "
            + "key:{}; window:{} where inputWatermark:{}; outputWatermark:{}",
        key,
        directContext.window(),
        timerInternals.currentInputWatermarkTime(),
        timerInternals.currentOutputWatermarkTime());
    Instant eow = directContext.window().maxTimestamp();
    directContext.timers().deleteTimer(eow, TimeDomain.EVENT_TIME);
    Instant gc = LateDataUtils.garbageCollectionTime(directContext.window(), windowingStrategy);
    if (gc.isAfter(eow)) {
      directContext.timers().deleteTimer(gc, TimeDomain.EVENT_TIME);
    }
  }

  private boolean windowIsExpired(BoundedWindow w) {
    return timerInternals
        .currentInputWatermarkTime()
        .isAfter(w.maxTimestamp().plus(windowingStrategy.getAllowedLateness()));
  }
}
