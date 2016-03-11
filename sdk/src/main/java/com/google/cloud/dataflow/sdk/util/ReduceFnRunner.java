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

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey.GroupByKeyOnly;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.Timing;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window.ClosingBehavior;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.ReduceFnContextFactory.OnTriggerCallbacks;
import com.google.cloud.dataflow.sdk.util.ReduceFnContextFactory.StateStyle;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;
import com.google.cloud.dataflow.sdk.util.state.ReadableState;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces.WindowNamespace;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Manages the execution of a {@link ReduceFn} after a {@link GroupByKeyOnly} has partitioned the
 * {@link PCollection} by key.
 *
 * <p>The {@link #onTrigger} relies on a {@link TriggerRunner} to manage the execution of
 * the triggering logic. The {@code ReduceFnRunner}s responsibilities are:
 *
 * <ul>
 *   <li>Tracking the windows that are active (have buffered data) as elements arrive and
 *       triggers are fired.
 *   <li>Holding the watermark based on the timestamps of elements in a pane and releasing it
 *       when the trigger fires.
 *   <li>Calling the appropriate callbacks on {@link ReduceFn} based on trigger execution, timer
 *       firings, etc, and providing appropriate contexts to the {@link ReduceFn} for actions
 *       such as output.
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
   *   <li>It runs the trigger from the {@link WindowingStrategy}.</li>
   *   <li>It merges windows according to the {@link WindowingStrategy}.</li>
   *   <li>It chooses how to track active windows and clear out expired windows
   *       according to the {@link WindowingStrategy}, based on the allowed lateness and
   *       whether windows can merge.</li>
   *   <li>It decides whether to emit empty final panes according to whether the
   *       {@link WindowingStrategy} requires it.<li>
   *   <li>It uses discarding or accumulation mode according to the {@link WindowingStrategy}.</li>
   * </ul>
   */
  private final WindowingStrategy<Object, W> windowingStrategy;

  private final OutputWindowedValue<KV<K, OutputT>> outputter;

  private final StateInternals<K> stateInternals;

  private final Aggregator<Long, Long> droppedDueToClosedWindow;

  private final K key;

  private final OnMergeCallback onMergeCallback = new OnMergeCallback();

  /**
   * Track which windows are still active and which 'state address' windows contain state
   * for a merged window.
   *
   * <ul>
   * <li>State: Global map for all active windows for this computation and key.
   * <li>Lifetime: Cleared when no active windows need to be tracked. A window lives within
   * the active window set until its trigger is closed or the window is garbage collected.
   * </ul>
   */
  private final ActiveWindowSet<W> activeWindows;

  /**
   * Always a {@link SystemReduceFn}.
   *
   * <ul>
   * <li>State: A bag of accumulated values, or the intermediate result of a combiner.
   * <li>State style: RENAMED
   * <li>Merging: Concatenate or otherwise combine the state from each merged window.
   * <li>Lifetime: Cleared when a pane fires if DISCARDING_FIRED_PANES. Otherwise cleared
   * when trigger is finished or when the window is garbage collected.
   * </ul>
   */
  private final ReduceFn<K, InputT, OutputT, W> reduceFn;

  /**
   * Manage the setting and firing of timer events.
   *
   * <ul>
   * <li>Merging: End-of-window and garbage collection timers are cancelled when windows are
   * merged away. Timers created by triggers are never garbage collected and are left to
   * fire and be ignored.
   * <li>Lifetime: Timers automatically disappear after they fire.
   * </ul>
   */
  private final TimerInternals timerInternals;

  /**
   * Manage the execution and state for triggers.
   *
   * <ul>
   * <li>State: Tracks which sub-triggers have finished, and any additional state needed to
   * determine when the trigger should fire.
   * <li>State style: DIRECT
   * <li>Merging: Finished bits are explicitly managed. Other state is eagerly merged as
   * needed.
   * <li>Lifetime: Most trigger state is cleared when the final pane is emitted. However
   * the finished bits are left behind and must be cleared when the window is
   * garbage collected.
   * </ul>
   */
  private final TriggerRunner<W> triggerRunner;

  /**
   * Store the output watermark holds for each window.
   *
   * <ul>
   * <li>State: Bag of hold timestamps.
   * <li>State style: RENAMED
   * <li>Merging: Depending on {@link OutputTimeFn}, may need to be recalculated on merging.
   * When a pane fires it may be necessary to add (back) an end-of-window or garbage collection
   * hold.
   * <li>Lifetime: Cleared when a pane fires or when the window is garbage collected.
   * </ul>
   */
  private final WatermarkHold<W> watermarkHold;

  private final ReduceFnContextFactory<K, InputT, OutputT, W> contextFactory;

  /**
   * Store the previously emitted pane (if any) for each window.
   *
   * <ul>
   * <li>State: The previous {@link PaneInfo} passed to the user's {@link DoFn#processElement},
   * if any.
   * <li>Style style: DIRECT
   * <li>Merging: Always keyed by actual window, so does not depend on {@link #activeWindows}.
   * Cleared when window is merged away.
   * <li>Lifetime: Cleared when trigger is closed or window is garbage collected.
   * </ul>
   */
  private final PaneInfoTracker paneInfoTracker;

  /**
   * Store whether we've seen any elements for a window since the last pane was emitted.
   *
   * <ul>
   * <li>State: Unless DISCARDING_FIRED_PANES, a count of number of elements added so far.
   * <li>State style: RENAMED.
   * <li>Merging: Counts are summed when windows are merged.
   * <li>Lifetime: Cleared when pane fires or window is garbage collected.
   * </ul>
   */
  private final NonEmptyPanes<K, W> nonEmptyPanes;

  public ReduceFnRunner(
      K key,
      WindowingStrategy<?, W> windowingStrategy,
      StateInternals<K> stateInternals,
      TimerInternals timerInternals,
      WindowingInternals<?, KV<K, OutputT>> windowingInternals,
      Aggregator<Long, Long> droppedDueToClosedWindow,
      ReduceFn<K, InputT, OutputT, W> reduceFn,
      PipelineOptions options) {
    this.key = key;
    this.timerInternals = timerInternals;
    this.paneInfoTracker = new PaneInfoTracker(timerInternals);
    this.stateInternals = stateInternals;
    this.outputter = new OutputViaWindowingInternals<>(windowingInternals);
    this.droppedDueToClosedWindow = droppedDueToClosedWindow;
    this.reduceFn = reduceFn;

    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> objectWindowingStrategy =
        (WindowingStrategy<Object, W>) windowingStrategy;
    this.windowingStrategy = objectWindowingStrategy;

    this.nonEmptyPanes = NonEmptyPanes.create(this.windowingStrategy, this.reduceFn);

    // Note this may incur I/O to load persisted window set data.
    this.activeWindows = createActiveWindowSet();

    this.contextFactory =
        new ReduceFnContextFactory<K, InputT, OutputT, W>(key, reduceFn, this.windowingStrategy,
            stateInternals, this.activeWindows, timerInternals, windowingInternals, options);

    this.watermarkHold = new WatermarkHold<>(timerInternals, windowingStrategy);
    this.triggerRunner =
        new TriggerRunner<>(
            windowingStrategy.getTrigger(),
            new TriggerContextFactory<>(windowingStrategy, stateInternals, activeWindows));
  }

  private ActiveWindowSet<W> createActiveWindowSet() {
    return windowingStrategy.getWindowFn().isNonMerging()
        ? new NonMergingActiveWindowSet<W>()
        : new MergingActiveWindowSet<W>(windowingStrategy.getWindowFn(), stateInternals);
  }

  @VisibleForTesting
  boolean isFinished(W window) {
    return triggerRunner.isClosed(contextFactory.base(window, StateStyle.DIRECT).state());
  }

  /**
   * Incorporate {@code values} into the underlying reduce function, and manage holds, timers,
   * triggers, and window merging.
   *
   * <p>The general strategy is:
   * <ol>
   *   <li>Use {@link WindowedValue#getWindows} (itself determined using
   *       {@link WindowFn#assignWindows}) to determine which windows each element belongs to. Some
   *       of those windows will already have state associated with them. The rest are considered
   *       NEW.
   *   <li>Use {@link WindowFn#mergeWindows} to attempt to merge currently ACTIVE and NEW windows.
   *       Each NEW window will become either ACTIVE, MERGED, or EPHEMERAL. (See {@link
   *       ActiveWindowSet} for definitions of these terms.)
   *   <li>If at all possible, eagerly substitute EPHEMERAL windows with their ACTIVE state address
   *       windows before any state is associated with the EPHEMERAL window. In the common case that
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
    // If an incoming element introduces a new window, attempt to merge it into an existing
    // window eagerly. The outcome is stored in the ActiveWindowSet.
    collectAndMergeWindows(values);

    Set<W> windowsToConsider = new HashSet<>();

    // Process each element, using the updated activeWindows determined by collectAndMergeWindows.
    for (WindowedValue<InputT> value : values) {
      windowsToConsider.addAll(processElement(value));
    }

    // Trigger output from any window for which the trigger is ready
    for (W mergedWindow : windowsToConsider) {
      ReduceFn<K, InputT, OutputT, W>.Context directContext =
          contextFactory.base(mergedWindow, StateStyle.DIRECT);
      ReduceFn<K, InputT, OutputT, W>.Context renamedContext =
          contextFactory.base(mergedWindow, StateStyle.RENAMED);
      triggerRunner.prefetchShouldFire(mergedWindow, directContext.state());
      emitIfAppropriate(directContext, renamedContext);
    }

    // We're all done with merging and emitting elements so can compress the activeWindow state.
    activeWindows.removeEphemeralWindows();
  }

  public void persist() {
    activeWindows.persist();
  }

  /**
   * Extract the windows associated with the values, and invoke merge.
   */
  private void collectAndMergeWindows(Iterable<WindowedValue<InputT>> values) throws Exception {
    // No-op if no merging can take place
    if (windowingStrategy.getWindowFn().isNonMerging()) {
      return;
    }

    // Collect the windows from all elements (except those which are too late) and
    // make sure they are already in the active window set or are added as NEW windows.
    for (WindowedValue<?> value : values) {
      for (BoundedWindow untypedWindow : value.getWindows()) {
        @SuppressWarnings("unchecked")
        W window = (W) untypedWindow;

        ReduceFn<K, InputT, OutputT, W>.Context directContext =
            contextFactory.base(window, StateStyle.DIRECT);
        if (triggerRunner.isClosed(directContext.state())) {
          // This window has already been closed.
          // We will update the counter for this in the corresponding processElement call.
          continue;
        }

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

        // Add this window as NEW if we've not yet seen it.
        activeWindows.addNew(window);
      }
    }

    // Merge all of the active windows and retain a mapping from source windows to result windows.
    mergeActiveWindows();
  }

  private class OnMergeCallback implements ActiveWindowSet.MergeCallback<W> {
    /**
     * Called from the active window set to indicate {@code toBeMerged} (of which only
     * {@code activeToBeMerged} are ACTIVE and thus have state associated with them) will later
     * be merged into {@code mergeResult}.
     */
    @Override
    public void prefetchOnMerge(
        Collection<W> toBeMerged, Collection<W> activeToBeMerged, W mergeResult) throws Exception {
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
     * Called from the active window set to indicate {@code toBeMerged} (of which only
     * {@code activeToBeMerged} are ACTIVE and thus have state associated with them) are about
     * to be merged into {@code mergeResult}.
     */
    @Override
    public void onMerge(Collection<W> toBeMerged, Collection<W> activeToBeMerged, W mergeResult)
        throws Exception {
      // At this point activeWindows has NOT incorporated the results of the merge.
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

      // Have the trigger merge state as needed
      triggerRunner.onMerge(
          directMergeContext.window(), directMergeContext.timers(), directMergeContext.state());

      for (W active : activeToBeMerged) {
        if (active.equals(mergeResult)) {
          // Not merged away.
          continue;
        }
        // Cleanup flavor A: Currently ACTIVE window is about to become MERGED.
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

  private void mergeActiveWindows() throws Exception {
    activeWindows.merge(onMergeCallback);
  }

  /**
   * Process an element.
   * @param value the value being processed
   *
   * @return the set of windows in which the element was actually processed
   */
  private Collection<W> processElement(WindowedValue<InputT> value) throws Exception {
    // Redirect element windows to the ACTIVE windows they have been merged into.
    // The compressed representation (value, {window1, window2, ...}) actually represents
    // distinct elements (value, window1), (value, window2), ...
    // so if window1 and window2 merge, the resulting window will contain both copies
    // of the value.
    Collection<W> windows = new ArrayList<>();
    for (BoundedWindow untypedWindow : value.getWindows()) {
      @SuppressWarnings("unchecked")
      W window = (W) untypedWindow;
      W active = activeWindows.representative(window);
      Preconditions.checkState(active != null, "Window %s should have been added", window);
      windows.add(active);
    }

    // Prefetch in each of the windows if we're going to need to process triggers
    for (W window : windows) {
      ReduceFn<K, InputT, OutputT, W>.ProcessValueContext directContext = contextFactory.forValue(
          window, value.getValue(), value.getTimestamp(), StateStyle.DIRECT);
      triggerRunner.prefetchForValue(window, directContext.state());
    }

    // Process the element for each (representative) window it belongs to.
    for (W window : windows) {
      ReduceFn<K, InputT, OutputT, W>.ProcessValueContext directContext = contextFactory.forValue(
          window, value.getValue(), value.getTimestamp(), StateStyle.DIRECT);
      ReduceFn<K, InputT, OutputT, W>.ProcessValueContext renamedContext = contextFactory.forValue(
          window, value.getValue(), value.getTimestamp(), StateStyle.RENAMED);

      // Check to see if the triggerRunner thinks the window is closed. If so, drop that window.
      if (triggerRunner.isClosed(directContext.state())) {
        droppedDueToClosedWindow.addValue(1L);
        WindowTracing.debug(
            "ReduceFnRunner.processElement: Dropping element at {} for key:{}; window:{} "
            + "since window is no longer active at inputWatermark:{}; outputWatermark:{}",
            value.getTimestamp(), key, window, timerInternals.currentInputWatermarkTime(),
            timerInternals.currentOutputWatermarkTime());
        continue;
      }

      nonEmptyPanes.recordContent(renamedContext.state());

      // Make sure we've scheduled the end-of-window or garbage collection timer for this window.
      Instant timer = scheduleEndOfWindowOrGarbageCollectionTimer(directContext);

      // Hold back progress of the output watermark until we have processed the pane this
      // element will be included within. If the element is too late for that, place a hold at
      // the end-of-window or garbage collection time to allow empty panes to contribute elements
      // which won't be dropped due to lateness by a following computation (assuming the following
      // computation uses the same allowed lateness value...)
      @Nullable Instant hold = watermarkHold.addHolds(renamedContext);

      if (hold != null) {
        // Assert that holds have a proximate timer.
        boolean holdInWindow = !hold.isAfter(window.maxTimestamp());
        boolean timerInWindow = !timer.isAfter(window.maxTimestamp());
        Preconditions.checkState(
            holdInWindow == timerInWindow,
            "set a hold at %s, a timer at %s, which disagree as to whether they are in window %s",
            hold,
            timer,
            directContext.window());
      }

      // Execute the reduceFn, which will buffer the value as appropriate
      reduceFn.processValue(renamedContext);

      // Run the trigger to update its state
      triggerRunner.processValue(
          directContext.window(),
          directContext.timestamp(),
          directContext.timers(),
          directContext.state());
    }

    return windows;
  }

  /**
   * Called when an end-of-window, garbage collection, or trigger-specific timer fires.
   */
  public void onTimer(TimerData timer) throws Exception {
    // Which window is the timer for?
    Preconditions.checkArgument(timer.getNamespace() instanceof WindowNamespace,
        "Expected timer to be in WindowNamespace, but was in %s", timer.getNamespace());
    @SuppressWarnings("unchecked")
    WindowNamespace<W> windowNamespace = (WindowNamespace<W>) timer.getNamespace();
    W window = windowNamespace.getWindow();
    ReduceFn<K, InputT, OutputT, W>.Context directContext =
        contextFactory.base(window, StateStyle.DIRECT);
    ReduceFn<K, InputT, OutputT, W>.Context renamedContext =
        contextFactory.base(window, StateStyle.RENAMED);

    // Has this window had its trigger finish?
    // - The trigger may implement isClosed as constant false.
    // - If the window function does not support windowing then all windows will be considered
    // active.
    // So we must take conjunction of activeWindows and triggerRunner state.
    boolean windowIsActive =
        activeWindows.isActive(window) && !triggerRunner.isClosed(directContext.state());

    if (!windowIsActive) {
      WindowTracing.debug(
          "ReduceFnRunner.onTimer: Note that timer {} is for non-ACTIVE window {}", timer, window);
    }

    // If this is a garbage collection timer then we should trigger and garbage collect the window.
    Instant cleanupTime = window.maxTimestamp().plus(windowingStrategy.getAllowedLateness());
    boolean isGarbageCollection =
        TimeDomain.EVENT_TIME == timer.getDomain() && timer.getTimestamp().equals(cleanupTime);

    if (isGarbageCollection) {
      WindowTracing.debug(
          "ReduceFnRunner.onTimer: Cleaning up for key:{}; window:{} at {} with "
          + "inputWatermark:{}; outputWatermark:{}",
          key, window, timer.getTimestamp(), timerInternals.currentInputWatermarkTime(),
          timerInternals.currentOutputWatermarkTime());

      if (windowIsActive) {
        // We need to call onTrigger to emit the final pane if required.
        // The final pane *may* be ON_TIME if no prior ON_TIME pane has been emitted,
        // and the watermark has passed the end of the window.
        onTrigger(directContext, renamedContext, true/* isFinished */);
      }

      // Cleanup flavor B: Clear all the remaining state for this window since we'll never
      // see elements for it again.
      clearAllState(directContext, renamedContext, windowIsActive);
    } else {
      WindowTracing.debug(
          "ReduceFnRunner.onTimer: Triggering for key:{}; window:{} at {} with "
          + "inputWatermark:{}; outputWatermark:{}",
          key, window, timer.getTimestamp(), timerInternals.currentInputWatermarkTime(),
          timerInternals.currentOutputWatermarkTime());
      if (windowIsActive) {
        emitIfAppropriate(directContext, renamedContext);
      }

      // If this is an end-of-window timer then, we need to set a GC timer
      boolean isEndOfWindow = TimeDomain.EVENT_TIME == timer.getDomain()
          && timer.getTimestamp().equals(window.maxTimestamp());
      if (isEndOfWindow) {
        // Since we are processing an on-time firing we should schedule the garbage collection
        // timer. (If getAllowedLateness is zero then the timer event will be considered a
        // cleanup event and handled by the above).
        // Note we must do this even if the trigger is finished so that we are sure to cleanup
        // any final trigger tombstones.
        Preconditions.checkState(
            windowingStrategy.getAllowedLateness().isLongerThan(Duration.ZERO),
            "Unexpected zero getAllowedLateness");
        WindowTracing.debug(
            "ReduceFnRunner.onTimer: Scheduling cleanup timer for key:{}; window:{} at {} with "
            + "inputWatermark:{}; outputWatermark:{}",
            key, directContext.window(), cleanupTime, timerInternals.currentInputWatermarkTime(),
            timerInternals.currentOutputWatermarkTime());
        directContext.timers().setTimer(cleanupTime, TimeDomain.EVENT_TIME);
      }
    }
  }

  /**
   * Clear all the state associated with {@code context}'s window.
   * Should only be invoked if we know all future elements for this window will be considered
   * beyond allowed lateness.
   * This is a superset of the clearing done by {@link #emitIfAppropriate} below since:
   * <ol>
   * <li>We can clear the trigger state tombstone since we'll never need to ask about it again.
   * <li>We can clear any remaining garbage collection hold.
   * </ol>
   */
  private void clearAllState(
      ReduceFn<K, InputT, OutputT, W>.Context directContext,
      ReduceFn<K, InputT, OutputT, W>.Context renamedContext,
      boolean windowIsActive)
          throws Exception {
    if (windowIsActive) {
      // Since both the window is in the active window set AND the trigger was not yet closed,
      // it is possible we still have state.
      reduceFn.clearState(renamedContext);
      watermarkHold.clearHolds(renamedContext);
      nonEmptyPanes.clearPane(renamedContext.state());
      triggerRunner.clearState(
          directContext.window(), directContext.timers(), directContext.state());
    } else {
      // Needed only for backwards compatibility over UPDATE.
      // Clear any end-of-window or garbage collection holds keyed by the current window.
      // Only needed if:
      // - We have merging windows.
      // - We are DISCARDING_FIRED_PANES.
      // - A pane has fired.
      // - But the trigger is not (yet) closed.
      if (windowingStrategy.getMode() == AccumulationMode.DISCARDING_FIRED_PANES
          && !windowingStrategy.getWindowFn().isNonMerging()) {
        watermarkHold.clearHolds(directContext);
      }
    }
    paneInfoTracker.clear(directContext.state());
    if (activeWindows.isActive(directContext.window())) {
      // Don't need to track address state windows anymore.
      activeWindows.remove(directContext.window());
    }
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

  /**
   * Possibly emit a pane if a trigger is ready to fire or timers require it, and cleanup state.
   */
  private void emitIfAppropriate(ReduceFn<K, InputT, OutputT, W>.Context directContext,
      ReduceFn<K, InputT, OutputT, W>.Context renamedContext)
      throws Exception {
    if (!triggerRunner.shouldFire(
        directContext.window(), directContext.timers(), directContext.state())) {
      // Ignore unless trigger is ready to fire
      return;
    }

    // Inform the trigger of the transition to see if it is finished
    triggerRunner.onFire(directContext.window(), directContext.timers(), directContext.state());
    boolean isFinished = triggerRunner.isClosed(directContext.state());

    // Will be able to clear all element state after triggering?
    boolean shouldDiscard = shouldDiscardAfterFiring(isFinished);

    // Run onTrigger to produce the actual pane contents.
    // As a side effect it will clear all element holds, but not necessarily any
    // end-of-window or garbage collection holds.
    onTrigger(directContext, renamedContext, isFinished);

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

  /**
   * Do we need to emit a pane?
   */
  private boolean needToEmit(boolean isEmpty, boolean isFinished, PaneInfo.Timing timing) {
    if (!isEmpty) {
      // The pane has elements.
      return true;
    }
    if (timing == Timing.ON_TIME) {
      // This is the unique ON_TIME pane.
      return true;
    }
    if (isFinished && windowingStrategy.getClosingBehavior() == ClosingBehavior.FIRE_ALWAYS) {
      // This is known to be the final pane, and the user has requested it even when empty.
      return true;
    }
    return false;
  }

  /**
   * Run the {@link ReduceFn#onTrigger} method and produce any necessary output.
   */
  private void onTrigger(
      final ReduceFn<K, InputT, OutputT, W>.Context directContext,
      ReduceFn<K, InputT, OutputT, W>.Context renamedContext,
      boolean isFinished)
          throws Exception {
    // Prefetch necessary states
    ReadableState<Instant> outputTimestampFuture =
        watermarkHold.extractAndRelease(renamedContext, isFinished).readLater();
    ReadableState<PaneInfo> paneFuture =
        paneInfoTracker.getNextPaneInfo(directContext, isFinished).readLater();
    ReadableState<Boolean> isEmptyFuture =
        nonEmptyPanes.isEmpty(renamedContext.state()).readLater();

    reduceFn.prefetchOnTrigger(directContext.state());
    triggerRunner.prefetchOnFire(directContext.window(), directContext.state());

    // Calculate the pane info.
    final PaneInfo pane = paneFuture.read();
    // Extract the window hold, and as a side effect clear it.
    final Instant outputTimestamp = outputTimestampFuture.read();

    // Only emit a pane if it has data or empty panes are observable.
    if (needToEmit(isEmptyFuture.read(), isFinished, pane.getTiming())) {
      // Run reduceFn.onTrigger method.
      final List<W> windows = Collections.singletonList(directContext.window());
      ReduceFn<K, InputT, OutputT, W>.OnTriggerContext renamedTriggerContext =
          contextFactory.forTrigger(directContext.window(), paneFuture, StateStyle.RENAMED,
              new OnTriggerCallbacks<OutputT>() {
                @Override
                public void output(OutputT toOutput) {
                  // We're going to output panes, so commit the (now used) PaneInfo.
                  // TODO: This is unnecessary if the trigger isFinished since the saved
                  // state will be immediately deleted.
                  paneInfoTracker.storeCurrentPaneInfo(directContext, pane);

                  // Output the actual value.
                  outputter.outputWindowedValue(
                      KV.of(key, toOutput), outputTimestamp, windows, pane);
                }
              });

      reduceFn.onTrigger(renamedTriggerContext);
    }
  }

  /**
   * Make sure we'll eventually have a timer fire which will tell us to garbage collect
   * the window state. For efficiency we may need to do this in two steps rather
   * than one. Return the time at which the timer will fire.
   *
   * <ul>
   * <li>If allowedLateness is zero then we'll garbage collect at the end of the window.
   * For simplicity we'll set our own timer for this situation even though an
   * {@link AfterWatermark} trigger may have also set an end-of-window timer.
   * ({@code setTimer} is idempotent.)
   * <li>If allowedLateness is non-zero then we could just always set a timer for the garbage
   * collection time. However if the windows are large (eg hourly) and the allowedLateness is small
   * (eg seconds) then we'll end up with nearly twice the number of timers in-flight. So we
   * instead set an end-of-window timer and then roll that forward to a garbage collection timer
   * when it fires. We use the input watermark to distinguish those cases.
   * </ul>
   */
  private Instant scheduleEndOfWindowOrGarbageCollectionTimer(
      ReduceFn<?, ?, ?, W>.Context directContext) {
    Instant inputWM = timerInternals.currentInputWatermarkTime();
    Instant endOfWindow = directContext.window().maxTimestamp();
    Instant fireTime;
    String which;
    if (inputWM != null && endOfWindow.isBefore(inputWM)) {
      fireTime = endOfWindow.plus(windowingStrategy.getAllowedLateness());
      which = "garbage collection";
    } else {
      fireTime = endOfWindow;
      which = "end-of-window";
    }
    WindowTracing.trace(
        "ReduceFnRunner.scheduleEndOfWindowOrGarbageCollectionTimer: Scheduling {} timer at {} for "
            + "key:{}; window:{} where inputWatermark:{}; outputWatermark:{}",
        which,
        fireTime,
        key,
        directContext.window(),
        inputWM,
        timerInternals.currentOutputWatermarkTime());
    directContext.timers().setTimer(fireTime, TimeDomain.EVENT_TIME);
    return fireTime;
  }

  private void cancelEndOfWindowAndGarbageCollectionTimers(ReduceFn<?, ?, ?, W>.Context context) {
    WindowTracing.debug(
        "ReduceFnRunner.cancelEndOfWindowAndGarbageCollectionTimers: Deleting timers for "
        + "key:{}; window:{} where inputWatermark:{}; outputWatermark:{}",
        key, context.window(), timerInternals.currentInputWatermarkTime(),
        timerInternals.currentOutputWatermarkTime());
    Instant timer = context.window().maxTimestamp();
    context.timers().deleteTimer(timer, TimeDomain.EVENT_TIME);
    if (windowingStrategy.getAllowedLateness().isLongerThan(Duration.ZERO)) {
      timer = timer.plus(windowingStrategy.getAllowedLateness());
      context.timers().deleteTimer(timer, TimeDomain.EVENT_TIME);
    }
  }

  /**
   * An object that can output a value with all of its windowing information. This is a deliberately
   * restricted subinterface of {@link WindowingInternals} to express how it is used here.
   */
  private interface OutputWindowedValue<OutputT> {
    void outputWindowedValue(OutputT output, Instant timestamp,
        Collection<? extends BoundedWindow> windows, PaneInfo pane);
  }

  private static class OutputViaWindowingInternals<OutputT>
      implements OutputWindowedValue<OutputT> {

    private final WindowingInternals<?, OutputT> windowingInternals;

    public OutputViaWindowingInternals(WindowingInternals<?, OutputT> windowingInternals) {
      this.windowingInternals = windowingInternals;
    }

    @Override
    public void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      windowingInternals.outputWindowedValue(output, timestamp, windows, pane);
    }

  }
}
