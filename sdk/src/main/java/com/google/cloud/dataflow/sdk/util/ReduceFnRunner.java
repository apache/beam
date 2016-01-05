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

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey.GroupByKeyOnly;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.Timing;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window.ClosingBehavior;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.ActiveWindowSet.MergeCallback;
import com.google.cloud.dataflow.sdk.util.ReduceFnContextFactory.OnTriggerCallbacks;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces.WindowNamespace;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages the execution of a {@link ReduceFn} after a {@link GroupByKeyOnly} has partitioned the
 * {@link PCollection} by key.
 *
 * <p>The {@link #onTrigger} relies on a {@link TriggerRunner} to manage the execution of
 * the triggering logic. The {@code ReduceFnRunner}s responsibilities are:
 *
 * <ul>
 * <li>Tracking the windows that are active (have buffered data) as elements arrive and
 * triggers are fired.
 * <li>Holding the watermark based on the timestamps of elements in a pane and releasing it
 * when the trigger fires.
 * <li>Dropping data that exceeds the maximum allowed lateness.
 * <li>Calling the appropriate callbacks on {@link ReduceFn} based on trigger execution, timer
 * firings, etc.
 * <li>Scheduling garbage collection of state associated with a specific window, and making that
 * happen when the appropriate timer fires.
 * </ul>
 *
 * @param <K> The type of key being processed.
 * @param <InputT> The type of values associated with the key.
 * @param <OutputT> The output type that will be produced for each key.
 * @param <W> The type of windows this operates on.
 */
public class ReduceFnRunner<K, InputT, OutputT, W extends BoundedWindow> {
  public static final String DROPPED_DUE_TO_CLOSED_WINDOW_COUNTER = "DroppedDueToClosedWindow";
  public static final String DROPPED_DUE_TO_LATENESS_COUNTER = "DroppedDueToLateness";

  private final WindowingStrategy<Object, W> windowingStrategy;

  private final WindowingInternals<?, KV<K, OutputT>> windowingInternals;

  private final Aggregator<Long, Long> droppedDueToClosedWindow;
  private final Aggregator<Long, Long> droppedDueToLateness;

  private final K key;

  /**
   * Track which windows are still active and which 'state address' windows contain state
   * for a merged window.
   *
   * <p>In general, when windows are merged we prefer to defer merging their state until the
   * overall state is needed. In other words, we prefer to merge state 'lazily' (on read)
   * instead of 'eagerly' (on merge).
   */
  private final ActiveWindowSet<W> activeWindows;

  /**
   * User's reduce function (or {@link SystemReduceFn} for simple GroupByKey operations).
   * May store its own state.
   *
   * <ul>
   * <li>Merging: Uses {@link #activeWindows} to determine the 'state address' windows under which
   * state is read and written. Merging may be done lazily, in which case state is merged
   * only when a pane fires.
   * <li>Lifetime: Possibly cleared when a pane fires. Always cleared when a window is
   * garbage collected.
   * </ul>
   */
  private final ReduceFn<K, InputT, OutputT, W> reduceFn;

  /**
   * Manage the setting and firing of timer events.
   *
   * <ul>
   * <li>Merging: Timers are cancelled when windows are merged away.
   * <li>Lifetime: Timers automatically disappear after they fire.
   * </ul>
   */
  private final TimerInternals timerInternals;

  /**
   * Manage the execution and state for triggers.
   *
   * <ul>
   * <li>Merging: All state is keyed by actual window, so does not depend on {@link #activeWindows}.
   * Individual triggers know how to eagerly merge their state on merge.
   * <li>Lifetime: Most trigger state is cleared when the final pane is emitted. However
   * a tombstone is left behind which must be cleared when the window is garbage collected.
   * </ul>
   */
  private final TriggerRunner<W> triggerRunner;

  /**
   * Store the output watermark holds for each window.
   *
   * <ul>
   * <li>Merging: Generally uses {@link #activeWindows} to maintain the 'state address' windows
   * under which holds are stored, and holds are merged lazily only when a pane fires.
   * However there are two special cases:
   * <ul>
   * <li>Depending on the window's {@link OutputTimeFn}, it is possible holds need to be read,
   * recalculated, cleared, and added back on merging.
   * <li>When a pane fires it may be necessary to add (back) an end-of-window or
   * garbage collection hold. If the current window is no longer active these holds will
   * be associated with the current window.
   * </ul>
   * <li>Lifetime: Cleared when a pane fires or when the window is garbage collected.
   * </ul>
   */
  private final WatermarkHold<W> watermarkHold;

  private final ReduceFnContextFactory<K, InputT, OutputT, W> contextFactory;

  /**
   * Store the previously emitted pane (if any) for each window.
   *
   * <ul>
   * <li>Merging: Always keyed by actual window, so does not depend on {@link #activeWindows}.
   * Cleared when window is merged away.
   * <li>Lifetime: Cleared when trigger is finished or window is garbage collected.
   * </ul>
   */
  private final PaneInfoTracker paneInfoTracker;

  /**
   * Store whether we've seen any elements for a window since the last pane was emitted.
   *
   * <ul>
   * <li>Merging: Uses {@link #activeWindows} determine the state address windows under which
   * counts are stored. Merging is done lazily when checking if a pane needs to fire.
   * <li>Lifetime: Cleared when pane fires or window is garbage collected.
   * </ul>
   */
  private final NonEmptyPanes<W> nonEmptyPanes;

  public ReduceFnRunner(K key, WindowingStrategy<?, W> windowingStrategy,
      TimerInternals timerInternals, WindowingInternals<?, KV<K, OutputT>> windowingInternals,
      Aggregator<Long, Long> droppedDueToClosedWindow, Aggregator<Long, Long> droppedDueToLateness,
      ReduceFn<K, InputT, OutputT, W> reduceFn) {
    this.key = key;
    this.timerInternals = timerInternals;
    this.paneInfoTracker = new PaneInfoTracker(timerInternals);
    this.windowingInternals = windowingInternals;
    this.droppedDueToClosedWindow = droppedDueToClosedWindow;
    this.droppedDueToLateness = droppedDueToLateness;
    this.reduceFn = reduceFn;

    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> objectWindowingStrategy =
        (WindowingStrategy<Object, W>) windowingStrategy;
    this.windowingStrategy = objectWindowingStrategy;

    this.nonEmptyPanes = NonEmptyPanes.create(this.windowingStrategy, this.reduceFn);
    // Note this may trigger a GetData request to load the existing window set.
    this.activeWindows = createActiveWindowSet();
    this.contextFactory =
        new ReduceFnContextFactory<K, InputT, OutputT, W>(key, reduceFn, this.windowingStrategy,
            this.windowingInternals.stateInternals(), this.activeWindows, timerInternals);

    this.watermarkHold = new WatermarkHold<>(timerInternals, windowingStrategy);
    this.triggerRunner = new TriggerRunner<>(
        windowingStrategy.getTrigger(),
        new TriggerContextFactory<>(
            windowingStrategy, this.windowingInternals.stateInternals(), activeWindows));
  }

  private ActiveWindowSet<W> createActiveWindowSet() {
    return windowingStrategy.getWindowFn().isNonMerging()
        ? new NonMergingActiveWindowSet<W>() : new MergingActiveWindowSet<W>(
               windowingStrategy.getWindowFn(), windowingInternals.stateInternals());
  }

  @VisibleForTesting
  boolean isFinished(W window) {
    return triggerRunner.isClosed(contextFactory.base(window).state());
  }

  /**
   * Incorporate {@code values} into the underlying reduce function, and manage holds, timers,
   * triggers, and window merging.
   *
   * <p>The general strategy is:
   * <ol>
   * <li>Use {@link WindowedValue#getWindows} (itself determined using
   * {@link WindowFn#assignWindows}) to determine which windows each element belongs to. Some of
   * those windows will already have state associated with them. The rest are considered NEW.
   * <li>Use {@link WindowFn#mergeWindows} to attempt to merge currently ACTIVE and NEW windows.
   * Each NEW window will become either ACTIVE, MERGED, or EPHEMERAL. (See {@link ActiveWindowSet}
   * for definitions of these terms.)
   * <li>If at all possible, eagerly substitute EPHEMERAL windows with their ACTIVE state address
   * windows before any state is associated with the EPHEMERAL window. In the common case that
   * windows for new elements are merged into existing ACTIVE windows then no additional storage
   * or merging overhead will be incurred.
   * <li>Otherwise, keep track of the state address windows for ACTIVE windows so that their
   * states can be merged on-demand when a pane fires.
   * <li>Process the element for each of the windows it's windows have been merged into according
   * to {@link ActiveWindowSet}. Processing may require running triggers, setting timers, setting
   * holds, and invoking {@link ReduceFn#onTrigger}.
   * </ol>
   */
  public void processElements(Iterable<WindowedValue<InputT>> values) {
    // Map from element window to the result of running its trigger.
    final Map<W, TriggerResult> results = Maps.newHashMap();

    if (!windowingStrategy.getWindowFn().isNonMerging()) {
      // If an incoming element introduces a new window, attempt to merge it into an existing
      // window eagerly. Otherwise track which state address windows are used to store the state
      // for each merged, active window.
      collectAndMergeWindows(values, results);
    }

    // Process each element, using the updated activeWindows determined by collectAndMergeWindows.
    for (WindowedValue<InputT> value : values) {
      processElement(results, value);
    }

    // Trigger output from any window that was triggered by merging or processing elements.
    for (Map.Entry<W, TriggerResult> result : results.entrySet()) {
      handleTriggerResult(
          contextFactory.base(result.getKey()), false/*isEndOfWindow*/, result.getValue());
    }

    // We're all done with merging and emitting elements so can compress the activeWindow state.
    activeWindows.removeEphemeralWindows();
  }

  public void persist() {
    activeWindows.persist();
  }

  /** Is {@code window} expired w.r.t. the garbage collection watermark? */
  private boolean canDropDueToExpiredWindow(W window) {
    Instant inputWM = timerInternals.currentInputWatermarkTime();
    return inputWM != null
        && window.maxTimestamp().plus(windowingStrategy.getAllowedLateness()).isBefore(inputWM);
  }

  /**
   * Extract the windows associated with the values, and invoke merge.
   *
   * @param results an output parameter that accumulates all of the windows that have had the
   *     trigger return FIRE or FIRE_AND_FINISH. Once present in this map, it is no longer
   *     necessary to evaluate triggers for the given window.
   */
  private void collectAndMergeWindows(
      Iterable<WindowedValue<InputT>> values, final Map<W, TriggerResult> results) {
    Set<W> currentlyActiveWindows = Sets.newHashSet(activeWindows.getActiveWindows());

    // Collect the windows from all elements (except those which are too late) and
    // make sure they are already in the active window set or are added as NEW windows.
    for (WindowedValue<?> value : values) {
      for (BoundedWindow untypedWindow : value.getWindows()) {
        @SuppressWarnings("unchecked")
        W window = (W) untypedWindow;

        if (canDropDueToExpiredWindow(window)) {
          // This element is too late to contribute to this window.
          // We will update the counter for this in the corresponding processElement call.
          continue;
        }

        ReduceFn<K, InputT, OutputT, W>.Context context = contextFactory.base(window);
        if (triggerRunner.isClosed(context.state())) {
          // This window has already been closed.
          // We will update the counter for this in the corresponding processElement call.
          continue;
        }
        // Add this window as NEW if we've not yet seen it.
        activeWindows.addNew(window);
      }
    }

    // Merge all of the active windows and retain a mapping from source windows to result windows.
    mergeActiveWindows(results);

    // Make sure we've scheduled timers for any ACTIVE windows we just introduced.
    // (Timers for ACTIVE windows which are now MERGED will have been discarded above.)
    for (W window : Sets.difference(activeWindows.getActiveWindows(), currentlyActiveWindows)) {
      scheduleEndOfWindowOrGarbageCollectionTimer(contextFactory.base(window));
    }
  }

  /**
   * Merge the active windows.
   *
   * @param results an output parameter that accumulates all of the windows that have had the
   *     trigger return FIRE or FIRE_AND_FINISH. Once present in this map, it is no longer
   *     necessary to evaluate triggers for the given window.
   */
  private void mergeActiveWindows(final Map<W, TriggerResult> results) {
    try {
      activeWindows.merge(new MergeCallback<W>() {
        @Override
        public void onMerge(Collection<W> toBeMerged, Collection<W> activeToBeMerged, W mergeResult)
            throws Exception {
          // At this point activeWindows has already incorporated the results of the merge.
          ReduceFn<K, InputT, OutputT, W>.OnMergeContext mergeResultContext =
              contextFactory.forMerge(toBeMerged, mergeResult);

          // Prefetch various state.
          triggerRunner.prefetchForMerge(mergeResultContext.state());

          // Run the reduceFn to perform any needed merging.
          try {
            reduceFn.onMerge(mergeResultContext);
          } catch (Exception e) {
            throw wrapMaybeUserException(e);
          }

          // Merge the watermark holds if the output time function is not just MIN.
          // Otherwise, leave all the merging window watermark holds where they are.
          watermarkHold.onMerge(mergeResultContext);

          // Have the trigger merge state as needed, and handle the result.
          TriggerResult result;
          try {
            result = triggerRunner.onMerge(mergeResultContext);
          } catch (Exception e) {
            Throwables.propagateIfPossible(e);
            throw new RuntimeException("Failed to merge the triggers", e);
          }

          if (result.isFire()) {
            results.put(mergeResult, result);
          }

          for (W active : activeToBeMerged) {
            if (active.equals(mergeResult)) {
              // Not merged away.
              continue;
            }
            WindowTracing.debug("ReduceFnRunner.mergeActiveWindows/onMerge: Merging {} into {}",
                active, mergeResult);
            // Currently ACTIVE window is about to become MERGED.
            ReduceFn<K, InputT, OutputT, W>.Context clearContext = contextFactory.base(active);
            // We are going to take care of any cleanup now, so cancel timers.
            cancelEndOfWindowAndGarbageCollectionTimers(clearContext);
            // All the trigger state has been merged. Clear any tombstones.
            triggerRunner.clearEverything(clearContext);
            // We no longer care about any previous panes of merged away windows. The
            // merge result window gets to start fresh if it is new.
            paneInfoTracker.clear(clearContext.state());
            // Any reduceFn state, watermark holds and non-empty pane state have either been
            // merged away or will be lazily merged when the next pane fires.
          }
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException("Exception while merging windows", e);
    }
  }

  /**
   * @param results a record of all of the windows that have had the trigger return FIRE or
   *     FIRE_AND_FINISH. Once present in this map, it is no longer necessary to evaluate triggers
   *     for the given result.
   * @param value the value being processed
   */
  private void processElement(Map<W, TriggerResult> results, WindowedValue<InputT> value) {
    // Redirect element windows to the ACTIVE windows they have been merged into.
    // It is possible two of the element's windows have been merged into the same window.
    // In that case we'll process the same element for the same window twice.
    @SuppressWarnings("unchecked")
    Collection<W> windows = new ArrayList<>();
    for (BoundedWindow untypedWindow : value.getWindows()) {
      @SuppressWarnings("unchecked")
      W window = (W) untypedWindow;
      if (canDropDueToExpiredWindow(window)) {
        // The element is too late for this window.
        droppedDueToLateness.addValue(1L);
        WindowTracing.debug(
            "ReduceFnRunner.processElement: Dropping element at {} for key:{}; window:{} "
            + "since too far behind inputWatermark:{}; outputWatermark:{}",
            value.getTimestamp(), key, window, timerInternals.currentInputWatermarkTime(),
            timerInternals.currentOutputWatermarkTime());
      } else {
        W active = activeWindows.representative(window);
        Preconditions.checkState(active != null, "Window %s should have been added", window);
        windows.add(active);
      }
    }

    // Prefetch in each of the windows if we're going to need to process triggers
    for (W window : windows) {
      if (!results.containsKey(window)) {
        ReduceFn<K, InputT, OutputT, W>.ProcessValueContext context =
            contextFactory.forValue(window, value.getValue(), value.getTimestamp());
        triggerRunner.prefetchForValue(context.state());
      }
    }

    // Process the element for each (representative) window it belongs to.
    for (W window : windows) {
      ReduceFn<K, InputT, OutputT, W>.ProcessValueContext context =
          contextFactory.forValue(window, value.getValue(), value.getTimestamp());

      // Check to see if the triggerRunner thinks the window is closed. If so, drop that window.
      if (!results.containsKey(window) && triggerRunner.isClosed(context.state())) {
        droppedDueToClosedWindow.addValue(1L);
        WindowTracing.debug(
            "ReduceFnRunner.processElement: Dropping element at {} for key:{}; window:{} "
            + "since window is no longer active at inputWatermark:{}; outputWatermark:{}",
            value.getTimestamp(), key, window, timerInternals.currentInputWatermarkTime(),
            timerInternals.currentOutputWatermarkTime());
        continue;
      }

      nonEmptyPanes.recordContent(context);

      // Make sure we've scheduled the end-of-window or garbage collection timer for this window
      // However if we have pre-merged then they will already have been scheduled.
      if (windowingStrategy.getWindowFn().isNonMerging()) {
        scheduleEndOfWindowOrGarbageCollectionTimer(context);
      }

      // Hold back progress of the output watermark until we have processed the pane this
      // element will be included within. Also add a hold at the end-of-window or garbage
      // collection time to allow empty panes to contribute elements which won't be dropped
      // due to lateness.
      watermarkHold.addHolds(context);

      // Execute the reduceFn, which will buffer the value as appropriate
      try {
        reduceFn.processValue(context);
      } catch (Exception e) {
        throw wrapMaybeUserException(e);
      }

      // Run the trigger and handle the result as appropriate
      if (!results.containsKey(window)) {
        try {
          TriggerResult result = triggerRunner.processValue(context);
          if (result.isFire()) {
            results.put(window, result);
          }
        } catch (Exception e) {
          Throwables.propagateIfPossible(e);
          throw new RuntimeException("Failed to run trigger", e);
        }
      }
    }
  }

  /**
   * Called when an end-of-window, garbage collection, or trigger-specific timer fires.
   */
  public void onTimer(TimerData timer) {
    // Which window is the timer for?
    Preconditions.checkArgument(timer.getNamespace() instanceof WindowNamespace,
        "Expected timer to be in WindowNamespace, but was in %s", timer.getNamespace());
    @SuppressWarnings("unchecked")
    WindowNamespace<W> windowNamespace = (WindowNamespace<W>) timer.getNamespace();
    W window = windowNamespace.getWindow();

    if (!activeWindows.isActive(window)) {
      WindowTracing.debug(
          "ReduceFnRunner.onTimer: Note that timer {} is for non-ACTIVE window {}", timer, window);
    }

    ReduceFn<K, InputT, OutputT, W>.Context context = contextFactory.base(window);

    // If this is an end-of-window timer then we should test if an AfterWatermark trigger
    // will fire.
    // It's fine if the window trigger has such trigger, this flag is only used to decide
    // if an emitted pane should be classified as ON_TIME.
    boolean isEndOfWindowTimer =
        TimeDomain.EVENT_TIME == timer.getDomain()
        && timer.getTimestamp().equals(window.maxTimestamp());

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

      if (activeWindows.isActive(window) && !triggerRunner.isClosed(context.state())) {
        // We need to call onTrigger to emit the final pane if required.
        // The final pane *may* be ON_TIME if:
        // - AllowedLateness = 0 (ie the timer is at end-of-window), and;
        // - The trigger fires on the end-of-window timer.
        boolean isWatermarkTrigger =
            isEndOfWindowTimer && runTriggersForTimer(context, timer).isFire();
        onTrigger(context, isWatermarkTrigger, true/*isFinish*/, false/*willStillBeActive*/);
      }

      // Clear all the state for this window since we'll never see elements for it again.
      try {
        clearAllState(context);
      } catch (Exception e) {
        Throwables.propagateIfInstanceOf(e, UserCodeException.class);
        throw new RuntimeException(
            "Exception while garbage collecting window " + windowNamespace.getWindow(), e);
      }
    } else {
      WindowTracing.debug(
          "ReduceFnRunner.onTimer: Triggering for key:{}; window:{} at {} with "
          + "inputWatermark:{}; outputWatermark:{}",
          key, window, timer.getTimestamp(), timerInternals.currentInputWatermarkTime(),
          timerInternals.currentOutputWatermarkTime());
      if (activeWindows.isActive(window) && !triggerRunner.isClosed(context.state())) {
        TriggerResult result = runTriggersForTimer(context, timer);
        handleTriggerResult(context, isEndOfWindowTimer, result);
      }

      if (isEndOfWindowTimer) {
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
            key, context.window(), cleanupTime, timerInternals.currentInputWatermarkTime(),
            timerInternals.currentOutputWatermarkTime());
        context.timers().setTimer(cleanupTime, TimeDomain.EVENT_TIME);
      }
    }
  }

  private TriggerResult runTriggersForTimer(
      ReduceFn<K, InputT, OutputT, W>.Context context, TimerData timer) {
    triggerRunner.prefetchForTimer(context.state());

    try {
      return triggerRunner.onTimer(context, timer);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException("Exception in onTimer for trigger", e);
    }
  }

  /**
   * Clear all the state associated with {@code context}'s window.
   * Should only be invoked if we know all future elements for this window will be considered
   * beyond allowed lateness.
   * This is a superset of the clearing done by {@link #handleTriggerResult} below since:
   * <ol>
   * <li>We can clear the trigger state tombstone since we'll never need to ask about it again.
   * <li>We can clear any remaining garbage collection hold.
   * </ol>
   */
  private void clearAllState(ReduceFn<K, InputT, OutputT, W>.Context context) throws Exception {
    boolean isActive = activeWindows.isActive(context.window());
    watermarkHold.clearHolds(context, isActive);
    if (isActive) {
      // The trigger never finished, so make sure we clear any remaining state.
      try {
        reduceFn.clearState(context);
      } catch (Exception e) {
        throw wrapMaybeUserException(e);
      }
      nonEmptyPanes.clearPane(context);
      activeWindows.remove(context.window());
    }
    triggerRunner.clearEverything(context);
    paneInfoTracker.clear(context.state());
  }

  /** Should the reduce function state be cleared? */
  private boolean shouldDiscardAfterFiring(TriggerResult result) {
    if (result.isFinish()) {
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
   * Possibly emit a pane if a trigger fired or timers require it, and cleanup state.
   */
  private void handleTriggerResult(ReduceFn<K, InputT, OutputT, W>.Context context,
      boolean isEndOfWindow, TriggerResult result) {
    if (!result.isFire()) {
      // Ignore unless trigger fired.
      return;
    }

    // If the trigger fired due to an end-of-window timer, treat it as an AfterWatermark trigger.
    boolean isWatermarkTrigger = isEndOfWindow;

    // Will be able to clear all element state after triggering?
    boolean shouldDiscard = shouldDiscardAfterFiring(result);

    // Run onTrigger to produce the actual pane contents.
    // As a side effect it will clear all element holds, but not necessarily any
    // end-of-window or garbage collection holds.
    onTrigger(context, isWatermarkTrigger, result.isFinish(), !shouldDiscard);

    // Now that we've triggered, the pane is empty.
    nonEmptyPanes.clearPane(context);

    // Cleanup buffered data if appropriate
    if (shouldDiscard) {
      // Clear the reduceFn state across all windows in the equivalence class for the current
      // window.
      try {
        reduceFn.clearState(context);
      } catch (Exception e) {
        throw wrapMaybeUserException(e);
      }

      // Remove the window from active set.
      // This will forget the equivalence class for this window.
      WindowTracing.debug("ReduceFnRunner.handleTriggerResult: removing {}", context.window());
      activeWindows.remove(context.window());

      if (!result.isFinish()) {
        // We still need to consider this window active since we may have had to add an
        // end-of-window or garbage collection hold above.
        activeWindows.addActive(context.window());
      }
    }

    if (result.isFinish()) {
      // If we're finishing, eagerly clear state to reduce pressure on the backend.
      // Leave behind a tombstone in the trigger runner so we know the trigger is finished.
      try {
        triggerRunner.clearState(context);
        paneInfoTracker.clear(context.state());
      } catch (Exception e) {
        Throwables.propagateIfPossible(e);
        throw new RuntimeException("Exception while clearing trigger state", e);
      }
      // No more watermark holds will be placed (even for end-of-window or garbage
      // collection holds).
    }
  }

  /**
   * Do we need to emit a pane?
   */
  private boolean needToEmit(
      boolean isEmpty, boolean isWatermarkTrigger, boolean isFinish, PaneInfo.Timing timing) {
    if (!isEmpty) {
      // The pane has elements.
      return true;
    }
    if (isWatermarkTrigger && timing == Timing.ON_TIME) {
      // This is the unique ON_TIME pane, triggered by an AfterWatermark.
      return true;
    }
    if (isFinish && windowingStrategy.getClosingBehavior() == ClosingBehavior.FIRE_ALWAYS) {
      // This is known to be the final pane, and the user has requested it even when empty.
      return true;
    }
    return false;
  }

  /**
   * Run the {@link ReduceFn#onTrigger} method and produce any necessary output.
   *
   * @param context the context for the pane to fire
   * @param isWatermarkTrigger true if this triggering is for an AfterWatermark trigger
   * @param isFinish true if this will be the last triggering processed
   */
  private void onTrigger(final ReduceFn<K, InputT, OutputT, W>.Context context,
      boolean isWatermarkTrigger, boolean isFinish, boolean willStillBeActive) {
    // Collect state.
    StateContents<Instant> outputTimestampFuture =
        watermarkHold.extractAndRelease(context, isFinish, willStillBeActive);
    StateContents<PaneInfo> paneFuture =
        paneInfoTracker.getNextPaneInfo(context, isWatermarkTrigger, isFinish);
    StateContents<Boolean> isEmptyFuture = nonEmptyPanes.isEmpty(context);

    reduceFn.prefetchOnTrigger(context.state());

    // Calculate the pane info.
    final PaneInfo pane = paneFuture.read();
    // Extract the window hold, and as a side effect clear it.
    final Instant outputTimestamp = outputTimestampFuture.read();

    // Only emit a pane if it has data or empty panes are observable.
    if (needToEmit(isEmptyFuture.read(), isWatermarkTrigger, isFinish, pane.getTiming())) {
      // Run reduceFn.onTrigger method.
      final List<W> windows = Collections.singletonList(context.window());
      ReduceFn<K, InputT, OutputT, W>.OnTriggerContext triggerContext = contextFactory.forTrigger(
          context.window(), paneFuture, new OnTriggerCallbacks<OutputT>() {
            @Override
            public void output(OutputT toOutput) {
              // We're going to output panes, so commit the (now used) PaneInfo.
              // TODO: Unnecessary if isFinal?
              paneInfoTracker.storeCurrentPaneInfo(context, pane);

              // Output the actual value.
              windowingInternals.outputWindowedValue(
                  KV.of(key, toOutput), outputTimestamp, windows, pane);
            }
          });

      try {
        reduceFn.onTrigger(triggerContext);
      } catch (Exception e) {
        throw wrapMaybeUserException(e);
      }
    }
  }

  private void scheduleEndOfWindowOrGarbageCollectionTimer(ReduceFn<?, ?, ?, W>.Context context) {
    Instant fireTime = context.window().maxTimestamp();
    String which = "end-of-window";
    Instant inputWM = timerInternals.currentInputWatermarkTime();
    if (inputWM != null && fireTime.isBefore(inputWM)) {
      fireTime = fireTime.plus(windowingStrategy.getAllowedLateness());
      which = "garbage collection";
      Preconditions.checkState(!fireTime.isBefore(inputWM),
          "Asking to set a timer at %s behind input watermark %s", fireTime, inputWM);
    }
    WindowTracing.trace(
        "ReduceFnRunner.scheduleEndOfWindowOrGarbageCollectionTimer: Scheduling {} timer at {} for "
        + "key:{}; window:{} where inputWatermark:{}; outputWatermark:{}",
        which, fireTime, key, context.window(), inputWM,
        timerInternals.currentOutputWatermarkTime());
    context.timers().setTimer(fireTime, TimeDomain.EVENT_TIME);
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

  //////////////////////////////////////////////////////////////////////////////////////////////////

  private RuntimeException wrapMaybeUserException(Throwable t) {
    if (reduceFn instanceof SystemReduceFn) {
      throw Throwables.propagate(t);
    } else {
      // Any exceptions that happen inside a non-system ReduceFn are considered user code.
      throw new UserCodeException(t);
    }
  }
}
