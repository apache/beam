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
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.Timing;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window.ClosingBehavior;
import com.google.cloud.dataflow.sdk.util.ActiveWindowSet.MergeCallback;
import com.google.cloud.dataflow.sdk.util.ReduceFnContextFactory.OnTriggerCallbacks;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces.WindowNamespace;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
  private final TimerInternals timerInternals;
  private final WindowingInternals<?, KV<K, OutputT>> windowingInternals;

  private final Aggregator<Long, Long> droppedDueToClosedWindow;
  private final Aggregator<Long, Long> droppedDueToLateness;

  private final TriggerRunner<W> triggerRunner;

  private final K key;
  private final ActiveWindowSet<W> activeWindows;
  private final WatermarkHold<W> watermarkHold;
  private final ReduceFnContextFactory<K, InputT, OutputT, W> contextFactory;
  private final ReduceFn<K, InputT, OutputT, W> reduceFn;
  private final PaneInfoTracker paneInfo;
  private final NonEmptyPanes<W> nonEmptyPanes;

  public ReduceFnRunner(
      K key,
      WindowingStrategy<?, W> windowingStrategy,
      TimerInternals timerInternals,
      WindowingInternals<?, KV<K, OutputT>> windowingInternals,
      Aggregator<Long, Long> droppedDueToClosedWindow,
      Aggregator<Long, Long> droppedDueToLateness,
      ReduceFn<K, InputT, OutputT, W> reduceFn) {
    this.key = key;
    this.timerInternals = timerInternals;
    this.paneInfo =  new PaneInfoTracker(timerInternals);
    this.windowingInternals = windowingInternals;
    this.droppedDueToClosedWindow = droppedDueToClosedWindow;
    this.droppedDueToLateness = droppedDueToLateness;
    this.reduceFn = reduceFn;

    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> objectWindowingStrategy =
        (WindowingStrategy<Object, W>) windowingStrategy;
    this.windowingStrategy = objectWindowingStrategy;

    this.nonEmptyPanes = NonEmptyPanes.create(this.windowingStrategy, this.reduceFn);
    this.activeWindows = createActiveWindowSet();
    this.contextFactory = new ReduceFnContextFactory<K, InputT, OutputT, W>(
        key, reduceFn, this.windowingStrategy, this.windowingInternals.stateInternals(),
        this.activeWindows, timerInternals);

    this.watermarkHold = new WatermarkHold<>(timerInternals, windowingStrategy);
    this.triggerRunner = new TriggerRunner<>(
        windowingStrategy.getTrigger(),
        new TriggerContextFactory<>(windowingStrategy, this.windowingInternals.stateInternals(),
            activeWindows));
  }

  private ActiveWindowSet<W> createActiveWindowSet() {
    return windowingStrategy.getWindowFn().isNonMerging()
        ? new NonMergingActiveWindowSet<W>()
        : new MergingActiveWindowSet<W>(
            windowingStrategy.getWindowFn(), windowingInternals.stateInternals());
  }

  @VisibleForTesting boolean isFinished(W window) {
    return triggerRunner.isClosed(contextFactory.base(window).state());
  }

  public void processElements(Iterable<WindowedValue<InputT>> values) {
    Function<W, W> windowMapping = Functions.identity();

    final Map<W, TriggerResult> results = Maps.newHashMap();

    // If windows might merge, extract the windows from all the values, and pre-merge them.
    if (!windowingStrategy.getWindowFn().isNonMerging()) {
      windowMapping = premergeForValues(values, results);
    }

    // Process the elements
    for (WindowedValue<InputT> value : values) {
      processElement(windowMapping, results, value);
    }

    // Trigger output from any window that was triggered by merging or processing elements.
    for (Map.Entry<W, TriggerResult> result : results.entrySet()) {
      handleTriggerResult(
          contextFactory.base(result.getKey()), false/*isEndOfWindow*/, result.getValue());
    }
  }

  /**
   * Extract the windows associated with the values, and invoke merge.
   *
   * @param results an output parameter that accumulates all of the windows that have had the
   *     trigger return FIRE or FIRE_AND_FINISH. Once present in this map, it is no longer
   *     necessary to evaluate triggers for the given window.
   * @return A function which maps the initial windows of the values to the intermediate windows
   *     they should be processed in.
   */
  private Function<W, W> premergeForValues(
      Iterable<WindowedValue<InputT>> values, final Map<W, TriggerResult> results) {
    // Add the windows from the values to the active window set, and keep track of which ones
    // were not previously in the active window set.
    Set<W> newWindows = addToActiveWindows(values);

    // Merge all of the active windows and retain a mapping from source windows to result windows.
    final Map<W, W> sourceWindowsToResultWindows = mergeActiveWindows(results);

    // For any new windows that survived merging, make sure we've scheduled cleanup
    for (W window : newWindows) {
      if (activeWindows.contains(window)) {
        scheduleEndOfWindowOrGarbageCollectionTimer(contextFactory.base(window));
      }
    }

    // Update our window mapping function.
    return new Function<W, W>() {
      @Override
      public W apply(W input) {
        W result = sourceWindowsToResultWindows.get(input);
        // If null, the initial window wasn't subject to any merging.
        return result == null ? input : result;
      }
    };
  }

  /**
   * Merge the active windows.
   *
   * @param results an output parameter that accumulates all of the windows that have had the
   *     trigger return FIRE or FIRE_AND_FINISH. Once present in this map, it is no longer
   *     necessary to evaluate triggers for the given window.
   * @return A map from initial windows of the values to the intermediate windows they should be
   *     processed in. The domain will be the windows that were merged into intermediate windows
   *     and the range is the intermediate windows that exist in the active window set.
   */
  private Map<W, W> mergeActiveWindows(final Map<W, TriggerResult> results) {
    final Map<W, W> sourceWindowsToResultWindows =
        Maps.newHashMapWithExpectedSize(activeWindows.size());

    try {
      activeWindows.merge(new MergeCallback<W>() {
        @Override
        public void onMerge(Collection<W> mergedWindows, W resultWindow, boolean isResultNew)
            throws Exception {
          // We only need to call onMerge with windows that were previously persisted.
          Collection<W> originalWindows = activeWindows.originalWindows(mergedWindows);
          if (!originalWindows.isEmpty()) {
            TriggerResult result =
                ReduceFnRunner.this.onMerge(originalWindows, resultWindow, isResultNew);
            if (result.isFire()) {
              results.put(resultWindow, result);
            }
          } else {
            // If there were no windows, then merging didn't rearrange the cleanup timers. Make
            // sure that we have one properly scheduled
            scheduleEndOfWindowOrGarbageCollectionTimer(contextFactory.base(resultWindow));
          }

          for (W mergedWindow : mergedWindows) {
            sourceWindowsToResultWindows.put(mergedWindow, resultWindow);

            // If the window wasn't in the persisted original set, then we scheduled cleanup above
            // but didn't pass it to merge to have the cleanup canceled. Do so here
            if (!originalWindows.contains(mergedWindow)) {
              cancelEndOfWindowAndGarbageCollectionTimers(contextFactory.base(mergedWindow));
            }
          }
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException("Exception while merging windows", e);
    }
    return sourceWindowsToResultWindows;
  }

  /** Is {@code value} late w.r.t. the garbage collection watermark? */
  private <T> boolean canDropDueToLateness(WindowedValue<T> value) {
    Instant inputWM = timerInternals.currentInputWatermarkTime();
    return inputWM != null
        && value.getTimestamp().isBefore(inputWM.minus(windowingStrategy.getAllowedLateness()));
  }

  /**
   * Add the initial windows from each of the values to the active window set. Returns the set of
   * new windows.
   */
  private Set<W> addToActiveWindows(Iterable<WindowedValue<InputT>> values) {
    Set<W> newWindows = new HashSet<>();
    for (WindowedValue<?> value : values) {
      if (canDropDueToLateness(value)) {
        // This value will be dropped (and reported in a counter) by processElement.
        // Hence it won't contribute to any new window.
        continue;
      }

      for (BoundedWindow untypedWindow : value.getWindows()) {
        @SuppressWarnings("unchecked")
        W window = (W) untypedWindow;
        ReduceFn<K, InputT, OutputT, W>.Context context = contextFactory.base(window);
        if (!triggerRunner.isClosed(context.state())) {
          if (activeWindows.add(window)) {
            newWindows.add(window);
          }
        }
      }
    }
    return newWindows;
  }

  /**
   * @param windowMapping a function which maps windows associated with the value to the window that
   *     it was merged into, and in which we should actually process the element
   * @param results a record of all of the windows that have had the trigger return FIRE or
   *     FIRE_AND_FINISH. Once present in this map, it is no longer necessary to evaluate triggers
   *     for the given result.
   * @param value the value being processed
   */
  private void processElement(
      Function<W, W> windowMapping, Map<W, TriggerResult> results, WindowedValue<InputT> value) {
    if (canDropDueToLateness(value)) {
      // Drop the element in all assigned windows if it is past the allowed lateness limit.
      droppedDueToLateness.addValue((long) value.getWindows().size());
      WindowTracing.debug(
          "processElement: Dropping element at {} for key:{} since too far "
          + "behind inputWatermark:{}; outputWatermark:{}",
          value.getTimestamp(), key, timerInternals.currentInputWatermarkTime(),
          timerInternals.currentOutputWatermarkTime());
      return;
    }

    // Only consider representative windows from among all windows in equivalence classes
    // induced by window merging.
    @SuppressWarnings("unchecked")
    Iterable<W> windows =
        FluentIterable.from((Collection<W>) value.getWindows()).transform(windowMapping);

    // Prefetch in each of the windows if we're going to need to process triggers
    for (W window : windows) {
      if (!results.containsKey(window)) {
        ReduceFn<K, InputT, OutputT, W>.ProcessValueContext context =
            contextFactory.forValue(window, value.getValue(), value.getTimestamp());
        triggerRunner.prefetchForValue(context.state());
      }
    }

    // And process each of the windows
    for (W window : windows) {
      ReduceFn<K, InputT, OutputT, W>.ProcessValueContext context =
          contextFactory.forValue(window, value.getValue(), value.getTimestamp());

      // Check to see if the triggerRunner thinks the window is closed. If so, drop that window.
      if (!results.containsKey(window) && triggerRunner.isClosed(context.state())) {
          droppedDueToClosedWindow.addValue(1L);
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
   * Make sure that all the state built up in this runner has been persisted.
   */
  public void persist() {
    activeWindows.persist();
  }

  /**
   * Called when windows merge.
   */
  public TriggerResult onMerge(
      Collection<W> mergedWindows, W resultWindow, boolean isResultWindowNew) {
    ReduceFn<K, InputT, OutputT, W>.OnMergeContext resultContext =
        contextFactory.forMerge(mergedWindows, resultWindow);

    // Schedule state reads for trigger execution.
    triggerRunner.prefetchForMerge(resultContext.state());

    // Run the reduceFn to perform any needed merging.
    try {
      reduceFn.onMerge(resultContext);
    } catch (Exception e) {
      throw wrapMaybeUserException(e);
    }

    // Merge the watermark hold
    watermarkHold.mergeHolds(resultContext);

    // Have the trigger merge state as needed, and handle the result.
    TriggerResult triggerResult;
    try {
      triggerResult = triggerRunner.onMerge(resultContext);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException("Failed to merge the triggers", e);
    }

    // Cleanup the trigger state in the old windows.
    for (W mergedWindow : mergedWindows) {
      if (!mergedWindow.equals(resultWindow)) {
        try {
          ReduceFn<K, InputT, OutputT, W>.Context mergedContext = contextFactory.base(mergedWindow);
          cancelEndOfWindowAndGarbageCollectionTimers(mergedContext);
          triggerRunner.clearEverything(mergedContext);
          paneInfo.clear(mergedContext.state());
        } catch (Exception e) {
          Throwables.propagateIfPossible(e);
          throw new RuntimeException("Exception while clearing trigger state", e);
        }
      }
    }

    // Schedule cleanup if the window is new. Do this after cleaning up the old state in case one
    // of them had a timer at the same point.
    if (isResultWindowNew) {
      scheduleEndOfWindowOrGarbageCollectionTimer(resultContext);
    }

    return triggerResult;
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
    // If the window is subject to merging then all timers should have been cleared upon merge.
    Preconditions.checkState(
        !windowingStrategy.getWindowFn().isNonMerging() || activeWindows.contains(window),
        "Received timer %s for inactive window %s", timer, window);

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
          "onTimer: Cleaning up for key:{}; window:{} at {} with "
          + "inputWatermark:{}; outputWatermark:{}",
          key, window, timer.getTimestamp(), timerInternals.currentInputWatermarkTime(),
          timerInternals.currentOutputWatermarkTime());

      if (activeWindows.contains(window) && !triggerRunner.isClosed(context.state())) {
        // We need to call onTrigger to emit the final pane if required.
        // The final pane *may* be ON_TIME if:
        // - AllowedLateness = 0 (ie the timer is at end-of-window), and;
        // - The trigger fires on the end-of-window timer.
        boolean isWatermarkTrigger =
            isEndOfWindowTimer && runTriggersForTimer(context, timer).isFire();
        onTrigger(context, isWatermarkTrigger, true/*isFinish*/);
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
          "onTimer: Triggering for key:{}; window:{} at {} with "
          + "inputWatermark:{}; outputWatermark:{}",
          key, window, timer.getTimestamp(), timerInternals.currentInputWatermarkTime(),
          timerInternals.currentOutputWatermarkTime());
      boolean isFinish = false;
      if (activeWindows.contains(window) && !triggerRunner.isClosed(context.state())) {
        TriggerResult result = runTriggersForTimer(context, timer);
        handleTriggerResult(context, isEndOfWindowTimer, result);
        isFinish = result.isFinish();
      }

      if (isEndOfWindowTimer && !isFinish) {
        // Since we are processing an on-time firing we should schedule the garbage collection
        // timer. (If getAllowedLateness is zero then the timer event will be considered a
        // cleanup event and handled by the above).
        Preconditions.checkState(
            windowingStrategy.getAllowedLateness().isLongerThan(Duration.ZERO),
            "Unexpected zero getAllowedLateness");
        WindowTracing.debug(
            "onTimer: Scheduling cleanup timer for key:{}; window:{} at {} with "
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
    nonEmptyPanes.clearPane(context);
    try {
      reduceFn.clearState(context);
    } catch (Exception e) {
      throw wrapMaybeUserException(e);
    }
    triggerRunner.clearEverything(context);
    paneInfo.clear(context.state());
    watermarkHold.clear(context);
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

    // Run onTrigger to produce the actual pane contents.
    // As a side effect it will clear all element holds, but not necessarily any
    // end-of-window or garbage collection holds.
    onTrigger(context, isWatermarkTrigger, result.isFinish());

    // Now that we've triggered, the pane is empty.
    nonEmptyPanes.clearPane(context);

    // Cleanup buffered data if appropriate
    if (shouldDiscardAfterFiring(result)) {
      // Clear the reduceFn state
      try {
        reduceFn.clearState(context);
      } catch (Exception e) {
        throw wrapMaybeUserException(e);
      }

      // Remove the window from active set -- nothing is buffered.
      activeWindows.remove(context.window());
    }

    if (result.isFinish()) {
      // If we're finishing, clear up the trigger tree as well.
      // However, we'll leave behind a tombstone so we know the trigger is finished.
      try {
        triggerRunner.clearState(context);
        paneInfo.clear(context.state());
      } catch (Exception e) {
        Throwables.propagateIfPossible(e);
        throw new RuntimeException("Exception while clearing trigger state", e);
      }
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
      boolean isWatermarkTrigger, boolean isFinish) {
    // Collect state.
    StateContents<Instant> outputTimestampFuture =
        watermarkHold.extractAndRelease(context, isFinish);
    StateContents<PaneInfo> paneFuture =
        paneInfo.getNextPaneInfo(context, isWatermarkTrigger, isFinish);
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
              paneInfo.storeCurrentPaneInfo(context, pane);

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
        "scheduleTimer: Scheduling {} timer at {} for "
        + "key:{}; window:{} where inputWatermark:{}; outputWatermark:{}",
        which, fireTime, key, context.window(), inputWM,
        timerInternals.currentOutputWatermarkTime());
    context.timers().setTimer(fireTime, TimeDomain.EVENT_TIME);
  }

  private void cancelEndOfWindowAndGarbageCollectionTimers(ReduceFn<?, ?, ?, W>.Context context) {
    WindowTracing.debug(
        "cancelTimer: Deleting timers for "
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
