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
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;

import org.joda.time.Instant;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    this.watermarkHold = new WatermarkHold<>(windowingStrategy);
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
      handleTriggerResult(contextFactory.base(result.getKey()), false, result.getValue());
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
        scheduleCleanup(contextFactory.base(window));
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
            scheduleCleanup(contextFactory.base(resultWindow));
          }

          for (W mergedWindow : mergedWindows) {
            sourceWindowsToResultWindows.put(mergedWindow, resultWindow);

            // If the window wasn't in the persisted original set, then we scheduled cleanup above
            // but didn't pass it to merge to have the cleanup canceled. Do so here
            if (!originalWindows.contains(mergedWindow)) {
              cancelCleanup(contextFactory.base(mergedWindow));
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

  /**
   * Add the initial windows from each of the values to the active window set. Returns the set of
   * new windows.
   */
  private Set<W> addToActiveWindows(Iterable<WindowedValue<InputT>> values) {
    Set<W> newWindows = new HashSet<>();
    for (WindowedValue<?> value : values) {
      if (getLateness(value.getTimestamp()).isPastAllowedLateness) {
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
  private void processElement(Function<W, W> windowMapping, Map<W, TriggerResult> results,
      WindowedValue<InputT> value) {
    Lateness lateness = getLateness(value.getTimestamp());
    if (lateness.isPastAllowedLateness) {
      // Drop the element in all assigned windows if it is past the allowed lateness limit.
      droppedDueToLateness.addValue((long) value.getWindows().size());
      return;
    }

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

      // Make sure we've scheduled the cleanup timer for this window, if the premerge didn't already
      // do that.
      if (windowingStrategy.getWindowFn().isNonMerging()) {
        // Since non-merging window functions don't track the active window set, we always schedule
        // cleanup.
        scheduleCleanup(context);
      }

      // Update the watermark hold since the value will be part of the next pane.
      watermarkHold.addHold(context, lateness.isLate);

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

  private void holdForEmptyPanes(ReduceFn<K, InputT, OutputT, W>.Context context) {
    if (timerInternals.currentWatermarkTime().isAfter(context.window().maxTimestamp())) {
      watermarkHold.holdForFinal(context);
    } else {
      watermarkHold.holdForOnTime(context);
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
          cancelCleanup(mergedContext);
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
      scheduleCleanup(resultContext);
    }

    return triggerResult;
  }

  /**
   * Called when a timer fires.
   */
  public void onTimer(TimerData timer) {
    if (!(timer.getNamespace() instanceof WindowNamespace)) {
      throw new IllegalArgumentException(
          "Expected WindowNamespace, but was " + timer.getNamespace());
    }

    @SuppressWarnings("unchecked")
    WindowNamespace<W> windowNamespace = (WindowNamespace<W>) timer.getNamespace();
    W window = windowNamespace.getWindow();
    if (!activeWindows.contains(window) && windowingStrategy.getWindowFn().isNonMerging()) {
      throw new IllegalStateException(
          "Internal Error: Received timer " + timer + " for inactive window: " + window);
    }

    ReduceFn<K, InputT, OutputT, W>.Context context = contextFactory.base(window);

    // If this timer firing is at the watermark, then it may cause a trigger firing of an
    // AfterWatermark trigger.
    boolean isAtWatermark = TimeDomain.EVENT_TIME == timer.getDomain()
        && !timer.getTimestamp().isBefore(window.maxTimestamp());

    if (shouldCleanup(timer, window)) {
      // We're going to cleanup the window. We want to treat any potential output from this as
      // the at-watermark firing if the current time is the at-watermark firing and there was a
      // trigger waiting for it.
      if (isAtWatermark) {
        TriggerResult timerResult = runTriggersForTimer(context, timer);
        isAtWatermark = (timerResult != null && timerResult.isFire());
      }

      // Do the actual cleanup
      try {
        doCleanup(context, isAtWatermark);
      } catch (Exception e) {
        Throwables.propagateIfInstanceOf(e, UserCodeException.class);
        throw new RuntimeException(
            "Exception while garbage collecting window " + windowNamespace.getWindow(), e);
      }
    } else {
      if (activeWindows.contains(window) && !triggerRunner.isClosed(context.state())) {
        handleTriggerResult(context, isAtWatermark, runTriggersForTimer(context, timer));
      }

      if (TimeDomain.EVENT_TIME == timer.getDomain()
          // If we processed an on-time firing, we should schedule the GC timer.
          && timer.getTimestamp().isEqual(window.maxTimestamp())) {
        scheduleCleanup(context);
      }
    }
  }

  /**
   * Return true if either the timer looks like a cleanup timer or the current watermark is so far
   * gone that we should cleanup the window.
   */
  private boolean shouldCleanup(TimerData timer, W window) {
    return TimeDomain.EVENT_TIME == timer.getDomain()
        && (isCleanupTime(window, timer.getTimestamp())
            || isCleanupTime(window, timerInternals.currentWatermarkTime()));
  }

  @Nullable
  private TriggerResult runTriggersForTimer(
      ReduceFn<K, InputT, OutputT, W>.Context context, TimerData timer) {

    triggerRunner.prefetchForTimer(context.state());

    // Skip timers for windows that were closed by triggers, but haven't expired yet.
    if (triggerRunner.isClosed(context.state())) {
      return null;
    }

    try {
      return triggerRunner.onTimer(context, timer);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException("Exception in onTimer for trigger", e);
    }
  }

  /** Called when the cleanup timer has fired for the given window. */
  private void doCleanup(
      ReduceFn<K, InputT, OutputT, W>.Context context, boolean maybeAtWatermark) throws Exception {
    // If the window isn't closed, or if we should always fire a final pane, then trigger output
    if (!triggerRunner.isClosed(context.state())
        || windowingStrategy.getClosingBehavior() == ClosingBehavior.FIRE_ALWAYS) {
      onTrigger(context, maybeAtWatermark, true /* isFinal */);
    }

    // Cleanup the associated state.
    nonEmptyPanes.clearPane(context);
    try {
      reduceFn.clearState(context);
    } catch (Exception e) {
      throw wrapMaybeUserException(e);
    }
    triggerRunner.clearEverything(context);
    paneInfo.clear(context.state());
    watermarkHold.releaseOnTime(context);
  }

  private void handleTriggerResult(
      ReduceFn<K, InputT, OutputT, W>.Context context,
      boolean maybeAtWatermark, TriggerResult result) {
    // Unless the trigger is firing, there is nothing to do.
    if (!result.isFire()) {
      return;
    }

    // Run onTrigger to produce the actual pane contents.
    onTrigger(context, maybeAtWatermark, result.isFinish());

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
      try {
        triggerRunner.clearState(context);
        paneInfo.clear(context.state());
        watermarkHold.releaseFinal(context);
      } catch (Exception e) {
        Throwables.propagateIfPossible(e);
        throw new RuntimeException("Exception while clearing trigger state", e);
      }
    }
  }

  public static <T> StateContents<T> stateContentsOf(final T value) {
    return new StateContents<T>() {
      @Override
      public T read() {
        return value;
      }
    };
  }

  /**
   * Run the {@link ReduceFn#onTrigger} method and produce any necessary output.
   *
   * @param context the context for the pane to fire
   * @param isAtWatermark true if this triggering is for an AfterWatermark trigger
   * @param isFinal true if this will be the last triggering processed
   */
  private void onTrigger(final ReduceFn<K, InputT, OutputT, W>.Context context,
      boolean isAtWatermark, boolean isFinal) {
    StateContents<Instant> outputTimestampFuture = watermarkHold.extractAndRelease(context);
    StateContents<PaneInfo> paneFuture =
        paneInfo.getNextPaneInfo(context, isAtWatermark, isFinal);
    StateContents<Boolean> isEmptyFuture = nonEmptyPanes.isEmpty(context);

    reduceFn.prefetchOnTrigger(context.state());

    final PaneInfo pane = paneFuture.read();
    final Instant outputTimestamp = outputTimestampFuture.read();

    boolean shouldOutput =
        // If the pane is not empty
        !isEmptyFuture.read()
        // or this is the final pane, and the user has asked for it even if its empty
        || (isFinal && windowingStrategy.getClosingBehavior() == ClosingBehavior.FIRE_ALWAYS)
        // or this is the on-time firing, and the user explicitly requested it.
        || (isAtWatermark && pane.getTiming() == Timing.ON_TIME);

    // We've consumed the empty pane hold by reading it, so reinstate that, if necessary.
    if (!isFinal) {
      holdForEmptyPanes(context);
    }

    // If there is nothing to output, we're done.
    if (!shouldOutput) {
      return;
    }

    // Run reduceFn.onTrigger method.
    final List<W> windows = Collections.singletonList(context.window());
    ReduceFn<K, InputT, OutputT, W>.OnTriggerContext triggerContext = contextFactory.forTrigger(
        context.window(), paneFuture, new OnTriggerCallbacks<OutputT>() {
          @Override
          public void output(OutputT toOutput) {
            // We're going to output panes, so commit the (now used) PaneInfo.
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

  private Instant cleanupTime(W window) {
    return window.maxTimestamp().plus(windowingStrategy.getAllowedLateness());
  }

  /** Return true if {@code timestamp} is past the cleanup time for {@code window}. */
  private boolean isCleanupTime(W window, Instant timestamp) {
    return !timestamp.isBefore(cleanupTime(window));
  }

  private void scheduleCleanup(ReduceFn<?, ?, ?, W>.Context context) {
    if (timerInternals.currentWatermarkTime().isAfter(context.window().maxTimestamp())) {
      context.timers().setTimer(cleanupTime(context.window()), TimeDomain.EVENT_TIME);
    } else {
      context.timers().setTimer(context.window().maxTimestamp(), TimeDomain.EVENT_TIME);
    }
  }

  private void cancelCleanup(ReduceFn<?, ?, ?, W>.Context context) {
    context.timers().deleteTimer(cleanupTime(context.window()), TimeDomain.EVENT_TIME);
    context.timers().deleteTimer(context.window().maxTimestamp(), TimeDomain.EVENT_TIME);
  }

  private boolean shouldDiscardAfterFiring(TriggerResult result) {
    return result.isFinish()
        || (result.isFire()
            && AccumulationMode.DISCARDING_FIRED_PANES == windowingStrategy.getMode());
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  private enum Lateness {
    NOT_LATE(false, false),
    LATE(true, false),
    PAST_ALLOWED_LATENESS(true, true);

    private final boolean isLate;
    private final boolean isPastAllowedLateness;

    private Lateness(boolean isLate, boolean isPastAllowedLateness) {
      this.isLate = isLate;
      this.isPastAllowedLateness = isPastAllowedLateness;
    }
  }

  private Lateness getLateness(Instant timestamp) {
    Instant latestAllowed =
        timerInternals.currentWatermarkTime().minus(windowingStrategy.getAllowedLateness());
    if (timestamp.isBefore(latestAllowed)) {
      return Lateness.PAST_ALLOWED_LATENESS;
    } else if (timestamp.isBefore(timerInternals.currentWatermarkTime())) {
      return Lateness.LATE;
    } else {
      return Lateness.NOT_LATE;
    }
  }

  private RuntimeException wrapMaybeUserException(Throwable t) {
    if (reduceFn instanceof SystemReduceFn) {
      throw Throwables.propagate(t);
    } else {
      // Any exceptions that happen inside a non-system ReduceFn are considered user code.
      throw new UserCodeException(t);
    }
  }
}
