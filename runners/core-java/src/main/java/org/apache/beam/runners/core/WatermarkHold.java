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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Implements the logic to hold the output watermark for a computation back until it has seen all
 * the elements it needs based on the input watermark for the computation.
 *
 * <p>The backend ensures the output watermark can never progress beyond the input watermark for a
 * computation. GroupAlsoByWindows computations may add a 'hold' to the output watermark in order to
 * prevent it progressing beyond a time within a window. The hold will be 'cleared' when the
 * associated pane is emitted.
 *
 * <p>This class is only intended for use by {@link ReduceFnRunner}. The two evolve together and
 * will likely break any other uses.
 *
 * @param <W> The kind of {@link BoundedWindow} the hold is for.
 */
class WatermarkHold<W extends BoundedWindow> implements Serializable {
  /** Return tag for state containing the output watermark hold used for elements. */
  public static <W extends BoundedWindow>
      StateTag<WatermarkHoldState> watermarkHoldTagForTimestampCombiner(
          TimestampCombiner timestampCombiner) {
    return StateTags.makeSystemTagInternal(
        StateTags.<W>watermarkStateInternal("hold", timestampCombiner));
  }

  /**
   * Tag for state containing end-of-window and garbage collection output watermark holds. (We can't
   * piggy-back on the data hold state since the timestampCombiner may be {@link
   * TimestampCombiner#EARLIEST}, in which case every pane will would take the end-of-window time as
   * its element time.)
   */
  @VisibleForTesting
  public static final StateTag<WatermarkHoldState> EXTRA_HOLD_TAG =
      StateTags.makeSystemTagInternal(
          StateTags.watermarkStateInternal("extra", TimestampCombiner.EARLIEST));

  // [BEAM-420] Seems likely these should all be transient or this class should not be Serializable
  @SuppressFBWarnings("SE_BAD_FIELD")
  private final TimerInternals timerInternals;

  private final WindowingStrategy<?, W> windowingStrategy;
  private final StateTag<WatermarkHoldState> elementHoldTag;

  public WatermarkHold(TimerInternals timerInternals, WindowingStrategy<?, W> windowingStrategy) {
    this.timerInternals = timerInternals;
    this.windowingStrategy = windowingStrategy;
    this.elementHoldTag =
        watermarkHoldTagForTimestampCombiner(windowingStrategy.getTimestampCombiner());
  }

  /**
   * Add a hold to prevent the output watermark progressing beyond the (possibly adjusted) timestamp
   * of the element in {@code context}.
   *
   * <p>The target time for the aggregated output is shifted by the {@link WindowFn} and combined
   * with a {@link TimestampCombiner} to determine where the output watermark is held.
   *
   * <p>If the target time would be late, then we do not set this hold, but instead add the hold to
   * allow a final output at GC time.
   *
   * <p>See https://s.apache.org/beam-lateness for the full design of how late data and watermarks
   * interact.
   */
  public @Nullable Instant addHolds(ReduceFn<?, ?, ?, W>.ProcessValueContext context) {
    Instant hold = addElementHold(context.timestamp(), context);
    if (hold == null) {
      hold = addGarbageCollectionHold(context, false /*paneIsEmpty*/);
    }
    return hold;
  }

  /**
   * Return {@code timestamp}, possibly shifted forward in time according to the window strategy's
   * output time function.
   */
  private Instant shift(Instant timestamp, W window) {
    Instant shifted =
        windowingStrategy
            .getTimestampCombiner()
            .assign(window, windowingStrategy.getWindowFn().getOutputTime(timestamp, window));
    // Don't call checkState(), to avoid calling BoundedWindow.formatTimestamp() every time
    if (shifted.isBefore(timestamp)) {
      throw new IllegalStateException(
          String.format(
              "TimestampCombiner moved element from %s to earlier time %s for window %s",
              BoundedWindow.formatTimestamp(timestamp),
              BoundedWindow.formatTimestamp(shifted),
              window));
    }
    checkState(
        timestamp.isAfter(window.maxTimestamp()) || !shifted.isAfter(window.maxTimestamp()),
        "TimestampCombiner moved element from %s to %s which is beyond end of " + "window %s",
        timestamp,
        shifted,
        window);

    return shifted;
  }

  /**
   * Attempt to add an 'element hold'. Return the {@link Instant} at which the hold was added (ie
   * the element timestamp plus any forward shift requested by the {@link
   * WindowingStrategy#getTimestampCombiner}), or {@literal null} if no hold was added. The hold is
   * only added if both:
   *
   * <ol>
   *   <li>The backend will be able to respect it. In other words the output watermark cannot be
   *       ahead of the proposed hold time.
   *   <li>A timer will be set (by {@link ReduceFnRunner}) to clear the hold by the end of the
   *       window. In other words the input watermark cannot be ahead of the end of the window.
   * </ol>
   *
   * The hold ensures the pane which incorporates the element is will not be considered late by any
   * downstream computation when it is eventually emitted.
   */
  private @Nullable Instant addElementHold(
      Instant timestamp, ReduceFn<?, ?, ?, W>.Context context) {
    // Give the window function a chance to move the hold timestamp forward to encourage progress.
    // (A later hold implies less impediment to the output watermark making progress, which in
    // turn encourages end-of-window triggers to fire earlier in following computations.)
    Instant elementHold = shift(timestamp, context.window());

    Instant outputWM = timerInternals.currentOutputWatermarkTime();
    Instant inputWM = timerInternals.currentInputWatermarkTime();

    String which;
    boolean tooLate;
    // TODO: These case labels could be tightened.
    // See the case analysis in addHolds above for the motivation.
    if (outputWM != null && elementHold.isBefore(outputWM)) {
      which = "too late to effect output watermark";
      tooLate = true;
    } else if (context.window().maxTimestamp().isBefore(inputWM)) {
      which = "too late for end-of-window timer";
      tooLate = true;
    } else {
      which = "on time";
      tooLate = false;
      checkState(
          !elementHold.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE),
          "Element hold %s is beyond end-of-time",
          elementHold);
      context.state().access(elementHoldTag).add(elementHold);
    }
    WindowTracing.trace(
        "WatermarkHold.addHolds: element hold at {} is {} for "
            + "key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
        elementHold,
        which,
        context.key(),
        context.window(),
        inputWM,
        outputWM);

    return tooLate ? null : elementHold;
  }

  /**
   * Attempt to add a 'garbage collection hold' if it is required. Return the {@link Instant} at
   * which the hold was added (ie the end of window time plus allowed lateness), or {@literal null}
   * if no hold was added.
   *
   * <p>A garbage collection hold is added in two situations:
   *
   * <ol>
   *   <li>An incoming element has a timestamp earlier than the output watermark, and was too late
   *       for placing the usual element hold or an end of window hold. Place the garbage collection
   *       hold so that we can guarantee when the pane is finally triggered its output will not be
   *       dropped due to excessive lateness by any downstream computation.
   *   <li>The {@link WindowingStrategy#getClosingBehavior()} is {@link
   *       ClosingBehavior#FIRE_ALWAYS}, and thus we guarantee a final pane will be emitted for all
   *       windows which saw at least one element. Again, the garbage collection hold guarantees
   *       that any empty final pane can be given a timestamp which will not be considered beyond
   *       allowed lateness by any downstream computation.
   * </ol>
   *
   * <p>We use {@code paneIsEmpty} to distinguish cases 1 and 2.
   */
  private @Nullable Instant addGarbageCollectionHold(
      ReduceFn<?, ?, ?, W>.Context context, boolean paneIsEmpty) {
    Instant outputWM = timerInternals.currentOutputWatermarkTime();
    Instant inputWM = timerInternals.currentInputWatermarkTime();
    Instant gcHold = LateDataUtils.garbageCollectionTime(context.window(), windowingStrategy);

    if (gcHold.isBefore(inputWM)) {
      WindowTracing.trace(
          "{}.addGarbageCollectionHold: gc hold would be before the input watermark "
              + "for key:{}; window: {}; inputWatermark: {}; outputWatermark: {}",
          getClass().getSimpleName(),
          context.key(),
          context.window(),
          inputWM,
          outputWM);
      return null;
    }

    if (paneIsEmpty
        && context.windowingStrategy().getClosingBehavior() == ClosingBehavior.FIRE_IF_NON_EMPTY) {
      WindowTracing.trace(
          "WatermarkHold.addGarbageCollectionHold: garbage collection hold at {} is unnecessary "
              + "since empty pane and FIRE_IF_NON_EMPTY for key:{}; window:{}; inputWatermark:{}; "
              + "outputWatermark:{}",
          gcHold,
          context.key(),
          context.window(),
          inputWM,
          outputWM);
      return null;
    }

    if (!gcHold.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      // If the garbage collection hold is past the timestamp we can represent, instead truncate
      // to the maximum timestamp that is not positive infinity. This ensures all windows will
      // eventually be garbage collected.
      gcHold = BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.millis(1L));
    }
    checkState(
        !gcHold.isBefore(inputWM),
        "Garbage collection hold %s cannot be before input watermark %s",
        gcHold,
        inputWM);
    checkState(
        !gcHold.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE),
        "Garbage collection hold %s is beyond end-of-time",
        gcHold);
    // Same EXTRA_HOLD_TAG vs elementHoldTag discussion as in addEndOfWindowHold above.
    context.state().access(EXTRA_HOLD_TAG).add(gcHold);

    WindowTracing.trace(
        "WatermarkHold.addGarbageCollectionHold: garbage collection hold at {} is on time for "
            + "key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
        gcHold,
        context.key(),
        context.window(),
        inputWM,
        outputWM);
    return gcHold;
  }

  /** Prefetch watermark holds in preparation for merging. */
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT") // just prefetch calls to readLater
  public void prefetchOnMerge(MergingStateAccessor<?, W> context) {
    Map<W, WatermarkHoldState> map = context.accessInEachMergingWindow(elementHoldTag);
    WatermarkHoldState result = context.access(elementHoldTag);
    if (map.isEmpty()) {
      // Nothing to prefetch.
      return;
    }
    if (map.size() == 1
        && map.values().contains(result)
        && result.getTimestampCombiner().dependsOnlyOnEarliestTimestamp()) {
      // Nothing to merge if our source and sink is the same.
      return;
    }
    if (result.getTimestampCombiner().dependsOnlyOnWindow()) {
      // No need to read existing holds since we will just clear.
      return;
    }
    // Prefetch.
    for (WatermarkHoldState source : map.values()) {
      source.readLater();
    }
  }

  /**
   * Updates the watermark hold when windows merge if it is possible the merged value does not equal
   * all of the existing holds. For example, if the new window implies a later watermark hold, then
   * earlier holds may be released.
   */
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT") // just prefetch calls to readLater
  public void onMerge(ReduceFn<?, ?, ?, W>.OnMergeContext context) {
    WindowTracing.debug(
        "WatermarkHold.onMerge: for key:{}; window:{}; inputWatermark:{}; " + "outputWatermark:{}",
        context.key(),
        context.window(),
        timerInternals.currentInputWatermarkTime(),
        timerInternals.currentOutputWatermarkTime());
    Collection<WatermarkHoldState> sources =
        context.state().accessInEachMergingWindow(elementHoldTag).values();
    WatermarkHoldState result = context.state().access(elementHoldTag);
    if (sources.isEmpty()) {
      // No element holds to merge.
    } else if (sources.size() == 1
        && sources.contains(result)
        && result.getTimestampCombiner().dependsOnlyOnEarliestTimestamp()) {
      // Nothing to merge if our source and sink is the same.
    } else if (result.getTimestampCombiner().dependsOnlyOnWindow()) {
      // Clear sources.
      for (WatermarkHoldState source : sources) {
        source.clear();
      }
      // Update directly from window-derived hold.
      addElementHold(BoundedWindow.TIMESTAMP_MIN_VALUE, context);
    } else {
      // Prefetch.
      for (WatermarkHoldState source : sources) {
        source.readLater();
      }
      // Read and merge.
      Instant mergedHold = null;
      for (ReadableState<Instant> source : sources) {
        Instant sourceOutputTime = source.read();
        if (sourceOutputTime != null) {
          if (mergedHold == null) {
            mergedHold = sourceOutputTime;
          } else {
            mergedHold =
                result.getTimestampCombiner().merge(context.window(), mergedHold, sourceOutputTime);
          }
        }
      }
      // Clear sources.
      for (WatermarkHoldState source : sources) {
        source.clear();
      }
      // Write merged value if there was one.
      if (mergedHold != null) {
        result.add(mergedHold);
      }
    }

    // If we had a cheap way to determine if we have an element hold then we could
    // avoid adding an unnecessary end-of-window or garbage collection hold.
    // Simply reading the above merged watermark would impose an additional read for the
    // common case that the active window has just one underlying state address window and
    // the hold depends on the min of the element timestamps.
    // At least one merged window must be non-empty for the merge to have been triggered.
    StateMerging.clear(context.state(), EXTRA_HOLD_TAG);
    addGarbageCollectionHold(context, false /*paneIsEmpty*/);
  }

  /** Result of {@link #extractAndRelease}. */
  public static class OldAndNewHolds {
    public final Instant oldHold;
    public final @Nullable Instant newHold;

    public OldAndNewHolds(Instant oldHold, @Nullable Instant newHold) {
      this.oldHold = oldHold;
      this.newHold = newHold;
    }
  }

  public void prefetchExtract(final ReduceFn<?, ?, ?, W>.Context context) {
    context.state().access(elementHoldTag).readLater();
    context.state().access(EXTRA_HOLD_TAG).readLater();
  }

  /**
   * Return (a future for) the earliest hold for {@code context}. Clear all the holds after reading,
   * but add/restore an end-of-window or garbage collection hold if required.
   *
   * <p>The returned timestamp is the output timestamp according to the {@link TimestampCombiner}
   * from the windowing strategy of this {@link WatermarkHold}, combined across all the non-late
   * elements in the current pane. If there is no such value the timestamp is the end of the window.
   */
  public ReadableState<OldAndNewHolds> extractAndRelease(
      final ReduceFn<?, ?, ?, W>.Context context, final boolean isFinished) {
    WindowTracing.debug(
        "WatermarkHold.extractAndRelease: for key:{}; window:{}; inputWatermark:{}; "
            + "outputWatermark:{}",
        context.key(),
        context.window(),
        timerInternals.currentInputWatermarkTime(),
        timerInternals.currentOutputWatermarkTime());
    final WatermarkHoldState elementHoldState = context.state().access(elementHoldTag);
    final WatermarkHoldState extraHoldState = context.state().access(EXTRA_HOLD_TAG);
    return new ReadableState<OldAndNewHolds>() {
      @Override
      public ReadableState<OldAndNewHolds> readLater() {
        elementHoldState.readLater();
        extraHoldState.readLater();
        return this;
      }

      @Override
      public OldAndNewHolds read() {
        // Read both the element and extra holds.
        @Nullable Instant elementHold = elementHoldState.read();
        @Nullable Instant extraHold = extraHoldState.read();
        @Nullable Instant oldHold;
        // Find the minimum, accounting for null.
        if (elementHold == null) {
          oldHold = extraHold;
        } else if (extraHold == null) {
          oldHold = elementHold;
        } else if (elementHold.isBefore(extraHold)) {
          oldHold = elementHold;
        } else {
          oldHold = extraHold;
        }
        if (oldHold == null || oldHold.isAfter(context.window().maxTimestamp())) {
          // If no hold (eg because all elements came in before the output watermark), or
          // the hold was for garbage collection, take the end of window as the result.
          WindowTracing.debug(
              "WatermarkHold.extractAndRelease.read: clipping from {} to end of window "
                  + "for key:{}; window:{}",
              oldHold,
              context.key(),
              context.window());
          oldHold = context.window().maxTimestamp();
        }
        WindowTracing.debug(
            "WatermarkHold.extractAndRelease.read: clearing for key:{}; window:{}",
            context.key(),
            context.window());

        // Clear the underlying state to allow the output watermark to progress.
        elementHoldState.clear();
        extraHoldState.clear();

        @Nullable Instant newHold = null;
        if (!isFinished) {
          newHold = addGarbageCollectionHold(context, true /*paneIsEmpty*/);
        }

        return new OldAndNewHolds(oldHold, newHold);
      }
    };
  }

  /** Clear any remaining holds. */
  public void clearHolds(ReduceFn<?, ?, ?, W>.Context context) {
    WindowTracing.debug(
        "WatermarkHold.clearHolds: For key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
        context.key(),
        context.window(),
        timerInternals.currentInputWatermarkTime(),
        timerInternals.currentOutputWatermarkTime());
    context.state().access(elementHoldTag).clear();
    context.state().access(EXTRA_HOLD_TAG).clear();
  }

  /** Return the current data hold, or null if none. Does not clear. For debugging only. */
  public @Nullable Instant getDataCurrent(ReduceFn<?, ?, ?, W>.Context context) {
    return context.state().access(elementHoldTag).read();
  }
}
