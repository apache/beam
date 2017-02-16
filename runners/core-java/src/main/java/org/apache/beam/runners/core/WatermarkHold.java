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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.beam.sdk.util.state.WatermarkHoldState;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Implements the logic to hold the output watermark for a computation back
 * until it has seen all the elements it needs based on the input watermark for the
 * computation.
 *
 * <p>The backend ensures the output watermark can never progress beyond the
 * input watermark for a computation. GroupAlsoByWindows computations may add a 'hold'
 * to the output watermark in order to prevent it progressing beyond a time within a window.
 * The hold will be 'cleared' when the associated pane is emitted.
 *
 * <p>This class is only intended for use by {@link ReduceFnRunner}. The two evolve together and
 * will likely break any other uses.
 *
 * @param <W> The kind of {@link BoundedWindow} the hold is for.
 */
class WatermarkHold<W extends BoundedWindow> implements Serializable {
  /**
   * Return tag for state containing the output watermark hold
   * used for elements.
   */
  public static <W extends BoundedWindow>
      StateTag<Object, WatermarkHoldState<W>> watermarkHoldTagForOutputTimeFn(
          OutputTimeFn<? super W> outputTimeFn) {
    return StateTags.<Object, WatermarkHoldState<W>>makeSystemTagInternal(
        StateTags.<W>watermarkStateInternal("hold", outputTimeFn));
  }

  /**
   * Tag for state containing end-of-window and garbage collection output watermark holds.
   * (We can't piggy-back on the data hold state since the outputTimeFn may be
   * {@link OutputTimeFns#outputAtLatestInputTimestamp()}, in which case every pane will
   * would take the end-of-window time as its element time.)
   */
  @VisibleForTesting
  public static final StateTag<Object, WatermarkHoldState<BoundedWindow>> EXTRA_HOLD_TAG =
      StateTags.makeSystemTagInternal(StateTags.watermarkStateInternal(
          "extra", OutputTimeFns.outputAtEarliestInputTimestamp()));

  private final TimerInternals timerInternals;
  private final WindowingStrategy<?, W> windowingStrategy;
  private final StateTag<Object, WatermarkHoldState<W>> elementHoldTag;

  public WatermarkHold(TimerInternals timerInternals, WindowingStrategy<?, W> windowingStrategy) {
    this.timerInternals = timerInternals;
    this.windowingStrategy = windowingStrategy;
    this.elementHoldTag = watermarkHoldTagForOutputTimeFn(windowingStrategy.getOutputTimeFn());
  }

  /**
   * Add a hold to prevent the output watermark progressing beyond the (possibly adjusted) timestamp
   * of the element in {@code context}. We allow the actual hold time to be shifted later by
   * {@link OutputTimeFn#assignOutputTime}, but no further than the end of the window. The hold will
   * remain until cleared by {@link #extractAndRelease}. Return the timestamp at which the hold
   * was placed, or {@literal null} if no hold was placed.
   *
   * <p>In the following we'll write {@code E} to represent an element's timestamp after passing
   * through the window strategy's output time function, {@code IWM} for the local input watermark,
   * {@code OWM} for the local output watermark, and {@code GCWM} for the garbage collection
   * watermark (which is at {@code IWM - getAllowedLateness}). Time progresses from left to right,
   * and we write {@code [ ... ]} to denote a bounded window with implied lower bound.
   *
   * <p>Note that the GCWM will be the same as the IWM if {@code getAllowedLateness}
   * is {@code ZERO}.
   *
   * <p>Here are the cases we need to handle. They are conceptually considered in the
   * sequence written since if getAllowedLateness is ZERO the GCWM is the same as the IWM.
   * <ol>
   * <li>(Normal)
   * <pre>
   *          |
   *      [   | E        ]
   *          |
   *         IWM
   * </pre>
   * This is, hopefully, the common and happy case. The element is locally on-time and can
   * definitely make it to an {@code ON_TIME} pane which we can still set an end-of-window timer
   * for. We place an element hold at E, which may contribute to the {@code ON_TIME} pane's
   * timestamp (depending on the output time function). Thus the OWM will not proceed past E
   * until the next pane fires.
   *
   * <li>(Discard - no target window)
   * <pre>
   *                       |                            |
   *      [     E        ] |                            |
   *                       |                            |
   *                     GCWM  <-getAllowedLateness->  IWM
   * </pre>
   * The element is very locally late. The window has been garbage collected, thus there
   * is no target pane E could be assigned to. We discard E.
   *
   * <li>(Unobservably late)
   * <pre>
   *          |    |
   *      [   | E  |     ]
   *          |    |
   *         OWM  IWM
   * </pre>
   * The element is locally late, however we can still treat this case as for 'Normal' above
   * since the IWM has not yet passed the end of the window and the element is ahead of the
   * OWM. In effect, we get to 'launder' the locally late element and consider it as locally
   * on-time because no downstream computation can observe the difference.
   *
   * <li>(Maybe late 1)
   * <pre>
   *          |            |
   *      [   | E        ] |
   *          |            |
   *         OWM          IWM
   * </pre>
   * The end-of-window timer may have already fired for this window, and thus an {@code ON_TIME}
   * pane may have already been emitted. However, if timer firings have been delayed then it
   * is possible the {@code ON_TIME} pane has not yet been emitted. We can't place an element
   * hold since we can't be sure if it will be cleared promptly. Thus this element *may* find
   * its way into an {@code ON_TIME} pane, but if so it will *not* contribute to that pane's
   * timestamp. We may however set a garbage collection hold if required.
   *
   * <li>(Maybe late 2)
   * <pre>
   *               |   |
   *      [     E  |   | ]
   *               |   |
   *              OWM IWM
   * </pre>
   * The end-of-window timer has not yet fired, so this element may still appear in an
   * {@code ON_TIME} pane. However the element is too late to contribute to the output
   * watermark hold, and thus won't contribute to the pane's timestamp. We can still place an
   * end-of-window hold.
   *
   * <li>(Maybe late 3)
   * <pre>
   *               |       |
   *      [     E  |     ] |
   *               |       |
   *              OWM     IWM
   * </pre>
   * As for the (Maybe late 2) case, however we don't even know if the end-of-window timer
   * has already fired, or it is about to fire. We can place only the garbage collection hold,
   * if required.
   *
   * <li>(Definitely late)
   * <pre>
   *                       |   |
   *      [     E        ] |   |
   *                       |   |
   *                      OWM IWM
   * </pre>
   * The element is definitely too late to make an {@code ON_TIME} pane. We are too late to
   * place an end-of-window hold. We can still place a garbage collection hold if required.
   *
   * </ol>
   */
  @Nullable
  public Instant addHolds(ReduceFn<?, ?, ?, W>.ProcessValueContext context) {
    Instant hold = addElementHold(context);
    if (hold == null) {
      hold = addEndOfWindowOrGarbageCollectionHolds(context, false/*paneIsEmpty*/);
    }
    return hold;
  }

  /**
   * Return {@code timestamp}, possibly shifted forward in time according to the window
   * strategy's output time function.
   */
  private Instant shift(Instant timestamp, W window) {
    Instant shifted = windowingStrategy.getOutputTimeFn().assignOutputTime(timestamp, window);
    checkState(!shifted.isBefore(timestamp),
        "OutputTimeFn moved element from %s to earlier time %s for window %s",
        BoundedWindow.formatTimestamp(timestamp),
        BoundedWindow.formatTimestamp(shifted),
        window);
    checkState(timestamp.isAfter(window.maxTimestamp())
            || !shifted.isAfter(window.maxTimestamp()),
        "OutputTimeFn moved element from %s to %s which is beyond end of "
            + "window %s",
        timestamp, shifted, window);

    return shifted;
  }

  /**
   * Attempt to add an 'element hold'. Return the {@link Instant} at which the hold was
   * added (ie the element timestamp plus any forward shift requested by the
   * {@link WindowingStrategy#getOutputTimeFn}), or {@literal null} if no hold was added.
   * The hold is only added if both:
   * <ol>
   * <li>The backend will be able to respect it. In other words the output watermark cannot
   * be ahead of the proposed hold time.
   * <li>A timer will be set (by {@link ReduceFnRunner}) to clear the hold by the end of the
   * window. In other words the input watermark cannot be ahead of the end of the window.
   * </ol>
   * The hold ensures the pane which incorporates the element is will not be considered late by
   * any downstream computation when it is eventually emitted.
   */
  @Nullable
  private Instant addElementHold(ReduceFn<?, ?, ?, W>.ProcessValueContext context) {
    // Give the window function a chance to move the hold timestamp forward to encourage progress.
    // (A later hold implies less impediment to the output watermark making progress, which in
    // turn encourages end-of-window triggers to fire earlier in following computations.)
    Instant elementHold = shift(context.timestamp(), context.window());

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
      checkState(!elementHold.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE),
          "Element hold %s is beyond end-of-time", elementHold);
      context.state().access(elementHoldTag).add(elementHold);
    }
    WindowTracing.trace(
        "WatermarkHold.addHolds: element hold at {} is {} for "
        + "key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
        elementHold, which, context.key(), context.window(), inputWM,
        outputWM);

    return tooLate ? null : elementHold;
  }

  /**
   * Add an end-of-window hold or, if too late for that, a garbage collection hold (if required).
   * Return the {@link Instant} at which hold was added, or {@literal null} if no hold was added.
   */
  @Nullable
  private Instant addEndOfWindowOrGarbageCollectionHolds(
      ReduceFn<?, ?, ?, W>.Context context, boolean paneIsEmpty) {
    Instant hold = addEndOfWindowHold(context, paneIsEmpty);
    if (hold == null) {
      hold = addGarbageCollectionHold(context, paneIsEmpty);
    }
    return hold;
  }

  /**
   * Attempt to add an 'end-of-window hold'. Return the {@link Instant} at which the hold was added
   * (ie the end of window time), or {@literal null} if no end of window hold is possible and we
   * should fallback to a garbage collection hold.
   *
   * <p>We only add the hold if we can be sure a timer will be set (by {@link ReduceFnRunner})
   * to clear it. In other words, the input watermark cannot be ahead of the end of window time.
   *
   * <p>An end-of-window hold is added in two situations:
   * <ol>
   * <li>An incoming element came in behind the output watermark (so we are too late for placing
   * the usual element hold), but it may still be possible to include the element in an
   * {@link Timing#ON_TIME} pane. We place the end of window hold to ensure that pane will
   * not be considered late by any downstream computation.
   * <li>We guarantee an {@link Timing#ON_TIME} pane will be emitted for all windows which saw at
   * least one element, even if that {@link Timing#ON_TIME} pane is empty. Thus when elements in
   * a pane are processed due to a fired trigger we must set both an end of window timer and an end
   * of window hold. Again, the hold ensures the {@link Timing#ON_TIME} pane will not be considered
   * late by any downstream computation.
   * </ol>
   */
  @Nullable
  private Instant addEndOfWindowHold(ReduceFn<?, ?, ?, W>.Context context, boolean paneIsEmpty) {
    Instant outputWM = timerInternals.currentOutputWatermarkTime();
    Instant inputWM = timerInternals.currentInputWatermarkTime();
    Instant eowHold = context.window().maxTimestamp();

    if (eowHold.isBefore(inputWM)) {
      WindowTracing.trace(
          "WatermarkHold.addEndOfWindowHold: end-of-window hold at {} is too late for "
              + "end-of-window timer for key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
          eowHold, context.key(), context.window(), inputWM, outputWM);
      return null;
    }

    checkState(outputWM == null || !eowHold.isBefore(outputWM),
        "End-of-window hold %s cannot be before output watermark %s",
        eowHold, outputWM);
    checkState(!eowHold.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE),
        "End-of-window hold %s is beyond end-of-time", eowHold);
    // If paneIsEmpty then this hold is just for empty ON_TIME panes, so we want to keep
    // the hold away from the combining function in elementHoldTag.
    // However if !paneIsEmpty then it could make sense  to use the elementHoldTag here.
    // Alas, onMerge is forced to add an end of window or garbage collection hold without
    // knowing whether an element hold is already in place (stopping to check is too expensive).
    // This it would end up adding an element hold at the end of the window which could
    // upset the elementHoldTag combining function.
    context.state().access(EXTRA_HOLD_TAG).add(eowHold);
    WindowTracing.trace(
        "WatermarkHold.addEndOfWindowHold: end-of-window hold at {} is on time for "
            + "key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
        eowHold, context.key(), context.window(), inputWM, outputWM);
    return eowHold;
  }

  /**
   * Attempt to add a 'garbage collection hold' if it is required. Return the {@link Instant} at
   * which the hold was added (ie the end of window time plus allowed lateness),
   * or {@literal null} if no hold was added.
   *
   * <p>We only add the hold if it is distinct from what would be added by
   * {@link #addEndOfWindowHold}. In other words, {@link WindowingStrategy#getAllowedLateness}
   * must be non-zero.
   *
   * <p>A garbage collection hold is added in two situations:
   * <ol>
   * <li>An incoming element came in behind the output watermark, and was too late for placing
   * the usual element hold or an end of window hold. Place the garbage collection hold so that
   * we can guarantee when the pane is finally triggered its output will not be dropped due to
   * excessive lateness by any downstream computation.
   * <li>The {@link WindowingStrategy#getClosingBehavior()} is
   * {@link ClosingBehavior#FIRE_ALWAYS}, and thus we guarantee a final pane will be emitted
   * for all windows which saw at least one element. Again, the garbage collection hold guarantees
   * that any empty final pane can be given a timestamp which will not be considered beyond
   * allowed lateness by any downstream computation.
   * </ol>
   *
   * <p>We use {@code paneIsEmpty} to distinguish cases 1 and 2.
   */
  @Nullable
  private Instant addGarbageCollectionHold(
      ReduceFn<?, ?, ?, W>.Context context, boolean paneIsEmpty) {
    Instant outputWM = timerInternals.currentOutputWatermarkTime();
    Instant inputWM = timerInternals.currentInputWatermarkTime();
    Instant eow = context.window().maxTimestamp();
    Instant gcHold = eow.plus(windowingStrategy.getAllowedLateness());

    if (!windowingStrategy.getAllowedLateness().isLongerThan(Duration.ZERO)) {
      WindowTracing.trace(
          "WatermarkHold.addGarbageCollectionHold: garbage collection hold at {} is unnecessary "
              + "since no allowed lateness for key:{}; window:{}; inputWatermark:{}; "
              + "outputWatermark:{}",
          gcHold, context.key(), context.window(), inputWM, outputWM);
      return null;
    }

    if (paneIsEmpty && context.windowingStrategy().getClosingBehavior()
        == ClosingBehavior.FIRE_IF_NON_EMPTY) {
      WindowTracing.trace(
          "WatermarkHold.addGarbageCollectionHold: garbage collection hold at {} is unnecessary "
              + "since empty pane and FIRE_IF_NON_EMPTY for key:{}; window:{}; inputWatermark:{}; "
              + "outputWatermark:{}",
          gcHold, context.key(), context.window(), inputWM, outputWM);
      return null;
    }

    checkState(!gcHold.isBefore(inputWM),
        "Garbage collection hold %s cannot be before input watermark %s",
        gcHold, inputWM);
    checkState(!gcHold.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE),
        "Garbage collection hold %s is beyond end-of-time", gcHold);
    // Same EXTRA_HOLD_TAG vs elementHoldTag discussion as in addEndOfWindowHold above.
    context.state().access(EXTRA_HOLD_TAG).add(gcHold);

    WindowTracing.trace(
        "WatermarkHold.addGarbageCollectionHold: garbage collection hold at {} is on time for "
            + "key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
        gcHold, context.key(), context.window(), inputWM, outputWM);
    return gcHold;
  }

  /**
   * Prefetch watermark holds in preparation for merging.
   */
  public void prefetchOnMerge(MergingStateAccessor<?, W> state) {
    StateMerging.prefetchWatermarks(state, elementHoldTag);
  }

  /**
   * Updates the watermark hold when windows merge if it is possible the merged value does
   * not equal all of the existing holds. For example, if the new window implies a later
   * watermark hold, then earlier holds may be released.
   */
  public void onMerge(ReduceFn<?, ?, ?, W>.OnMergeContext context) {
    WindowTracing.debug("WatermarkHold.onMerge: for key:{}; window:{}; inputWatermark:{}; "
            + "outputWatermark:{}",
        context.key(), context.window(), timerInternals.currentInputWatermarkTime(),
        timerInternals.currentOutputWatermarkTime());
    StateMerging.mergeWatermarks(context.state(), elementHoldTag, context.window());
    // If we had a cheap way to determine if we have an element hold then we could
    // avoid adding an unnecessary end-of-window or garbage collection hold.
    // Simply reading the above merged watermark would impose an additional read for the
    // common case that the active window has just one underlying state address window and
    // the hold depends on the min of the element timestamps.
    // At least one merged window must be non-empty for the merge to have been triggered.
    StateMerging.clear(context.state(), EXTRA_HOLD_TAG);
    addEndOfWindowOrGarbageCollectionHolds(context, false /*paneIsEmpty*/);
  }

  /**
   * Result of {@link #extractAndRelease}.
   */
  public static class OldAndNewHolds {
    public final Instant oldHold;
    @Nullable
    public final Instant newHold;

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
   * Return (a future for) the earliest hold for {@code context}. Clear all the holds after
   * reading, but add/restore an end-of-window or garbage collection hold if required.
   *
   * <p>The returned timestamp is the output timestamp according to the {@link OutputTimeFn}
   * from the windowing strategy of this {@link WatermarkHold}, combined across all the non-late
   * elements in the current pane. If there is no such value the timestamp is the end
   * of the window.
   */
  public ReadableState<OldAndNewHolds> extractAndRelease(
      final ReduceFn<?, ?, ?, W>.Context context, final boolean isFinished) {
    WindowTracing.debug(
        "WatermarkHold.extractAndRelease: for key:{}; window:{}; inputWatermark:{}; "
            + "outputWatermark:{}",
        context.key(), context.window(), timerInternals.currentInputWatermarkTime(),
        timerInternals.currentOutputWatermarkTime());
    final WatermarkHoldState<W> elementHoldState = context.state().access(elementHoldTag);
    final WatermarkHoldState<BoundedWindow> extraHoldState = context.state().access(EXTRA_HOLD_TAG);
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
        Instant elementHold = elementHoldState.read();
        Instant extraHold = extraHoldState.read();
        Instant oldHold;
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
          // If no hold (eg because all elements came in behind the output watermark), or
          // the hold was for garbage collection, take the end of window as the result.
          WindowTracing.debug(
              "WatermarkHold.extractAndRelease.read: clipping from {} to end of window "
              + "for key:{}; window:{}",
              oldHold, context.key(), context.window());
          oldHold = context.window().maxTimestamp();
        }
        WindowTracing.debug("WatermarkHold.extractAndRelease.read: clearing for key:{}; window:{}",
            context.key(), context.window());

        // Clear the underlying state to allow the output watermark to progress.
        elementHoldState.clear();
        extraHoldState.clear();

        @Nullable Instant newHold = null;
        if (!isFinished) {
          // Only need to leave behind an end-of-window or garbage collection hold
          // if future elements will be processed.
          newHold = addEndOfWindowOrGarbageCollectionHolds(context, true /*paneIsEmpty*/);
        }

        return new OldAndNewHolds(oldHold, newHold);
      }
    };
  }

  /**
   * Clear any remaining holds.
   */
  public void clearHolds(ReduceFn<?, ?, ?, W>.Context context) {
    WindowTracing.debug(
        "WatermarkHold.clearHolds: For key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
        context.key(), context.window(), timerInternals.currentInputWatermarkTime(),
        timerInternals.currentOutputWatermarkTime());
    context.state().access(elementHoldTag).clear();
    context.state().access(EXTRA_HOLD_TAG).clear();
  }

  /**
   * Return the current data hold, or null if none. Does not clear. For debugging only.
   */
  @Nullable
  public Instant getDataCurrent(ReduceFn<?, ?, ?, W>.Context context) {
    return context.state().access(elementHoldTag).read();
  }
}
