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

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window.ClosingBehavior;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.WatermarkStateInternal;
import com.google.common.annotations.VisibleForTesting;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;

/**
 * Implements the logic needed to hold the output watermark back to emit
 * values with specific timestamps.
 *
 * @param <W> The kind of {@link BoundedWindow} the hold is for.
 */
public class WatermarkHold<W extends BoundedWindow> implements Serializable {

  /** Watermark hold used for the actual data-based hold. */
  @VisibleForTesting static final String DATA_HOLD_ID = "hold";
  @VisibleForTesting static StateTag<WatermarkStateInternal> watermarkHoldTagForOutputTimeFn(
      OutputTimeFn<?> outputTimeFn) {
    return StateTags.makeSystemTagInternal(
        StateTags.watermarkStateInternal(DATA_HOLD_ID, outputTimeFn));
  }

  private final WindowingStrategy<?, W> windowingStrategy;
  private final StateTag<WatermarkStateInternal> watermarkHoldTag;

  public WatermarkHold(WindowingStrategy<?, W> windowingStrategy) {
    this.windowingStrategy = windowingStrategy;
    this.watermarkHoldTag = watermarkHoldTagForOutputTimeFn(windowingStrategy.getOutputTimeFn());
  }

  /**
   * Update the watermark hold to include the appropriate output timestamp for the value in
   * {@code c}.
   *
   * <p>If the value was not late, then the input watermark must be less than the timestamp, and we
   * can use {@link WindowFn#getOutputTime} to determine the appropriate output time.
   *
   * <p>If the value was late, we pessimistically assume the worst and attempt to hold the watermark
   * to {@link BoundedWindow#maxTimestamp()} plus {@link WindowingStrategy#getAllowedLateness()}.
   * That allows us to output the result at {@link BoundedWindow#maxTimestamp()} without being
   * dropped.
   */
  public void addHold(ReduceFn<?, ?, ?, W>.ProcessValueContext c, boolean isLate) {
    Instant outputTime = isLate
        ? c.window().maxTimestamp().plus(windowingStrategy.getAllowedLateness())
        : windowingStrategy.getOutputTimeFn().assignOutputTime(c.timestamp(), c.window());
    c.state().access(watermarkHoldTag).add(outputTime);
  }

  /**
   * Updates the watermark hold when windows merge. For example, if the new window implies
   * a later watermark hold, then earlier holds may be released.
   */
  public void mergeHolds(final ReduceFn<?, ?, ?, W>.OnMergeContext c) {
    // If the output hold depends only on the window, then there may not be a hold in place
    // for the new merged window, so add one.
    if (windowingStrategy.getOutputTimeFn().dependsOnlyOnWindow()) {
      Instant arbitraryTimestamp = new Instant(0);
      c.state().access(watermarkHoldTag).add(
          windowingStrategy.getOutputTimeFn().assignOutputTime(
              arbitraryTimestamp,
              c.window()));
    }

    c.state().accessAcrossMergedWindows(watermarkHoldTag).releaseExtraneousHolds();
  }

  /**
   * Returns the combined timestamp at which the output watermark was being held and releases
   * the hold.
   *
   * <p>The returned timestamp is the output timestamp according to the {@link OutputTimeFn}
   * from the windowing strategy of this {@link WatermarkHold}, combined across all the non-late
   * elements in the current pane.
   */
  public StateContents<Instant> extractAndRelease(final ReduceFn<?, ?, ?, W>.Context c) {
    final WatermarkStateInternal dataHold = c.state().accessAcrossMergedWindows(watermarkHoldTag);
    final StateContents<Instant> holdFuture = dataHold.get();
    return new StateContents<Instant>() {
      @Override
      public Instant read() {
        Instant hold = holdFuture.read();
        if (hold == null || hold.isAfter(c.window().maxTimestamp())) {
          hold = c.window().maxTimestamp();
        }

        // Clear the underlying state to allow the output watermark to progress.
        dataHold.clear();

        return hold;
      }
    };
  }

  public void holdForOnTime(final ReduceFn<?, ?, ?, W>.Context c) {
    c.state().accessAcrossMergedWindows(watermarkHoldTag).add(c.window().maxTimestamp());
  }

  public void holdForFinal(final ReduceFn<?, ?, ?, W>.Context c) {
    if (c.windowingStrategy().getClosingBehavior() == ClosingBehavior.FIRE_ALWAYS) {
      c.state().accessAcrossMergedWindows(watermarkHoldTag)
           .add(c.window().maxTimestamp().plus(c.windowingStrategy().getAllowedLateness()));
    }
  }

  public void releaseOnTime(final ReduceFn<?, ?, ?, W>.Context c) {
    c.state().accessAcrossMergedWindows(watermarkHoldTag).clear();

    if (c.windowingStrategy().getClosingBehavior() == ClosingBehavior.FIRE_ALWAYS
        && c.windowingStrategy().getAllowedLateness().isLongerThan(Duration.ZERO)) {
      holdForFinal(c);
    }
  }

  public void releaseFinal(final ReduceFn<?, ?, ?, W>.Context c) {
    c.state().accessAcrossMergedWindows(watermarkHoldTag).clear();
  }
}
