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

  private static final long serialVersionUID = 0L;

  /** Watermark hold used for the actual data-based hold. */
  @VisibleForTesting static final StateTag<WatermarkStateInternal> DATA_HOLD_TAG =
      StateTags.makeSystemTagInternal(StateTags.watermarkStateInternal("hold"));

  private final WindowingStrategy<?, W> windowingStrategy;

  public WatermarkHold(WindowingStrategy<?, W> windowingStrategy) {
    this.windowingStrategy = windowingStrategy;
  }

  /**
   * Update the watermark hold to include the timestamp of the value in {@code c}.
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
    Instant holdTo = isLate
        ? c.window().maxTimestamp().plus(windowingStrategy.getAllowedLateness())
        : windowingStrategy.getWindowFn().getOutputTime(c.timestamp(), c.window());
    c.state().access(DATA_HOLD_TAG).add(holdTo);
  }

  /**
   * Get information from the watermark hold for outputting.
   *
   * <p> The output timestamp is the minimum of getOutputTimestamp applied to the non-late elements
   * that arrived in the current pane.
   */
  public StateContents<Instant> extractAndRelease(final ReduceFn<?, ?, ?, W>.Context c) {
    final WatermarkStateInternal dataHold = c.state().accessAcrossMergedWindows(DATA_HOLD_TAG);
    final StateContents<Instant> holdFuture = dataHold.get();
    return new StateContents<Instant>() {
      @Override
      public Instant read() {
        Instant hold = holdFuture.read();
        if (hold == null || hold.isAfter(c.window().maxTimestamp())) {
          hold = c.window().maxTimestamp();
        }

        // Clear the bag (to release the watermark)
        dataHold.clear();

        return hold;
      }
    };
  }

  public void holdForOnTime(final ReduceFn<?, ?, ?, W>.Context c) {
    c.state().accessAcrossMergedWindows(DATA_HOLD_TAG).add(c.window().maxTimestamp());
  }

  public void holdForFinal(final ReduceFn<?, ?, ?, W>.Context c) {
    if (c.windowingStrategy().getClosingBehavior() == ClosingBehavior.FIRE_ALWAYS) {
      c.state().accessAcrossMergedWindows(DATA_HOLD_TAG)
           .add(c.window().maxTimestamp().plus(c.windowingStrategy().getAllowedLateness()));
    }
  }

  public void releaseOnTime(final ReduceFn<?, ?, ?, W>.Context c) {
    c.state().accessAcrossMergedWindows(DATA_HOLD_TAG).clear();

    if (c.windowingStrategy().getClosingBehavior() == ClosingBehavior.FIRE_ALWAYS
        && c.windowingStrategy().getAllowedLateness().isLongerThan(Duration.ZERO)) {
      holdForFinal(c);
    }
  }

  public void releaseFinal(final ReduceFn<?, ?, ?, W>.Context c) {
    c.state().accessAcrossMergedWindows(DATA_HOLD_TAG).clear();
  }
}
