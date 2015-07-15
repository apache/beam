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

  @VisibleForTesting static final StateTag<WatermarkStateInternal> HOLD_TAG =
      StateTags.watermarkStateInternal("watermark_hold");

  private final Duration allowedLateness;

  public WatermarkHold(Duration allowedLateness) {
    this.allowedLateness = allowedLateness;
  }

  public void addHold(TriggerExecutor<?, ?, ?, W>.Context c, Instant timestamp, boolean isLate) {
    // If the element was late, then we want to put a hold in at the maxTimestamp for the end
    // of the window plus the allowed lateness to ensure that we don't output something
    // that is dropably late.
    Instant holdTo = isLate
        ? c.window().maxTimestamp().plus(allowedLateness)
        : timestamp;
    c.access(HOLD_TAG).add(holdTo);
  }

  /**
   * Get the timestamp to use for output. This is computed as the minimum timestamp
   * of any non-late elements that arrived in the current pane.
   */
  public Instant extractAndRelease(TriggerExecutor<?, ?, ?, W>.Context c) {
    WatermarkStateInternal holdingBag = c.accessAcrossSources(HOLD_TAG);

    Instant hold = holdingBag.get().read();
    if (hold != null && hold.isAfter(c.window().maxTimestamp())) {
      hold = c.window().maxTimestamp();
    }
    holdingBag.clear();
    return hold;
  }
}
