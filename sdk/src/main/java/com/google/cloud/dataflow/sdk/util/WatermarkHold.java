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

import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.annotations.VisibleForTesting;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements the logic needed to hold the output watermark back to emit
 * values with specific timestamps.
 *
 * @param <W> The kind of {@link BoundedWindow} the hold is for.
 */
public class WatermarkHold<W extends BoundedWindow> implements Serializable {

  private static final long serialVersionUID = 0L;

  @VisibleForTesting static final CodedTupleTag<Instant> EARLIEST_ELEMENT_TAG =
      CodedTupleTag.of("__watermark_hold", InstantCoder.of());

  private final Duration allowedLateness;

  public WatermarkHold(Duration allowedLateness) {
    this.allowedLateness = allowedLateness;
  }

  private final Map<W, Instant> inMemoryBuffer = new HashMap<>();

  public void addHold(OutputBuffer.Context<?, W> c, Instant timestamp, boolean isLate) {
    // If the element was late, then we want to put a hold in at the maxTimestamp for the end
    // of the window plus the allowed lateness to ensure that we don't output something
    // that is dropably late.
    Instant holdTo = isLate
        ? c.window().maxTimestamp().plus(allowedLateness)
        : timestamp;
    Instant storedEarliest = inMemoryBuffer.get(c.window());
    if (storedEarliest == null || holdTo.isBefore(storedEarliest)) {
      inMemoryBuffer.put(c.window(), holdTo);
    }
  }

  /**
   * Get the timestamp to use for output. This is computed as the minimum timestamp
   * of any non-late elements that arrived in the current pane.
   */
  public Instant extractAndRelease(OutputBuffer.Context<?, W> c) throws IOException {
    // Normally, output at the earliest non-late element in the pane.
    // If the pane is empty or all the elements were late, output at window.maxTimestamp().
    Instant earliest = c.window().maxTimestamp();

    // MIN of already-persisted values for the source windows.
    for (Instant hold : c.readBuffers(EARLIEST_ELEMENT_TAG, c.sourceWindows())) {
      if (hold != null && hold.isBefore(earliest)) {
        earliest = hold;
      }
    }

    // MIN of not-yet-persisted values for the source windows.
    for (W window : c.sourceWindows()) {
      Instant hold = inMemoryBuffer.remove(window);
      if (hold != null && hold.isBefore(earliest)) {
        earliest = hold;
      }
    }

    c.clearBuffers(EARLIEST_ELEMENT_TAG, c.sourceWindows());
    return earliest;
  }

  public void release(OutputBuffer.Context<?, W> c) throws IOException {
    c.clearBuffers(EARLIEST_ELEMENT_TAG, c.sourceWindows());
    for (W window : c.sourceWindows()) {
      inMemoryBuffer.remove(window);
    }
  }

  public void flush(OutputBuffer.Context<?, W> c) throws IOException {
    for (Map.Entry<W, Instant> entry : inMemoryBuffer.entrySet()) {
      c.addToBuffer(entry.getKey(), EARLIEST_ELEMENT_TAG, entry.getValue(), entry.getValue());
    }
    inMemoryBuffer.clear();
  }
}
