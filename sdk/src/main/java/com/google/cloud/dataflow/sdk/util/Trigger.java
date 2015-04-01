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
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;

import org.joda.time.Instant;

import java.io.IOException;

/**
 * Interface to use for controlling when output for a specific key and window is triggered.
 *
 * <p> This functionality is experimental and likely to change.
 *
 * @param <W> the window that this trigger applies to
 */
public interface Trigger<W extends BoundedWindow> {

  /**
   * Types of timers that are supported.
   */
  public enum TimeDomain {
    /**
     * Timers that fire based on the timestamp of events. Once set, the timer will fire when the
     * system watermark passes the specified time.
     */
    EVENT_TIME,

    /**
     * Timers that fire based on the current processing time. Once set, the timer will fire at some
     * point when the system time is after the specified time.
     */
    PROCESSING_TIME;
  }

  /**
   * Status of the element in the window.
   */
  public enum WindowStatus {
    /** This element caused us to start actively managing the given window. */
    NEW,
    /** This window was already under active management before the arrival of this element. */
    EXISTING,
    /** The WindowSet doesn't track the windows actively being managed. */
    UNKNOWN;
  }

  /**
   * Enumeration of the possible results for a trigger.
   */
  public enum TriggerResult {
    FIRE(true, false),
    CONTINUE(false, false),
    FIRE_AND_FINISH(true, true),
    FINISH(false, true);

    private boolean finish;
    private boolean fire;

    private TriggerResult(boolean fire, boolean finish) {
      this.fire = fire;
      this.finish = finish;
    }

    public boolean isFire() {
      return fire;
    }

    public boolean isFinish() {
      return finish;
    }
  }

  /**
   * Information is that is made available to triggers, eg., setting timers.
   */
  public interface TriggerContext<W extends BoundedWindow>  {

    /**
     * Set a timer to fire for the given window at the specified time.
     *
     * TODO: Support processing time
     * TODO: Support per-trigger timers.
     */
    void setTimer(W window, Instant timestamp, TimeDomain timeDomain) throws IOException;

    /**
     * Delete a timer that has been set for the specified window.
     */
    void deleteTimer(W window, TimeDomain timeDomain) throws IOException;

    /**
     * The current processing time.
     */
    Instant currentProcessingTime();

    /**
     * Updates the value stored in keyed state for the given window.
     */
    <T> void store(CodedTupleTag<T> tag, W window, T value) throws IOException;

    /**
     * Removes the data associated with the given tag from {@code KeyedState}.
     * @throws IOException
     */
    <T> void remove(CodedTupleTag<T> tag, W window) throws IOException;

    /**
     * Lookup the value stored in keyed state.
     */
    <T> T lookup(CodedTupleTag<T> tag, W window) throws IOException;

    /**
     * Lookup the value stored in a bunch of windows.
     */
    <T> Iterable<T> lookup(CodedTupleTag<T> tag, Iterable<W> windows) throws IOException;
  }

  /**
   * Called immediately after an element is first incorporated into a window.
   *
   * @param c the context to interact with
   * @param value the element that was incorporated
   * @param window the window the element was assigned to
   */
  TriggerResult onElement(
      TriggerContext<W> c, Object value, W window, WindowStatus status) throws Exception;

  /**
   * Called immediately after windows have been merged.
   *
   * @param c the context to interact with
   * @param oldWindows the windows that were merged
   * @param newWindow the window that resulted from merging
   */
  TriggerResult onMerge(TriggerContext<W> c, Iterable<W> oldWindows, W newWindow) throws Exception;

  /**
   * Called after a timer fires.
   *
   * @param c the context to interact with
   * @param window the timer is being fired for
   */
  TriggerResult onTimer(TriggerContext<W> c, W window) throws Exception;
}
