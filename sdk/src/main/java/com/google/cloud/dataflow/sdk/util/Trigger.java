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
import java.util.List;
import java.util.Map;

/**
 * Interface to use for controlling when output for a specific key and window is triggered.
 *
 * <p> This functionality is experimental and likely to change.
 *
 * @param <W> the window that this trigger applies to
 */
public abstract class Trigger<W extends BoundedWindow> {

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

    public static TriggerResult valueOf(boolean fire, boolean finish) {
      if (fire && finish) {
        return FIRE_AND_FINISH;
      } else if (fire) {
        return FIRE;
      } else if (finish) {
        return FINISH;
      } else {
        return CONTINUE;
      }
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
    <T> Map<W, T> lookup(CodedTupleTag<T> tag, Iterable<W> windows) throws IOException;

    /**
     * Create a {@code TriggerContext} for executing in the given child.
     */
    TriggerContext<W> forChild(int childIndex);
  }

  /**
   * Called immediately after an element is first incorporated into a window.
   *
   * @param c the context to interact with
   * @param value the element that was incorporated
   * @param window the window the element was assigned to
   */
  public abstract TriggerResult onElement(
      TriggerContext<W> c, Object value, W window, WindowStatus status) throws Exception;

  /**
   * Called immediately after windows have been merged.
   *
   * <p>This will only be called if the trigger hasn't finished in any of the {@code oldWindows}.
   * If it had finished, we assume that it is also finished in the resulting window.
   *
   * <p>The implementation does not need to clear out any state associated with the old windows.
   * That will automatically be done by the trigger execution layer.
   *
   * @param c the context to interact with
   * @param oldWindows the windows that were merged
   * @param newWindow the window that resulted from merging
   */
  public abstract TriggerResult onMerge(
      TriggerContext<W> c, Iterable<W> oldWindows, W newWindow) throws Exception;

  /**
   * Called after a timer fires.
   *
   * @param c the context to interact with
   * @param triggerId identifier for the trigger that the timer is for.
   */
  public abstract TriggerResult onTimer(
      TriggerContext<W> c, TriggerId<W> triggerId) throws Exception;

  /**
   * Clear any state associated with this trigger in the given window.
   *
   * <p>This is called after a trigger has indicated it will never fire again. The trigger system
   * keeps enough information to know that the trigger is finished, so this trigger should clear all
   * of its state.
   *
   * @param c the context to interact with
   * @param window the window that is being cleared
   */
  public abstract void clear(TriggerContext<W> c, W window) throws Exception;

  /**
   * Identifies a unique trigger instance, by the window it is in and the path through the trigger
   * tree.
   *
   * @param <W> The type of windows the trigger operates in.
   */
  public static class TriggerId<W extends BoundedWindow> {
    private final W window;
    private final List<Integer> subTriggers;

    TriggerId(W window, List<Integer> subTriggers) {
      this.window = window;
      this.subTriggers = subTriggers;
    }

    /**
     * Return a trigger ID that is applicable for the specific child.
     */
    public TriggerId<W> forChildTrigger() {
      return new TriggerId<>(window, subTriggers.subList(1, subTriggers.size()));
    }

    public W getWindow() {
      return window;
    }

    /**
     * Return true if this trigger ID corresponds to a child of the current trigger.
     */
    public boolean isForChild() {
      return subTriggers.size() > 0;
    }

    /**
     * Return the index of the child this trigger ID is for.
     */
    public int getChildIndex() {
      return subTriggers.get(0);
    }

    public Iterable<Integer> getPath() {
      return subTriggers;
    }
  }
}
