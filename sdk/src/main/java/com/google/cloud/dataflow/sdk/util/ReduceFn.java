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
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.util.state.MergeableState;
import com.google.cloud.dataflow.sdk.util.state.State;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateTag;

import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * Specification for processing to happen after elements have been grouped by key.
 *
 * @param <K> The type of key being processed.
 * @param <InputT> The type of input values associated with the key.
 * @param <OutputT> The output type that will be produced for each key.
 * @param <W> The type of windows this operates on.
 */
public abstract class ReduceFn<K, InputT, OutputT, W extends BoundedWindow>
    implements Serializable {

  private static final long serialVersionUID = 0L;

  /** Interface for interacting with persistent state. */
  public interface StateContext {
    /** Access the storage for the given {@code address} in the current window. */
    <StateT extends State> StateT access(StateTag<StateT> address);

    /**
     * Access the storage for the given {@code address} in all of the windows that were
     * merged into the current window including the current window.
     *
     * <p> If no windows were merged, this reads from just the current window.
     */
    <StateT extends MergeableState<?, ?>> StateT accessAcrossMergedWindows(
        StateTag<StateT> address);
  }

  /** Interface for interacting with persistent state within {@link #onMerge}. */
  public interface MergingStateContext extends StateContext {
    /**
     * Access a merged view of the storage for the given {@code address}
     * in all of the windows being merged.
     */
    public abstract <StateT extends MergeableState<?, ?>> StateT accessAcrossMergingWindows(
        StateTag<StateT> address);

    /** Access a map from windows being merged to the associated {@code StateT}. */
    public abstract <StateT extends State> Map<BoundedWindow, StateT> accessInEachMergingWindow(
        StateTag<StateT> address);
  }

  /**
   * Interface for interacting with time.
   */
  public interface Timers {
    /**
     * Sets a timer to fire when the watermark or processing time is beyond the given timestamp.
     * Timers are not guaranteed to fire immediately, but will be delivered at some time afterwards.
     *
     * <p>As with {@link StateContext}, timers are implicitly scoped to the current window. All
     * timer firings for a window will be received, but the implementation should choose to ignore
     * those that are not applicable.
     *
     * @param timestamp the time at which the trigger’s {@link Trigger#onTimer} callback should
     *        execute
     * @param timeDomain the domain that the {@code timestamp} applies to
     */
    public abstract void setTimer(Instant timestamp, TimeDomain timeDomain);

    /**
     * Removes the timer set in this trigger context for the given {@code window}, {@code timestmap}
     * and {@code timeDomain}.
     */
    public abstract void deleteTimer(Instant timestamp, TimeDomain timeDomain);

    /** Returns the current processing time. */
    public abstract Instant currentProcessingTime();
  }

  /** Information accessible to all the processing methods in this {@code ReduceFn}. */
  public abstract class Context {
    /** Return the key that is being processed. */
    public abstract K key();

    /** The window that is being processed. */
    public abstract W window();

    /** Access the current {@link WindowingStrategy}. */
    public abstract WindowingStrategy<?, W> windowingStrategy();

    /** Return the interface for accessing state. */
    public abstract StateContext state();

    /** Return the interface for accessing timers. */
    public abstract Timers timers();
  }

  /** Information accessible within {@link #processValue}. */
  public abstract class ProcessValueContext extends Context {

    /** Return the actual value being processed. */
    public abstract InputT value();

    /** Return the timestamp associated with the value. */
    public abstract Instant timestamp();
  }

  /** Information accessible within {@link #onMerge}. */
  public abstract class OnMergeContext extends Context {
    /**
     * Return the collection of windows that were merged.
     *
     * <p> Note that this may include the result window.
     */
    public abstract Collection<W> mergingWindows();

    /** Return the interface for accessing state. */
    @Override
    public abstract MergingStateContext state();
  }

  /** Information accessible within {@link #onTrigger}. */
  public abstract class OnTriggerContext extends Context {

    /** Returns the {@link PaneInfo} for the trigger firing being processed. */
    public abstract PaneInfo paneInfo();

    /** Output the given value in the current window. */
    public abstract void output(OutputT value);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Called for each value of type {@code InputT} associated with the current key.
   */
  public abstract void processValue(ProcessValueContext c) throws Exception;

  /**
   * Called when windows are merged.
   *
   * <p> There are generally two strategies for implementing this and handling merging of state:
   * <ul>
   * <li> Lazily merge the state when outputting. This is especially easy if all the state is stored
   * in {@link MergeableState}, since an automatically merged view can be retrieved.
   * <li> Eagerly merge the state inside the {@link #onMerge} implementation. Load all the state
   * from the merging windows and write it back to the result window. In this case the state in the
   * result window should be cleared into between the read and write in case it was in the source
   * windows.
   * </ul>
   */
  public abstract void onMerge(OnMergeContext c) throws Exception;

  /**
   * Called when triggers fire.
   *
   * <p>Implementations of {@link ReduceFn} should call {@link OnTriggerContext#output} to emit
   * any results that should be included in the pane produced by this trigger firing.
   */
  public abstract void onTrigger(OnTriggerContext c) throws Exception;

  /**
   * Called before {@link onTrigger} is invoked to provide an opportunity to prefetch any needed
   * state.
   *
   * @param c Context to use prefetch from.
   */
  public void prefetchOnTrigger(StateContext c) { }

  /**
   * Called to clear any persisted state that the {@link ReduceFn} may be holding. This will be
   * called when the windowing is closing and will receive no future interactions.
   */
  public abstract void clearState(Context c) throws Exception;

  /**
   * Returns true if the there is no buffered state.
   */
  public abstract StateContents<Boolean> isEmpty(StateContext c);
}
