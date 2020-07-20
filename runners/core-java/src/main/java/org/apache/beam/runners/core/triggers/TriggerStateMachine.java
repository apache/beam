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
package org.apache.beam.runners.core.triggers;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.beam.runners.core.MergingStateAccessor;
import org.apache.beam.runners.core.StateAccessor;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * {@code Trigger}s control when the elements for a specific key and window are output. As elements
 * arrive, they are put into one or more windows by a {@link Window} transform and its associated
 * {@link WindowFn}, and then passed to the associated {@code Trigger} to determine if the {@code
 * Window}s contents should be output.
 *
 * <p>See {@link org.apache.beam.sdk.transforms.GroupByKey} and {@link Window} for more information
 * about how grouping with windows works.
 *
 * <p>The elements that are assigned to a window since the last time it was fired (or since the
 * window was created) are placed into the current window pane. Triggers are evaluated against the
 * elements as they are added. When the root trigger fires, the elements in the current pane will be
 * output. When the root trigger finishes (indicating it will never fire again), the window is
 * closed and any new elements assigned to that window are discarded.
 *
 * <p>Several predefined {@code Trigger}s are provided:
 *
 * <ul>
 *   <li>{@link AfterWatermarkStateMachine} for firing when the watermark passes a timestamp
 *       determined from either the end of the window or the arrival of the first element in a pane.
 *   <li>{@link AfterProcessingTimeStateMachine} for firing after some amount of processing time has
 *       elapsed (typically since the first element in a pane).
 *   <li>{@link AfterPaneStateMachine} for firing off a property of the elements in the current
 *       pane, such as the number of elements that have been assigned to the current pane.
 * </ul>
 *
 * <p>In addition, {@code Trigger}s can be combined in a variety of ways:
 *
 * <ul>
 *   <li>{@link RepeatedlyStateMachine#forever} to create a trigger that executes forever. Any time
 *       its argument finishes it gets reset and starts over. Can be combined with {@link
 *       TriggerStateMachine#orFinally} to specify a condition that causes the repetition to stop.
 *   <li>{@link AfterEachStateMachine#inOrder} to execute each trigger in sequence, firing each (and
 *       every) time that a trigger fires, and advancing to the next trigger in the sequence when it
 *       finishes.
 *   <li>{@link AfterFirstStateMachine#of} to create a trigger that fires after at least one of its
 *       arguments fires. An {@link AfterFirstStateMachine} trigger finishes after it fires once.
 *   <li>{@link AfterAllStateMachine#of} to create a trigger that fires after all least one of its
 *       arguments have fired at least once. An {@link AfterAllStateMachine} trigger finishes after
 *       it fires once.
 * </ul>
 *
 * <p>Each trigger tree is instantiated per-key and per-window. Every trigger in the tree is in one
 * of the following states:
 *
 * <ul>
 *   <li>Never Existed - before the trigger has started executing, there is no state associated with
 *       it anywhere in the system. A trigger moves to the executing state as soon as it processes
 *       in the current pane.
 *   <li>Executing - while the trigger is receiving items and may fire. While it is in this state,
 *       it may persist book-keeping information to persisted state, set timers, etc.
 *   <li>Finished - after a trigger finishes, all of its book-keeping data is cleaned up, and the
 *       system remembers only that it is finished. Entering this state causes us to discard any
 *       elements in the buffer for that window, as well.
 * </ul>
 *
 * <p>Once finished, a trigger cannot return itself back to an earlier state, however a composite
 * trigger could reset its sub-triggers.
 *
 * <p>Triggers should not build up any state internally since they may be recreated between
 * invocations of the callbacks. All important values should be persisted using state before the
 * callback returns.
 */
public abstract class TriggerStateMachine implements Serializable {

  /**
   * Interface for accessing information about the trigger being executed and other triggers in the
   * same tree.
   */
  public interface TriggerInfo {

    /**
     * Returns true if the windowing strategy of the current {@code PCollection} is a merging {@link
     * WindowFn}. If true, the trigger execution needs to keep enough information to support the
     * possibility of {@link TriggerStateMachine#onMerge} being called. If false, {@link
     * TriggerStateMachine#onMerge} will never be called.
     */
    boolean isMerging();

    /** Access the executable versions of the sub-triggers of the current trigger. */
    Iterable<ExecutableTriggerStateMachine> subTriggers();

    /** Access the executable version of the specified sub-trigger. */
    ExecutableTriggerStateMachine subTrigger(int subtriggerIndex);

    /** Returns true if the current trigger is marked finished. */
    boolean isFinished();

    /** Return true if the given subtrigger is marked finished. */
    boolean isFinished(int subtriggerIndex);

    /** Returns true if all the sub-triggers of the current trigger are marked finished. */
    boolean areAllSubtriggersFinished();

    /** Returns an iterable over the unfinished sub-triggers of the current trigger. */
    Iterable<ExecutableTriggerStateMachine> unfinishedSubTriggers();

    /** Returns the first unfinished sub-trigger. */
    ExecutableTriggerStateMachine firstUnfinishedSubTrigger();

    /**
     * Clears all keyed state for triggers in the current sub-tree and unsets all the associated
     * finished bits.
     */
    void resetTree() throws Exception;

    /** Sets the finished bit for the current trigger. */
    void setFinished(boolean finished);

    /** Sets the finished bit for the given sub-trigger. */
    void setFinished(boolean finished, int subTriggerIndex);
  }

  /**
   * Interact with properties of the trigger being executed, with extensions to deal with the
   * merging windows.
   */
  public interface MergingTriggerInfo extends TriggerInfo {

    /** Return true if the trigger is finished in any window being merged. */
    boolean finishedInAnyMergingWindow();

    /** Return true if the trigger is finished in all windows being merged. */
    boolean finishedInAllMergingWindows();
  }

  /**
   * Information accessible to all operational hooks in this {@code Trigger}.
   *
   * <p>Used directly in {@link TriggerStateMachine#shouldFire} and {@link
   * TriggerStateMachine#clear}, and extended with additional information in other methods.
   */
  public abstract static class TriggerContext {

    /** Returns the interface for accessing trigger info. */
    public abstract TriggerInfo trigger();

    /** Returns the interface for accessing persistent state. */
    public abstract StateAccessor<?> state();

    /** The window that the current context is executing in. */
    public abstract BoundedWindow window();

    /** Create a sub-context for the given sub-trigger. */
    public abstract TriggerContext forTrigger(ExecutableTriggerStateMachine trigger);

    /**
     * Removes the timer set in this trigger context for the given {@link Instant} and {@link
     * TimeDomain}.
     */
    public abstract void deleteTimer(Instant timestamp, TimeDomain domain);

    /** The current processing time. */
    public abstract Instant currentProcessingTime();

    /** The current synchronized upstream processing time or {@code null} if unknown. */
    public abstract @Nullable Instant currentSynchronizedProcessingTime();

    /** The current event time for the input or {@code null} if unknown. */
    public abstract @Nullable Instant currentEventTime();
  }

  /**
   * Extended {@link TriggerContext} containing information accessible to the {@link #onElement}
   * operational hook.
   */
  public abstract static class OnElementContext extends TriggerContext {
    /** The event timestamp of the element currently being processed. */
    public abstract Instant eventTimestamp();

    /**
     * Sets a timer to fire when the watermark or processing time is beyond the given timestamp.
     * Timers are not guaranteed to fire immediately, but will be delivered at some time afterwards.
     *
     * <p>As with {@link #state}, timers are implicitly scoped to the current window. All timer
     * firings for a window will be received, but the implementation should choose to ignore those
     * that are not applicable.
     *
     * @param timestamp the time at which the trigger should be re-evaluated
     * @param domain the domain that the {@code timestamp} applies to
     */
    public abstract void setTimer(Instant timestamp, TimeDomain domain);

    /** Create an {@code OnElementContext} for executing the given trigger. */
    @Override
    public abstract OnElementContext forTrigger(ExecutableTriggerStateMachine trigger);
  }

  /**
   * Extended {@link TriggerContext} containing information accessible to the {@link #onMerge}
   * operational hook.
   */
  public abstract static class OnMergeContext extends TriggerContext {
    /**
     * Sets a timer to fire when the watermark or processing time is beyond the given timestamp.
     * Timers are not guaranteed to fire immediately, but will be delivered at some time afterwards.
     *
     * <p>As with {@link #state}, timers are implicitly scoped to the current window. All timer
     * firings for a window will be received, but the implementation should choose to ignore those
     * that are not applicable.
     *
     * @param timestamp the time at which the trigger should be re-evaluated
     * @param domain the domain that the {@code timestamp} applies to
     */
    public abstract void setTimer(Instant timestamp, TimeDomain domain);

    /** Create an {@code OnMergeContext} for executing the given trigger. */
    @Override
    public abstract OnMergeContext forTrigger(ExecutableTriggerStateMachine trigger);

    @Override
    public abstract MergingStateAccessor<?, ?> state();

    @Override
    public abstract MergingTriggerInfo trigger();
  }

  protected final @Nullable List<TriggerStateMachine> subTriggers;

  protected TriggerStateMachine(@Nullable List<TriggerStateMachine> subTriggers) {
    this.subTriggers = subTriggers;
  }

  /** Called every time an element is incorporated into a window. */
  public abstract void onElement(OnElementContext c) throws Exception;

  /**
   * Called immediately after windows have been merged.
   *
   * <p>Leaf triggers should update their state by inspecting their status and any state in the
   * merging windows. Composite triggers should update their state by calling {@link
   * ExecutableTriggerStateMachine#invokeOnMerge} on their sub-triggers, and applying appropriate
   * logic.
   *
   * <p>A trigger such as {@link AfterWatermarkStateMachine#pastEndOfWindow} may no longer be
   * finished; it is the responsibility of the trigger itself to record this fact. It is forbidden
   * for a trigger to become finished due to {@link #onMerge}, as it has not yet fired the pending
   * elements that led to it being ready to fire.
   *
   * <p>The implementation does not need to clear out any state associated with the old windows.
   */
  public abstract void onMerge(OnMergeContext c) throws Exception;

  /**
   * Returns {@code true} if the current state of the trigger indicates that its condition is
   * satisfied and it is ready to fire.
   */
  public abstract boolean shouldFire(TriggerContext context) throws Exception;

  /**
   * Adjusts the state of the trigger to be ready for the next pane. For example, a {@link
   * RepeatedlyStateMachine} trigger will reset its inner trigger, since it has fired.
   *
   * <p>If the trigger is finished, it is the responsibility of the trigger itself to record that
   * fact via the {@code context}.
   */
  public abstract void onFire(TriggerContext context) throws Exception;

  /**
   * Called to allow the trigger to prefetch any state it will likely need to read from during an
   * {@link #onElement} call.
   */
  public void prefetchOnElement(StateAccessor<?> state) {
    if (subTriggers != null) {
      for (TriggerStateMachine trigger : subTriggers) {
        trigger.prefetchOnElement(state);
      }
    }
  }

  /**
   * Called to allow the trigger to prefetch any state it will likely need to read from during an
   * {@link #onMerge} call.
   */
  public void prefetchOnMerge(MergingStateAccessor<?, ?> state) {
    if (subTriggers != null) {
      for (TriggerStateMachine trigger : subTriggers) {
        trigger.prefetchOnMerge(state);
      }
    }
  }

  /**
   * Called to allow the trigger to prefetch any state it will likely need to read from during an
   * {@link #shouldFire} call.
   */
  public void prefetchShouldFire(StateAccessor<?> state) {
    if (subTriggers != null) {
      for (TriggerStateMachine trigger : subTriggers) {
        trigger.prefetchShouldFire(state);
      }
    }
  }

  /**
   * Called to allow the trigger to prefetch any state it will likely need to read from during an
   * {@link #onFire} call.
   */
  public void prefetchOnFire(StateAccessor<?> state) {
    if (subTriggers != null) {
      for (TriggerStateMachine trigger : subTriggers) {
        trigger.prefetchOnFire(state);
      }
    }
  }

  /**
   * Clear any state associated with this trigger in the given window.
   *
   * <p>This is called after a trigger has indicated it will never fire again. The trigger system
   * keeps enough information to know that the trigger is finished, so this trigger should clear all
   * of its state.
   */
  public void clear(TriggerContext c) throws Exception {
    if (subTriggers != null) {
      for (ExecutableTriggerStateMachine trigger : c.trigger().subTriggers()) {
        trigger.invokeClear(c);
      }
    }
  }

  public Iterable<TriggerStateMachine> subTriggers() {
    return subTriggers;
  }

  /** Returns whether this performs the same triggering as the given {@code Trigger}. */
  public boolean isCompatible(TriggerStateMachine other) {
    if (!getClass().equals(other.getClass())) {
      return false;
    }

    if (subTriggers == null) {
      return other.subTriggers == null;
    } else if (other.subTriggers == null) {
      return false;
    } else if (subTriggers.size() != other.subTriggers.size()) {
      return false;
    }

    for (int i = 0; i < subTriggers.size(); i++) {
      if (!subTriggers.get(i).isCompatible(other.subTriggers.get(i))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    String simpleName = getClass().getSimpleName();
    if (getClass().getEnclosingClass() != null) {
      simpleName = getClass().getEnclosingClass().getSimpleName() + "." + simpleName;
    }
    if (subTriggers == null || subTriggers.isEmpty()) {
      return simpleName;
    } else {
      return simpleName + "(" + Joiner.on(", ").join(subTriggers) + ")";
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TriggerStateMachine)) {
      return false;
    }
    TriggerStateMachine that = (TriggerStateMachine) obj;
    return Objects.equals(getClass(), that.getClass())
        && Objects.equals(subTriggers, that.subTriggers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), subTriggers);
  }

  /**
   * Specify an ending condition for this trigger. If the {@code until} fires then the combination
   * fires.
   *
   * <p>The expression {@code t1.orFinally(t2)} fires every time {@code t1} fires, and finishes as
   * soon as either {@code t1} finishes or {@code t2} fires, in which case it fires one last time
   * for {@code t2}. Both {@code t1} and {@code t2} are executed in parallel. This means that {@code
   * t1} may have fired since {@code t2} started, so not all of the elements that {@code t2} has
   * seen are necessarily in the current pane.
   *
   * <p>For example the final firing of the following trigger may only have 1 element:
   *
   * <pre>{@code
   * Repeatedly.forever(AfterPane.elementCountAtLeast(2))
   *     .orFinally(AfterPane.elementCountAtLeast(5))
   * }</pre>
   */
  public TriggerStateMachine orFinally(TriggerStateMachine until) {
    return new OrFinallyStateMachine(this, until);
  }
}
