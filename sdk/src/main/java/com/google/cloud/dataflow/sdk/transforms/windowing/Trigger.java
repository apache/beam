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

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.util.ExecutableTrigger;
import com.google.cloud.dataflow.sdk.util.ReduceFn;
import com.google.cloud.dataflow.sdk.util.ReduceFn.MergingStateContext;
import com.google.cloud.dataflow.sdk.util.ReduceFn.StateContext;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.common.base.Joiner;

import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

/**
 * {@code Trigger}s control when the elements for a specific key and window are output. As elements
 * arrive, they are put into one or more windows by a {@link Window} transform and its associated
 * {@link WindowFn}, and then passed to the associated {@code Trigger} to determine if the
 * {@code Window}s contents should be output.
 *
 * <p> See {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey} and {@link Window}
 * for more information about how grouping with windows works.
 *
 * <p>The elements that are assigned to a window since the last time it was fired (or since the
 * window was created) are placed into the current window pane. Triggers are evaluated against the
 * elements as they are added. When the root trigger fires, the elements in the current pane will be
 * output. When the root trigger finishes (indicating it will never fire again), the window is
 * closed and any new elements assigned to that window are discarded.
 *
 * <p>Several predefined {@code Trigger}s are provided:
 * <ul>
 *   <li> {@link AfterWatermark} for firing when the watermark passes a timestamp determined from
 *   either the end of the window or the arrival of the first element in a pane.
 *   <li> {@link AfterProcessingTime} for firing after some amount of processing time has elapsed
 *   (typically since the first element in a pane).
 *   <li> {@link AfterPane} for firing off a property of the elements in the current pane, such as
 *   the number of elements that have been assigned to the current pane.
 * </ul>
 *
 * <p>In addition, {@code Trigger}s can be combined in a variety of ways:
 * <ul>
 *   <li> {@link Repeatedly#forever} to create a trigger that executes forever. Any time its
 *   argument finishes it gets reset and starts over. Can be combined with
 *   {@link Trigger#orFinally} to specify a condition that causes the repetition to stop.
 *   <li> {@link AfterEach#inOrder} to execute each trigger in sequence, firing each (and every)
 *   time that a trigger fires, and advancing to the next trigger in the sequence when it finishes.
 *   <li> {@link AfterFirst#of} to create a trigger that fires after at least one of its arguments
 *   fires. An {@link AfterFirst} trigger finishes after it fires once.
 *   <li> {@link AfterAll#of} to create a trigger that fires after all least one of its arguments
 *   have fired at least once. An {@link AfterFirst} trigger finishes after it fires once.
 * </ul>
 *
 * <p>Each trigger tree is instantiated per-key and per-window. Every trigger in the tree is in one
 * of the following states:
 * <ul>
 *   <li> Never Existed - before the trigger has started executing, there is no state associated
 *   with it anywhere in the system. A trigger moves to the executing state as soon as it
 *   processes in the current pane.
 *   <li> Executing - while the trigger is receiving items and may fire. While it is in this state,
 *   it may persist book-keeping information to persisted state, set timers, etc.
 *   <li> Finished - after a trigger finishes, all of its book-keeping data is cleaned up, and the
 *   system remembers only that it is finished. Entering this state causes us to discard any
 *   elements in the buffer for that window, as well.
 * </ul>
 *
 * <p>Once finished, a trigger cannot return itself back to an earlier state, however a composite
 * trigger could reset its sub-triggers.
 *
 * <p> Triggers should not build up any state internally since they may be recreated
 * between invocations of the callbacks. All important values should be persisted using
 * state before the callback returns.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
@Experimental(Experimental.Kind.TRIGGER)
public abstract class Trigger<W extends BoundedWindow> implements Serializable {

  private static final long serialVersionUID = 0L;

  /**
   * {@code TriggerResult} enumerates the possible result a trigger can have when it is executed.
   */
  public enum TriggerResult {
    FIRE(true, false),
    CONTINUE(false, false),
    FIRE_AND_FINISH(true, true);

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
   * {@code TriggerResult} enumerates the possible result a trigger can have when it is merged.
   */
  public enum MergeResult {
    FIRE(true, false, TriggerResult.FIRE),
    CONTINUE(false, false, TriggerResult.CONTINUE),
    FIRE_AND_FINISH(true, true, TriggerResult.FIRE_AND_FINISH),

    /**
     * A trigger can only return {@code ALREADY_FINISHED} from {@code onMerge}, and it should only
     * be returned if the trigger was previously finished in at least one window.
     *
     * <p> Returning this indicates that the sub-trigger should be treated as finished in the output
     * window.
     */
    ALREADY_FINISHED(false, true, null);

    private boolean finish;
    private boolean fire;
    private TriggerResult triggerResult;

    private MergeResult(boolean fire, boolean finish, TriggerResult triggerResult) {
      this.fire = fire;
      this.finish = finish;
      this.triggerResult = triggerResult;
    }

    public boolean isFire() {
      return fire;
    }

    public boolean isFinish() {
      return finish;
    }

    public TriggerResult getTriggerResult() {
      return triggerResult;
    }
  }

  /**
   * Interface for accessing information about the trigger being executed and other triggers in the
   * same tree.
   */
  public interface TriggerInfo<W extends BoundedWindow> {

    /**
     * Returns true if the windowing strategy of the current {@code PCollection} is a merging
     * WindowFn. If true, the trigger execution needs to keep enough information to support the
     * possibility of {@link Trigger#onMerge} being called. If false, {@link Trigger#onMerge} will
     * never be called.
     */
    boolean isMerging();

    /**
     * Access the executable versions of the sub-triggers of the current trigger.
     */
    Iterable<ExecutableTrigger<W>> subTriggers();

    /**
     * Access the executable version of the specified sub-trigger.
     */
    ExecutableTrigger<W> subTrigger(int subtriggerIndex);

    /**
     * Returns true if the current trigger is marked finished.
     */
    boolean isFinished();

    /**
     * Returns true if all the sub-triggers of the current trigger are marked finished.
     */
    boolean areAllSubtriggersFinished();

    /**
     * Returns an iterable over the unfinished sub-triggers of the current trigger.
     */
    Iterable<ExecutableTrigger<W>> unfinishedSubTriggers();

    /**
     * Returns the first unfinished sub-trigger.
     */
    ExecutableTrigger<W> firstUnfinishedSubTrigger();

    /**
     * Clears all keyed state for triggers in the current sub-tree and unsets all the associated
     * finished bits.
     */
    void resetTree() throws Exception;

    /**
     * Sets the finished bit for the current trigger.
     */
    void setFinished(boolean finished);
  }

  /**
   * Interact with properties of the trigger being executed, with extensions to deal with the
   * merging windows.
   */
  public interface MergingTriggerInfo<W extends BoundedWindow> extends TriggerInfo<W> {

    /** Return true if the trigger is finished in any window being merged. */
    public abstract boolean finishedInAnyMergingWindow();

    /** Return the merging windows in which the trigger is finished. */
    public abstract Iterable<W> getFinishedMergingWindows();
  }

  /**
   * Information accessible to all of the callbacks that are executed on a {@link Trigger}.
   */
  public abstract class TriggerContext {

    /** Returns the interface for accessing trigger info. */
    public abstract TriggerInfo<W> trigger();

    /** Returns the interface for accessing persistent state. */
    public abstract StateContext state();

    /** The window that the current context is executing in. */
    public abstract W window();

    /** Returns the interface for accessing timers. */
    public abstract ReduceFn.Timers timers();

    /** Create a sub-context for the given sub-trigger. */
    public abstract TriggerContext forTrigger(ExecutableTrigger<W> trigger);
  }

  /**
   * Details about an invocation of {@link Trigger#onElement}.
   */
  public abstract class OnElementContext extends TriggerContext {
    /** The element being handled by this call to {@link Trigger#onElement}. */
    public abstract Object element();

    /** The event timestamp of the element currently being processed. */
    public abstract Instant eventTimestamp();

    /** Create an {@code OnElementContext} for executing the given trigger. */
    @Override
    public abstract OnElementContext forTrigger(ExecutableTrigger<W> trigger);
  }

  /**
   * Details about an invocation of {@link Trigger#onMerge}.
   */
  public abstract class OnMergeContext extends TriggerContext {
    /** The old windows that were merged. */
    public abstract Iterable<W> oldWindows();

    /** Create an {@code OnMergeContext} for executing the given trigger. */
    @Override
    public abstract OnMergeContext forTrigger(ExecutableTrigger<W> trigger);

    @Override
    public abstract MergingStateContext state();

    @Override
    public abstract MergingTriggerInfo<W> trigger();
  }

  /**
   * Details about an invocation of {@link Trigger#onTimer}.
   */
  public abstract class OnTimerContext extends TriggerContext {

    /** Returns the time that the timer was set for. */
    public abstract Instant timestamp();

    /** Returns the time domain that thet timer was set for. */
    public abstract TimeDomain timeDomain();

    /**
     * Create an {@code OnTimerContext} for executing the given trigger.
     */
    @Override
    public abstract OnTimerContext forTrigger(ExecutableTrigger<W> trigger);
  }

  @Nullable
  protected final List<Trigger<W>> subTriggers;

  protected Trigger(@Nullable List<Trigger<W>> subTriggers) {
    this.subTriggers = subTriggers;
  }


  /**
   * Called immediately after an element is first incorporated into a window.
   */
  public abstract TriggerResult onElement(OnElementContext c) throws Exception;

  /**
   * Called immediately after windows have been merged.
   *
   * <p> Leaf triggers should determine their result by inspecting their status and any state
   * in the merging windows. Composite triggers should determine their result by calling
   * {@link ExecutableTrigger#invokeMerge} on their sub-triggers, and applying appropriate logic.
   *
   * <p> A trigger can only return {@link MergeResult#ALREADY_FINISHED} if it is marked as finished
   * in at least one of the windows being merged.
   *
   * <p>The implementation does not need to clear out any state associated with the old windows.
   */
  public abstract MergeResult onMerge(OnMergeContext c) throws Exception;

  /**
   * Called when a timer has fired for the current window. Composite triggers should pass the event
   * to all sub-triggers. Triggers that set timers should verify the timer matches what they set
   * before processing the firing.
   */
  public abstract TriggerResult onTimer(OnTimerContext c) throws Exception;

  /**
   * Called to allow the trigger to prefetch any state it will likely need to read from during
   * an {@link #onElement} call.
   *
   * @param state StateContext to prefetch from.
   */
  public void prefetchOnElement(StateContext state) {
    if (subTriggers != null) {
      for (Trigger<W> trigger : subTriggers) {
        trigger.prefetchOnElement(state);
      }
    }
  }

  /**
   * Called to allow the trigger to prefetch any state it will likely need to read from during
   * an {@link #onMerge} call.
   *
   * @param state StateContext to prefetch from.
   */
  public void prefetchOnMerge(MergingStateContext state) {
    if (subTriggers != null) {
      for (Trigger<W> trigger : subTriggers) {
        trigger.prefetchOnMerge(state);
      }
    }
  }

  /**
   * Called to allow the trigger to prefetch any state it will likely need to read from during
   * an {@link #onTimer} call.
   *
   * @param state StateContext to prefetch from.
   */
  public void prefetchOnTimer(StateContext state) {
    if (subTriggers != null) {
      for (Trigger<W> trigger : subTriggers) {
        trigger.prefetchOnTimer(state);
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
      for (ExecutableTrigger<W> trigger : c.trigger().subTriggers()) {
        trigger.invokeClear(c);
      }
    }
  }

  public Iterable<Trigger<W>> subTriggers() {
    return subTriggers;
  }

  /**
   * Return a trigger to use after a {@code GroupByKey} to preserve the
   * intention of this trigger. Specifically, triggers that are time based
   * and intended to provide speculative results should continue providing
   * speculative results. Triggers that fire once (or multiple times) should
   * continue firing once (or multiple times).
   */
  public Trigger<W> getContinuationTrigger() {
    if (subTriggers == null) {
      return getContinuationTrigger(null);
    }

    List<Trigger<W>> subTriggerContinuations = new ArrayList<>();
    for (Trigger<W> subTrigger : subTriggers) {
      subTriggerContinuations.add(subTrigger.getContinuationTrigger());
    }
    return getContinuationTrigger(subTriggerContinuations);
  }

  /**
   * Return the {@link #getContinuationTrigger} of this {@code Trigger}. For convenience, this
   * is provided the continuation trigger of each of the sub-triggers.
   */
  protected abstract Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers);

  /**
   * Returns a bound in watermark time by which this trigger would have fired at least once
   * for a given window had there been input data.  This is a static property of a trigger
   * that does not depend on its state.
   *
   * <p> For triggers that do not fire based on the watermark advancing, returns
   * {@link BoundedWindow#TIMESTAMP_MAX_VALUE}.
   *
   * <p> This estimate is used to determine that there are no elements in a side-input window, which
   * causes the default value to be used instead.
   */
  public abstract Instant getWatermarkThatGuaranteesFiring(W window);

  /**
   * Returns whether this performs the same triggering as the given {@code Trigger}.
   */
  public boolean isCompatible(Trigger<?> other) {
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
    if (subTriggers == null || subTriggers.size() == 0) {
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
    if (!(obj instanceof Trigger)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    Trigger<W> that = (Trigger<W>) obj;
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
   * <p> The expression {@code t1.orFinally(t2)} fires every time {@code t1} fires, and finishes
   * as soon as either {@code t1} finishes or {@code t2} fires, in which case it fires one last time
   * for {@code t2}. Both {@code t1} and {@code t2} are executed in parallel. This means that
   * {@code t1} may have fired since {@code t2} started, so not all of the elements that {@code t2}
   * has seen are necessarily in the current pane.
   *
   * <p>For example the final firing of the following trigger may only have 1 element:
   * <pre> {@code
   * Repeatedly.forever(AfterPane.elementCountAtLeast(2))
   *     .orFinally(AfterPane.elementCountAtLeast(5))
   * } </pre>
   *
   * <p> Note that if {@code t1} is {@link OnceTrigger}, then {@code t1.orFinally(t2)} is the same
   * as {@code AfterFirst.of(t1, t2)}.
   */
  public Trigger<W> orFinally(OnceTrigger<W> until) {
    return new OrFinallyTrigger<W>(this, until);
  }

  /**
   * {@link Trigger}s that are guaranteed to fire at most once should extend from this, rather
   * than the general {@link Trigger} class to indicate that behavior.
   *
   * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
   *            {@code AtMostOnceTrigger}
   */
  public abstract static class OnceTrigger<W extends BoundedWindow> extends Trigger<W> {
    private static final long serialVersionUID = 0L;

    protected OnceTrigger(List<Trigger<W>> subTriggers) {
      super(subTriggers);
    }

    @Override
    public final OnceTrigger<W> getContinuationTrigger() {
      Trigger<W> continuation = super.getContinuationTrigger();
      if (!(continuation instanceof OnceTrigger)) {
        throw new IllegalStateException("Continuation of a OnceTrigger must be a OnceTrigger");
      }
      return (OnceTrigger<W>) continuation;
    }
  }
}
