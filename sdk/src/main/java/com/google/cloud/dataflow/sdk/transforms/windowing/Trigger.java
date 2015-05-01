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
import com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.annotations.VisibleForTesting;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * {@code Trigger}s control when the elements for a specific key and window are output. As elements
 * arrive, they are put into one or more windows by the {@code Window} by the {@link WindowFn}, and
 * then passed to the associated {@code Trigger} to determine if the {@code Window}s contents should
 * be output.
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
 *   it may persist book-keeping information to {@link KeyedState}, set timers, etc.
 *   <li> Finished - after a trigger finishes, all of its book-keeping data is cleaned up, and the
 *   system remembers only that it is finished. Entering this state causes us to discard any
 *   elements in the buffer for that window, as well.
 * </ul>
 *
 * <p>Once finished, a trigger cannot return itself back to an earlier state, however a composite
 * trigger could reset its sub-triggers.
 *
 * <p> Triggers should not build up any state internally since they may be recreated
 * between invocations of the callbacks. All important values should be persisted to
 * {@link KeyedState} before the callback returns.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
@Experimental(Experimental.Kind.TRIGGER)
public abstract class Trigger<W extends BoundedWindow> implements Serializable {

  private static final long serialVersionUID = 0L;

  /**
   * {@code TimeDomain} specifies whether an operation is based on
   * timestamps of elements or current "real-world" time as reported while processing.
   */
  public enum TimeDomain {
    /**
     * The {@code EVENT_TIME} domain corresponds to the timestamps on the elemnts. Time advances
     * on the system watermark advances.
     */
    EVENT_TIME,

    /**
     * The {@code PROCESSING_TIME} domain corresponds to the current to the current (system) time.
     * This is advanced during exeuction of the Dataflow pipeline.
     */
    PROCESSING_TIME;
  }

  /**
   * {@code WindowStatus} indicates the status of the window that an element is being processed in.
   */
  public enum WindowStatus {
    /**
     * The arrival of this element started a new pane. Either the window is entirely new, or we had
     * previously fired a trigger that caused us to output the earlier elements.
     */
    NEW,

    /** This element was added to a pane that was already being managed. */
    EXISTING,

    /**
     * The window set doesn’t track the windows being managed, so it is not known whether the pane
     * is new. The trigger can track windows on its own if necessary.
     */
    UNKNOWN;
  }

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
   * Information accessible to all of the callbacks that are executed on a {@link Trigger}.
   *
   * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
   *            {@link TriggerContext}
   */
  public interface TriggerContext<W extends BoundedWindow>  {

    /**
     * Sets a timer to fire when the watermark or processing time is beyond the given timestamp.
     * Timers are not gauranteed to fire immediately, but will be delivered at some time afterwards.
     *
     * <p>Each trigger can have a single timer in per {@code timeDomain} and {@code window}. If the
     * trigger has already set a timer for a given domain and window, then setting overwrites it.
     *
     * @param window the window the timer is being set for.
     * @param timestamp the time at which the trigger’s {@link Trigger#onTimer} callback should
     *        execute
     * @param timeDomain the domain that the {@code timestamp} applies to
     */
    void setTimer(W window, Instant timestamp, TimeDomain timeDomain) throws IOException;

    /**
     * Removes the timer set in this trigger context for the given {@code window} and
     * {@code timeDomain}.
     */
    void deleteTimer(W window, TimeDomain timeDomain) throws IOException;

    /**
     * Returns the current processing time.
     */
    Instant currentProcessingTime();

    /**
     * Updates the value stored in keyed state for the given {@code tag} and {@code window}.
     */
    <T> void store(CodedTupleTag<T> tag, W window, T value) throws IOException;

    /**
     * Removes the keyed state associated with the given {@code tag} and {@code window}.
     */
    <T> void remove(CodedTupleTag<T> tag, W window) throws IOException;

    /**
     * Lookup the value stored for the given {@code tag} and {@code window}.
     */
    <T> T lookup(CodedTupleTag<T> tag, W window) throws IOException;

    /**
     * Lookup the value stored for a given {@code tag} in a bunch of {@code window}s.
     */
    <T> Map<W, T> lookup(CodedTupleTag<T> tag, Iterable<W> windows) throws IOException;

    /**
     * Create a {@code TriggerContext} for executing a given sub-trigger.
     */
    TriggerContext<W> forChild(int childIndex);
  }

  /**
   * Details about an invocation of {@link Trigger#onElement}.
   *
   * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
   *            {@code OnElementEvent}
   */
  public static class OnElementEvent<W> {
    private Object value;
    private Instant timestamp;
    private W window;
    private WindowStatus status;

    public OnElementEvent(Object value, Instant timestamp, W window, WindowStatus status) {
      this.value = value;
      this.timestamp = timestamp;
      this.window = window;
      this.status = status;
    }

    /**
     * The element being handled by this call to {@link Trigger#onElement}.
     */
    public Object element() {
      return value;
    }

    /**
     * The event timestamp of the element being processed.
     */
    public Instant eventTimestamp() {
      return timestamp;
    }

    /**
     * The window into which the element was assigned.
     */
    public W window() {
      return window;
    }

    /**
     * The status of the window to which the element was assigned.
     */
    public WindowStatus windowStatus() {
      return status;
    }
  }

  /**
   * Called immediately after an element is first incorporated into a window.
   *
   * @param c the context to interact with
   * @param e an event describing the cause of this callback being executed
   */
  public abstract TriggerResult onElement(
      TriggerContext<W> c, OnElementEvent<W> e) throws Exception;

  /**
   * Details about an invocation of {@link Trigger#onMerge}.
   *
   * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
   *            {@code OnMergeEvent}
   */
  public static class OnMergeEvent<W> {
    private Iterable<W> oldWindows;
    private W newWindow;

    public OnMergeEvent(Iterable<W> oldWindows, W newWindow) {
      this.oldWindows = oldWindows;
      this.newWindow = newWindow;
    }

    /**
     * The old windows that were merged.
     */
    public Iterable<W> oldWindows() {
      return oldWindows;
    }

    /**
     * The new window produced by merging the {@link #oldWindows()}.
     */
    public W newWindow() {
      return newWindow;
    }
  }

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
   * @param e an event describnig the cause of this callback being executed
   */
  public abstract TriggerResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception;

  /**
   * Details about an invocation of {@link Trigger#onTimer}.
   *
   * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
   *            {@code OnTimerEvent}
   */
  public static class OnTimerEvent<W extends BoundedWindow> {

    private TriggerId<W> triggerId;

    public OnTimerEvent(TriggerId<W> triggerId) {
      this.triggerId = triggerId;
    }

    public W window() {
      return triggerId.window;
    }

    public TriggerId<W> triggerId() {
      return triggerId;
    }

    /**
     * Remove the outer layer from the path to the desired timer. This produces an
     * {@code OnTimerEvent} suitable for passing to a subtrigger.
     */
    public OnTimerEvent<W> withoutOuterTrigger() {
      return new OnTimerEvent<W>(triggerId.withoutOuterTrigger());
    }

    /**
     * Return the index of the child this trigger ID is for.
     */
    public int getChildIndex() {
      return triggerId.getPath().iterator().next();
    }

    /**
     * Return true if the timer event is for the current layer.
     */
    public boolean isForCurrentLayer() {
      return !triggerId.getPath().iterator().hasNext();
    }
  }

  /**
   * Called when a timer has fired for the trigger or one of its sub-triggers.
   *
   * @param c the context to interact with
   * @param e identifier for the trigger that the timer is for.
   */
  public abstract TriggerResult onTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception;

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
   * Return true if the trigger is guaranteed to never finish.
   */
  public boolean willNeverFinish() {
    return false;
  }

  /**
   * Returns a bound in watermark time by which this trigger would have fired at least once
   * for a given window had there been input data.  This is a static property of a trigger
   * that does not depend on its state.
   *
   * <p> For triggers that do not fire based on the watermark advancing, returns
   * {@link BoundedWindow#TIMESTAMP_MAX_VALUE}.
   */
  public abstract Instant getWatermarkCutoff(W window);

  /**
   * Returns whether this performs the same triggering as the given {@code Trigger}.
   */
  public abstract boolean isCompatible(Trigger<?> other);

  /**
   * Identifies a unique {@link Trigger} instance, by the window it is in
   * and the path through the {@link Trigger} tree.
   *
   * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
   *            {@code TriggerId}
   */
  public static class TriggerId<W extends BoundedWindow> {
    private final W window;
    private final List<Integer> subTriggers;

    public TriggerId(W window, List<Integer> subTriggers) {
      this.window = window;
      this.subTriggers = subTriggers;
    }

    /**
     * Return a trigger ID that is applicable for the sub-trigger.
     */
    public TriggerId<W> withoutOuterTrigger() {
      return new TriggerId<>(window, subTriggers.subList(1, subTriggers.size()));
    }

    public W window() {
      return window;
    }

    public Iterable<Integer> getPath() {
      return subTriggers;
    }
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
   * <p> TODO: Add checks that an AtMostOnceTrigger never returns TriggerResult.FIRE.
   *
   * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
   *            {@code AtMostOnceTrigger}
   */
  public abstract static class OnceTrigger<W extends BoundedWindow> extends Trigger<W> {
    private static final long serialVersionUID = 0L;
  }

  /**
   * Executes the {@code actual} trigger until it finishes or until the {@code until} trigger fires.
   */
  @VisibleForTesting static class OrFinallyTrigger<W extends BoundedWindow> extends Trigger<W> {

    private static final int ACTUAL = 0;
    private static final int UNTIL = 1;
    private static final long serialVersionUID = 0L;

    private Trigger<W> actual;
    private OnceTrigger<W> until;

    @VisibleForTesting OrFinallyTrigger(Trigger<W> actual, OnceTrigger<W> until) {
      this.actual = actual;
      this.until = until;
    }

    @Override
    public TriggerResult onElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
      TriggerResult untilResult = until.onElement(c.forChild(UNTIL), e);
      if (untilResult != TriggerResult.CONTINUE) {
        return TriggerResult.FIRE_AND_FINISH;
      }

      return actual.onElement(c.forChild(ACTUAL), e);
    }

    @Override
    public TriggerResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
      TriggerResult untilResult = until.onMerge(c.forChild(UNTIL), e);
      if (untilResult != TriggerResult.CONTINUE) {
        return TriggerResult.FIRE_AND_FINISH;
      }

      return actual.onMerge(c.forChild(ACTUAL), e);
    }

    @Override
    public TriggerResult onTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception {
      if (e.isForCurrentLayer()) {
        throw new IllegalStateException("Until shouldn't receive any timers.");
      } else if (e.getChildIndex() == ACTUAL) {
        return actual.onTimer(c.forChild(ACTUAL), e.withoutOuterTrigger());
      } else {
        if (until.onTimer(c.forChild(UNTIL), e.withoutOuterTrigger()) != TriggerResult.CONTINUE) {
          return TriggerResult.FIRE_AND_FINISH;
        }
      }

      return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TriggerContext<W> c, W window) throws Exception {
      actual.clear(c.forChild(ACTUAL), window);
      until.clear(c.forChild(UNTIL), window);
    }

    @Override
    public boolean willNeverFinish() {
      return false;
    }

    @Override
    public Instant getWatermarkCutoff(W window) {
      // This trigger fires once either the trigger or the until trigger fires.
      Instant actualDeadline = actual.getWatermarkCutoff(window);
      Instant untilDeadline = until.getWatermarkCutoff(window);
      return actualDeadline.isBefore(untilDeadline) ? actualDeadline : untilDeadline;
    }

    @Override
    public boolean isCompatible(Trigger<?> other) {
      if (!(other instanceof OrFinallyTrigger)) {
        return false;
      }

      OrFinallyTrigger<?> that = (OrFinallyTrigger<?>) other;
      return actual.isCompatible(that.actual)
          && until.isCompatible(that.until);
    }
  }
}
