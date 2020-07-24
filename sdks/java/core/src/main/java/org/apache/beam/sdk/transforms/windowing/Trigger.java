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
package org.apache.beam.sdk.transforms.windowing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Triggers control when the elements for a specific key and window are output. As elements arrive,
 * they are put into one or more windows by a {@link Window} transform and its associated {@link
 * WindowFn}, and then passed to the associated {@link Trigger} to determine if the {@link
 * BoundedWindow Window's} contents should be output.
 *
 * <p>See {@link GroupByKey} and {@link Window} for more information about how grouping with windows
 * works.
 *
 * <p>The elements that are assigned to a window since the last time it was fired (or since the
 * window was created) are placed into the current window pane. Triggers are evaluated against the
 * elements as they are added. When the root trigger fires, the elements in the current pane will be
 * output. When the root trigger finishes (indicating it will never fire again), the window is
 * closed and any new elements assigned to that window are discarded.
 *
 * <p>Several predefined triggers are provided:
 *
 * <ul>
 *   <li>{@link AfterWatermark} for firing when the watermark passes a timestamp determined from
 *       either the end of the window or the arrival of the first element in a pane.
 *   <li>{@link AfterProcessingTime} for firing after some amount of processing time has elapsed
 *       (typically since the first element in a pane).
 *   <li>{@link AfterPane} for firing off a property of the elements in the current pane, such as
 *       the number of elements that have been assigned to the current pane.
 * </ul>
 *
 * <p>In addition, triggers can be combined in a variety of ways:
 *
 * <ul>
 *   <li>{@link Repeatedly#forever} to create a trigger that executes forever. Any time its argument
 *       finishes it gets reset and starts over. Can be combined with {@link Trigger#orFinally} to
 *       specify a condition that causes the repetition to stop.
 *   <li>{@link AfterEach#inOrder} to execute each trigger in sequence, firing each (and every) time
 *       that a trigger fires, and advancing to the next trigger in the sequence when it finishes.
 *   <li>{@link AfterFirst#of} to create a trigger that fires after at least one of its arguments
 *       fires. An {@link AfterFirst} trigger finishes after it fires once.
 *   <li>{@link AfterAll#of} to create a trigger that fires after all least one of its arguments
 *       have fired at least once. An {@link AfterAll} trigger finishes after it fires once.
 * </ul>
 */
@Experimental(Kind.TRIGGER)
public abstract class Trigger implements Serializable {

  protected final List<Trigger> subTriggers;

  protected Trigger(List<Trigger> subTriggers) {
    this.subTriggers = subTriggers;
  }

  protected Trigger() {
    this(Collections.emptyList());
  }

  public List<Trigger> subTriggers() {
    return subTriggers;
  }

  /**
   * Return a trigger to use after a {@link GroupByKey} to preserve the intention of this trigger.
   * Specifically, triggers that are time based and intended to provide speculative results should
   * continue providing speculative results. Triggers that fire once (or multiple times) should
   * continue firing once (or multiple times).
   *
   * <p>If this method is not overridden, its default implementation delegates its behavior to
   * {@link #getContinuationTrigger(List)} which is expected to be implemented by subclasses.
   */
  public Trigger getContinuationTrigger() {
    if (subTriggers == null) {
      return getContinuationTrigger(null);
    }

    List<Trigger> subTriggerContinuations = new ArrayList<>();
    for (Trigger subTrigger : subTriggers) {
      subTriggerContinuations.add(subTrigger.getContinuationTrigger());
    }
    return getContinuationTrigger(subTriggerContinuations);
  }

  /**
   * Subclasses should override this to return the {@link #getContinuationTrigger} of this {@link
   * Trigger}. For convenience, this is provided the continuation trigger of each of the
   * sub-triggers in the same order as {@link #subTriggers}.
   *
   * @param continuationTriggers contains the result of {@link #getContinuationTrigger()} on each of
   *     the {@code subTriggers} in the same order.
   */
  protected abstract Trigger getContinuationTrigger(List<Trigger> continuationTriggers);

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Returns a bound in event time by which this trigger would have fired at least once for a
   * given window had there been input data.
   *
   * <p>For triggers that do not fire based on the watermark advancing, returns {@link
   * BoundedWindow#TIMESTAMP_MAX_VALUE}.
   *
   * <p>This estimate may be used, for example, to determine that there are no elements in a
   * side-input window, which causes the default value to be used instead.
   */
  @Internal
  public abstract Instant getWatermarkThatGuaranteesFiring(BoundedWindow window);

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Indicates whether this trigger may "finish". A top level trigger that finishes can cause
   * data loss, so is rejected by GroupByKey validation.
   */
  @Internal
  public abstract boolean mayFinish();

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Returns whether this performs the same triggering as the given {@link Trigger}.
   */
  @Internal
  public boolean isCompatible(Trigger other) {
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
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Trigger)) {
      return false;
    }
    Trigger that = (Trigger) obj;
    return Objects.equals(getClass(), that.getClass())
        && Objects.equals(subTriggers, that.subTriggers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), subTriggers);
  }

  /**
   * Specify an ending condition for this trigger. If the {@code until} {@link Trigger} fires then
   * the combination fires.
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
   *
   * <p>Note that if {@code t1} is {@link OnceTrigger}, then {@code t1.orFinally(t2)} is the same as
   * {@code AfterFirst.of(t1, t2)}.
   */
  public OrFinallyTrigger orFinally(OnceTrigger until) {
    return new OrFinallyTrigger(this, until);
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Triggers that are guaranteed to fire at most once should extend {@link OnceTrigger} rather
   * than the general {@link Trigger} class to indicate that behavior.
   */
  @Internal
  public abstract static class OnceTrigger extends Trigger {
    protected OnceTrigger(List<Trigger> subTriggers) {
      super(subTriggers);
    }

    @Override
    public final boolean mayFinish() {
      return true;
    }

    @Override
    public final OnceTrigger getContinuationTrigger() {
      Trigger continuation = super.getContinuationTrigger();
      if (!(continuation instanceof OnceTrigger)) {
        throw new IllegalStateException("Continuation of a OnceTrigger must be a OnceTrigger");
      }
      return (OnceTrigger) continuation;
    }
  }
}
