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

import com.google.common.collect.Ordering;
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.joda.time.Instant;

/**
 * <b><i>(Experimental)</i></b> A function from timestamps of input values to the timestamp for a
 * computed value.
 *
 * <p>The function is represented via three components:
 * <ol>
 *   <li>{@link #assignOutputTime} calculates an output timestamp for any input
 *       value in a particular window.</li>
 *   <li>The output timestamps for all non-late input values within a window are combined
 *       according to {@link #combine combine()}, a commutative and associative operation on
 *       the output timestamps.</li>
 *   <li>The output timestamp when windows merge is provided by {@link #merge merge()}.</li>
 * </ol>
 *
 * <p>This abstract class cannot be subclassed directly, by design: it may grow
 * in consumer-compatible ways that require mutually-exclusive default implementations. To
 * create a concrete subclass, extend {@link OutputTimeFn.Defaults} or
 * {@link OutputTimeFn.DependsOnlyOnWindow}. Note that as long as this class remains
 * experimental, we may also choose to change it in arbitrary backwards-incompatible ways.
 *
 * @param <W> the type of window. Contravariant: methods accepting any subtype of
 * {@code OutputTimeFn<W>} should use the parameter type {@code OutputTimeFn<? super W>}.
 */
@Experimental(Experimental.Kind.OUTPUT_TIME)
public abstract class OutputTimeFn<W extends BoundedWindow> implements Serializable {

  protected OutputTimeFn() { }

  /**
   * Returns the output timestamp to use for data depending on the given
   * {@code inputTimestamp} in the specified {@code window}.
   *
   * <p>The result of this method must be between {@code inputTimestamp} and
   * {@code window.maxTimestamp()} (inclusive on both sides).
   *
   * <p>This function must be monotonic across input timestamps. Specifically, if {@code A < B},
   * then {@code assignOutputTime(A, window) <= assignOutputTime(B, window)}.
   *
   * <p>For a {@link WindowFn} that doesn't produce overlapping windows, this can (and typically
   * should) just return {@code inputTimestamp}. In the presence of overlapping windows, it is
   * suggested that the result in later overlapping windows is past the end of earlier windows
   * so that the later windows don't prevent the watermark from
   * progressing past the end of the earlier window.
   *
   * <p>See the overview of {@link OutputTimeFn} for the consistency properties required
   * between {@link #assignOutputTime}, {@link #combine}, and {@link #merge}.
   */
  public abstract Instant assignOutputTime(Instant inputTimestamp, W window);

  /**
   * Combines the given output times, which must be from the same window, into an output time
   * for a computed value.
   *
   * <ul>
   *   <li>{@code combine} must be commutative: {@code combine(a, b).equals(combine(b, a))}.</li>
   *   <li>{@code combine} must be associative:
   *       {@code combine(a, combine(b, c)).equals(combine(combine(a, b), c))}.</li>
   * </ul>
   */
  public abstract Instant combine(Instant outputTime, Instant otherOutputTime);

  /**
   * Merges the given output times, presumed to be combined output times for windows that
   * are merging, into an output time for the {@code resultWindow}.
   *
   * <p>When windows {@code w1} and {@code w2} merge to become a new window {@code w1plus2},
   * then {@link #merge} must be implemented such that the output time is the same as
   * if all timestamps were assigned in {@code w1plus2}. Formally:
   *
   * <p>{@code fn.merge(w, fn.assignOutputTime(t1, w1), fn.assignOutputTime(t2, w2))}
   *
   * <p>must be equal to
   *
   * <p>{@code fn.combine(fn.assignOutputTime(t1, w1plus2), fn.assignOutputTime(t2, w1plus2))}
   *
   * <p>If the assigned time depends only on the window, the correct implementation of
   * {@link #merge merge()} necessarily returns the result of
   * {@link #assignOutputTime assignOutputTime(t1, w1plus2)}
   * (which equals {@link #assignOutputTime assignOutputTime(t2, w1plus2)}.
   * Defaults for this case are provided by {@link DependsOnlyOnWindow}.
   *
   * <p>For many other {@link OutputTimeFn} implementations, such as taking the earliest or latest
   * timestamp, this will be the same as {@link #combine combine()}. Defaults for this
   * case are provided by {@link Defaults}.
   */
  public abstract Instant merge(W intoWindow, Iterable<? extends Instant> mergingTimestamps);

  /**
   * Returns {@code true} if the result of combination of many output timestamps actually depends
   * only on the earliest.
   *
   * <p>This may allow optimizations when it is very efficient to retrieve the earliest timestamp
   * to be combined.
   */
  public abstract boolean dependsOnlyOnEarliestInputTimestamp();

  /**
   * Returns {@code true} if the result does not depend on what outputs were combined but only
   * the window they are in. The canonical example is if all timestamps are sure to
   * be the end of the window.
   *
   * <p>This may allow optimizations, since it is typically very efficient to retrieve the window
   * and combining output timestamps is not necessary.
   *
   * <p>If the assigned output time for an implementation depends only on the window, consider
   * extending {@link DependsOnlyOnWindow}, which returns {@code true} here and also provides
   * a framework for easily implementing a correct {@link #merge}, {@link #combine} and
   * {@link #assignOutputTime}.
   */
  public abstract boolean dependsOnlyOnWindow();

  /**
   * <b><i>(Experimental)</i></b> Default method implementations for {@link OutputTimeFn} where the
   * output time depends on the input element timestamps and possibly the window.
   *
   * <p>To complete an implementation, override {@link #assignOutputTime}, at a minimum.
   *
   * <p>By default, {@link #combine} and {@link #merge} return the earliest timestamp of their
   * inputs.
   */
  public abstract static class Defaults<W extends BoundedWindow> extends OutputTimeFn<W> {

    protected Defaults() {
      super();
    }

    /**
     * {@inheritDoc}
     *
     * @return the earlier of the two timestamps.
     */
    @Override
    public Instant combine(Instant outputTimestamp, Instant otherOutputTimestamp) {
      return Ordering.natural().min(outputTimestamp, otherOutputTimestamp);
    }

    /**
     * {@inheritDoc}
     *
     * @return the result of {@link #combine combine(outputTimstamp, otherOutputTimestamp)},
     * by default.
     */
    @Override
    public Instant merge(W resultWindow, Iterable<? extends Instant> mergingTimestamps) {
      return OutputTimeFns.combineOutputTimes(this, mergingTimestamps);
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code false} by default. An {@link OutputTimeFn} that is known to depend only on the
     * window should extend {@link OutputTimeFn.DependsOnlyOnWindow}.
     */
    @Override
    public boolean dependsOnlyOnWindow() {
      return false;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true} by default.
     */
    @Override
    public boolean dependsOnlyOnEarliestInputTimestamp() {
      return false;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true} if the two {@link OutputTimeFn} instances have the same class, by
     *         default.
     */
    @Override
    public boolean equals(Object other) {
      if (other == null) {
        return false;
      }

      return this.getClass().equals(other.getClass());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass());
    }
  }

  /**
   * <b><i>(Experimental)</i></b> Default method implementations for {@link OutputTimeFn} when the
   * output time depends only on the window.
   *
   * <p>To complete an implementation, override {@link #assignOutputTime(BoundedWindow)}.
   */
  public abstract static class DependsOnlyOnWindow<W extends BoundedWindow>
      extends OutputTimeFn<W> {

    protected DependsOnlyOnWindow() {
      super();
    }

    /**
     * Returns the output timestamp to use for data in the specified {@code window}.
     *
     * <p>Note that the result of this method must be between the maximum possible input timestamp
     * in {@code window} and {@code window.maxTimestamp()} (inclusive on both sides).
     *
     * <p>For example, using {@code Sessions.withGapDuration(gapDuration)}, we know that all input
     * timestamps must lie at least {@code gapDuration} from the end of the session, so
     * {@code window.maxTimestamp() - gapDuration} is an acceptable assigned timestamp.
     *
     * @see #assignOutputTime(Instant, BoundedWindow)
     */
    protected abstract Instant assignOutputTime(W window);

    /**
     * {@inheritDoc}
     *
     * @return the result of {#link assignOutputTime(BoundedWindow) assignOutputTime(window)}.
     */
    @Override
    public final Instant assignOutputTime(Instant timestamp, W window) {
      return assignOutputTime(window);
    }

    /**
     * {@inheritDoc}
     *
     * @return the same timestamp as both argument timestamps, which are necessarily equal.
     */
    @Override
    public final Instant combine(Instant outputTimestamp, Instant otherOutputTimestamp) {
      return outputTimestamp;
    }

    /**
     * {@inheritDoc}
     *
     * @return the result of
     * {@link #assignOutputTime(BoundedWindow) assignOutputTime(resultWindow)}.
     */
    @Override
    public final Instant merge(W resultWindow, Iterable<? extends Instant> mergingTimestamps) {
      return assignOutputTime(resultWindow);
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true}.
     */
    @Override
    public final boolean dependsOnlyOnWindow() {
      return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true}. Since the output time depends only on the window, it can
     * certainly be ascertained given a single input timestamp.
     */
    @Override
    public final boolean dependsOnlyOnEarliestInputTimestamp() {
      return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true} if the two {@link OutputTimeFn} instances have the same class, by
     *         default.
     */
    @Override
    public boolean equals(Object other) {
      if (other == null) {
        return false;
      }

      return this.getClass().equals(other.getClass());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass());
    }
  }
}
