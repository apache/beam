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
package com.google.cloud.dataflow.sdk.transforms.windowing;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.common.collect.Ordering;

import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Collection;

/**
 * The argument to the {@link Window} transform used to assign elements into
 * windows and to determine how windows are merged.  See {@link Window} for more
 * information on how {@code WindowFn}s are used and for a library of
 * predefined {@code WindowFn}s.
 *
 * <p>Users will generally want to use the predefined
 * {@code WindowFn}s, but it is also possible to create new
 * subclasses.
 *
 * <p>To create a custom {@code WindowFn}, inherit from this class and override all required
 * methods.  If no merging is required, inherit from {@link NonMergingWindowFn}
 * instead.  If no merging is required and each element is assigned to a single window, inherit from
 * {@code PartitioningWindowFn}.  Inheriting from the most specific subclass will enable more
 * optimizations in the runner.
 *
 * @param <T> type of elements being windowed
 * @param <W> {@link BoundedWindow} subclass used to represent the
 *            windows used by this {@code WindowFn}
 */
public abstract class WindowFn<T, W extends BoundedWindow>
    implements Serializable {
  /**
   * Information available when running {@link #assignWindows}.
   */
  public abstract class AssignContext {
    /**
     * Returns the current element.
     */
    public abstract T element();

    /**
     * Returns the timestamp of the current element.
     */
    public abstract Instant timestamp();

    /**
     * Returns the windows the current element was in, prior to this
     * {@code WindowFn} being called.
     */
    public abstract Collection<? extends BoundedWindow> windows();
  }

  /**
   * Given a timestamp and element, returns the set of windows into which it
   * should be placed.
   */
  public abstract Collection<W> assignWindows(AssignContext c) throws Exception;

  /**
   * Information available when running {@link #mergeWindows}.
   */
  public abstract class MergeContext {
    /**
     * Returns the current set of windows.
     */
    public abstract Collection<W> windows();

    /**
     * Signals to the framework that the windows in {@code toBeMerged} should
     * be merged together to form {@code mergeResult}.
     *
     * <p>{@code toBeMerged} should be a subset of {@link #windows}
     * and disjoint from the {@code toBeMerged} set of previous calls
     * to {@code merge}.
     *
     * <p>{@code mergeResult} must either not be in {@link #windows} or be in
     * {@code toBeMerged}.
     *
     * @throws IllegalArgumentException if any elements of toBeMerged are not
     * in windows(), or have already been merged
     */
    public abstract void merge(Collection<W> toBeMerged, W mergeResult)
        throws Exception;
  }

  /**
   * Does whatever merging of windows is necessary.
   *
   * <p>See {@link MergeOverlappingIntervalWindows#mergeWindows} for an
   * example of how to override this method.
   */
  public abstract void mergeWindows(MergeContext c) throws Exception;

  /**
   * Returns whether this performs the same merging as the given
   * {@code WindowFn}.
   */
  public abstract boolean isCompatible(WindowFn<?, ?> other);

  /**
   * Returns the {@link Coder} used for serializing the windows used
   * by this windowFn.
   */
  public abstract Coder<W> windowCoder();

  /**
   * Returns the window of the side input corresponding to the given window of
   * the main input.
   *
   * <p>Authors of custom {@code WindowFn}s should override this.
   */
  public abstract W getSideInputWindow(final BoundedWindow window);

  /**
   * @deprecated Implement {@link #getOutputTimeFn} to return one of the appropriate
   * {@link OutputTimeFns}, or a custom {@link OutputTimeFn} extending
   * {@link OutputTimeFn.Defaults}.
   */
  @Deprecated
  @Experimental(Kind.OUTPUT_TIME)
  public Instant getOutputTime(Instant inputTimestamp, W window) {
    return getOutputTimeFn().assignOutputTime(inputTimestamp, window);
  }

  /**
   * Provides a default implementation for {@link WindowingStrategy#getOutputTimeFn()}.
   * See the full specification there.
   *
   * <p>If this {@link WindowFn} doesn't produce overlapping windows, this need not (and probably
   * should not) override any of the default implementations in {@link OutputTimeFn.Defaults}.
   *
   * <p>If this {@link WindowFn} does produce overlapping windows that can be predicted here, it is
   * suggested that the result in later overlapping windows is past the end of earlier windows so
   * that the later windows don't prevent the watermark from progressing past the end of the earlier
   * window.
   *
   * <p>For example, a timestamp in a sliding window should be moved past the beginning of the next
   * sliding window. See {@link SlidingWindows#getOutputTimeFn}.
   */
  @Experimental(Kind.OUTPUT_TIME)
  public OutputTimeFn<? super W> getOutputTimeFn() {
    return new OutputAtEarliestAssignedTimestamp<>(this);
  }

  /**
   * Returns true if this {@code WindowFn} never needs to merge any windows.
   */
  public boolean isNonMerging() {
    return false;
  }

  /**
   * Returns true if this {@code WindowFn} assigns each element to a single window.
   */
  public boolean assignsToSingleWindow() {
    return false;
  }

  /**
   * A compatibility adapter that will return the assigned timestamps according to the
   * {@link WindowFn}, which was the prior policy. Specifying the assigned output timestamps
   * on the {@link WindowFn} is now deprecated.
   */
  private static class OutputAtEarliestAssignedTimestamp<W extends BoundedWindow>
      extends OutputTimeFn.Defaults<W> {

    private final WindowFn<?, W> windowFn;

    public OutputAtEarliestAssignedTimestamp(WindowFn<?, W> windowFn) {
      this.windowFn = windowFn;
    }

    /**
     * {@inheritDoc}
     *
     * @return the result of {@link WindowFn#getOutputTime windowFn.getOutputTime()}.
     */
    @Override
    @SuppressWarnings("deprecation") // this is an adapter for the deprecated behavior
    public Instant assignOutputTime(Instant timestamp, W window) {
      return windowFn.getOutputTime(timestamp, window);
    }

    @Override
    public Instant combine(Instant outputTime, Instant otherOutputTime) {
      return Ordering.natural().min(outputTime, otherOutputTime);
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true}. When the {@link OutputTimeFn} is not overridden by {@link WindowFn}
     *         or {@link WindowingStrategy}, the minimum output timestamp is taken, which depends
     *         only on the minimum input timestamp by monotonicity of {@link #assignOutputTime}.
     */
    @Override
    public boolean dependsOnlyOnEarliestInputTimestamp() {
      return true;
    }
  }
}
