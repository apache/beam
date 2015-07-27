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

import com.google.cloud.dataflow.sdk.coders.Coder;

import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Collection;

/**
 * The argument to the {@link Window} transform used to assign elements into
 * windows and to determine how windows are merged.  See {@link Window} for more
 * information on how {@code WindowFn}s are used and for a library of
 * predefined {@code WindowFn}s.
 *
 * <p> Users will generally want to use the predefined
 * {@code WindowFn}s, but it is  also possible to create new
 * subclasses.
 * TODO: Describe how to properly create {@code WindowFn}s.
 *
 * @param <T> type of elements being windowed
 * @param <W> {@link BoundedWindow} subclass used to represent the
 *            windows used by this {@code WindowFn}
 */
public abstract class WindowFn<T, W extends BoundedWindow>
    implements Serializable {
  private static final long serialVersionUID = 0;

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
     * <p> {@code toBeMerged} should be a subset of {@link #windows}
     * and disjoint from the {@code toBeMerged} set of previous calls
     * to {@code merge}.
     *
     * <p> {@code mergeResult} must either not be in {@link #windows} or be in
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
   * <p> See {@link MergeOverlappingIntervalWindows#mergeWindows} for an
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
   * <p> Authors of custom {@code WindowFn}s should override this.
   */
  public abstract W getSideInputWindow(final BoundedWindow window);

  /**
   * Returns the output timestamp to use for data depending on the given {@code inputTimestamp}
   * in the specified {@code window}.
   *
    * <p> The result must be between {@code inputTimestamp} and {@code window.maxTimestamp()}
   * (inclusive on both sides). If this {@link WindowFn} doesn't produce overlapping windows,
   * this can (and typically should) just return {@code inputTimestamp}. If this does produce
   * overlapping windows, it is suggested that the that the result in later overlapping windows is
   * past the end of earlier windows so that the later windows don't prevent the watermark from
   * progressing past the end of the earlier window.
   *
   * <p> Each {@code KV<K, Iterable<V>>} produced from a {@code GroupByKey} will be output at a
   * timestamp that is the minimum of {@code getOutputTime} applied to the timestamp of all of
   * the non-late {@code KV<K, V>} that were used as input to the {@code GroupByKey}. The watermark
   * is also prevented from advancing past this minimum timestamp until after the
   * {@code KV<K, Iterable<V>>} has been output.
   *
   * <p> This function should be monotonic across input timestamps. Specifically, if {@code A < B},
   * then {@code getOutputTime(A, window) <= getOutputTime(B, window)}.
   */
  public abstract Instant getOutputTime(Instant inputTimestamp, W window);

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
}
