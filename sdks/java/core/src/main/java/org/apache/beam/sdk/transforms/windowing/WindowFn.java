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
import java.util.Collection;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

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
    implements Serializable, HasDisplayData {
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
     * Returns the window of the current element prior to this
     * {@code WindowFn} being called.
     */
    public abstract BoundedWindow window();
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
   * Returns the default {@link WindowMappingFn} to use to map main input windows to side input
   * windows. This should accept arbitrary main input windows, and produce a {@link BoundedWindow}
   * that can be produced by this {@link WindowFn}.
   */
  public abstract WindowMappingFn<W> getDefaultWindowMappingFn();

  /**
   * Returns the output timestamp to use for data depending on the given
   * {@code inputTimestamp} in the specified {@code window}.
   *
   * <p>The result of this method must be between {@code inputTimestamp} and
   * {@code window.maxTimestamp()} (inclusive on both sides).
   *
   * <p>This function must be monotonic across input timestamps. Specifically, if {@code A < B},
   * then {@code getOutputTime(A, window) <= getOutputTime(B, window)}.
   *
   * <p>For a {@link WindowFn} that doesn't produce overlapping windows, this can (and typically
   * should) just return {@code inputTimestamp}. In the presence of overlapping windows, it is
   * suggested that the result in later overlapping windows is past the end of earlier windows
   * so that the later windows don't prevent the watermark from
   * progressing past the end of the earlier window.
   */
  @Experimental(Kind.OUTPUT_TIME)
  public Instant getOutputTime(Instant inputTimestamp, W window) {
    return inputTimestamp;
  }

  /**
   * Returns true if this {@code WindowFn} never needs to merge any windows.
   */
  public boolean isNonMerging() {
    return false;
  }

  /**
   * Returns a {@link TypeDescriptor} capturing what is known statically about the window type of
   * this {@link WindowFn} instance's most-derived class.
   *
   * <p>In the normal case of a concrete {@link WindowFn} subclass with no generic type parameters
   * of its own (including anonymous inner classes), this will be a complete non-generic type.
   */
  public TypeDescriptor<W> getWindowTypeDescriptor() {
    return new TypeDescriptor<W>(this) {};
  }

  /**
   * {@inheritDoc}
   *
   * <p>By default, does not register any display data. Implementors may override this method
   * to provide their own display data.
   */
  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
  }
}
