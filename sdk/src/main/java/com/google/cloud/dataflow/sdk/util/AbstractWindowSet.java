/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.values.KV;

import java.util.Arrays;
import java.util.Collection;

/**
 * Abstract class representing a set of active windows for a key.
 */
abstract class AbstractWindowSet<K, VI, VO, W extends BoundedWindow> {
  /**
   * Hook for determining how to keep track of active windows and when they
   * should be marked as complete.
   */
  interface ActiveWindowManager<W> {
    /**
     * Notes that a window has been added to the active set.
     *
     * <p> The given window must not already be active.
     */
    void addWindow(W window) throws Exception;

    /**
     * Notes that a window has been explicitly removed from the active set.
     *
     * <p> The given window must currently be active.
     *
     * <p> Windows are implicitly removed from the active set when they are
     * complete, and this method will not be called.  This method is called when
     * a window is merged into another and thus is no longer active.
     */
    void removeWindow(W window) throws Exception;
  }

  /**
   * Wrapper around AbstractWindowSet that provides the MergeContext interface.
   */
  static class WindowMergeContext<T, W extends BoundedWindow>
      extends WindowFn<T, W>.MergeContext {
    private final AbstractWindowSet<?, ?, ?, W> windowSet;

    @SuppressWarnings("unchecked")
    public WindowMergeContext(
        AbstractWindowSet<?, ?, ?, W> windowSet,
        WindowFn<?, W> windowFn) {
      ((WindowFn<T, W>) windowFn).super();
      this.windowSet = windowSet;
    }

    @Override public Collection<W> windows() {
      return windowSet.windows();
    }

    @Override public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
      windowSet.merge(toBeMerged, mergeResult);
    }
  }

  protected final K key;
  protected final WindowFn<?, W> windowFn;
  protected final Coder<VI> inputCoder;
  protected final DoFnProcessContext<?, KV<K, VO>> context;
  protected final ActiveWindowManager<W> activeWindowManager;

  protected AbstractWindowSet(
      K key,
      WindowFn<?, W> windowFn,
      Coder<VI> inputCoder,
      DoFnProcessContext<?, KV<K, VO>> context,
      ActiveWindowManager<W> activeWindowManager) {
    this.key = key;
    this.windowFn = windowFn;
    this.inputCoder = inputCoder;
    this.context = context;
    this.activeWindowManager = activeWindowManager;
  }

  /**
   * Returns the set of known windows.
   */
  protected abstract Collection<W> windows();

  /**
   * Returns the final value of the elements in the given window.
   *
   * <p> Illegal to call if the window does not exist in the set.
   */
  protected abstract VO finalValue(W window) throws Exception;

  /**
   * Adds the given value in the given window to the set.
   *
   * <p> If the window already exists, puts the element into that window.
   * If not, adds the window to the set first, then puts the element
   * in the window.
   */
  protected abstract void put(W window, VI value) throws Exception;

  /**
   * Removes the given window from the set.
   *
   * <p> Illegal to call if the window does not exist in the set.
   *
   * <p> {@code AbstractWindowSet} subclasses may throw
   * {@link UnsupportedOperationException} if they do not support removing
   * windows.
   */
  protected abstract void remove(W window) throws Exception;

  /**
   * Instructs this set to merge the windows in toBeMerged into mergeResult.
   *
   * <p> {@code toBeMerged} should be a subset of {@link #windows}
   * and disjoint from the {@code toBeMerged} set of previous calls
   * to {@code merge}.
   *
   * <p> {@code mergeResult} must either not be in {@link @windows} or be in
   * {@code toBeMerged}.
   *
   * <p> {@code AbstractWindowSet} subclasses may throw
   * {@link UnsupportedOperationException} if they do not support merging windows.
   */
  protected abstract void merge(Collection<W> toBeMerged, W mergeResult) throws Exception;

  /**
   * Returns whether this window set contains the given window.
   *
   * <p> {@code AbstractWindowSet} subclasses may throw
   * {@link UnsupportedOperationException} if they do not support querying for
   * which windows are active.  If this is the case, callers must ensure they
   * do not call {@link #finalValue} on non-existent windows.
   */
  protected abstract boolean contains(W window);

  /**
   * Marks the window as complete, causing its elements to be emitted.
   */
  public void markCompleted(W window) throws Exception {
    VO value = finalValue(window);
    remove(window);
    context.outputWindowedValue(
        KV.of(key, value),
        window.maxTimestamp(),
        Arrays.asList(window));
  }

  /**
   * Hook for WindowSets to take action before they are deleted.
   */
  protected void flush() throws Exception {}
}
