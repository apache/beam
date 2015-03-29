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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.Trigger.WindowStatus;

import java.util.Collection;

/**
 * Abstract class representing a set of active windows for a key.
 */
abstract class AbstractWindowSet<K, VI, VO, W extends BoundedWindow> {
  protected final K key;
  protected final WindowFn<?, W> windowFn;
  protected final Coder<VI> inputCoder;
  protected final DoFn<?, ?>.ProcessContext context;

  protected AbstractWindowSet(
      K key,
      WindowFn<?, W> windowFn,
      Coder<VI> inputCoder,
      DoFn<?, ?>.ProcessContext context) {
    this.key = key;
    this.windowFn = windowFn;
    this.inputCoder = inputCoder;
    this.context = context;
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
  protected abstract WindowStatus put(W window, VI value) throws Exception;

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
   * <p> {@code mergeResult} must either not be in {@link #windows} or be in
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
   * Hook for WindowSets to take action before they are deleted.
   */
  protected void persist() throws Exception {}

  public K getKey() {
    return key;
  }
}
