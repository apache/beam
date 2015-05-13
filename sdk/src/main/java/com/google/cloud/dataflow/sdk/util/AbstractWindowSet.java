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
import com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.WindowStatus;

import java.io.Serializable;
import java.util.Collection;

/**
 * Abstract class representing a set of active windows for a key.
 */
abstract class AbstractWindowSet<K, InputT, OutputT, W extends BoundedWindow> {

  /**
   * Factory for creating a window set.
   */
  public interface Factory<K, InputT, OutputT, W extends BoundedWindow> extends Serializable {
    public AbstractWindowSet<K, InputT, OutputT, W> create(
        K key,
        Coder<W> windowCoder,
        KeyedState keyedState,
        WindowingInternals<?, ?> windowingInternals) throws Exception;
  }

  /**
   * Return the {@code AbstractWindowSet.Factory} that will produce the appropriate kind of window
   * set for the given windowing strategy.
   */
  public static <K, V, W extends BoundedWindow> Factory<K, V, Iterable<V>, W> factoryFor(
      WindowingStrategy<?, W> windowingStrategy, Coder<V> inputCoder) {
    if (windowingStrategy.getWindowFn().isNonMerging()) {
      return NonMergingBufferingWindowSet.<K, V, W>factory(inputCoder);
    } else {
      return BufferingWindowSet.<K, V, W>factory(inputCoder);
    }
  }

  protected final K key;
  protected final Coder<W> windowCoder;
  protected final Coder<InputT> inputCoder;
  protected final KeyedState keyedState;
  protected final WindowingInternals<?, ?> windowingInternals;

  protected AbstractWindowSet(
      K key,
      Coder<W> windowCoder,
      Coder<InputT> inputCoder,
      KeyedState keyedState,
      WindowingInternals<?, ?> windowingInternals) {
    this.key = key;
    this.windowCoder = windowCoder;
    this.inputCoder = inputCoder;
    this.keyedState = keyedState;
    this.windowingInternals = windowingInternals;
  }

  /**
   * Returns the set of known windows.
   */
  protected abstract Collection<W> windows();

  /**
   * Returns the final value of the elements in the given window.
   *
   * <p> Returns null if the window does not exist in the set.
   */
  protected abstract OutputT finalValue(W window) throws Exception;

  /**
   * Adds the given value in the given window to the set.
   *
   * <p> If the window already exists, puts the element into that window.
   * If not, adds the window to the set first, then puts the element
   * in the window.
   */
  protected abstract WindowStatus put(W window, InputT value) throws Exception;

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
   * {@link UnsupportedOperationException} if they do not support querying
   * for which windows are active.  If this is the case, callers must ensure
   * they do not call {@link #finalValue} on non-existent windows.
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
