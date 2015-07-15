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

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;

import java.util.Collection;

/**
 * Tracks the windows that are active.
 *
 * @param <W> the types of windows being managed
 */
public interface ActiveWindowSet<W extends BoundedWindow> {

  /**
   * Callback for {@link #mergeIfAppropriate}.
   */
  public interface MergeCallback<W extends BoundedWindow> {
    void onMerge(Collection<W> mergedWindows, W resultWindow, boolean isResultNew) throws Exception;
  }

  /**
   * Save any state changes needed.
   */
  void persist();

  /**
   * Add a window to the {@code ActiveWindowSet}.
   *
   * @return false if the window was definitely not-active before being added, true if it either
   *     was already active, or the implementation doesn't have enough information to know.
   */
  boolean add(W window);

  /**
   * Remove a window from the {@code ActiveWindowSet}.
   */
  void remove(W window);

  /**
   * Invoke {@code merge} on the associated {@code WindowFn}, and return true if the {@code window}
   * still exists afterwards.
   */
  boolean mergeIfAppropriate(W window, MergeCallback<W> mergeCallback) throws Exception;

  /**
   * Return the set of windows that were merged to produce {@code window}. If the associated
   * {@code WindowFn} never merges windows, then this should return the singleton list containing
   * {@code window}.
   */
  Iterable<W> sourceWindows(W window);
}
