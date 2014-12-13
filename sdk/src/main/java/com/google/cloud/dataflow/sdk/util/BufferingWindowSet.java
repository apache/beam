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

import static com.google.cloud.dataflow.sdk.util.WindowUtils.bufferTag;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.google.cloud.dataflow.sdk.coders.SetCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowingFn;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.KV;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A WindowSet allowing windows to be merged and deleted.
 */
class BufferingWindowSet<K, V, W extends BoundedWindow>
    extends AbstractWindowSet<K, V, Iterable<V>, W> {
  /**
   * Tag for storing the merge tree, the data structure that keeps
   * track of which windows have been merged together.
   */
  private final CodedTupleTag<Map<W, Set<W>>> mergeTreeTag =
      CodedTupleTag.of(
          "mergeTree",
          MapCoder.of(
              windowingFn.windowCoder(),
              SetCoder.of(windowingFn.windowCoder())));

  /**
   * A map of live windows to windows that were merged into them.
   *
   * <p> The keys of the map correspond to the set of (merged) windows and the values
   * are the no-longer-present windows that were merged into the keys.  A given
   * window can appear in both the key and value of a single entry, but other at
   * most once across all keys and values.
   */
  private final Map<W, Set<W>> mergeTree;

  /**
   * Used to determine if writing the mergeTree (which is relatively stable)
   * is necessary.
   */
  private final Map<W, Set<W>> originalMergeTree;

  protected BufferingWindowSet(
      K key,
      WindowingFn<?, W> windowingFn,
      Coder<V> inputCoder,
      DoFnProcessContext<?, KV<K, Iterable<V>>> context,
      ActiveWindowManager<W> activeWindowManager) throws Exception {
    super(key, windowingFn, inputCoder, context, activeWindowManager);

    mergeTree = emptyIfNull(
        context.context.stepContext.lookup(Arrays.asList(mergeTreeTag))
        .get(mergeTreeTag));

    originalMergeTree = deepCopy(mergeTree);
  }

  @Override
  public void put(W window, V value) throws Exception {
    context.context.stepContext.writeToTagList(
        bufferTag(window, windowingFn.windowCoder(), inputCoder),
        value,
        context.timestamp());
    if (!mergeTree.containsKey(window)) {
      mergeTree.put(window, new HashSet<W>());
      activeWindowManager.addWindow(window);
    }
  }

  @Override
  public void remove(W window) throws Exception {
    mergeTree.remove(window);
    activeWindowManager.removeWindow(window);
  }

  @Override
  public void merge(Collection<W> otherWindows, W newWindow) throws Exception {
    Set<W> subWindows = mergeTree.get(newWindow);
    if (subWindows == null) {
      subWindows = new HashSet<>();
    }
    for (W other : otherWindows) {
      if (!mergeTree.containsKey(other)) {
        throw new IllegalArgumentException("Tried to merge a non-existent window: " + other);
      }
      subWindows.addAll(mergeTree.get(other));
      subWindows.add(other);
      remove(other);
    }
    mergeTree.put(newWindow, subWindows);
    activeWindowManager.addWindow(newWindow);
  }

  @Override
  public Collection<W> windows() {
    return Collections.unmodifiableSet(mergeTree.keySet());
  }

  @Override
  public boolean contains(W window) {
    return mergeTree.containsKey(window);
  }

  @Override
  protected Iterable<V> finalValue(W window) throws Exception {
    if (!contains(window)) {
      throw new IllegalStateException("finalValue called for non-existent window");
    }

    List<V> toEmit = new ArrayList<>();
    // This is the set of windows that we're currently emitting.
    Set<W> curWindows = new HashSet<>();
    curWindows.add(window);
    curWindows.addAll(mergeTree.get(window));

    // This is the set of unflushed windows (for preservation detection).
    Set<W> otherWindows = new HashSet<>();
    for (Map.Entry<W, Set<W>> entry : mergeTree.entrySet()) {
      if (!entry.getKey().equals(window)) {
        otherWindows.add(entry.getKey());
        otherWindows.addAll(entry.getValue());
      }
    }

    for (W curWindow : curWindows) {
      Iterable<V> items = context.context.stepContext.readTagList(bufferTag(
          curWindow, windowingFn.windowCoder(), inputCoder));
      for (V item : items) {
        toEmit.add(item);
      }
      context.context.stepContext.deleteTagList(bufferTag(
          curWindow, windowingFn.windowCoder(), inputCoder));
    }

    return toEmit;
  }

  @Override
  public void flush() throws Exception {
    if (!mergeTree.equals(originalMergeTree)) {
      context.context.stepContext.store(mergeTreeTag, mergeTree);
    }
  }

  private static <W> Map<W, Set<W>> emptyIfNull(Map<W, Set<W>> input) {
    if (input == null) {
      return new HashMap<>();
    } else {
      for (Map.Entry<W, Set<W>> entry : input.entrySet()) {
        if (entry.getValue() == null) {
          entry.setValue(new HashSet<W>());
        }
      }
      return input;
    }
  }

  private Map<W, Set<W>> deepCopy(Map<W, Set<W>> mergeTree) {
    Map<W, Set<W>> newMergeTree = new HashMap<>();
    for (Map.Entry<W, Set<W>> entry : mergeTree.entrySet()) {
      newMergeTree.put(entry.getKey(), new HashSet(entry.getValue()));
    }
    return newMergeTree;
  }
}
