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

import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.google.cloud.dataflow.sdk.coders.SetCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.ValueState;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link ActiveWindowSet} used with {@link WindowFn WindowFns} that support
 * merging.
 *
 * @param <W> the types of windows being managed
 */
public class MergingActiveWindowSet<W extends BoundedWindow>
    implements ActiveWindowSet<W> {

  private final WindowFn<Object, W> windowFn;

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

  private final ValueState<Map<W, Set<W>>> mergeTreeValue;

  public MergingActiveWindowSet(WindowFn<Object, W> windowFn, StateInternals state) {
    this.windowFn = windowFn;

    StateTag<ValueState<Map<W, Set<W>>>> mergeTreeAddr = StateTags.makeSystemTagInternal(
        StateTags.value("tree",
            MapCoder.of(windowFn.windowCoder(), SetCoder.of(windowFn.windowCoder()))));
    this.mergeTreeValue = state.state(StateNamespaces.global(), mergeTreeAddr);
    this.mergeTree = emptyIfNull(mergeTreeValue.get().read());

    originalMergeTree = deepCopy(mergeTree);
  }

  @Override
  public void persist() {
    if (!mergeTree.equals(originalMergeTree)) {
      mergeTreeValue.set(mergeTree);
    }
  }

  @Override
  public boolean add(W window) {
    if (mergeTree.containsKey(window)) {
      return false;
    }

    mergeTree.put(window, new HashSet<W>());
    return true;
  }

  @Override
  public void remove(W window) {
    mergeTree.remove(window);
  }

  private class MergeContextImpl extends WindowFn<Object, W>.MergeContext {

    private MergeCallback<W> mergeCallback;

    public MergeContextImpl(MergeCallback<W> mergeCallback) {
      windowFn.super();
      this.mergeCallback = mergeCallback;
    }

    @Override
    public Collection<W> windows() {
      return mergeTree.keySet();
    }

    @Override
    public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
      boolean isResultNew = !mergeTree.containsKey(mergeResult);
      recordMerge(toBeMerged, mergeResult);
      mergeCallback.onMerge(toBeMerged, mergeResult, isResultNew);
    }
  }

  @Override
  public boolean mergeIfAppropriate(W window, MergeCallback<W> mergeCallback) throws Exception {
    windowFn.mergeWindows(new MergeContextImpl(mergeCallback));
    return window == null || mergeTree.containsKey(window);
  }

  @Override
  public Iterable<W> sourceWindows(W window) {
    Set<W> curWindows = new HashSet<>();
    curWindows.add(window);

    Set<W> sourceWindows = mergeTree.get(window);
    if (sourceWindows != null) {
      curWindows.addAll(sourceWindows);
    }
    return curWindows;
  }

  private void recordMerge(Collection<W> otherWindows, W newWindow) throws Exception {
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
      mergeTree.remove(other);
    }
    mergeTree.put(newWindow, subWindows);
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
      newMergeTree.put(entry.getKey(), new HashSet<W>(entry.getValue()));
    }
    return newMergeTree;
  }
}
