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

import static com.google.cloud.dataflow.sdk.util.WindowUtils.bufferTag;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.google.cloud.dataflow.sdk.coders.SetCoder;
import com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.WindowStatus;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;

import java.io.IOException;
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

  public static <K, V, W extends BoundedWindow>
  AbstractWindowSet.Factory<K, V, Iterable<V>, W> factory(final Coder<V> inputCoder) {
    return new AbstractWindowSet.Factory<K, V, Iterable<V>, W>() {

      private static final long serialVersionUID = 0L;

      @Override
      public AbstractWindowSet<K, V, Iterable<V>, W> create(K key,
          Coder<W> windowCoder, KeyedState keyedState,
          WindowingInternals<?, ?> windowingInternals) throws Exception {
        return new BufferingWindowSet<>(
            key, windowCoder, inputCoder, keyedState, windowingInternals);
      }
    };
  }

  /**
   * Tag for storing the merge tree, the data structure that keeps
   * track of which windows have been merged together.
   */
  private final CodedTupleTag<Map<W, Set<W>>> mergeTreeTag =
      CodedTupleTag.of("mergeTree", MapCoder.of(windowCoder, SetCoder.of(windowCoder)));

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
      Coder<W> windowCoder,
      Coder<V> inputCoder,
      KeyedState keyedState,
      WindowingInternals<?, ?> windowingInternals) throws Exception {
    super(key, windowCoder, inputCoder, keyedState, windowingInternals);

    mergeTree = emptyIfNull(
        keyedState.lookup(Arrays.asList(mergeTreeTag))
        .get(mergeTreeTag));

    originalMergeTree = deepCopy(mergeTree);
  }

  @Override
  public WindowStatus put(W window, V value) throws Exception {
    windowingInternals.writeToTagList(bufferTag(window, windowCoder, inputCoder), value);

    if (!mergeTree.containsKey(window)) {
      mergeTree.put(window, new HashSet<W>());
      return WindowStatus.NEW;
    } else {
      return WindowStatus.EXISTING;
    }
  }

  @Override
  public void remove(W window) throws Exception {
    Set<W> subWindows = mergeTree.get(window);
    if (subWindows == null) {
      return;
    }

    for (W w : subWindows) {
      windowingInternals.deleteTagList(bufferTag(w, windowCoder, inputCoder));
    }
    windowingInternals.deleteTagList(bufferTag(window, windowCoder, inputCoder));
    mergeTree.remove(window);
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
      mergeTree.remove(other);
    }
    mergeTree.put(newWindow, subWindows);
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
      return null;
    }

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

    List<CodedTupleTag<V>> bufferTags = FluentIterable.from(curWindows)
        .transform(new Function<W, CodedTupleTag<V>>() {
          @Override
          public CodedTupleTag<V> apply(W input) {
            try {
              return bufferTag(input, windowCoder, inputCoder);
            } catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }
        })
        .toList();

    // The FluentIterable#toList creates a copy, so the result will never be mutated.
    List<V> toEmit = FluentIterable
        .from(windowingInternals.readTagList(bufferTags).values())
        .transformAndConcat(Functions.<Iterable<V>>identity())
        .toList();
    return toEmit;
  }

  @Override
  public void persist() throws Exception {
    if (!mergeTree.equals(originalMergeTree)) {
      keyedState.store(mergeTreeTag, mergeTree);
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
      newMergeTree.put(entry.getKey(), new HashSet<W>(entry.getValue()));
    }
    return newMergeTree;
  }
}
