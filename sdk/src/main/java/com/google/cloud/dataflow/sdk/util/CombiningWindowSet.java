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

import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Iterators;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A WindowSet for combine accumulators.
 * It merges accumulators when windows are added or merged.
 *
 * @param <K> key tyep
 * @param <VA> accumulator type
 * @param <W> window type
 */
public class CombiningWindowSet<K, VA, W extends BoundedWindow>
    extends AbstractWindowSet<K, VA, VA, W> {

  private final CodedTupleTag<Iterable<W>> windowListTag =
      CodedTupleTag.of("liveWindowsList", IterableCoder.of(windowFn.windowCoder()));

  private final KeyedCombineFn<K, ?, VA, ?> combineFn;
  private final Set<W> liveWindows;
  private boolean liveWindowsModified;

  protected CombiningWindowSet(
      K key,
      WindowFn<?, W> windowFn,
      KeyedCombineFn<K, ?, VA, ?> combineFn,
      Coder<VA> inputCoder,
      DoFnProcessContext<?, KV<K, VA>> context,
      ActiveWindowManager<W> activeWindowManager) throws Exception {
    super(key, windowFn, inputCoder, context, activeWindowManager);
    this.combineFn = combineFn;
    liveWindows = new HashSet<W>();
    Iterators.addAll(liveWindows,
                     emptyIfNull(context.keyedState().lookup(windowListTag)).iterator());
    liveWindowsModified = false;
  }

  @Override
  protected Collection<W> windows() {
    return Collections.unmodifiableSet(liveWindows);
  }

  @Override
  protected VA finalValue(W window) throws Exception {
    return context.keyedState().lookup(
        bufferTag(window, windowFn.windowCoder(), inputCoder));
  }

  @Override
  protected void put(W window, VA value) throws Exception {
    CodedTupleTag<VA> tag = bufferTag(window, windowFn.windowCoder(), inputCoder);
    VA va = context.keyedState().lookup(tag);
    VA newValue;
    if (va == null) {
      newValue = value;
    } else {
      newValue = combineFn.mergeAccumulators(key, Arrays.asList(value, va));
    }
    context.keyedState().store(tag, newValue);
    activeWindowManager.addWindow(window);
    liveWindowsModified = liveWindows.add(window);
  }

  @Override
  protected void remove(W window) throws Exception {
    context.keyedState().remove(bufferTag(window, windowFn.windowCoder(), inputCoder));
    activeWindowManager.addWindow(window);
    liveWindowsModified = liveWindows.remove(window);
  }

  @Override
  protected void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
    List<VA> accumulators = Lists.newArrayList();
    for (W w : toBeMerged) {
      VA va = context.keyedState().lookup(
          bufferTag(w, windowFn.windowCoder(), inputCoder));
      // TODO: determine whether null means no value associated with the tag, b/19201776.
      if (va != null) {
        accumulators.add(va);
      }
      remove(w);
    }
    VA mergedVa = combineFn.mergeAccumulators(key, accumulators);
    put(mergeResult, mergedVa);
  }

  @Override
  protected boolean contains(W window) {
    return liveWindows.contains(window);
  }

  private static <T> Iterable<T> emptyIfNull(Iterable<T> list) {
    if (list == null) {
      return Collections.emptyList();
    } else {
      return list;
    }
  }

  @Override
  protected void flush() throws Exception {
    if (liveWindowsModified) {
      context.keyedState().store(windowListTag, liveWindows);
      liveWindowsModified = false;
    }
  }
}
