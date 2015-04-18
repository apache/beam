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
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.WindowStatus;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A WindowSet for combine accumulators.
 * It merges accumulators when windows are added or merged.
 *
 * @param <K> key type
 * @param <VI> value input type
 * @param <VA> accumulator type
 * @param <VO> value output type
 * @param <W> window type
 */
public class CombiningWindowSet<K, VI, VA, VO, W extends BoundedWindow>
    extends AbstractWindowSet<K, VI, VO, W> {

  public static <K, VI, VA, VO, W extends BoundedWindow>
  AbstractWindowSet.Factory<K, VI, VO, W> factory(
      final KeyedCombineFn<K, VI, VA, VO> combineFn,
      final Coder<K> keyCoder, final Coder<VI> inputCoder) {
    return new AbstractWindowSet.Factory<K, VI, VO, W>() {

      private static final long serialVersionUID = 0L;

      @Override
      public AbstractWindowSet<K, VI, VO, W> create(K key,
          Coder<W> windowCoder, KeyedState keyedState,
          WindowingInternals<?, ?> windowingInternals) throws Exception {
        return new CombiningWindowSet<>(
            key, windowCoder, combineFn, keyCoder, inputCoder, keyedState, windowingInternals);
      }
    };
  }

  private final CodedTupleTag<Iterable<W>> windowListTag =
      CodedTupleTag.of("liveWindowsList", IterableCoder.of(windowCoder));

  private final KeyedCombineFn<K, VI, VA, VO> combineFn;
  private final Set<W> liveWindows;
  private final Coder<VA> accumulatorCoder;
  private boolean liveWindowsModified;

  protected CombiningWindowSet(
      K key,
      Coder<W> windowCoder,
      KeyedCombineFn<K, VI, VA, VO> combineFn,
      Coder<K> keyCoder,
      Coder<VI> inputValueCoder,
      KeyedState keyedState,
      WindowingInternals<?, ?> windowingInternals) throws Exception {
    super(key, windowCoder, inputValueCoder, keyedState, windowingInternals);
    this.combineFn = combineFn;
    liveWindows = new HashSet<W>();
    Iterators.addAll(liveWindows,
                     emptyIfNull(keyedState.lookup(windowListTag)).iterator());
    liveWindowsModified = false;
    // TODO: Use the pipeline's registry once the TODO in GroupByKey is resolved.
    CoderRegistry coderRegistry = new CoderRegistry();
    coderRegistry.registerStandardCoders();
    accumulatorCoder = combineFn.getAccumulatorCoder(coderRegistry, keyCoder, inputValueCoder);
  }

  @Override
  protected Collection<W> windows() {
    return Collections.unmodifiableSet(liveWindows);
  }

  @Override
  protected VO finalValue(W window) throws Exception {
    VA accumulator = lookupAccumulator(window);
    if (accumulator == null) {
      return null;
    }

    return combineFn.extractOutput(key, accumulator);
  }

  @Override
  protected WindowStatus put(W window, VI value) throws Exception {
    VA accumulator = lookupAccumulator(window);
    if (accumulator == null) {
      storeAccumulator(window, combineFn.addInput(key, combineFn.createAccumulator(key), value));
      return WindowStatus.NEW;
    } else {
      storeAccumulator(window, combineFn.addInput(key, accumulator, value));
      return WindowStatus.EXISTING;
    }
  }

  @Override
  protected void remove(W window) throws Exception {
    if (contains(window)) {
      keyedState.remove(accumulatorTag(window));
      liveWindowsModified = liveWindows.remove(window);
    }
  }

  @Override
  protected void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
    List<VA> accumulators = Lists.newArrayList();
    for (W window : toBeMerged) {
      VA accumulator = Preconditions.checkNotNull(lookupAccumulator(window));
      accumulators.add(accumulator);
      remove(window);
    }
    VA mergedAccumulator = combineFn.mergeAccumulators(key, accumulators);
    storeAccumulator(mergeResult, mergedAccumulator);
  }

  private CodedTupleTag<VA> accumulatorTag(W window) throws Exception {
    // TODO: Cache this.
    return bufferTag(window, windowCoder, accumulatorCoder);
  }

  private void storeAccumulator(W window, VA accumulator) throws Exception {
    keyedState.store(accumulatorTag(window), accumulator);
    liveWindowsModified = liveWindows.add(window);
  }

  private VA lookupAccumulator(W window) throws Exception {
    return keyedState.lookup(accumulatorTag(window));
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
  protected void persist() throws Exception {
    if (liveWindowsModified) {
      keyedState.store(windowListTag, liveWindows);
      liveWindowsModified = false;
    }
  }
}
