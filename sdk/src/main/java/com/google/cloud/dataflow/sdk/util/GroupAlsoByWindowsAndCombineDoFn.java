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

import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Maps;

import org.joda.time.Instant;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * {@link GroupAlsoByWindowsDoFn} that uses combiner to accumulate input elements for non-merging
 * window functions with the default triggering strategy.
 *
 * @param <K> key type
 * @param <InputT> value input type
 * @param <AccumT> accumulator type
 * @param <OutputT> value output type
 * @param <W> window type
 */
@SuppressWarnings("serial")
public class GroupAlsoByWindowsAndCombineDoFn<K, InputT, AccumT, OutputT, W extends BoundedWindow>
    extends GroupAlsoByWindowsDoFn<K, InputT, OutputT, W> {

  private final KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn;

  public GroupAlsoByWindowsAndCombineDoFn(
      KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
    this.combineFn = combineFn;
  }

  @Override
  public void processElement(ProcessContext c) throws Exception {
    K key = c.element().getKey();
    Iterator<WindowedValue<InputT>> iterator = c.element().getValue().iterator();

    PriorityQueue<W> liveWindows =
        new PriorityQueue<>(11, new Comparator<BoundedWindow>() {
          @Override
          public int compare(BoundedWindow w1, BoundedWindow w2) {
            return Long.signum(w1.maxTimestamp().getMillis() - w2.maxTimestamp().getMillis());
          }
        });

    Map<W, AccumT> accumulators = Maps.newHashMap();
    Map<W, Instant> minTimestamps = Maps.newHashMap();

    while (iterator.hasNext()) {
      WindowedValue<InputT> e = iterator.next();

      @SuppressWarnings("unchecked")
      Collection<W> windows = (Collection<W>) e.getWindows();
      for (W w : windows) {
        Instant timestamp = minTimestamps.get(w);
        if (timestamp == null || timestamp.compareTo(e.getTimestamp()) > 0) {
          minTimestamps.put(w, e.getTimestamp());
        } else {
          minTimestamps.put(w, timestamp);
        }

        AccumT accum = accumulators.get(w);
        checkState((timestamp == null && accum == null) || (timestamp != null && accum != null));
        if (accum == null) {
          accum = combineFn.createAccumulator(key);
          liveWindows.add(w);
        }
        accum = combineFn.addInput(key, accum, e.getValue());
        accumulators.put(w, accum);
      }

      while (!liveWindows.isEmpty()
          && liveWindows.peek().maxTimestamp().isBefore(e.getTimestamp())) {
        closeWindow(key, liveWindows.poll(), accumulators, minTimestamps, c);
      }
    }

    while (!liveWindows.isEmpty()) {
      closeWindow(key, liveWindows.poll(), accumulators, minTimestamps, c);
    }
  }

  private void closeWindow(
      K key, W w, Map<W, AccumT> accumulators, Map<W, Instant> minTimestamps, ProcessContext c) {
    AccumT accum = accumulators.remove(w);
    Instant timestamp = minTimestamps.remove(w);
    checkState(accum != null && timestamp != null);
    c.windowingInternals().outputWindowedValue(
        KV.of(key, combineFn.extractOutput(key, accum)),
        timestamp,
        Arrays.asList(w),
        PaneInfo.DEFAULT_PANE);
  }
}
