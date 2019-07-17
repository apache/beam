/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink.translation.functions;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

/**
 * A Flink combine runner that first sorts the elements by window and then does one pass that merges
 * windows and outputs results.
 */
public class SortingFlinkCombineRunner<K, InputT, AccumT, OutputT, W extends BoundedWindow>
    extends AbstractFlinkCombineRunner<K, InputT, AccumT, OutputT, W> {

  @Override
  public void combine(
      FlinkCombiner<K, InputT, AccumT, OutputT> flinkCombiner,
      WindowingStrategy<Object, W> windowingStrategy,
      SideInputReader sideInputReader,
      PipelineOptions options,
      Iterable<WindowedValue<KV<K, InputT>>> elements,
      Collector<WindowedValue<KV<K, OutputT>>> out)
      throws Exception {

    @SuppressWarnings("unchecked")
    TimestampCombiner timestampCombiner =
        (TimestampCombiner) windowingStrategy.getTimestampCombiner();
    WindowFn<Object, W> windowFn = windowingStrategy.getWindowFn();

    // get all elements so that we can sort them, has to fit into
    // memory
    // this seems very unprudent, but correct, for now
    List<WindowedValue<KV<K, InputT>>> sortedInput = Lists.newArrayList();
    for (WindowedValue<KV<K, InputT>> inputValue : elements) {
      for (WindowedValue<KV<K, InputT>> exploded : inputValue.explodeWindows()) {
        sortedInput.add(exploded);
      }
    }
    sortedInput.sort(
        Comparator.comparing(o -> Iterables.getOnlyElement(o.getWindows()).maxTimestamp()));

    if (!windowingStrategy.getWindowFn().isNonMerging()) {
      // merge windows, we have to do it in an extra pre-processing step and
      // can't do it as we go since the window of early elements would not
      // be correct when calling the CombineFn
      mergeWindow(sortedInput);
    }

    // iterate over the elements that are sorted by window timestamp
    final Iterator<WindowedValue<KV<K, InputT>>> iterator = sortedInput.iterator();

    // create accumulator using the first elements key
    WindowedValue<KV<K, InputT>> currentValue = iterator.next();
    K key = currentValue.getValue().getKey();
    W currentWindow = (W) Iterables.getOnlyElement(currentValue.getWindows());
    InputT firstValue = currentValue.getValue().getValue();
    AccumT accumulator =
        flinkCombiner.firstInput(
            key, firstValue, options, sideInputReader, currentValue.getWindows());

    // we use this to keep track of the timestamps assigned by the TimestampCombiner
    Instant windowTimestamp =
        timestampCombiner.assign(
            currentWindow, windowFn.getOutputTime(currentValue.getTimestamp(), currentWindow));

    while (iterator.hasNext()) {
      WindowedValue<KV<K, InputT>> nextValue = iterator.next();
      W nextWindow = (W) Iterables.getOnlyElement(nextValue.getWindows());

      if (currentWindow.equals(nextWindow)) {
        // continue accumulating and merge windows

        InputT value = nextValue.getValue().getValue();
        accumulator =
            flinkCombiner.addInput(
                key, accumulator, value, options, sideInputReader, currentValue.getWindows());

        windowTimestamp =
            timestampCombiner.combine(
                windowTimestamp,
                timestampCombiner.assign(
                    currentWindow,
                    windowFn.getOutputTime(nextValue.getTimestamp(), currentWindow)));

      } else {
        // emit the value that we currently have
        out.collect(
            WindowedValue.of(
                KV.of(
                    key,
                    flinkCombiner.extractOutput(
                        key, accumulator, options, sideInputReader, currentValue.getWindows())),
                windowTimestamp,
                currentWindow,
                PaneInfo.NO_FIRING));

        currentWindow = nextWindow;
        currentValue = nextValue;
        InputT value = nextValue.getValue().getValue();
        accumulator =
            flinkCombiner.firstInput(
                key, value, options, sideInputReader, currentValue.getWindows());
        windowTimestamp =
            timestampCombiner.assign(
                currentWindow, windowFn.getOutputTime(nextValue.getTimestamp(), currentWindow));
      }
    }

    // emit the final accumulator
    out.collect(
        WindowedValue.of(
            KV.of(
                key,
                flinkCombiner.extractOutput(
                    key, accumulator, options, sideInputReader, currentValue.getWindows())),
            windowTimestamp,
            currentWindow,
            PaneInfo.NO_FIRING));
  }

  /**
   * Merge windows. This assumes that the list of elements is sorted by window-end timestamp. This
   * replaces windows in the input list.
   */
  private void mergeWindow(List<WindowedValue<KV<K, InputT>>> elements) {
    int currentStart = 0;
    IntervalWindow currentWindow =
        (IntervalWindow) Iterables.getOnlyElement(elements.get(0).getWindows());

    for (int i = 1; i < elements.size(); i++) {
      WindowedValue<KV<K, InputT>> nextValue = elements.get(i);
      IntervalWindow nextWindow = (IntervalWindow) Iterables.getOnlyElement(nextValue.getWindows());
      if (currentWindow.intersects(nextWindow)) {
        // we continue
        currentWindow = currentWindow.span(nextWindow);
      } else {
        // retrofit the merged window to all windows up to "currentStart"
        for (int j = i - 1; j >= currentStart; j--) {
          WindowedValue<KV<K, InputT>> value = elements.get(j);
          elements.set(
              j,
              WindowedValue.of(
                  value.getValue(), value.getTimestamp(), currentWindow, value.getPane()));
        }
        currentStart = i;
        currentWindow = nextWindow;
      }
    }
    if (currentStart < elements.size() - 1) {
      // we have to retrofit the last batch
      for (int j = elements.size() - 1; j >= currentStart; j--) {
        WindowedValue<KV<K, InputT>> value = elements.get(j);
        elements.set(
            j,
            WindowedValue.of(
                value.getValue(), value.getTimestamp(), currentWindow, value.getPane()));
      }
    }
  }
}
