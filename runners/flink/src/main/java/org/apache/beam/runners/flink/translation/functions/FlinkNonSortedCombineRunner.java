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

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.java.tuple.Tuple2;
import org.joda.time.Instant;

/**
 * A combine runner that use map to combine values while the {@link FlinkCombineRunner}
 * combine values after sorted.
 *
 * <p>Refer to {@link org.apache.beam.runners.core.ReduceFnRunner}.
 */
public class FlinkNonSortedCombineRunner<
    K, InputT, AccumT, OutputT, W extends BoundedWindow>
    extends FlinkCombineRunner<K, InputT, AccumT, OutputT, W> {

  private final WindowFn<Object, W> windowFn;

  public FlinkNonSortedCombineRunner(
      FlinkCombiner<K, InputT, AccumT, OutputT> flinkCombiner,
      WindowingStrategy<Object, W> windowingStrategy,
      SideInputReader sideInputReader,
      Output<K, OutputT> out,
      PipelineOptions options) {
    super(flinkCombiner, windowingStrategy, sideInputReader, out, options);
    windowFn = windowingStrategy.getWindowFn();
  }

  @Override
  public void combine(Iterable<WindowedValue<KV<K, InputT>>> elements) throws Exception {

    @SuppressWarnings("unchecked")
    OutputTimeFn<? super BoundedWindow> outputTimeFn =
        (OutputTimeFn<? super BoundedWindow>) windowingStrategy.getOutputTimeFn();

    // Flink Iterable can be iterated over only once.
    List<WindowedValue<KV<K, InputT>>> inputs = new ArrayList<>();
    Iterables.addAll(inputs, elements);

    Set<W> windows = collectWindows(inputs);
    Map<W, W> windowToMergeResult = mergeWindows(windows);

    // Combine all windowedValues into map
    Map<W, Tuple2<AccumT, Instant>> mapState = new HashMap<>();
    Iterator<WindowedValue<KV<K, InputT>>> iterator = inputs.iterator();
    WindowedValue<KV<K, InputT>> currentValue = iterator.next();
    K key = currentValue.getValue().getKey();
    do {
      for (BoundedWindow w : currentValue.getWindows()) {
        @SuppressWarnings("unchecked")
        W currentWindow = (W) w;
        W mergedWindow = windowToMergeResult.get(currentWindow);
        mergedWindow = mergedWindow == null ? currentWindow : mergedWindow;
        Set<W> singletonW = Collections.singleton(mergedWindow);
        Tuple2<AccumT, Instant> accumAndInstant = mapState.get(mergedWindow);
        if (accumAndInstant == null) {
          AccumT accumT = flinkCombiner.firstInput(key, currentValue.getValue().getValue(),
              options, sideInputReader, singletonW);
          Instant windowTimestamp =
              outputTimeFn.assignOutputTime(currentValue.getTimestamp(), mergedWindow);
          accumAndInstant = new Tuple2<>(accumT, windowTimestamp);
          mapState.put(mergedWindow, accumAndInstant);
        } else {
          accumAndInstant.f0 = flinkCombiner.addInput(key, accumAndInstant.f0,
              currentValue.getValue().getValue(), options, sideInputReader, singletonW);
          accumAndInstant.f1 = outputTimeFn.combine(accumAndInstant.f1,
              outputTimeFn.assignOutputTime(currentValue.getTimestamp(), mergedWindow));
        }
      }
      if (iterator.hasNext()) {
        currentValue = iterator.next();
      } else {
        break;
      }
    } while (true);

    // Output the final value of combiners
    for (Map.Entry<W, Tuple2<AccumT, Instant>> entry : mapState.entrySet()) {
      AccumT accumulator = entry.getValue().f0;
      Instant windowTimestamp = entry.getValue().f1;
      out.output(
          WindowedValue.of(
              KV.of(key, flinkCombiner.extractOutput(key, accumulator,
                  options, sideInputReader, Collections.singleton(entry.getKey()))),
              windowTimestamp,
              entry.getKey(),
              PaneInfo.NO_FIRING));
    }

  }

  private Map<W, W> mergeWindows(Set<W> windows) throws Exception {
    if (windowingStrategy.getWindowFn().isNonMerging()) {
      // Return an empty map, indicating that every window is not merged.
      return Collections.emptyMap();
    }

    Map<W, W> windowToMergeResult = new HashMap<>();
    windowFn.mergeWindows(new MergeContextImpl(windows, windowToMergeResult));
    return windowToMergeResult;
  }

  private class MergeContextImpl extends WindowFn<Object, W>.MergeContext {

    private Set<W> windows;
    private Map<W, W> windowToMergeResult;

    MergeContextImpl(Set<W> windows, Map<W, W> windowToMergeResult) {
      windowFn.super();
      this.windows = windows;
      this.windowToMergeResult = windowToMergeResult;
    }

    @Override
    public Collection<W> windows() {
      return windows;
    }

    @Override
    public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
      for (W w : toBeMerged) {
        windowToMergeResult.put(w, mergeResult);
      }
    }
  }

  private Set<W> collectWindows(Iterable<WindowedValue<KV<K, InputT>>> values) {
    Set<W> windows = new HashSet<>();
    for (WindowedValue<?> value : values) {
      for (BoundedWindow untypedWindow : value.getWindows()) {
        @SuppressWarnings("unchecked")
        W window = (W) untypedWindow;
        windows.add(window);
      }
    }
    return windows;
  }

}
