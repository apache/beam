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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.PerKeyCombineFnRunner;
import org.apache.beam.runners.core.PerKeyCombineFnRunners;
import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

/**
 * This is the second part for executing a {@link org.apache.beam.sdk.transforms.Combine.PerKey}
 * on Flink, the second part is {@link FlinkReduceFunction}. This function performs the final
 * combination of the pre-combined values after a shuffle.
 *
 * <p>The input to {@link #reduce(Iterable, Collector)} are elements of the same key but
 * for different windows. We have to ensure that we only combine elements of matching
 * windows.
 */
public class FlinkReduceFunction<K, AccumT, OutputT, W extends BoundedWindow>
    extends RichGroupReduceFunction<WindowedValue<KV<K, AccumT>>, WindowedValue<KV<K, OutputT>>> {

  protected final CombineFnBase.PerKeyCombineFn<K, ?, AccumT, OutputT> combineFn;

  protected final WindowingStrategy<?, W> windowingStrategy;

  protected final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;

  protected final SerializedPipelineOptions serializedOptions;

  public FlinkReduceFunction(
      CombineFnBase.PerKeyCombineFn<K, ?, AccumT, OutputT> keyedCombineFn,
      WindowingStrategy<?, W> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions pipelineOptions) {

    this.combineFn = keyedCombineFn;

    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;

    this.serializedOptions = new SerializedPipelineOptions(pipelineOptions);

  }

  @Override
  public void reduce(
      Iterable<WindowedValue<KV<K, AccumT>>> elements,
      Collector<WindowedValue<KV<K, OutputT>>> out) throws Exception {

    PipelineOptions options = serializedOptions.getPipelineOptions();

    FlinkSideInputReader sideInputReader =
        new FlinkSideInputReader(sideInputs, getRuntimeContext());

    PerKeyCombineFnRunner<K, ?, AccumT, OutputT> combineFnRunner =
        PerKeyCombineFnRunners.create(combineFn);

    @SuppressWarnings("unchecked")
    OutputTimeFn<? super BoundedWindow> outputTimeFn =
        (OutputTimeFn<? super BoundedWindow>) windowingStrategy.getOutputTimeFn();


    // get all elements so that we can sort them, has to fit into
    // memory
    // this seems very unprudent, but correct, for now
    ArrayList<WindowedValue<KV<K, AccumT>>> sortedInput = Lists.newArrayList();
    for (WindowedValue<KV<K, AccumT>> inputValue: elements) {
      for (WindowedValue<KV<K, AccumT>> exploded: inputValue.explodeWindows()) {
        sortedInput.add(exploded);
      }
    }
    Collections.sort(sortedInput, new Comparator<WindowedValue<KV<K, AccumT>>>() {
      @Override
      public int compare(
          WindowedValue<KV<K, AccumT>> o1,
          WindowedValue<KV<K, AccumT>> o2) {
        return Iterables.getOnlyElement(o1.getWindows()).maxTimestamp()
            .compareTo(Iterables.getOnlyElement(o2.getWindows()).maxTimestamp());
      }
    });

    // iterate over the elements that are sorted by window timestamp
    //
    final Iterator<WindowedValue<KV<K, AccumT>>> iterator = sortedInput.iterator();

    // get the first accumulator
    WindowedValue<KV<K, AccumT>> currentValue = iterator.next();
    K key = currentValue.getValue().getKey();
    BoundedWindow currentWindow = Iterables.getFirst(currentValue.getWindows(), null);
    AccumT accumulator = currentValue.getValue().getValue();

    // we use this to keep track of the timestamps assigned by the OutputTimeFn,
    // in FlinkPartialReduceFunction we already merge the timestamps assigned
    // to individual elements, here we just merge them
    List<Instant> windowTimestamps = new ArrayList<>();
    windowTimestamps.add(currentValue.getTimestamp());

    while (iterator.hasNext()) {
      WindowedValue<KV<K, AccumT>> nextValue = iterator.next();
      BoundedWindow nextWindow = Iterables.getOnlyElement(nextValue.getWindows());

      if (nextWindow.equals(currentWindow)) {
        // continue accumulating
        accumulator = combineFnRunner.mergeAccumulators(
            key, ImmutableList.of(accumulator, nextValue.getValue().getValue()),
            options, sideInputReader, currentValue.getWindows());

        windowTimestamps.add(nextValue.getTimestamp());
      } else {
        // emit the value that we currently have
        out.collect(
            WindowedValue.of(
                KV.of(key, combineFnRunner.extractOutput(key, accumulator,
                    options, sideInputReader, currentValue.getWindows())),
                outputTimeFn.merge(currentWindow, windowTimestamps),
                currentWindow,
                PaneInfo.NO_FIRING));

        windowTimestamps.clear();

        currentWindow = nextWindow;
        currentValue = nextValue;
        accumulator = nextValue.getValue().getValue();
        windowTimestamps.add(nextValue.getTimestamp());
      }

    }

    // emit the final accumulator
    out.collect(
        WindowedValue.of(
            KV.of(key, combineFnRunner.extractOutput(key, accumulator,
                options, sideInputReader, currentValue.getWindows())),
            outputTimeFn.merge(currentWindow, windowTimestamps),
            currentWindow,
            PaneInfo.NO_FIRING));
  }
}
