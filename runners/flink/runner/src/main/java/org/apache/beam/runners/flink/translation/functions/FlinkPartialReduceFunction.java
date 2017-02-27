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
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
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
import org.apache.flink.api.common.functions.RichGroupCombineFunction;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

/**
 * This is is the first step for executing a {@link org.apache.beam.sdk.transforms.Combine.PerKey}
 * on Flink. The second part is {@link FlinkReduceFunction}. This function performs a local
 * combine step before shuffling while the latter does the final combination after a shuffle.
 *
 * <p>The input to {@link #combine(Iterable, Collector)} are elements of the same key but
 * for different windows. We have to ensure that we only combine elements of matching
 * windows.
 */
public class FlinkPartialReduceFunction<K, InputT, AccumT, W extends BoundedWindow>
    extends RichGroupCombineFunction<WindowedValue<KV<K, InputT>>, WindowedValue<KV<K, AccumT>>> {

  protected final CombineFnBase.PerKeyCombineFn<K, InputT, AccumT, ?> combineFn;

  protected final WindowingStrategy<?, W> windowingStrategy;

  protected final SerializedPipelineOptions serializedOptions;

  protected final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;

  public FlinkPartialReduceFunction(
      CombineFnBase.PerKeyCombineFn<K, InputT, AccumT, ?> combineFn,
      WindowingStrategy<?, W> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions pipelineOptions) {

    this.combineFn = combineFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializedPipelineOptions(pipelineOptions);

  }

  @Override
  public void combine(
      Iterable<WindowedValue<KV<K, InputT>>> elements,
      Collector<WindowedValue<KV<K, AccumT>>> out) throws Exception {

    PipelineOptions options = serializedOptions.getPipelineOptions();

    FlinkSideInputReader sideInputReader =
        new FlinkSideInputReader(sideInputs, getRuntimeContext());

    PerKeyCombineFnRunner<K, InputT, AccumT, ?> combineFnRunner =
        PerKeyCombineFnRunners.create(combineFn);

    @SuppressWarnings("unchecked")
    OutputTimeFn<? super BoundedWindow> outputTimeFn =
        (OutputTimeFn<? super BoundedWindow>) windowingStrategy.getOutputTimeFn();

    // get all elements so that we can sort them, has to fit into
    // memory
    // this seems very unprudent, but correct, for now
    ArrayList<WindowedValue<KV<K, InputT>>> sortedInput = Lists.newArrayList();
    for (WindowedValue<KV<K, InputT>> inputValue : elements) {
      for (WindowedValue<KV<K, InputT>> exploded : inputValue.explodeWindows()) {
        sortedInput.add(exploded);
      }
    }
    Collections.sort(sortedInput, new Comparator<WindowedValue<KV<K, InputT>>>() {
      @Override
      public int compare(
          WindowedValue<KV<K, InputT>> o1,
          WindowedValue<KV<K, InputT>> o2) {
        return Iterables.getOnlyElement(o1.getWindows()).maxTimestamp()
            .compareTo(Iterables.getOnlyElement(o2.getWindows()).maxTimestamp());
      }
    });

    // iterate over the elements that are sorted by window timestamp
    //
    final Iterator<WindowedValue<KV<K, InputT>>> iterator = sortedInput.iterator();

    // create accumulator using the first elements key
    WindowedValue<KV<K, InputT>> currentValue = iterator.next();
    K key = currentValue.getValue().getKey();
    BoundedWindow currentWindow = Iterables.getFirst(currentValue.getWindows(), null);
    InputT firstValue = currentValue.getValue().getValue();
    AccumT accumulator = combineFnRunner.createAccumulator(key,
        options, sideInputReader, currentValue.getWindows());
    accumulator = combineFnRunner.addInput(key, accumulator, firstValue,
        options, sideInputReader, currentValue.getWindows());

    // we use this to keep track of the timestamps assigned by the OutputTimeFn
    Instant windowTimestamp =
        outputTimeFn.assignOutputTime(currentValue.getTimestamp(), currentWindow);

    while (iterator.hasNext()) {
      WindowedValue<KV<K, InputT>> nextValue = iterator.next();
      BoundedWindow nextWindow = Iterables.getOnlyElement(nextValue.getWindows());

      if (nextWindow.equals(currentWindow)) {
        // continue accumulating
        InputT value = nextValue.getValue().getValue();
        accumulator = combineFnRunner.addInput(key, accumulator, value,
            options, sideInputReader, currentValue.getWindows());

        windowTimestamp = outputTimeFn.combine(
            windowTimestamp,
            outputTimeFn.assignOutputTime(nextValue.getTimestamp(), currentWindow));

      } else {
        // emit the value that we currently have
        out.collect(
            WindowedValue.of(
                KV.of(key, accumulator),
                windowTimestamp,
                currentWindow,
                PaneInfo.NO_FIRING));

        currentWindow = nextWindow;
        currentValue = nextValue;
        InputT value = nextValue.getValue().getValue();
        accumulator = combineFnRunner.createAccumulator(key,
            options, sideInputReader, currentValue.getWindows());
        accumulator = combineFnRunner.addInput(key, accumulator, value,
            options, sideInputReader, currentValue.getWindows());
        windowTimestamp = outputTimeFn.assignOutputTime(nextValue.getTimestamp(), currentWindow);
      }
    }

    // emit the final accumulator
    out.collect(
        WindowedValue.of(
            KV.of(key, accumulator),
            windowTimestamp,
            currentWindow,
            PaneInfo.NO_FIRING));
  }
}
