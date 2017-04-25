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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.runners.core.PerKeyCombineFnRunner;
import org.apache.beam.runners.core.PerKeyCombineFnRunners;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

/**
 * A CombineRunner that execute a {@link org.apache.beam.sdk.transforms.Combine.PerKey}
 * on Flink. It unify the logical of merging/non-merging and partial/reduce combine.
 *
 * <p>The input to {@link #combine(Iterable)} are elements of the same key but
 * for different windows.
 */
public class FlinkCombineRunner<K, InputT, AccumT, OutputT, W extends BoundedWindow> {

  protected FlinkCombiner<K, InputT, AccumT, OutputT> flinkCombiner;
  protected final WindowingStrategy<Object, W> windowingStrategy;
  protected SideInputReader sideInputReader;
  protected Output<K, OutputT> out;
  protected PipelineOptions options;

  public FlinkCombineRunner(
      FlinkCombiner<K, InputT, AccumT, OutputT> flinkCombiner,
      WindowingStrategy<Object, W> windowingStrategy,
      SideInputReader sideInputReader,
      Output<K, OutputT> out,
      PipelineOptions options) {
    this.flinkCombiner = flinkCombiner;
    this.windowingStrategy = windowingStrategy;
    this.sideInputReader = sideInputReader;
    this.out = out;
    this.options = options;
  }

  public void combine(Iterable<WindowedValue<KV<K, InputT>>> elements) throws Exception {

    @SuppressWarnings("unchecked")
    OutputTimeFn<? super BoundedWindow> outputTimeFn =
        (OutputTimeFn<? super BoundedWindow>) windowingStrategy.getOutputTimeFn();

    // get all elements so that we can sort them, has to fit into
    // memory
    // this seems very unprudent, but correct, for now
    List<WindowedValue<KV<K, InputT>>> sortedInput = Lists.newArrayList();
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
    BoundedWindow currentWindow = Iterables.getOnlyElement(currentValue.getWindows());
    InputT firstValue = currentValue.getValue().getValue();
    AccumT accumulator = flinkCombiner.firstInput(
        key, firstValue, options, sideInputReader, currentValue.getWindows());

    // we use this to keep track of the timestamps assigned by the OutputTimeFn
    Instant windowTimestamp =
        outputTimeFn.assignOutputTime(currentValue.getTimestamp(), currentWindow);

    while (iterator.hasNext()) {
      WindowedValue<KV<K, InputT>> nextValue = iterator.next();
      BoundedWindow nextWindow = Iterables.getOnlyElement(nextValue.getWindows());

      if (currentWindow.equals(nextWindow)) {
        // continue accumulating and merge windows

        InputT value = nextValue.getValue().getValue();
        accumulator = flinkCombiner.addInput(key, accumulator, value,
            options, sideInputReader, currentValue.getWindows());

        windowTimestamp = outputTimeFn.combine(
            windowTimestamp,
            outputTimeFn.assignOutputTime(nextValue.getTimestamp(), currentWindow));

      } else {
        // emit the value that we currently have
        out.output(
            WindowedValue.of(
                KV.of(key, flinkCombiner.extractOutput(key, accumulator,
                    options, sideInputReader, currentValue.getWindows())),
                windowTimestamp,
                currentWindow,
                PaneInfo.NO_FIRING));

        currentWindow = nextWindow;
        currentValue = nextValue;
        InputT value = nextValue.getValue().getValue();
        accumulator = flinkCombiner.firstInput(key, value,
            options, sideInputReader, currentValue.getWindows());
        windowTimestamp = outputTimeFn.assignOutputTime(nextValue.getTimestamp(), currentWindow);
      }

    }

    // emit the final accumulator
    out.output(
        WindowedValue.of(
            KV.of(key, flinkCombiner.extractOutput(key, accumulator,
                options, sideInputReader, currentValue.getWindows())),
            windowTimestamp,
            currentWindow,
            PaneInfo.NO_FIRING));
  }

  /**
   * Merge windows. This assumes that the list of elements is sorted by window-end timestamp.
   * This replaces windows in the input list.
   */
  private void mergeWindow(List<WindowedValue<KV<K, InputT>>> elements) {
    int currentStart = 0;
    IntervalWindow currentWindow =
        (IntervalWindow) Iterables.getOnlyElement(elements.get(0).getWindows());

    for (int i = 1; i < elements.size(); i++) {
      WindowedValue<KV<K, InputT>> nextValue = elements.get(i);
      IntervalWindow nextWindow =
          (IntervalWindow) Iterables.getOnlyElement(nextValue.getWindows());
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

  /**
   * A combiner that decorator the {@link CombineFnBase.PerKeyCombineFn} for
   * unify partial and reduce funtion.
   */
  public interface FlinkCombiner<K, InputT, AccumT, OutputT>{

    AccumT firstInput(K key, InputT value, PipelineOptions options,
                    SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

    AccumT addInput(K key, AccumT accumulator, InputT value, PipelineOptions options,
                    SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

    OutputT extractOutput(K key, AccumT accumulator, PipelineOptions options,
                          SideInputReader sideInputReader,
                          Collection<? extends BoundedWindow> windows);
  }

  /**
   * A non-pre-shuffle combiner.
   */
  public static class NoShuffleFlinkCombiner<K, InputT, AccumT, OutputT> implements
      FlinkCombiner<K, InputT, AccumT, OutputT> {

    private final PerKeyCombineFnRunner<K, InputT, AccumT, OutputT> combineFnRunner;

    public NoShuffleFlinkCombiner(
        CombineFnBase.PerKeyCombineFn<K, InputT, AccumT, OutputT> combineFn) {
      combineFnRunner = PerKeyCombineFnRunners.create(combineFn);
    }

    @Override
    public AccumT firstInput(
        K key, InputT value, PipelineOptions options, SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      AccumT accumulator =
          combineFnRunner.createAccumulator(key, options, sideInputReader, windows);
      return combineFnRunner.addInput(key, accumulator, value, options, sideInputReader, windows);
    }

    @Override
    public AccumT addInput(
        K key, AccumT accumulator, InputT value, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return combineFnRunner.addInput(key, accumulator, value, options, sideInputReader, windows);
    }

    @Override
    public OutputT extractOutput(
        K key, AccumT accumulator, PipelineOptions options, SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnRunner.extractOutput(key, accumulator, options, sideInputReader, windows);
    }
  }

  /**
   * A pre-combine combiner.
   */
  public static class PartialFlinkCombiner<K, InputT, AccumT> implements
      FlinkCombiner<K, InputT, AccumT, AccumT> {

    private final PerKeyCombineFnRunner<K, InputT, AccumT, ?> combineFnRunner;

    public PartialFlinkCombiner(CombineFnBase.PerKeyCombineFn<K, InputT, AccumT, ?> combineFn) {
      combineFnRunner = PerKeyCombineFnRunners.create(combineFn);
    }

    @Override
    public AccumT firstInput(
        K key, InputT value, PipelineOptions options, SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      AccumT accumulator =
          combineFnRunner.createAccumulator(key, options, sideInputReader, windows);
      return combineFnRunner.addInput(key, accumulator, value, options, sideInputReader, windows);
    }

    @Override
    public AccumT addInput(
        K key, AccumT accumulator, InputT value, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return combineFnRunner.addInput(key, accumulator, value, options, sideInputReader, windows);
    }

    @Override
    public AccumT extractOutput(
        K key, AccumT accumulator, PipelineOptions options, SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return accumulator;
    }
  }

  /**
   * A final combine combiner.
   */
  public static class ReduceFlinkCombiner<K, AccumT, OutputT> implements
      FlinkCombiner<K, AccumT, AccumT, OutputT> {

    private final PerKeyCombineFnRunner<K, ?, AccumT, OutputT> combineFnRunner;

    public ReduceFlinkCombiner(CombineFnBase.PerKeyCombineFn<K, ?, AccumT, OutputT> combineFn) {
      combineFnRunner = PerKeyCombineFnRunners.create(combineFn);
    }

    @Override
    public AccumT firstInput(
        K key, AccumT value, PipelineOptions options, SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return value;
    }

    @Override
    public AccumT addInput(
        K key, AccumT accumulator, AccumT value, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return combineFnRunner.mergeAccumulators(
          key, ImmutableList.of(accumulator, value), options, sideInputReader, windows);
    }

    @Override
    public OutputT extractOutput(
        K key, AccumT accumulator, PipelineOptions options, SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnRunner.extractOutput(key, accumulator, options, sideInputReader, windows);
    }
  }

  /**
   * Emit the value.
   */
  public interface Output<K, OutputT> {
    void output(WindowedValue<KV<K, OutputT>> output);
  }

  /**
   * A Flink Output for Combine Runner.
   */
  public static class FlinkReduceOutput<K, OutputT> implements
      FlinkCombineRunner.Output<K, OutputT> {

    private Collector<WindowedValue<KV<K, OutputT>>> collector;

    public FlinkReduceOutput(Collector<WindowedValue<KV<K, OutputT>>> collector) {
      this.collector = collector;
    }

    @Override
    public void output(WindowedValue<KV<K, OutputT>> output) {
      collector.collect(output);
    }
  }

}
