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
package org.apache.beam.runners.dataflow.worker.util;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.joda.time.Instant;

/**
 * {@link BatchGroupAlsoByWindowFn} that uses combiner to accumulate input elements for non-merging
 * window functions with the default triggering strategy.
 *
 * @param <K> key type
 * @param <InputT> value input type
 * @param <AccumT> accumulator type
 * @param <OutputT> value output type
 * @param <W> window type
 */
class BatchGroupAlsoByWindowAndCombineFn<K, InputT, AccumT, OutputT, W extends BoundedWindow>
    extends BatchGroupAlsoByWindowFn<K, InputT, OutputT> {
  private final GlobalCombineFn<InputT, AccumT, OutputT> perKeyCombineFn;
  private final WindowingStrategy<Object, W> windowingStrategy;

  public BatchGroupAlsoByWindowAndCombineFn(
      WindowingStrategy<?, W> strategy, GlobalCombineFn<InputT, AccumT, OutputT> perKeyCombineFn) {
    this.perKeyCombineFn = perKeyCombineFn;

    // To make a MergeContext that is compatible with the type of windowFn, we need to remove
    // the wildcard from the element type.
    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, W> objectWindowingStrategy = (WindowingStrategy<Object, W>) strategy;
    this.windowingStrategy = objectWindowingStrategy;
  }

  @Override
  public void processElement(
      KV<K, Iterable<WindowedValue<InputT>>> element,
      PipelineOptions options,
      StepContext stepContext,
      SideInputReader sideInputReader,
      OutputWindowedValue<KV<K, OutputT>> output)
      throws Exception {
    final PerKeyCombineFnRunner<K, InputT, AccumT, OutputT> perKeyCombineFnRunner;
    if (perKeyCombineFn instanceof CombineFn) {
      perKeyCombineFnRunner =
          new KeyedCombineFnRunner((CombineFn<InputT, AccumT, OutputT>) perKeyCombineFn);
    } else {
      perKeyCombineFnRunner =
          new KeyedCombineFnWithContextRunner(
              options,
              (CombineFnWithContext<InputT, AccumT, OutputT>) perKeyCombineFn,
              sideInputReader);
    }

    final K key = element.getKey();
    Iterator<WindowedValue<InputT>> iterator = element.getValue().iterator();

    final PriorityQueue<W> liveWindows =
        new PriorityQueue<>(
            11,
            (w1, w2) -> Long.signum(w1.maxTimestamp().getMillis() - w2.maxTimestamp().getMillis()));

    final Map<W, AccumT> accumulators = Maps.newHashMap();
    final Map<W, Instant> accumulatorOutputTimestamps = Maps.newHashMap();

    WindowFn<Object, W>.MergeContext mergeContext =
        windowingStrategy.getWindowFn().new MergeContext() {
          @Override
          public Collection<W> windows() {
            return liveWindows;
          }

          @Override
          public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
            List<AccumT> accumsToBeMerged = Lists.newArrayListWithCapacity(toBeMerged.size());
            List<Instant> timestampsToBeMerged = Lists.newArrayListWithCapacity(toBeMerged.size());
            for (W window : toBeMerged) {
              accumsToBeMerged.add(accumulators.remove(window));
              timestampsToBeMerged.add(accumulatorOutputTimestamps.remove(window));
            }
            liveWindows.removeAll(toBeMerged);

            Instant mergedOutputTimestamp =
                windowingStrategy.getTimestampCombiner().merge(mergeResult, timestampsToBeMerged);
            accumulatorOutputTimestamps.put(mergeResult, mergedOutputTimestamp);
            liveWindows.add(mergeResult);
            AccumT accum = perKeyCombineFnRunner.mergeAccumulators(accumsToBeMerged, mergeResult);
            accumulators.put(mergeResult, accum);
          }
        };

    while (iterator.hasNext()) {
      WindowedValue<InputT> e = iterator.next();

      @SuppressWarnings("unchecked")
      Collection<W> windows = (Collection<W>) e.getWindows();
      for (W window : windows) {
        Instant outputTime =
            windowingStrategy
                .getTimestampCombiner()
                .assign(
                    window,
                    windowingStrategy.getWindowFn().getOutputTime(e.getTimestamp(), window));
        Instant accumulatorOutputTime = accumulatorOutputTimestamps.get(window);
        if (accumulatorOutputTime == null) {
          accumulatorOutputTimestamps.put(window, outputTime);
        } else {
          accumulatorOutputTimestamps.put(
              window,
              windowingStrategy.getTimestampCombiner().combine(outputTime, accumulatorOutputTime));
        }

        AccumT accum = accumulators.get(window);
        checkState(
            (accumulatorOutputTime == null && accum == null)
                || (accumulatorOutputTime != null && accum != null),
            "accumulator and accumulatorOutputTime should both be null or both be non-null");
        if (accum == null) {
          accum = perKeyCombineFnRunner.createAccumulator(window);
          liveWindows.add(window);
        }
        accum = perKeyCombineFnRunner.addInput(accum, e.getValue(), window);
        accumulators.put(window, accum);
      }

      windowingStrategy.getWindowFn().mergeWindows(mergeContext);

      while (!liveWindows.isEmpty()
          && liveWindows.peek().maxTimestamp().isBefore(e.getTimestamp())) {
        closeWindow(
            perKeyCombineFnRunner,
            key,
            liveWindows.poll(),
            accumulators,
            accumulatorOutputTimestamps,
            output);
      }
    }

    // To have gotten here, we've either not had any elements added, or we've only run merge
    // and then closed windows. We don't need to retry merging.
    while (!liveWindows.isEmpty()) {
      closeWindow(
          perKeyCombineFnRunner,
          key,
          liveWindows.poll(),
          accumulators,
          accumulatorOutputTimestamps,
          output);
    }
  }

  private void closeWindow(
      PerKeyCombineFnRunner<K, InputT, AccumT, OutputT> perKeyCombineFnRunner,
      K key,
      W window,
      Map<W, AccumT> accumulators,
      Map<W, Instant> accumulatorOutputTimes,
      OutputWindowedValue<KV<K, OutputT>> output) {
    AccumT accum = accumulators.remove(window);
    Instant timestamp = accumulatorOutputTimes.remove(window);
    checkState(accum != null && timestamp != null);
    output.outputWindowedValue(
        KV.of(key, perKeyCombineFnRunner.extractOutput(accum, window)),
        timestamp,
        Arrays.asList(window),
        PaneInfo.ON_TIME_AND_ONLY_FIRING);
  }

  private interface PerKeyCombineFnRunner<K, InputT, AccumT, OutputT> {
    public AccumT createAccumulator(BoundedWindow w);

    public AccumT addInput(AccumT accumulator, InputT input, BoundedWindow w);

    public AccumT mergeAccumulators(Iterable<AccumT> accumulators, BoundedWindow w);

    public OutputT extractOutput(AccumT accumulator, BoundedWindow w);
  }

  private class KeyedCombineFnRunner implements PerKeyCombineFnRunner<K, InputT, AccumT, OutputT> {
    private final CombineFn<InputT, AccumT, OutputT> keyedCombineFn;

    private KeyedCombineFnRunner(CombineFn<InputT, AccumT, OutputT> keyedCombineFn) {
      this.keyedCombineFn = keyedCombineFn;
    }

    @Override
    public AccumT createAccumulator(BoundedWindow w) {
      return keyedCombineFn.createAccumulator();
    }

    @Override
    public AccumT addInput(AccumT accumulator, InputT input, BoundedWindow w) {
      return keyedCombineFn.addInput(accumulator, input);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators, BoundedWindow w) {
      return keyedCombineFn.mergeAccumulators(accumulators);
    }

    @Override
    public OutputT extractOutput(AccumT accumulator, BoundedWindow w) {
      return keyedCombineFn.extractOutput(accumulator);
    }
  }

  private class KeyedCombineFnWithContextRunner
      implements PerKeyCombineFnRunner<K, InputT, AccumT, OutputT> {
    private final PipelineOptions options;
    private final CombineFnWithContext<InputT, AccumT, OutputT> keyedCombineFnWithContext;
    private final SideInputReader sideInputReader;

    private KeyedCombineFnWithContextRunner(
        PipelineOptions options,
        CombineFnWithContext<InputT, AccumT, OutputT> keyedCombineFnWithContext,
        SideInputReader sideInputReader) {
      this.options = options;
      this.keyedCombineFnWithContext = keyedCombineFnWithContext;
      this.sideInputReader = sideInputReader;
    }

    @Override
    public AccumT createAccumulator(BoundedWindow w) {
      CombineWithContext.Context context = createFromComponents(options, sideInputReader, w);
      return keyedCombineFnWithContext.createAccumulator(context);
    }

    @Override
    public AccumT addInput(AccumT accumulator, InputT input, BoundedWindow w) {
      CombineWithContext.Context context = createFromComponents(options, sideInputReader, w);
      return keyedCombineFnWithContext.addInput(accumulator, input, context);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators, BoundedWindow w) {
      CombineWithContext.Context context = createFromComponents(options, sideInputReader, w);
      return keyedCombineFnWithContext.mergeAccumulators(accumulators, context);
    }

    @Override
    public OutputT extractOutput(AccumT accumulator, BoundedWindow w) {
      CombineWithContext.Context context = createFromComponents(options, sideInputReader, w);
      return keyedCombineFnWithContext.extractOutput(accumulator, context);
    }

    private CombineWithContext.Context createFromComponents(
        final PipelineOptions pipelineOptions,
        final SideInputReader sideInputReader,
        final BoundedWindow mainInputWindow) {
      return new CombineWithContext.Context() {
        @Override
        public PipelineOptions getPipelineOptions() {
          return pipelineOptions;
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view) {
          return sideInputReader.get(
              view, view.getWindowMappingFn().getSideInputWindow(mainInputWindow));
        }
      };
    }
  }
}
