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

import java.util.Collection;
import org.apache.beam.runners.core.GlobalCombineFnRunner;
import org.apache.beam.runners.core.GlobalCombineFnRunners;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.flink.util.Collector;

/**
 * Abstract base for runners that execute a {@link org.apache.beam.sdk.transforms.Combine.PerKey}.
 * This unifies processing of merging/non-merging and partial/final combines.
 *
 * <p>The input to {@link #combine( FlinkCombiner, WindowingStrategy, SideInputReader,
 * PipelineOptions, Iterable, Collector)} are elements of the same key but * for different windows.
 */
public abstract class AbstractFlinkCombineRunner<
    K, InputT, AccumT, OutputT, W extends BoundedWindow> {

  /**
   * Consumes {@link WindowedValue WindowedValues} and produces combined output to the given output.
   */
  public abstract void combine(
      FlinkCombiner<K, InputT, AccumT, OutputT> flinkCombiner,
      WindowingStrategy<Object, W> windowingStrategy,
      SideInputReader sideInputReader,
      PipelineOptions options,
      Iterable<WindowedValue<KV<K, InputT>>> elements,
      Collector<WindowedValue<KV<K, OutputT>>> out)
      throws Exception;

  /**
   * Adapter interface that allows using a {@link CombineFnBase.GlobalCombineFn} to either produce
   * the {@code AccumT} as output or to combine several accumulators into an {@code OutputT}. The
   * former would be used for a partial combine while the latter is used for the final merging of
   * accumulators.
   */
  public interface FlinkCombiner<K, InputT, AccumT, OutputT> {

    AccumT firstInput(
        K key,
        InputT value,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows);

    AccumT addInput(
        K key,
        AccumT accumulator,
        InputT value,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows);

    OutputT extractOutput(
        K key,
        AccumT accumulator,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows);
  }

  /**
   * A straight wrapper of {@link CombineFnBase.GlobalCombineFn} that takes in {@code InputT} and
   * emits {@code OutputT}.
   */
  public static class CompleteFlinkCombiner<K, InputT, AccumT, OutputT>
      implements FlinkCombiner<K, InputT, AccumT, OutputT> {

    private final GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFnRunner;

    public CompleteFlinkCombiner(CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT> combineFn) {
      combineFnRunner = GlobalCombineFnRunners.create(combineFn);
    }

    @Override
    public AccumT firstInput(
        K key,
        InputT value,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      AccumT accumulator = combineFnRunner.createAccumulator(options, sideInputReader, windows);
      return combineFnRunner.addInput(accumulator, value, options, sideInputReader, windows);
    }

    @Override
    public AccumT addInput(
        K key,
        AccumT accumulator,
        InputT value,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnRunner.addInput(accumulator, value, options, sideInputReader, windows);
    }

    @Override
    public OutputT extractOutput(
        K key,
        AccumT accumulator,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnRunner.extractOutput(accumulator, options, sideInputReader, windows);
    }
  }

  /** A partial combiner that takes in {@code InputT} and produces {@code AccumT}. */
  public static class PartialFlinkCombiner<K, InputT, AccumT>
      implements FlinkCombiner<K, InputT, AccumT, AccumT> {

    private final GlobalCombineFnRunner<InputT, AccumT, ?> combineFnRunner;

    public PartialFlinkCombiner(CombineFnBase.GlobalCombineFn<InputT, AccumT, ?> combineFn) {
      combineFnRunner = GlobalCombineFnRunners.create(combineFn);
    }

    @Override
    public AccumT firstInput(
        K key,
        InputT value,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      AccumT accumulator = combineFnRunner.createAccumulator(options, sideInputReader, windows);
      return combineFnRunner.addInput(accumulator, value, options, sideInputReader, windows);
    }

    @Override
    public AccumT addInput(
        K key,
        AccumT accumulator,
        InputT value,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnRunner.addInput(accumulator, value, options, sideInputReader, windows);
    }

    @Override
    public AccumT extractOutput(
        K key,
        AccumT accumulator,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return accumulator;
    }
  }

  /** A final combiner that takes in {@code AccumT} and produces {@code OutputT}. */
  public static class FinalFlinkCombiner<K, AccumT, OutputT>
      implements FlinkCombiner<K, AccumT, AccumT, OutputT> {

    private final GlobalCombineFnRunner<?, AccumT, OutputT> combineFnRunner;

    public FinalFlinkCombiner(CombineFnBase.GlobalCombineFn<?, AccumT, OutputT> combineFn) {
      combineFnRunner = GlobalCombineFnRunners.create(combineFn);
    }

    @Override
    public AccumT firstInput(
        K key,
        AccumT value,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return value;
    }

    @Override
    public AccumT addInput(
        K key,
        AccumT accumulator,
        AccumT value,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnRunner.mergeAccumulators(
          ImmutableList.of(accumulator, value), options, sideInputReader, windows);
    }

    @Override
    public OutputT extractOutput(
        K key,
        AccumT accumulator,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnRunner.extractOutput(accumulator, options, sideInputReader, windows);
    }
  }
}
