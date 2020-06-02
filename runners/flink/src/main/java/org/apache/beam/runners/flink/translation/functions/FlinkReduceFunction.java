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

import java.util.Map;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * This is the second part for executing a {@link org.apache.beam.sdk.transforms.Combine.PerKey} on
 * Flink, the second part is {@link FlinkReduceFunction}. This function performs the final
 * combination of the pre-combined values after a shuffle.
 *
 * <p>The input to {@link #reduce(Iterable, Collector)} are elements of the same key but for
 * different windows. We have to ensure that we only combine elements of matching windows.
 */
public class FlinkReduceFunction<K, AccumT, OutputT, W extends BoundedWindow>
    extends RichGroupReduceFunction<WindowedValue<KV<K, AccumT>>, WindowedValue<KV<K, OutputT>>> {

  protected final CombineFnBase.GlobalCombineFn<?, AccumT, OutputT> combineFn;

  protected final WindowingStrategy<Object, W> windowingStrategy;

  // TODO: Remove side input functionality since liftable Combines no longer have side inputs.
  protected final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;

  protected final SerializablePipelineOptions serializedOptions;

  /** WindowedValues has been exploded and pre-grouped by window. */
  private final boolean groupedByWindow;

  public FlinkReduceFunction(
      CombineFnBase.GlobalCombineFn<?, AccumT, OutputT> combineFn,
      WindowingStrategy<Object, W> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions pipelineOptions) {
    this(combineFn, windowingStrategy, sideInputs, pipelineOptions, false);
  }

  public FlinkReduceFunction(
      CombineFnBase.GlobalCombineFn<?, AccumT, OutputT> combineFn,
      WindowingStrategy<Object, W> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions pipelineOptions,
      boolean groupedByWindow) {
    this.combineFn = combineFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializablePipelineOptions(pipelineOptions);
    this.groupedByWindow = groupedByWindow;
  }

  @Override
  public void open(Configuration parameters) {
    // Initialize FileSystems for any coders which may want to use the FileSystem,
    // see https://issues.apache.org/jira/browse/BEAM-8303
    FileSystems.setDefaultPipelineOptions(serializedOptions.get());
  }

  @Override
  public void reduce(
      Iterable<WindowedValue<KV<K, AccumT>>> elements, Collector<WindowedValue<KV<K, OutputT>>> out)
      throws Exception {

    PipelineOptions options = serializedOptions.get();

    FlinkSideInputReader sideInputReader =
        new FlinkSideInputReader(sideInputs, getRuntimeContext());

    AbstractFlinkCombineRunner<K, AccumT, AccumT, OutputT, W> reduceRunner;

    if (groupedByWindow) {
      reduceRunner = new SingleWindowFlinkCombineRunner<>();
    } else {
      if (!windowingStrategy.getWindowFn().isNonMerging()
          && !windowingStrategy.getWindowFn().windowCoder().equals(IntervalWindow.getCoder())) {
        reduceRunner = new HashingFlinkCombineRunner<>();
      } else {
        reduceRunner = new SortingFlinkCombineRunner<>();
      }
    }

    reduceRunner.combine(
        new AbstractFlinkCombineRunner.FinalFlinkCombiner<>(combineFn),
        windowingStrategy,
        sideInputReader,
        options,
        elements,
        out);
  }
}
