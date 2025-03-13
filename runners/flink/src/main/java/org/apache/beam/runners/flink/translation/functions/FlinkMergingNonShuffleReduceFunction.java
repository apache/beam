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
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Special version of {@link FlinkReduceFunction} that supports merging windows.
 *
 * <p>This is different from the pair of function for the non-merging windows case in that we cannot
 * do combining before the shuffle because elements would not yet be in their correct windows for
 * side-input access.
 */
public class FlinkMergingNonShuffleReduceFunction<
        K, InputT, AccumT, OutputT, W extends BoundedWindow>
    extends RichGroupReduceFunction<WindowedValue<KV<K, InputT>>, WindowedValue<KV<K, OutputT>>> {

  private final CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT> combineFn;

  private final WindowingStrategy<Object, W> windowingStrategy;

  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;

  private final SerializablePipelineOptions serializedOptions;

  public FlinkMergingNonShuffleReduceFunction(
      CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT> combineFn,
      WindowingStrategy<Object, W> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions pipelineOptions) {

    this.combineFn = combineFn;

    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;

    this.serializedOptions = new SerializablePipelineOptions(pipelineOptions);
  }

  @Override
  public void open(Configuration parameters) {
    // Initialize FileSystems for any coders which may want to use the FileSystem,
    // see https://issues.apache.org/jira/browse/BEAM-8303
    FileSystems.setDefaultPipelineOptions(serializedOptions.get());
  }

  @Override
  public void reduce(
      Iterable<WindowedValue<KV<K, InputT>>> elements, Collector<WindowedValue<KV<K, OutputT>>> out)
      throws Exception {

    PipelineOptions options = serializedOptions.get();

    FlinkSideInputReader sideInputReader =
        new FlinkSideInputReader(sideInputs, getRuntimeContext());

    AbstractFlinkCombineRunner<K, InputT, AccumT, OutputT, W> reduceRunner;
    if (windowingStrategy.getWindowFn() instanceof Sessions) {
      reduceRunner = new SortingFlinkCombineRunner<>();
    } else {
      reduceRunner = new HashingFlinkCombineRunner<>();
    }

    reduceRunner.combine(
        new AbstractFlinkCombineRunner.CompleteFlinkCombiner<>(combineFn),
        windowingStrategy,
        sideInputReader,
        options,
        elements,
        out);
  }
}
