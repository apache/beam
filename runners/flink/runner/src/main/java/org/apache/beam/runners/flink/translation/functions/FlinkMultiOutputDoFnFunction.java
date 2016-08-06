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

import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Encapsulates a {@link OldDoFn} that uses side outputs
 * inside a Flink {@link org.apache.flink.api.common.functions.RichMapPartitionFunction}.
 *
 * We get a mapping from {@link org.apache.beam.sdk.values.TupleTag} to output index
 * and must tag all outputs with the output number. Afterwards a filter will filter out
 * those elements that are not to be in a specific output.
 */
public class FlinkMultiOutputDoFnFunction<InputT, OutputT>
    extends RichMapPartitionFunction<WindowedValue<InputT>, WindowedValue<RawUnionValue>> {

  private final OldDoFn<InputT, OutputT> doFn;
  private final SerializedPipelineOptions serializedOptions;

  private final Map<TupleTag<?>, Integer> outputMap;

  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;

  private final boolean requiresWindowAccess;
  private final boolean hasSideInputs;

  private final WindowingStrategy<?, ?> windowingStrategy;

  public FlinkMultiOutputDoFnFunction(
      OldDoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions options,
      Map<TupleTag<?>, Integer> outputMap) {
    this.doFn = doFn;
    this.serializedOptions = new SerializedPipelineOptions(options);
    this.outputMap = outputMap;

    this.requiresWindowAccess = doFn instanceof OldDoFn.RequiresWindowAccess;
    this.hasSideInputs = !sideInputs.isEmpty();
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;
  }

  @Override
  public void mapPartition(
      Iterable<WindowedValue<InputT>> values,
      Collector<WindowedValue<RawUnionValue>> out) throws Exception {

    FlinkProcessContext<InputT, OutputT> context = new FlinkMultiOutputProcessContext<>(
        serializedOptions.getPipelineOptions(),
        getRuntimeContext(),
        doFn,
        windowingStrategy,
        out,
        outputMap,
        sideInputs);

    this.doFn.startBundle(context);

    if (!requiresWindowAccess || hasSideInputs) {
      // we don't need to explode the windows
      for (WindowedValue<InputT> value : values) {
        context = context.forWindowedValue(value);
        doFn.processElement(context);
      }
    } else {
      // we need to explode the windows because we have per-window
      // side inputs and window access also only works if an element
      // is in only one window
      for (WindowedValue<InputT> value : values) {
        for (WindowedValue<InputT> explodedValue: value.explodeWindows()) {
          context = context.forWindowedValue(value);
          doFn.processElement(context);
        }
      }
    }


    this.doFn.finishBundle(context);
  }
}
