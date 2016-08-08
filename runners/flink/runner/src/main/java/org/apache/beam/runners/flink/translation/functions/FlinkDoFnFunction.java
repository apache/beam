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
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Encapsulates a {@link OldDoFn}
 * inside a Flink {@link org.apache.flink.api.common.functions.RichMapPartitionFunction}.
 */
public class FlinkDoFnFunction<InputT, OutputT>
    extends RichMapPartitionFunction<WindowedValue<InputT>, WindowedValue<OutputT>> {

  private final OldDoFn<InputT, OutputT> doFn;
  private final SerializedPipelineOptions serializedOptions;

  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;

  private final boolean requiresWindowAccess;
  private final boolean hasSideInputs;

  private final WindowingStrategy<?, ?> windowingStrategy;

  public FlinkDoFnFunction(
      OldDoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions options) {
    this.doFn = doFn;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializedPipelineOptions(options);
    this.windowingStrategy = windowingStrategy;

    this.requiresWindowAccess = doFn instanceof OldDoFn.RequiresWindowAccess;
    this.hasSideInputs = !sideInputs.isEmpty();
  }

  @Override
  public void mapPartition(
      Iterable<WindowedValue<InputT>> values,
      Collector<WindowedValue<OutputT>> out) throws Exception {

    FlinkProcessContext<InputT, OutputT> context = new FlinkProcessContext<>(
        serializedOptions.getPipelineOptions(),
        getRuntimeContext(),
        doFn,
        windowingStrategy,
        out,
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

    // set the windowed value to null so that the logic
    // or outputting in finishBundle kicks in
    context = context.forWindowedValue(null);
    this.doFn.finishBundle(context);
  }

}
