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
import java.util.Map;
import org.apache.beam.runners.core.OldDoFn;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

/** {@link OldDoFn.ProcessContext} for {@link FlinkDoFnFunction} with a single main output. */
class FlinkSingleOutputProcessContext<InputT, OutputT>
    extends FlinkProcessContextBase<InputT, OutputT> {

  private final Collector<WindowedValue<OutputT>> collector;

  FlinkSingleOutputProcessContext(
      PipelineOptions pipelineOptions,
      RuntimeContext runtimeContext,
      OldDoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      Collector<WindowedValue<OutputT>> collector) {
    super(pipelineOptions, runtimeContext, doFn, windowingStrategy, sideInputs);
    this.collector = collector;
  }

  @Override
  protected void outputWithTimestampAndWindow(
      OutputT value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo pane) {
    collector.collect(WindowedValue.of(value, timestamp, windows, pane));
  }

  @Override
  public <T> void sideOutput(TupleTag<T> tag, T value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T value, Instant timestamp) {
    throw new UnsupportedOperationException();
  }
}
