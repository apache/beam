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
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

/**
 * {@link OldDoFn.ProcessContext} for {@link FlinkMultiOutputDoFnFunction} that supports side
 * outputs.
 */
class FlinkMultiOutputProcessContext<InputT, OutputT>
    extends FlinkProcessContextBase<InputT, OutputT> {

  private final Collector<WindowedValue<RawUnionValue>> collector;
  private final Map<TupleTag<?>, Integer> outputMap;

  FlinkMultiOutputProcessContext(
      PipelineOptions pipelineOptions,
      RuntimeContext runtimeContext,
      OldDoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      Collector<WindowedValue<RawUnionValue>> collector,
      Map<TupleTag<?>, Integer> outputMap) {
    super(pipelineOptions, runtimeContext, doFn, windowingStrategy, sideInputs);
    this.collector = collector;
    this.outputMap = outputMap;
  }

  @Override
  protected void outputWithTimestampAndWindow(
      OutputT value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo pane) {
    collector.collect(WindowedValue.of(new RawUnionValue(0, value), timestamp, windows, pane));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void sideOutput(TupleTag<T> tag, T value) {
    if (windowedValue != null) {
      sideOutputWithTimestamp(tag, value, windowedValue.getTimestamp());
    } else {
      sideOutputWithTimestamp(tag, value, null);
    }
  }

  @Override
  public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T value, Instant timestamp) {
    Integer index = outputMap.get(tag);

    if (index == null) {
      throw new IllegalArgumentException("Unknown side output tag: " + tag);
    }

    outputUnionValue(value, timestamp, new RawUnionValue(index, value));
  }

  private <T> void outputUnionValue(T value, Instant timestamp, RawUnionValue unionValue) {
    if (windowedValue == null) {
      // we are in startBundle() or finishBundle()

      try {
        Collection<? extends BoundedWindow> windows =
            windowingStrategy
                .getWindowFn()
                .assignWindows(
                    new FlinkNoElementAssignContext(
                        windowingStrategy.getWindowFn(), value, timestamp));

        collector.collect(
            WindowedValue.of(
                unionValue,
                timestamp != null ? timestamp : new Instant(Long.MIN_VALUE),
                windows,
                PaneInfo.NO_FIRING));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      collector.collect(
          WindowedValue.of(
              unionValue,
              windowedValue.getTimestamp(),
              windowedValue.getWindows(),
              windowedValue.getPane()));
    }
  }
}
