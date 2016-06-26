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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.TupleTag;

import org.apache.flink.util.Collector;
import org.joda.time.Instant;

import java.util.Map;

/**
 * A wrapper for the {@link org.apache.beam.sdk.transforms.ParDo.BoundMulti} Beam transformation.
 * */
public class FlinkParDoBoundMultiWrapper<IN, OUT> extends FlinkAbstractParDoWrapper<IN, OUT, RawUnionValue> {

  private final TupleTag<?> mainTag;
  private final Map<TupleTag<?>, Integer> outputLabels;

  public FlinkParDoBoundMultiWrapper(PipelineOptions options, WindowingStrategy<?, ?> windowingStrategy, DoFn<IN, OUT> doFn, TupleTag<?> mainTag, Map<TupleTag<?>, Integer> tagsToLabels) {
    super(options, windowingStrategy, doFn);
    this.mainTag = checkNotNull(mainTag);
    this.outputLabels = checkNotNull(tagsToLabels);
  }

  @Override
  public void outputWithTimestampHelper(WindowedValue<IN> inElement, OUT output, Instant timestamp, Collector<WindowedValue<RawUnionValue>> collector) {
    checkTimestamp(inElement, timestamp);
    Integer index = outputLabels.get(mainTag);
    collector.collect(makeWindowedValue(
        new RawUnionValue(index, output),
        timestamp,
        inElement.getWindows(),
        inElement.getPane()));
  }

  @Override
  public <T> void sideOutputWithTimestampHelper(WindowedValue<IN> inElement, T output, Instant timestamp, Collector<WindowedValue<RawUnionValue>> collector, TupleTag<T> tag) {
    checkTimestamp(inElement, timestamp);
    Integer index = outputLabels.get(tag);
    if (index != null) {
      collector.collect(makeWindowedValue(
          new RawUnionValue(index, output),
          timestamp,
          inElement.getWindows(),
          inElement.getPane()));
    }
  }

  @Override
  public WindowingInternals<IN, OUT> windowingInternalsHelper(WindowedValue<IN> inElement, Collector<WindowedValue<RawUnionValue>> outCollector) {
    throw new RuntimeException("FlinkParDoBoundMultiWrapper is just an internal operator serving as " +
        "an intermediate transformation for the ParDo.BoundMulti translation. windowingInternals() " +
        "is not available in this class.");
  }
}