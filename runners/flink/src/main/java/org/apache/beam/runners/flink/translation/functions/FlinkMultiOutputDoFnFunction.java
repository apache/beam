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
import org.apache.beam.runners.flink.translation.wrappers.SerializableFnAggregatorWrapper;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.ImmutableList;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates a {@link org.apache.beam.sdk.transforms.DoFn} that uses side outputs
 * inside a Flink {@link org.apache.flink.api.common.functions.RichMapPartitionFunction}.
 *
 * We get a mapping from {@link org.apache.beam.sdk.values.TupleTag} to output index
 * and must tag all outputs with the output number. Afterwards a filter will filter out
 * those elements that are not to be in a specific output.
 */
public class FlinkMultiOutputDoFnFunction<IN, OUT> extends RichMapPartitionFunction<IN, RawUnionValue> {

  private final DoFn<IN, OUT> doFn;
  private final SerializedPipelineOptions serializedPipelineOptions;
  private final Map<TupleTag<?>, Integer> outputMap;

  public FlinkMultiOutputDoFnFunction(DoFn<IN, OUT> doFn, PipelineOptions options, Map<TupleTag<?>, Integer> outputMap) {
    this.doFn = doFn;
    this.serializedPipelineOptions = new SerializedPipelineOptions(options);
    this.outputMap = outputMap;
  }

  @Override
  public void mapPartition(Iterable<IN> values, Collector<RawUnionValue> out) throws Exception {
    ProcessContext context = new ProcessContext(doFn, out);
    this.doFn.startBundle(context);
    for (IN value : values) {
      context.inValue = value;
      doFn.processElement(context);
    }
    this.doFn.finishBundle(context);
  }

  private class ProcessContext extends DoFn<IN, OUT>.ProcessContext {

    IN inValue;
    Collector<RawUnionValue> outCollector;

    public ProcessContext(DoFn<IN, OUT> fn, Collector<RawUnionValue> outCollector) {
      fn.super();
      this.outCollector = outCollector;
    }

    @Override
    public IN element() {
      return this.inValue;
    }

    @Override
    public Instant timestamp() {
      return Instant.now();
    }

    @Override
    public BoundedWindow window() {
      return GlobalWindow.INSTANCE;
    }

    @Override
    public PaneInfo pane() {
      return PaneInfo.NO_FIRING;
    }

    @Override
    public WindowingInternals<IN, OUT> windowingInternals() {
      return null;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return serializedPipelineOptions.getPipelineOptions();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      List<T> sideInput = getRuntimeContext().getBroadcastVariable(view.getTagInternal()
          .getId());
      List<WindowedValue<?>> windowedValueList = new ArrayList<>(sideInput.size());
      for (T input : sideInput) {
        windowedValueList.add(WindowedValue.of(input, Instant.now(), ImmutableList.of(GlobalWindow.INSTANCE), pane()));
      }
      return view.fromIterableInternal(windowedValueList);
    }

    @Override
    public void output(OUT value) {
      // assume that index 0 is the default output
      outCollector.collect(new RawUnionValue(0, value));
    }

    @Override
    public void outputWithTimestamp(OUT output, Instant timestamp) {
      // not FLink's way, just output normally
      output(output);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void sideOutput(TupleTag<T> tag, T value) {
      Integer index = outputMap.get(tag);
      if (index != null) {
        outCollector.collect(new RawUnionValue(index, value));
      }
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      sideOutput(tag, output);
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
      SerializableFnAggregatorWrapper<AggInputT, AggOutputT> wrapper = new SerializableFnAggregatorWrapper<>(combiner);
      getRuntimeContext().addAccumulator(name, wrapper);
      return null;
    }

  }
}
