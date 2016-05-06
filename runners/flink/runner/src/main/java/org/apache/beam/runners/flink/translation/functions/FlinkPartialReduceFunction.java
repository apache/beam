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

import org.apache.beam.runners.flink.translation.wrappers.SerializableFnAggregatorWrapper;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.PerKeyCombineFnRunner;
import org.apache.beam.sdk.util.PerKeyCombineFnRunners;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.ImmutableList;

import org.apache.flink.api.common.functions.RichGroupCombineFunction;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Flink {@link org.apache.flink.api.common.functions.GroupCombineFunction} for executing a
 * {@link org.apache.beam.sdk.transforms.Combine.PerKey} operation. This reads the input
 * {@link org.apache.beam.sdk.values.KV} elements VI, extracts the key and emits accumulated
 * values which have the intermediate format VA.
 */
public class FlinkPartialReduceFunction<K, VI, VA>
    extends RichGroupCombineFunction<KV<K, VI>, KV<K, VA>> {

  private final CombineFnBase.PerKeyCombineFn<K, VI, VA, ?> combineFn;

  private final DoFn<Iterable<KV<K, VI>>, KV<K, VA>> doFn;

  public FlinkPartialReduceFunction(CombineFnBase.PerKeyCombineFn<K, VI, VA, ?> combineFn) {
    this.combineFn = combineFn;

    // dummy DoFn because we need one for ProcessContext
    this.doFn = new DoFn<Iterable<KV<K, VI>>, KV<K, VA>>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {

      }
    };
  }

  @Override
  public void combine(Iterable<KV<K, VI>> elements, Collector<KV<K, VA>> out) throws Exception {

    ProcessContext processContext = new ProcessContext(doFn, elements, out);
    PerKeyCombineFnRunner<K, VI, VA, ?> combineFnRunner = PerKeyCombineFnRunners.create(combineFn);


    final Iterator<KV<K, VI>> iterator = elements.iterator();
    // create accumulator using the first elements key
    KV<K, VI> first = iterator.next();
    K key = first.getKey();
    VI value = first.getValue();
    VA accumulator = combineFnRunner.createAccumulator(key, processContext);

    accumulator = combineFnRunner.addInput(key, accumulator, value, processContext);

    while (iterator.hasNext()) {
      value = iterator.next().getValue();
      accumulator = combineFnRunner.addInput(key, accumulator, value, processContext);
    }

    out.collect(KV.of(key, accumulator));
  }

  private class ProcessContext extends DoFn<Iterable<KV<K, VI>>, KV<K, VA>>.ProcessContext {

    private final DoFn<Iterable<KV<K, VI>>, KV<K, VA>> fn;

    private final Collector<KV<K, VA>> collector;

    private final Iterable<KV<K, VI>> element;

    private ProcessContext(
        DoFn<Iterable<KV<K, VI>>, KV<K, VA>> function,
        Iterable<KV<K, VI>> element,
        Collector<KV<K, VA>> outCollector) {
      function.super();
      super.setupDelegateAggregators();

      this.fn = function;
      this.element = element;
      this.collector = outCollector;
    }

    @Override
    public Iterable<KV<K, VI>> element() {
      return this.element;
    }

    @Override
    public Instant timestamp() {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException("Not supported.");

    }

    @Override
    public PaneInfo pane() {
      return PaneInfo.NO_FIRING;
    }

    @Override
    public WindowingInternals<Iterable<KV<K, VI>>, KV<K, VA>> windowingInternals() {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      List<T> sideInput = getRuntimeContext().getBroadcastVariable(view.getTagInternal().getId());
      List<WindowedValue<?>> windowedValueList = new ArrayList<>(sideInput.size());
      for (T input : sideInput) {
        windowedValueList.add(WindowedValue.of(input, Instant.now(), ImmutableList.of(GlobalWindow.INSTANCE), pane()));
      }
      return view.fromIterableInternal(windowedValueList);
    }

    @Override
    public void output(KV<K, VA> output) {
      collector.collect(output);
    }

    @Override
    public void outputWithTimestamp(KV<K, VA> output, Instant timestamp) {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(
        String name,
        Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
      SerializableFnAggregatorWrapper<AggInputT, AggOutputT> wrapper =
          new SerializableFnAggregatorWrapper<>(combiner);
      getRuntimeContext().addAccumulator(name, wrapper);
      return wrapper;
    }
  }
}
