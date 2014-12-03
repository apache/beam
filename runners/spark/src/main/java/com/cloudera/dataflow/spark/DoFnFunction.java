/**
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
package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.streaming.KeyedState;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class DoFnFunction<I, O> implements FlatMapFunction<Iterator<I>, O> {

  private final DoFn<I, O> fn;
  private final SparkRuntimeContext runtimeContext;
  private final Map<TupleTag<?>, BroadcastHelper<?>> sideInputs;

  public DoFnFunction(
      DoFn<I, O> fn,
      SparkRuntimeContext runtime,
      Map<TupleTag<?>, BroadcastHelper<?>> sideInputs) {
    this.fn = fn;
    this.runtimeContext = runtime;
    this.sideInputs = sideInputs;
  }

  @Override
  public Iterable<O> call(Iterator<I> iter) throws Exception {
    ProcCtxt<I, O> ctxt = new ProcCtxt(fn);
    fn.startBatch(ctxt);
    while (iter.hasNext()) {
      ctxt.element = iter.next();
      fn.processElement(ctxt);
    }
    fn.finishBatch(ctxt);
    return ctxt.outputs;
  }

  private class ProcCtxt<I, O> extends DoFn<I, O>.ProcessContext {

    private List<O> outputs = new LinkedList<>();
    private I element;

    public ProcCtxt(DoFn<I, O> fn) {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return runtimeContext.getPipelineOptions();
    }

    @Override
    public synchronized void output(O o) {
      outputs.add(o);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tupleTag, T t) {
      // A no-op if we don't know about it ahead of time
    }

    @Override
    public <AI, AA, AO> Aggregator<AI> createAggregator(
        String named,
        Combine.CombineFn<? super AI, AA, AO> combineFn) {
      return runtimeContext.createAggregator(named, combineFn);
    }

    @Override
    public <AI, AO> Aggregator<AI> createAggregator(
        String named,
        SerializableFunction<Iterable<AI>, AO> sfunc) {
      return runtimeContext.createAggregator(named, sfunc);
    }

    @Override
    public <T> T sideInput(TupleTag<T> tag) {
      BroadcastHelper<T> bh = (BroadcastHelper<T>) sideInputs.get(tag);
      return bh == null ? null : bh.getValue();
    }

    @Override
    public I element() {
      return element;
    }

    @Override
    public KeyedState keyedState() {
      throw new UnsupportedOperationException();
    }
  }
}
