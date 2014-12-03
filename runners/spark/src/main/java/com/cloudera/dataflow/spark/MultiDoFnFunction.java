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
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;

class MultiDoFnFunction<I, O> implements PairFlatMapFunction<Iterator<I>, TupleTag<?>, Object> {

  private final DoFn<I, O> fn;
  private final TupleTag<?> mainOutputTag;
  private final Map<TupleTag<?>, BroadcastHelper<?>> sideInputs;

  public MultiDoFnFunction(DoFn<I, O> fn, TupleTag<O> mainOutputTag) {
    this(fn, mainOutputTag, ImmutableMap.<TupleTag<?>, BroadcastHelper<?>>of());
  }

  public MultiDoFnFunction(DoFn<I, O> fn,
      TupleTag<O> mainOutputTag,
      Map<TupleTag<?>, BroadcastHelper<?>> sideInputs) {
    this.fn = fn;
    this.mainOutputTag = mainOutputTag;
    this.sideInputs = sideInputs;
  }

  @Override
  public Iterable<Tuple2<TupleTag<?>, Object>> call(Iterator<I> iter) throws Exception {
    ProcCtxt<I, O> ctxt = new ProcCtxt(fn);
    fn.startBatch(ctxt);
    while (iter.hasNext()) {
      ctxt.element = iter.next();
      fn.processElement(ctxt);
    }
    fn.finishBatch(ctxt);
    return Iterables.transform(ctxt.outputs.entries(),
        new Function<Map.Entry<TupleTag<?>, Object>, Tuple2<TupleTag<?>, Object>>() {
          public Tuple2<TupleTag<?>, Object> apply(Map.Entry<TupleTag<?>, Object> input) {
            return new Tuple2<TupleTag<?>, Object>(input.getKey(), input.getValue());
          }
        });
  }

  private class ProcCtxt<I, O> extends DoFn<I, O>.ProcessContext {

    private Multimap<TupleTag<?>, Object> outputs = LinkedListMultimap.create();
    private I element;

    public ProcCtxt(DoFn<I, O> fn) {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return null;
    }

    @Override
    public synchronized void output(O o) {
      outputs.put(mainOutputTag, o);
    }

    @Override
    public synchronized <T> void sideOutput(TupleTag<T> tag, T t) {
      outputs.put(tag, t);
    }

    @Override
    public <AI, AA, AO> Aggregator<AI> createAggregator(
        String named,
        Combine.CombineFn<? super AI, AA, AO> combineFn) {
      return null;
    }

    @Override
    public <AI, AO> Aggregator<AI> createAggregator(
        String named,
        SerializableFunction<Iterable<AI>, AO> sfunc) {
      return null;
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
