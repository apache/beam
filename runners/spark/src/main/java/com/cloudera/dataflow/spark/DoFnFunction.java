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

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.ImmutableList;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.joda.time.Instant;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Dataflow's Do functions correspond to Spark's FlatMap functions.
 *
 * @param <I> Input element type.
 * @param <O> Output element type.
 */
class DoFnFunction<I, O> implements FlatMapFunction<Iterator<I>, O> {
  private static final Logger LOG = Logger.getLogger(DoFnFunction.class.getName());

  private final DoFn<I, O> fn;
  private final SparkRuntimeContext runtimeContext;
  private final Map<TupleTag<?>, BroadcastHelper<?>> sideInputs;

  /**
   *
   * @param fn DoFunction to be wrapped.
   * @param runtime Runtime to apply function in.
   * @param sideInputs Side inputs used in DoFunction.
   */
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
    ProcCtxt<I, O> ctxt = new ProcCtxt<>(fn);
    //setup
    fn.startBundle(ctxt);
    //operation
    while (iter.hasNext()) {
      ctxt.element = iter.next();
      fn.processElement(ctxt);
    }
    //cleanup
    fn.finishBundle(ctxt);
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
    public <T> T sideInput(PCollectionView<T, ?> view) {
      return (T) sideInputs.get(view.getTagInternal()).getValue();
    }

    @Override
    public synchronized void output(O o) {
      outputs.add(o);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tupleTag, T t) {
      LOG.warning("sideoutput is an unsupported operation for DoFnFunctions. Use a " +
          "MultiDoFunction");
      throw new UnsupportedOperationException("sideOutput is an unsupported operation for " +
          "doFunctions, use a MultiDoFunction instead.");
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
    public I element() {
      return element;
    }

    @Override
    public DoFn.KeyedState keyedState() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void outputWithTimestamp(O output, Instant timestamp) {
      output(output);
    }

    @Override
    public Instant timestamp() {
      return Instant.now();
    }

    @Override
    public Collection<? extends BoundedWindow> windows() {
      return ImmutableList.of();
    }
  }
}
