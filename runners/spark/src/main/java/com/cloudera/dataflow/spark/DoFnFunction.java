/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Dataflow's Do functions correspond to Spark's FlatMap functions.
 *
 * @param <I> Input element type.
 * @param <O> Output element type.
 */
class DoFnFunction<I, O> implements FlatMapFunction<Iterator<I>, O> {
  private final DoFn<I, O> mFunction;
  private final SparkRuntimeContext mRuntimeContext;
  private final Map<TupleTag<?>, BroadcastHelper<?>> mSideInputs;

  /**
   * @param fn         DoFunction to be wrapped.
   * @param runtime    Runtime to apply function in.
   * @param sideInputs Side inputs used in DoFunction.
   */
  DoFnFunction(DoFn<I, O> fn,
               SparkRuntimeContext runtime,
               Map<TupleTag<?>, BroadcastHelper<?>> sideInputs) {
    this.mFunction = fn;
    this.mRuntimeContext = runtime;
    this.mSideInputs = sideInputs;
  }

  @Override
  public Iterable<O> call(Iterator<I> iter) throws Exception {
    ProcCtxt ctxt = new ProcCtxt(mFunction, mRuntimeContext, mSideInputs);
    ctxt.setup();
    mFunction.startBundle(ctxt);
    return ctxt.getOutputIterable(iter, mFunction);
  }

  private class ProcCtxt extends SparkProcessContext<I, O, O> {

    private final List<O> outputs = new LinkedList<>();

    ProcCtxt(DoFn<I, O> fn, SparkRuntimeContext runtimeContext, Map<TupleTag<?>,
        BroadcastHelper<?>> sideInputs) {
      super(fn, runtimeContext, sideInputs);
    }

    @Override
    public synchronized void output(O o) {
      outputs.add(o);
    }

    @Override
    protected void clearOutput() {
      outputs.clear();
    }

    protected Iterator<O> getOutputIterator() {
      return outputs.iterator();
    }
  }

}
