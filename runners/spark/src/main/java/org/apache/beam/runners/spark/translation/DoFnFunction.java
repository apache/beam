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

package org.apache.beam.runners.spark.translation;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Beam's Do functions correspond to Spark's FlatMap functions.
 *
 * @param <InputT> Input element type.
 * @param <OutputT> Output element type.
 */
public class DoFnFunction<InputT, OutputT>
    implements FlatMapFunction<Iterator<WindowedValue<InputT>>, WindowedValue<OutputT>> {
  private final Accumulator<NamedAggregators> accum;
  private final OldDoFn<InputT, OutputT> mFunction;
  private static final Logger LOG = LoggerFactory.getLogger(DoFnFunction.class);

  private final SparkRuntimeContext mRuntimeContext;
  private final Map<TupleTag<?>, BroadcastHelper<?>> mSideInputs;

  /**
   * @param accum      The Spark Accumulator that handles the Beam Aggregators.
   * @param fn         DoFunction to be wrapped.
   * @param runtime    Runtime to apply function in.
   * @param sideInputs Side inputs used in DoFunction.
   */
  public DoFnFunction(Accumulator<NamedAggregators> accum,
                      OldDoFn<InputT, OutputT> fn,
                      SparkRuntimeContext runtime,
                      Map<TupleTag<?>, BroadcastHelper<?>> sideInputs) {
    this.accum = accum;
    this.mFunction = fn;
    this.mRuntimeContext = runtime;
    this.mSideInputs = sideInputs;
  }

  /**
   * @param fn         DoFunction to be wrapped.
   * @param runtime    Runtime to apply function in.
   * @param sideInputs Side inputs used in DoFunction.
   */
  public DoFnFunction(OldDoFn<InputT, OutputT> fn,
                      SparkRuntimeContext runtime,
                      Map<TupleTag<?>, BroadcastHelper<?>> sideInputs) {
    this(null, fn, runtime, sideInputs);
  }

  @Override
  public Iterable<WindowedValue<OutputT>> call(Iterator<WindowedValue<InputT>> iter) throws
      Exception {
    ProcCtxt ctxt = new ProcCtxt(mFunction, mRuntimeContext, mSideInputs);
    ctxt.setup();
    try {
      mFunction.setup();
      mFunction.startBundle(ctxt);
      return ctxt.getOutputIterable(iter, mFunction);
    } catch (Exception e) {
      try {
        // this teardown handles exceptions encountered in setup() and startBundle(). teardown
        // after execution or due to exceptions in process element is called in the iterator
        // produced by ctxt.getOutputIterable returned from this method.
        mFunction.teardown();
      } catch (Exception teardownException) {
        LOG.error(
            "Suppressing exception while tearing down Function {}", mFunction, teardownException);
        e.addSuppressed(teardownException);
      }
      throw e;
    }
  }

  private class ProcCtxt extends SparkProcessContext<InputT, OutputT, WindowedValue<OutputT>> {

    private final List<WindowedValue<OutputT>> outputs = new LinkedList<>();

    ProcCtxt(OldDoFn<InputT, OutputT> fn, SparkRuntimeContext runtimeContext, Map<TupleTag<?>,
        BroadcastHelper<?>> sideInputs) {
      super(fn, runtimeContext, sideInputs);
    }

    @Override
    public synchronized void output(OutputT o) {
      outputs.add(windowedValue != null ? windowedValue.withValue(o) :
          WindowedValue.valueInGlobalWindow(o));
    }

    @Override
    public synchronized void output(WindowedValue<OutputT> o) {
      outputs.add(o);
    }

    @Override
    public Accumulator<NamedAggregators> getAccumulator() {
      if (accum == null) {
        throw new UnsupportedOperationException("SparkRunner does not provide Aggregator support "
             + "for DoFnFunction of type: " + mFunction.getClass().getCanonicalName());
      }
      return accum;
    }

    @Override
    protected void clearOutput() {
      outputs.clear();
    }

    @Override
    protected Iterator<WindowedValue<OutputT>> getOutputIterator() {
      return outputs.iterator();
    }
  }

}
