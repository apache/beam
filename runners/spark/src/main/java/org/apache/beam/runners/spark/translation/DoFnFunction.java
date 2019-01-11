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
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.FlatMapFunction;


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
  private final SparkRuntimeContext mRuntimeContext;
  private final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> mSideInputs;
  private final WindowFn<Object, ?> windowFn;

  /**
   * @param accum             The Spark Accumulator that handles the Beam Aggregators.
   * @param fn                DoFunction to be wrapped.
   * @param runtime           Runtime to apply function in.
   * @param sideInputs        Side inputs used in DoFunction.
   * @param windowFn          Input {@link WindowFn}.
   */
  public DoFnFunction(Accumulator<NamedAggregators> accum,
                      OldDoFn<InputT, OutputT> fn,
                      SparkRuntimeContext runtime,
                      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs,
                      WindowFn<Object, ?> windowFn) {
    this.accum = accum;
    this.mFunction = fn;
    this.mRuntimeContext = runtime;
    this.mSideInputs = sideInputs;
    this.windowFn = windowFn;
  }


  @Override
  public Iterable<WindowedValue<OutputT>> call(Iterator<WindowedValue<InputT>> iter) throws
      Exception {
    return new ProcCtxt(mFunction, mRuntimeContext, mSideInputs, windowFn)
        .callWithCtxt(iter);
  }

  private class ProcCtxt extends SparkProcessContext<InputT, OutputT, WindowedValue<OutputT>> {

    private final List<WindowedValue<OutputT>> outputs = new LinkedList<>();

    ProcCtxt(OldDoFn<InputT, OutputT> fn,
             SparkRuntimeContext runtimeContext,
             Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs,
             WindowFn<Object, ?> windowFn) {
      super(fn, runtimeContext, sideInputs, windowFn);
    }

    @Override
    protected synchronized void outputWindowedValue(WindowedValue<OutputT> o) {
      outputs.add(o);
    }

    @Override
    protected <T> void sideOutputWindowedValue(TupleTag<T> tag, WindowedValue<T> output) {
      throw new UnsupportedOperationException(
          "sideOutput is an unsupported operation for doFunctions, use a "
              + "MultiDoFunction instead.");
    }

    @Override
    public Accumulator<NamedAggregators> getAccumulator() {
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
