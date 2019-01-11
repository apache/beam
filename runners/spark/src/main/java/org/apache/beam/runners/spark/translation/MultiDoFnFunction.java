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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import java.util.Iterator;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 * DoFunctions ignore side outputs. MultiDoFunctions deal with side outputs by enriching the
 * underlying data with multiple TupleTags.
 *
 * @param <InputT> Input type for DoFunction.
 * @param <OutputT> Output type for DoFunction.
 */
public class MultiDoFnFunction<InputT, OutputT>
    implements PairFlatMapFunction<Iterator<WindowedValue<InputT>>, TupleTag<?>,
        WindowedValue<?>> {
  private final Accumulator<NamedAggregators> accum;
  private final OldDoFn<InputT, OutputT> mFunction;
  private final SparkRuntimeContext mRuntimeContext;
  private final TupleTag<OutputT> mMainOutputTag;
  private final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> mSideInputs;
  private final WindowFn<Object, ?> windowFn;

  /**
   * @param accum             The Spark Accumulator that handles the Beam Aggregators.
   * @param fn                DoFunction to be wrapped.
   * @param runtimeContext    Runtime to apply function in.
   * @param mainOutputTag     The main output {@link TupleTag}.
   * @param sideInputs        Side inputs used in DoFunction.
   * @param windowFn          Input {@link WindowFn}.
   */
  public MultiDoFnFunction(Accumulator<NamedAggregators> accum,
                           OldDoFn<InputT, OutputT> fn,
                           SparkRuntimeContext runtimeContext,
                           TupleTag<OutputT> mainOutputTag,
                           Map<TupleTag<?>, KV<WindowingStrategy<?, ?>,
                               BroadcastHelper<?>>> sideInputs,
                           WindowFn<Object, ?> windowFn) {
    this.accum = accum;
    this.mFunction = fn;
    this.mRuntimeContext = runtimeContext;
    this.mMainOutputTag = mainOutputTag;
    this.mSideInputs = sideInputs;
    this.windowFn = windowFn;
  }

  @Override
  public Iterable<Tuple2<TupleTag<?>, WindowedValue<?>>>
      call(Iterator<WindowedValue<InputT>> iter) throws Exception {
    return new ProcCtxt(mFunction, mRuntimeContext, mSideInputs, windowFn)
        .callWithCtxt(iter);
  }

  private class ProcCtxt
      extends SparkProcessContext<InputT, OutputT, Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final Multimap<TupleTag<?>, WindowedValue<?>> outputs = LinkedListMultimap.create();

    ProcCtxt(OldDoFn<InputT, OutputT> fn,
             SparkRuntimeContext runtimeContext,
             Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs,
             WindowFn<Object, ?> windowFn) {
      super(fn, runtimeContext, sideInputs, windowFn);
    }

    @Override
    protected synchronized void outputWindowedValue(WindowedValue<OutputT> o) {
      outputs.put(mMainOutputTag, o);
    }

    @Override
    protected <T> void sideOutputWindowedValue(TupleTag<T> tag, WindowedValue<T> output) {
      outputs.put(tag, output);
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
    protected Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> getOutputIterator() {
      return Iterators.transform(outputs.entries().iterator(),
          new Function<Map.Entry<TupleTag<?>, WindowedValue<?>>,
              Tuple2<TupleTag<?>, WindowedValue<?>>>() {
        @Override
        public Tuple2<TupleTag<?>, WindowedValue<?>> apply(Map.Entry<TupleTag<?>,
            WindowedValue<?>> input) {
          return new Tuple2<TupleTag<?>, WindowedValue<?>>(input.getKey(), input.getValue());
        }
      });
    }

  }
}
