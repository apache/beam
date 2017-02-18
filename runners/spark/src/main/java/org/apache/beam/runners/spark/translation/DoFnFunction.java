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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.aggregators.SparkAggregators;
import org.apache.beam.runners.spark.metrics.SparkMetricsContainer;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.runners.spark.util.SparkSideInputReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.FlatMapFunction;


/**
 * Beam's Do functions correspond to Spark's FlatMap functions.
 *
 * @param <InputT>  Input element type.
 * @param <OutputT> Output element type.
 */
public class DoFnFunction<InputT, OutputT>
    implements FlatMapFunction<Iterator<WindowedValue<InputT>>, WindowedValue<OutputT>> {

  private final Accumulator<NamedAggregators> aggregatorsAccum;
  private final Accumulator<SparkMetricsContainer> metricsAccum;
  private final String stepName;
  private final DoFn<InputT, OutputT> doFn;
  private final SparkRuntimeContext runtimeContext;
  private final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;

  /**
   * @param aggregatorsAccum  The Spark {@link Accumulator} that backs the Beam Aggregators.
   * @param doFn              The {@link DoFn} to be wrapped.
   * @param runtimeContext    The {@link SparkRuntimeContext}.
   * @param sideInputs        Side inputs used in this {@link DoFn}.
   * @param windowingStrategy Input {@link WindowingStrategy}.
   */
  public DoFnFunction(
      Accumulator<NamedAggregators> aggregatorsAccum,
      Accumulator<SparkMetricsContainer> metricsAccum,
      String stepName,
      DoFn<InputT, OutputT> doFn,
      SparkRuntimeContext runtimeContext,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    this.aggregatorsAccum = aggregatorsAccum;
    this.metricsAccum = metricsAccum;
    this.stepName = stepName;
    this.doFn = doFn;
    this.runtimeContext = runtimeContext;
    this.sideInputs = sideInputs;
    this.windowingStrategy = windowingStrategy;
  }

  @Override
  public Iterable<WindowedValue<OutputT>> call(
      Iterator<WindowedValue<InputT>> iter) throws Exception {
    DoFnOutputManager outputManager = new DoFnOutputManager();

    DoFnRunner<InputT, OutputT> doFnRunner =
        DoFnRunners.simpleRunner(
            runtimeContext.getPipelineOptions(),
            doFn,
            new SparkSideInputReader(sideInputs),
            outputManager,
            new TupleTag<OutputT>() {
            },
            Collections.<TupleTag<?>>emptyList(),
            new SparkProcessContext.NoOpStepContext(),
            new SparkAggregators.Factory(runtimeContext, aggregatorsAccum),
            windowingStrategy);

    DoFnRunner<InputT, OutputT> doFnRunnerWithMetrics =
        new DoFnRunnerWithMetrics<>(stepName, doFnRunner, metricsAccum);

    return new SparkProcessContext<>(doFn, doFnRunnerWithMetrics, outputManager)
        .processPartition(iter);
  }

  private class DoFnOutputManager
      implements SparkProcessContext.SparkOutputManager<WindowedValue<OutputT>> {

    private final List<WindowedValue<OutputT>> outputs = new LinkedList<>();

    @Override
    public void clear() {
      outputs.clear();
    }

    @Override
    public Iterator<WindowedValue<OutputT>> iterator() {
      return outputs.iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      outputs.add((WindowedValue<OutputT>) output);
    }
  }

}
