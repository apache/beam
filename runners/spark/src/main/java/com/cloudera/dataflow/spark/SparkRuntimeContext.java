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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

class SparkRuntimeContext implements Serializable {

  private Accumulator<Agg> accum;
  private Map<String, Aggregator> aggregators = new HashMap<>();

  public SparkRuntimeContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.accum = jsc.accumulator(new Agg(), new AggAccumParam());
  }

  public <T> T getAggregatorValue(String named, Class<T> typeClass) {
    return accum.value().getValue(named, typeClass);
  }

  public synchronized PipelineOptions getPipelineOptions() {
    return null; // TODO
  }

  public synchronized <AI, AO> Aggregator<AI> createAggregator(
      String named,
      SerializableFunction<Iterable<AI>, AO> sfunc) {
    Aggregator aggregator = aggregators.get(named);
    if (aggregator == null) {
      Agg.SerState<AI, AO> state = new Agg.SerState<>(sfunc);
      accum.add(new Agg(named, state));
      aggregator = new SparkAggregator(state);
      aggregators.put(named, aggregator);
    }
    return aggregator;
  }

  public synchronized <AI, AA, AO> Aggregator<AI> createAggregator(
      String named,
      Combine.CombineFn<? super AI, AA, AO> combineFn) {
    Aggregator aggregator = aggregators.get(named);
    if (aggregator == null) {
      Agg.CombineState<? super AI, AA, AO> state = new Agg.CombineState<>(combineFn);
      accum.add(new Agg(named, state));
      aggregator = new SparkAggregator(state);
      aggregators.put(named, aggregator);
    }
    return aggregator;
  }

  private static class SparkAggregator<VI> implements Aggregator<VI> {
    private final Agg.State<VI, ?, ?> state;

    public SparkAggregator(Agg.State<VI, ?, ?> state) {
      this.state = state;
    }

    @Override
    public void addValue(VI vi) {
      state.update(vi);
    }
  }
}
