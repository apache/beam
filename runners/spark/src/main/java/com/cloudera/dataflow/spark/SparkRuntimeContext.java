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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.common.reflect.TypeToken;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;

import com.cloudera.dataflow.spark.aggregators.AggAccumParam;
import com.cloudera.dataflow.spark.aggregators.NamedAggregators;

/**
 * The SparkRuntimeContext allows us to define useful features on the client side before our
 * data flow program is launched.
 */
public class SparkRuntimeContext implements Serializable {
  /**
   * An accumulator that is a map from names to aggregators.
   */
  private final Accumulator<NamedAggregators> accum;
  /**
   * Map fo names to dataflow aggregators.
   */
  private final Map<String, Aggregator<?>> aggregators = new HashMap<>();
  private transient CoderRegistry coderRegistry;

  SparkRuntimeContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.accum = jsc.accumulator(new NamedAggregators(), new AggAccumParam());
  }

  /**
   * Retrieves corresponding value of an aggregator.
   *
   * @param aggregatorName Name of the aggregator to retrieve the value of.
   * @param typeClass      Type class of value to be retrieved.
   * @param <T>            Type of object to be returned.
   * @return The value of the aggregator.
   */
  public <T> T getAggregatorValue(String aggregatorName, Class<T> typeClass) {
    return accum.value().getValue(aggregatorName, typeClass);
  }

  public synchronized PipelineOptions getPipelineOptions() {
    //TODO: Support this.
    throw new UnsupportedOperationException("getPipelineOptions is not yet supported.");
  }

  /**
   * Creates and aggregator and associates it with the specified name.
   *
   * @param named Name of aggregator.
   * @param sfunc Serializable function used in aggregation.
   * @param <IN>  Type of inputs to aggregator.
   * @param <OUT> Type of aggregator outputs.
   * @return Specified aggregator
   */
  public synchronized <IN, OUT> Aggregator<IN> createAggregator(
      String named,
      SerializableFunction<Iterable<IN>, OUT> sfunc) {
    @SuppressWarnings("unchecked")
    Aggregator<IN> aggregator = (Aggregator<IN>) aggregators.get(named);
    if (aggregator == null) {
      NamedAggregators.SerFunctionState<IN, OUT> state = new NamedAggregators
          .SerFunctionState<>(sfunc);
      accum.add(new NamedAggregators(named, state));
      aggregator = new SparkAggregator<>(state);
      aggregators.put(named, aggregator);
    }
    return aggregator;
  }

  /**
   * Creates and aggregator and associates it with the specified name.
   *
   * @param named     Name of aggregator.
   * @param combineFn Combine function used in aggregation.
   * @param <IN>      Type of inputs to aggregator.
   * @param <INTER>   Intermediate data type
   * @param <OUT>     Type of aggregator outputs.
   * @return Specified aggregator
   */
  public synchronized <IN, INTER, OUT> Aggregator<IN> createAggregator(
      String named,
      Combine.CombineFn<? super IN, INTER, OUT> combineFn) {
    @SuppressWarnings("unchecked")
    Aggregator<IN> aggregator = (Aggregator<IN>) aggregators.get(named);
    if (aggregator == null) {
      @SuppressWarnings("unchecked")
      NamedAggregators.CombineFunctionState<IN, INTER, OUT> state =
          new NamedAggregators.CombineFunctionState<>(
              (Combine.CombineFn<IN, INTER, OUT>) combineFn,
              (Coder<IN>) getCoder(combineFn),
              this);
      accum.add(new NamedAggregators(named, state));
      aggregator = new SparkAggregator<>(state);
      aggregators.put(named, aggregator);
    }
    return aggregator;
  }

  public CoderRegistry getCoderRegistry() {
    if (coderRegistry == null) {
      coderRegistry = new CoderRegistry();
      coderRegistry.registerStandardCoders();
    }
    return coderRegistry;
  }

  private Coder getCoder(Combine.CombineFn<?, ?, ?> combiner) {
    if (combiner.getClass() == Sum.SumIntegerFn.class) {
      return getCoderRegistry().getDefaultCoder(TypeToken.of(Integer.class));
    } else if (combiner.getClass() == Sum.SumLongFn.class) {
      return getCoderRegistry().getDefaultCoder(TypeToken.of(Long.class));
    } else if (combiner.getClass() == Sum.SumDoubleFn.class) {
      return getCoderRegistry().getDefaultCoder(TypeToken.of(Double.class));
    } else if (combiner.getClass() == Min.MinIntegerFn.class) {
      return getCoderRegistry().getDefaultCoder(TypeToken.of(Integer.class));
    } else if (combiner.getClass() == Min.MinLongFn.class) {
      return getCoderRegistry().getDefaultCoder(TypeToken.of(Long.class));
    } else if (combiner.getClass() == Min.MinDoubleFn.class) {
      return getCoderRegistry().getDefaultCoder(TypeToken.of(Double.class));
    } else if (combiner.getClass() == Max.MaxIntegerFn.class) {
      return getCoderRegistry().getDefaultCoder(TypeToken.of(Integer.class));
    } else if (combiner.getClass() == Max.MaxLongFn.class) {
      return getCoderRegistry().getDefaultCoder(TypeToken.of(Long.class));
    } else if (combiner.getClass() == Max.MaxDoubleFn.class) {
      return getCoderRegistry().getDefaultCoder(TypeToken.of(Double.class));
    } else {
      throw new IllegalArgumentException("unsupported combiner in Aggregator: "
              + combiner.getClass().getName());
    }
  }

  /**
   * Initialize spark aggregators exactly once.
   *
   * @param <IN> Type of element fed in to aggregator.
   */
  private static class SparkAggregator<IN> implements Aggregator<IN>, Serializable {
    private final NamedAggregators.State<IN, ?, ?> state;

    SparkAggregator(NamedAggregators.State<IN, ?, ?> state) {
      this.state = state;
    }

    @Override
    public void addValue(IN elem) {
      state.update(elem);
    }
  }
}
