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

import org.apache.beam.runners.spark.aggregators.AggAccumParam;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AggregatorValues;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * The SparkRuntimeContext allows us to define useful features on the client side before our
 * data flow program is launched.
 */
public class SparkRuntimeContext implements Serializable {
  /**
   * An accumulator that is a map from names to aggregators.
   */
  private final Accumulator<NamedAggregators> accum;

  private final String serializedPipelineOptions;

  /**
   * Map fo names to dataflow aggregators.
   */
  private final Map<String, Aggregator<?, ?>> aggregators = new HashMap<>();
  private transient CoderRegistry coderRegistry;

  SparkRuntimeContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.accum = jsc.accumulator(new NamedAggregators(), new AggAccumParam());
    this.serializedPipelineOptions = serializePipelineOptions(pipeline.getOptions());
  }

  private static String serializePipelineOptions(PipelineOptions pipelineOptions) {
    try {
      return new ObjectMapper().writeValueAsString(pipelineOptions);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize the pipeline options.", e);
    }
  }

  private static PipelineOptions deserializePipelineOptions(String serializedPipelineOptions) {
    try {
      return new ObjectMapper().readValue(serializedPipelineOptions, PipelineOptions.class);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize the pipeline options.", e);
    }
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

  public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator) {
    @SuppressWarnings("unchecked")
    Class<T> aggValueClass = (Class<T>) aggregator.getCombineFn().getOutputType().getRawType();
    final T aggregatorValue = getAggregatorValue(aggregator.getName(), aggValueClass);
    return new AggregatorValues<T>() {
      @Override
      public Collection<T> getValues() {
        return ImmutableList.of(aggregatorValue);
      }

      @Override
      public Map<String, T> getValuesAtSteps() {
        throw new UnsupportedOperationException("getValuesAtSteps is not supported.");
      }
    };
  }

  public synchronized PipelineOptions getPipelineOptions() {
    return deserializePipelineOptions(serializedPipelineOptions);
  }

  /**
   * Creates and aggregator and associates it with the specified name.
   *
   * @param named     Name of aggregator.
   * @param combineFn Combine function used in aggregation.
   * @param <InputT>      Type of inputs to aggregator.
   * @param <InterT>   Intermediate data type
   * @param <OutputT>     Type of aggregator outputs.
   * @return Specified aggregator
   */
  public synchronized <InputT, InterT, OutputT> Aggregator<InputT, OutputT> createAggregator(
      String named,
      Combine.CombineFn<? super InputT, InterT, OutputT> combineFn) {
    @SuppressWarnings("unchecked")
    Aggregator<InputT, OutputT> aggregator = (Aggregator<InputT, OutputT>) aggregators.get(named);
    if (aggregator == null) {
      @SuppressWarnings("unchecked")
      NamedAggregators.CombineFunctionState<InputT, InterT, OutputT> state =
          new NamedAggregators.CombineFunctionState<>(
              (Combine.CombineFn<InputT, InterT, OutputT>) combineFn,
              (Coder<InputT>) getCoder(combineFn),
              this);
      accum.add(new NamedAggregators(named, state));
      aggregator = new SparkAggregator<>(named, state);
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

  private Coder<?> getCoder(Combine.CombineFn<?, ?, ?> combiner) {
    try {
      if (combiner.getClass() == Sum.SumIntegerFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Integer.class));
      } else if (combiner.getClass() == Sum.SumLongFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Long.class));
      } else if (combiner.getClass() == Sum.SumDoubleFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Double.class));
      } else if (combiner.getClass() == Min.MinIntegerFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Integer.class));
      } else if (combiner.getClass() == Min.MinLongFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Long.class));
      } else if (combiner.getClass() == Min.MinDoubleFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Double.class));
      } else if (combiner.getClass() == Max.MaxIntegerFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Integer.class));
      } else if (combiner.getClass() == Max.MaxLongFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Long.class));
      } else if (combiner.getClass() == Max.MaxDoubleFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Double.class));
      } else {
        throw new IllegalArgumentException("unsupported combiner in Aggregator: "
            + combiner.getClass().getName());
      }
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException("Could not determine default coder for combiner", e);
    }
  }

  /**
   * Initialize spark aggregators exactly once.
   *
   * @param <InputT> Type of element fed in to aggregator.
   */
  private static class SparkAggregator<InputT, OutputT>
      implements Aggregator<InputT, OutputT>, Serializable {
    private final String name;
    private final NamedAggregators.State<InputT, ?, OutputT> state;

    SparkAggregator(String name, NamedAggregators.State<InputT, ?, OutputT> state) {
      this.name = name;
      this.state = state;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void addValue(InputT elem) {
      state.update(elem);
    }

    @Override
    public Combine.CombineFn<InputT, ?, OutputT> getCombineFn() {
      return state.getCombineFn();
    }
  }
}
