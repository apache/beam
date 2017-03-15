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

package org.apache.beam.runners.spark.aggregators;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.runners.core.AggregatorFactory;
import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.spark.Accumulator;

/**
 * A utility class for handling Beam {@link Aggregator}s.
 */
public class SparkAggregators {

  private static <T> AggregatorValues<T> valueOf(final Accumulator<NamedAggregators> accum,
                                                 final Aggregator<?, T> aggregator) {
    @SuppressWarnings("unchecked")
    Class<T> valueType = (Class<T>) aggregator.getCombineFn().getOutputType().getRawType();
    final T value = valueOf(accum, aggregator.getName(), valueType);

    return new AggregatorValues<T>() {

      @Override
      public Collection<T> getValues() {
        return ImmutableList.of(value);
      }

      @Override
      public Map<String, T> getValuesAtSteps() {
        throw new UnsupportedOperationException("getValuesAtSteps is not supported.");
      }
    };
  }

  private static <T> T valueOf(final Accumulator<NamedAggregators> accum,
                               final String aggregatorName,
                               final Class<T> typeClass) {
    return accum.value().getValue(aggregatorName, typeClass);
  }

  /**
   * Retrieves the value of an aggregator from a SparkContext instance.
   *
   * @param aggregator The aggregator whose value to retrieve
   * @param <T> The type of the aggregator's output
   * @return The value of the aggregator
   */
  public static <T> AggregatorValues<T> valueOf(final Aggregator<?, T> aggregator) {
    return valueOf(AggregatorsAccumulator.getInstance(), aggregator);
  }

  /**
   * Retrieves the value of an aggregator from a SparkContext instance.
   *
   * @param name Name of the aggregator to retrieve the value of.
   * @param typeClass      Type class of value to be retrieved.
   * @param <T>            Type of object to be returned.
   * @return The value of the aggregator.
   */
  public static <T> T valueOf(final String name, final Class<T> typeClass) {
    return valueOf(AggregatorsAccumulator.getInstance(), name, typeClass);
  }

  /**
   * An implementation of {@link AggregatorFactory} for the SparkRunner.
   */
  public static class Factory implements AggregatorFactory {

    private final SparkRuntimeContext runtimeContext;
    private final Accumulator<NamedAggregators> accumulator;

    public Factory(SparkRuntimeContext runtimeContext, Accumulator<NamedAggregators> accumulator) {
      this.runtimeContext = runtimeContext;
      this.accumulator = accumulator;
    }

    @Override
    public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
        Class<?> fnClass,
        ExecutionContext.StepContext stepContext,
        String aggregatorName,
        Combine.CombineFn<InputT, AccumT, OutputT> combine) {

      return runtimeContext.createAggregator(accumulator, aggregatorName, combine);
    }
  }

}
