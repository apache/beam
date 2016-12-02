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

package org.apache.beam.runners.spark;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * A utility class for retrieving aggregator values.
 */
class SparkAggregators {

  private static <T> AggregatorValues<T> valueOf(final Accumulator<NamedAggregators> accum,
                                                 final Aggregator<?, T> aggregator) {
    @SuppressWarnings("unchecked") final
    Class<T> aggValueClass = (Class<T>) aggregator.getCombineFn().getOutputType().getRawType();
    final T aggregatorValue = valueOf(accum, aggregator.getName(), aggValueClass);
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

  private static <T> T valueOf(final Accumulator<NamedAggregators> accum,
                               final String aggregatorName,
                               final Class<T> typeClass) {
    return accum.value().getValue(aggregatorName, typeClass);
  }

  /**
   * Retrieves the value of an aggregator from a SparkContext instance.
   *
   * @param aggregator The aggregator whose value to retrieve
   * @param javaSparkContext The SparkContext instance
   * @param <T> The type of the aggregator's output
   * @return The value of the aggregator
   */
  public static <T> AggregatorValues<T> valueOf(final Aggregator<?, T> aggregator,
                                                final JavaSparkContext javaSparkContext) {
    return valueOf(AccumulatorSingleton.getInstance(javaSparkContext), aggregator);
  }

  /**
   * Retrieves the value of an aggregator from a SparkContext instance.
   *
   * @param name Name of the aggregator to retrieve the value of.
   * @param typeClass      Type class of value to be retrieved.
   * @param <T>            Type of object to be returned.
   * @return The value of the aggregator.
   */
  public static <T> T valueOf(final String name,
                              final Class<T> typeClass,
                              final JavaSparkContext javaSparkContext) {
    return valueOf(AccumulatorSingleton.getInstance(javaSparkContext),
                   name,
                   typeClass);
  }
}
