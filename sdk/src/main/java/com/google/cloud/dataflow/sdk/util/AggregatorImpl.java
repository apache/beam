/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MAX;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MIN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

/**
 * An implementation of the {@code Aggregator} interface.
 *
 * @param <VI> the type of input values
 * @param <VA> the type of accumulator values
 * @param <VO> the type of output value
 */
public class AggregatorImpl<VI, VA, VO> implements Aggregator<VI> {

  private final Counter<VI> counter;

  /*
   * Constructs a new aggregator with the given name and aggregation logic
   * specified in the CombineFn argument. The underlying counter is
   * automatically added into the provided CounterSet.
   *
   * <p> If a counter with the same name already exists, it will be
   * reused, as long as it has the same type.
   */
  public AggregatorImpl(String name,
                        CombineFn<? super VI, VA, VO> combiner,
                        CounterSet.AddCounterMutator addCounterMutator) {
    this((Counter<VI>) constructCounter(name, combiner), addCounterMutator);
  }

  /*
   * Constructs a new aggregator with the given name and aggregation logic
   * specified in the SerializableFunction argument. The underlying counter is
   * automatically added into the provided CounterSet.
   *
   * <p> If a counter with the same name already exists, it will be
   * reused, as long as it has the same type.
   */
  public AggregatorImpl(String name,
                        SerializableFunction<Iterable<VI>, VO> combiner,
                        CounterSet.AddCounterMutator addCounterMutator) {
    this((Counter<VI>) constructCounter(name, combiner), addCounterMutator);
  }

  private AggregatorImpl(Counter<VI> counter,
                         CounterSet.AddCounterMutator addCounterMutator) {
    try {
      this.counter = addCounterMutator.addCounter(counter);
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(
          "aggregator's name collides with an existing aggregator "
          + "or system-provided counter of an incompatible type");
    }
  }

  private static Counter<?> constructCounter(String name, Object combiner) {
    if (combiner.getClass() == Sum.SumIntegerFn.class) {
      return Counter.ints(name, SUM);
    } else if (combiner.getClass() == Sum.SumLongFn.class) {
      return Counter.longs(name, SUM);
    } else if (combiner.getClass() == Sum.SumDoubleFn.class) {
      return Counter.doubles(name, SUM);
    } else if (combiner.getClass() == Min.MinIntegerFn.class) {
      return Counter.ints(name, MIN);
    } else if (combiner.getClass() == Min.MinLongFn.class) {
      return Counter.longs(name, MIN);
    } else if (combiner.getClass() == Min.MinDoubleFn.class) {
      return Counter.doubles(name, MIN);
    } else if (combiner.getClass() == Max.MaxIntegerFn.class) {
      return Counter.ints(name, MAX);
    } else if (combiner.getClass() == Max.MaxLongFn.class) {
      return Counter.longs(name, MAX);
    } else if (combiner.getClass() == Max.MaxDoubleFn.class) {
      return Counter.doubles(name, MAX);
    } else {
      throw new IllegalArgumentException("unsupported combiner in Aggregator: "
        + combiner.getClass().getName());
    }
  }

  @Override
  public void addValue(VI value) {
    counter.addValue(value);
  }
}
