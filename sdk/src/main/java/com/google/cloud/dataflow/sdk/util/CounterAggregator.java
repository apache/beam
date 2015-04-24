/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
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

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterProvider;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

/**
 * An implementation of the {@code Aggregator} interface that uses a
 * {@link Counter} as the underlying representation. Supports {@link CombineFn}s
 * from the {@link Sum}, {@link Min} and {@link Max} classes.
 *
 * @param <VI> the type of input values
 * @param <VA> the type of accumulator values
 * @param <VO> the type of output value
 */
public class CounterAggregator<VI, VA, VO> implements Aggregator<VI, VO> {

  private final Counter<VI> counter;
  private final CombineFn<VI, VA, VO> combiner;

  /**
   * Constructs a new aggregator with the given name and aggregation logic
   * specified in the CombineFn argument. The underlying counter is
   * automatically added into the provided CounterSet.
   *
   *  <p> If a counter with the same name already exists, it will be reused, as
   * long as it has the same type.
   */
  public CounterAggregator(String name, CombineFn<? super VI, VA, VO> combiner,
      CounterSet.AddCounterMutator addCounterMutator) {
    // Safe contravariant cast
    this(constructCounter(name, combiner), addCounterMutator,
        (CombineFn<VI, VA, VO>) combiner);
  }

  private CounterAggregator(Counter<VI> counter,
      CounterSet.AddCounterMutator addCounterMutator,
      CombineFn<VI, VA, VO> combiner) {
    try {
      this.counter = addCounterMutator.addCounter(counter);
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(
          "aggregator's name collides with an existing aggregator "
          + "or system-provided counter of an incompatible type");
    }
    this.combiner = combiner;
  }

  private static <T> Counter<T> constructCounter(String name,
      CombineFn<? super T, ?, ?> combiner) {
    if (combiner instanceof CounterProvider) {
      @SuppressWarnings("unchecked")
      CounterProvider<T> counterProvider = (CounterProvider<T>) combiner;
      return counterProvider.getCounter(name);
    } else {
      throw new IllegalArgumentException("unsupported combiner in Aggregator: "
        + combiner.getClass().getName());
    }
  }

  @Override
  public void addValue(VI value) {
    counter.addValue(value);
  }

  @Override
  public String getName() {
    return counter.getName();
  }

  @Override
  public CombineFn<VI, ?, VO> getCombineFn() {
    return combiner;
  }
}
