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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.common.Counter;
import org.apache.beam.sdk.util.common.CounterProvider;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.util.common.CounterSet.AddCounterMutator;
import com.google.common.annotations.VisibleForTesting;

/**
 * An implementation of the {@code Aggregator} interface that uses a
 * {@link Counter} as the underlying representation. Supports {@link CombineFn}s
 * from the {@link Sum}, {@link Min} and {@link Max} classes.
 *
 * @param <InputT> the type of input values
 * @param <AccumT> the type of accumulator values
 * @param <OutputT> the type of output value
 */
public class CounterAggregator<InputT, AccumT, OutputT>
    implements Aggregator<InputT, OutputT> {

  private static class CounterAggregatorFactory implements AggregatorFactory {
    private final AddCounterMutator addCounterMutator;

    private CounterAggregatorFactory(CounterSet.AddCounterMutator addCounterMutator) {
      this.addCounterMutator = addCounterMutator;
    }

    @Override
    public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
        Class<?> fnClass, ExecutionContext.StepContext stepContext,
        String userName, CombineFn<InputT, AccumT, OutputT> combine) {
      boolean isSystem = fnClass.isAnnotationPresent(SystemDoFnInternal.class);
      String mangledName = (isSystem ? "" : "user-") + stepContext.getStepName() + "-" + userName;

      return new CounterAggregator<>(mangledName, combine, addCounterMutator);
    }
  }

  private final Counter<InputT> counter;
  private final CombineFn<InputT, AccumT, OutputT> combiner;

  /**
   * Create a factory for producing {@link CounterAggregator CounterAggregators} backed by the given
   * {@link CounterSet.AddCounterMutator}.
   */
  public static AggregatorFactory factoryFor(
      CounterSet.AddCounterMutator addCounterMutator) {
    return new CounterAggregatorFactory(addCounterMutator);
  }

  /**
   * Constructs a new aggregator with the given name and aggregation logic
   * specified in the CombineFn argument. The underlying counter is
   * automatically added into the provided CounterSet.
   *
   *  <p>If a counter with the same name already exists, it will be reused, as
   * long as it has the same type.
   */
  @VisibleForTesting CounterAggregator(
      String name, CombineFn<? super InputT, AccumT, OutputT> combiner,
      CounterSet.AddCounterMutator addCounterMutator) {
    // Safe contravariant cast
    this(constructCounter(name, combiner), addCounterMutator,
        (CombineFn<InputT, AccumT, OutputT>) combiner);
  }

  private CounterAggregator(Counter<InputT> counter,
      CounterSet.AddCounterMutator addCounterMutator,
      CombineFn<InputT, AccumT, OutputT> combiner) {
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
  public void addValue(InputT value) {
    counter.addValue(value);
  }

  @Override
  public String getName() {
    return counter.getFlatName();
  }

  @Override
  public CombineFn<InputT, ?, OutputT> getCombineFn() {
    return combiner;
  }
}
