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
package org.apache.beam.sdk.extensions.sql.udf;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An aggregate function that can be executed as part of a SQL query.
 *
 * <p>AggregateFn contains a subset of the functionality of {@code
 * org.apache.beam.sdk.transforms.Combine.CombineFn}.
 *
 * @param <InputT> type of input values
 * @param <AccumT> type of mutable accumulator values
 * @param <OutputT> type of output values
 */
public interface AggregateFn<
    InputT extends @Nullable Object,
    AccumT extends @Nullable Object,
    OutputT extends @Nullable Object> {

  /**
   * Returns a new, mutable accumulator value, representing the accumulation of zero input values.
   */
  AccumT createAccumulator();

  /**
   * Adds the given input value to the given accumulator, returning the new accumulator value.
   *
   * @param mutableAccumulator may be modified and returned for efficiency
   * @param input should not be mutated
   */
  AccumT addInput(AccumT mutableAccumulator, InputT input);

  /**
   * Returns an accumulator representing the accumulation of all the input values accumulated in the
   * merging accumulators.
   *
   * @param mutableAccumulator This accumulator may be modified and returned for efficiency.
   * @param immutableAccumulators These other accumulators should not be mutated, because they may
   *     be shared with other code and mutating them could lead to incorrect results or data
   *     corruption.
   */
  AccumT mergeAccumulators(AccumT mutableAccumulator, Iterable<AccumT> immutableAccumulators);

  /**
   * Returns the output value that is the result of combining all the input values represented by
   * the given accumulator.
   *
   * @param mutableAccumulator can be modified for efficiency
   */
  OutputT extractOutput(AccumT mutableAccumulator);
}
