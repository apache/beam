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
package org.apache.beam.sdk.state;

import javax.annotation.Nonnull;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * A {@link ReadableState} cell defined by a {@link CombineFn}, accepting multiple input values,
 * combining them as specified into accumulators, and producing a single output value.
 *
 * <p>Implementations of this form of state are expected to implement {@link #add} efficiently, not
 * via a sequence of read-modify-write.
 *
 * @param <InputT> the type of values added to the state
 * @param <AccumT> the type of accumulator
 * @param <OutputT> the type of value extracted from the state
 */
@Experimental(Kind.STATE)
public interface CombiningState<InputT, AccumT, OutputT> extends GroupingState<InputT, OutputT> {

  @Override
  @Nonnull
  OutputT read();

  /**
   * Read the merged accumulator for this state cell. It is implied that reading the state involves
   * reading the accumulator, so {@link #readLater} is sufficient to prefetch for this.
   */
  AccumT getAccum();

  /**
   * Add an accumulator to this state cell. Depending on implementation this may immediately merge
   * it with the previous accumulator, or may buffer this accumulator for a future merge.
   */
  void addAccum(AccumT accum);

  /** Merge the given accumulators according to the underlying {@link CombineFn}. */
  AccumT mergeAccumulators(Iterable<AccumT> accumulators);

  @Override
  CombiningState<InputT, AccumT, OutputT> readLater();
}
