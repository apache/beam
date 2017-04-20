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
package org.apache.beam.runners.flink.translation.wrappers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * Wrapper that wraps a {@link org.apache.beam.sdk.transforms.Combine.CombineFn}
 * in a Flink {@link org.apache.flink.api.common.accumulators.Accumulator} for using
 * the function as an aggregator in a {@link org.apache.beam.sdk.transforms.ParDo}
 * operation.
 */
public class SerializableFnAggregatorWrapper<InputT, OutputT>
    implements Aggregator<InputT, OutputT>, Accumulator<InputT, Serializable> {

  private OutputT aa;
  private Combine.CombineFn<InputT, ?, OutputT> combiner;

  public SerializableFnAggregatorWrapper(Combine.CombineFn<InputT, ?, OutputT> combiner) {
    this.combiner = combiner;
    resetLocal();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void add(InputT value) {
    this.aa = combiner.apply(ImmutableList.of((InputT) aa, value));
  }

  @Override
  public Serializable getLocalValue() {
    return (Serializable) aa;
  }

  @Override
  public void resetLocal() {
    this.aa = combiner.apply(ImmutableList.<InputT>of());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void merge(Accumulator<InputT, Serializable> other) {
    this.aa = combiner.apply(ImmutableList.of((InputT) aa, (InputT) other.getLocalValue()));
  }

  @Override
  public void addValue(InputT value) {
    add(value);
  }

  @Override
  public String getName() {
    return "Aggregator :" + combiner.toString();
  }

  @Override
  public Combine.CombineFn<InputT, ?, OutputT> getCombineFn() {
    return combiner;
  }

  @Override
  public Accumulator<InputT, Serializable> clone() {
    try {
      super.clone();
    } catch (CloneNotSupportedException e) {
      // Flink Accumulators cannot throw CloneNotSupportedException, work around that.
      throw new RuntimeException(e);
    }

    // copy it by merging
    OutputT resultCopy = combiner.apply(Lists.newArrayList((InputT) aa));
    SerializableFnAggregatorWrapper<InputT, OutputT> result = new
        SerializableFnAggregatorWrapper<>(combiner);

    result.aa = resultCopy;
    return result;
  }
}
