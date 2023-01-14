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
package org.apache.beam.runners.spark.structuredstreaming.aggregators;

import org.apache.spark.util.AccumulatorV2;

/** {@link AccumulatorV2} implementation for {@link NamedAggregators}. */
public class NamedAggregatorsAccumulator extends AccumulatorV2<NamedAggregators, NamedAggregators> {
  private static final NamedAggregators empty = new NamedAggregators();

  private NamedAggregators value;

  public NamedAggregatorsAccumulator(NamedAggregators value) {
    this.value = value;
  }

  @Override
  public boolean isZero() {
    return value.equals(empty);
  }

  @Override
  public NamedAggregatorsAccumulator copy() {
    NamedAggregators newContainer = new NamedAggregators();
    newContainer.merge(value);
    return new NamedAggregatorsAccumulator(newContainer);
  }

  @Override
  public void reset() {
    this.value = new NamedAggregators();
  }

  @Override
  public void add(NamedAggregators other) {
    this.value.merge(other);
  }

  @Override
  public void merge(AccumulatorV2<NamedAggregators, NamedAggregators> other) {
    this.value.merge(other.value());
  }

  @Override
  public NamedAggregators value() {
    return this.value;
  }
}
