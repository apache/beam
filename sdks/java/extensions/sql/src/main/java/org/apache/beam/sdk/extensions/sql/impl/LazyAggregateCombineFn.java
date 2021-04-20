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
package org.apache.beam.sdk.extensions.sql.impl;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.udf.AggregateFn;
import org.apache.beam.sdk.transforms.Combine;

/**
 * {@link org.apache.beam.sdk.transforms.Combine.CombineFn} that wraps an {@link AggregateFn}. The
 * {@link AggregateFn} is lazily instantiated so it doesn't have to be serialized/deserialized.
 */
public class LazyAggregateCombineFn<InputT, AccumT, OutputT>
    extends Combine.CombineFn<InputT, AccumT, OutputT> {
  private final List<String> functionPath;
  private final String jarPath;
  private transient @Nullable AggregateFn<InputT, AccumT, OutputT> aggregateFn = null;

  public LazyAggregateCombineFn(List<String> functionPath, String jarPath) {
    this.functionPath = functionPath;
    this.jarPath = jarPath;
  }

  private AggregateFn<InputT, AccumT, OutputT> getAggregateFn() {
    if (aggregateFn == null) {
      JavaUdfLoader loader = new JavaUdfLoader();
      aggregateFn = loader.loadAggregateFunction(functionPath, jarPath);
    }
    return aggregateFn;
  }

  @Override
  public AccumT createAccumulator() {
    return getAggregateFn().createAccumulator();
  }

  @Override
  public AccumT addInput(AccumT mutableAccumulator, InputT input) {
    return getAggregateFn().addInput(mutableAccumulator, input);
  }

  @Override
  public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
    Iterator<AccumT> it = accumulators.iterator();
    AccumT first = it.next();
    it.remove();
    return getAggregateFn().mergeAccumulators(first, accumulators);
  }

  @Override
  public OutputT extractOutput(AccumT accumulator) {
    return getAggregateFn().extractOutput(accumulator);
  }
}
