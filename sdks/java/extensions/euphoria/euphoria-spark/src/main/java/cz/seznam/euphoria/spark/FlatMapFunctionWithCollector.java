/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Objects;

class FlatMapFunctionWithCollector<IN, OUT> implements FlatMapFunction<IN, OUT> {

  private final InnerFunction<IN, OUT> function;
  private final AccumulatorProvider accumulators;

  private transient FunctionCollectorMem<OUT> cachedCollector;

  FlatMapFunctionWithCollector(InnerFunction<IN, OUT> function,
                               AccumulatorProvider accumulators) {
    this.function = Objects.requireNonNull(function);
    this.accumulators = Objects.requireNonNull(accumulators);
  }

  @Override
  public Iterator<OUT> call(IN in) throws Exception {
    return function.call(in, getCollector());
  }

  FunctionCollectorMem<OUT> getCollector() {
    if (cachedCollector == null) {
      cachedCollector = new FunctionCollectorMem<>(accumulators);
    }
    return cachedCollector;
  }

  interface InnerFunction<IN, OUT> extends Serializable {
    Iterator<OUT> call(IN in, FunctionCollectorMem<OUT> collector) throws Exception;
  }
}
