/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterators;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;
import java.util.Objects;

class UnaryFunctorWrapper<WID extends Window, IN, OUT>
        implements FlatMapFunction<SparkElement<WID, IN>, SparkElement<WID, OUT>> {

  private final UnaryFunctor<IN, OUT> functor;
  private final AccumulatorProvider accumulators;

  private transient FunctionCollectorMem<OUT> cachedCollector;

  public UnaryFunctorWrapper(UnaryFunctor<IN, OUT> functor,
                             AccumulatorProvider accumulators) {
    this.functor = Objects.requireNonNull(functor);
    this.accumulators = Objects.requireNonNull(accumulators);
  }

  @Override
  public Iterator<SparkElement<WID, OUT>> call(SparkElement<WID, IN> elem) {
    final WID window = elem.getWindow();
    final long timestamp = getTimestamp(elem);

    FunctionCollectorMem<OUT> collector = getContext();

    // setup user collector
    collector.clear();
    collector.setWindow(window);

    functor.apply(elem.getElement(), collector);

    // wrap output in WindowedElement
    return Iterators.transform(collector.getOutputIterator(),
            e -> new SparkElement<>(window, timestamp, e));
  }

  protected long getTimestamp(SparkElement<WID, IN> elem) {
    return elem.getTimestamp();
  }

  private FunctionCollectorMem<OUT> getContext() {
    if (cachedCollector == null) {
      cachedCollector = new FunctionCollectorMem<>(accumulators);
    }
    return cachedCollector;
  }
}
