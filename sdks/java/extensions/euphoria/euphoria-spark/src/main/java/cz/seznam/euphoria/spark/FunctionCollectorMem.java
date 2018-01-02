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
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.Context;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link Collector} that holds all the
 * data in memory.
 */
class FunctionCollectorMem<T> extends FunctionCollector<T> {

  private final List<T> elements = new ArrayList<>(1);

  public FunctionCollectorMem(AccumulatorProvider accumulators) {
    super(accumulators);
  }

  @Override
  public void collect(T elem) {
    elements.add(elem);
  }

  @Override
  public Context asContext() {
    return this;
  }

  /**
   * Clears all stored elements.
   */
  public void clear() {
    elements.clear();
  }

  public Iterator<T> getOutputIterator() {
    // wrap output in WindowedElement
    return elements.iterator();
  }
}
