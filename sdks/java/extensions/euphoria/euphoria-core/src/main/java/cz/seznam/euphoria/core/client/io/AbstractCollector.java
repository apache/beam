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
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.accumulators.Counter;
import cz.seznam.euphoria.core.client.accumulators.Histogram;
import cz.seznam.euphoria.core.client.accumulators.Timer;

/**
 * Abstract implementation of a collector supporting access to accumulators.
 */
public abstract class AbstractCollector<T> implements Collector<T> {

  private final AccumulatorProvider accumulators;

  public AbstractCollector(AccumulatorProvider accumulators) {
    this.accumulators = accumulators;
  }

  @Override
  public Counter getCounter(String name) {
    return accumulators.getCounter(name);
  }

  @Override
  public Histogram getHistogram(String name) {
    return accumulators.getHistogram(name);
  }

  @Override
  public Timer getTimer(String name) {
    return accumulators.getTimer(name);
  }
}
