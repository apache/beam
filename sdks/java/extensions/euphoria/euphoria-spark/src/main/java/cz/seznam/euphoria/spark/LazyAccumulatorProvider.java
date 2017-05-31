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
import cz.seznam.euphoria.core.client.accumulators.Counter;
import cz.seznam.euphoria.core.client.accumulators.Histogram;
import cz.seznam.euphoria.core.client.accumulators.Timer;
import cz.seznam.euphoria.core.util.Settings;

import java.io.Serializable;
import java.util.Objects;

class LazyAccumulatorProvider implements AccumulatorProvider, Serializable {
  private final Factory factory;
  private final Settings settings;

  private transient AccumulatorProvider accumulators;

  LazyAccumulatorProvider(Factory factory, Settings settings) {
    this.factory = Objects.requireNonNull(factory);
    this.settings = Objects.requireNonNull(settings);
  }

  @Override
  public Counter getCounter(String name) {
    return getAccumulatorProvider().getCounter(name);
  }

  @Override
  public Histogram getHistogram(String name) {
    return getAccumulatorProvider().getHistogram(name);
  }

  @Override
  public Timer getTimer(String name) {
    return getAccumulatorProvider().getTimer(name);
  }

  AccumulatorProvider getAccumulatorProvider() {
    if (accumulators == null) {
      accumulators = factory.create(settings);
    }

    return accumulators;
  }
}
