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
package cz.seznam.euphoria.executor.local;

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.accumulators.Counter;
import cz.seznam.euphoria.core.client.accumulators.Histogram;
import cz.seznam.euphoria.core.client.accumulators.Timer;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.util.Settings;

import java.util.Objects;
import java.util.function.Supplier;

class WindowedElementCollector<T> implements Context, Collector<T> {
  private final cz.seznam.euphoria.executor.local.Collector<Datum> wrap;
  private final Supplier<Long> stampSupplier;
  private final AccumulatorProvider.Factory accumulatorFactory;
  private final Settings settings;

  private AccumulatorProvider accumulators;
  protected Window window;

  WindowedElementCollector(cz.seznam.euphoria.executor.local.Collector<Datum> wrap,
                           Supplier<Long> stampSupplier,
                           AccumulatorProvider.Factory accumulatorFactory,
                           Settings settings) {
    this.wrap = Objects.requireNonNull(wrap);
    this.stampSupplier = stampSupplier;
    this.accumulatorFactory = Objects.requireNonNull(accumulatorFactory);
    this.settings = Objects.requireNonNull(settings);
  }

  @Override
  public void collect(T elem) {
    long stamp = Math.min(stampSupplier.get(), window.maxTimestamp() - 1);

    wrap.collect(Datum.of(window, elem, stamp));
  }

  @Override
  public Context asContext() {
    return this;
  }

  void setWindow(Window window) {
    this.window = window;
  }

  @Override
  public Window<?> getWindow() {
    return window;
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

  private AccumulatorProvider getAccumulatorProvider() {
    if (accumulators == null) {
      accumulators = accumulatorFactory.create(settings);
    }

    return accumulators;
  }
}
