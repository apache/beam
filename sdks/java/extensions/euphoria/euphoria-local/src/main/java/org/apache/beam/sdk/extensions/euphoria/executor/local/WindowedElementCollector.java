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
package org.apache.beam.sdk.extensions.euphoria.executor.local;

import java.util.Objects;
import java.util.function.Supplier;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Context;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;

class WindowedElementCollector<T> implements Context, Collector<T> {
  private final org.apache.beam.sdk.extensions.euphoria.executor.local.Collector<Datum> wrap;
  private final Supplier<Long> stampSupplier;
  private final AccumulatorProvider.Factory accumulatorFactory;
  private final Settings settings;
  protected Window window;
  private AccumulatorProvider accumulators;

  WindowedElementCollector(
      org.apache.beam.sdk.extensions.euphoria.executor.local.Collector<Datum> wrap,
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

  @Override
  public Window<?> getWindow() {
    return window;
  }

  void setWindow(Window window) {
    this.window = window;
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
