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

package org.apache.beam.sdk.extensions.euphoria.beam;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Translation of accumulators to {@link Metrics}. Metrics from same type of accumulator will have
 * same namespace and will differ only in given name. */
public class BeamAccumulatorProvider implements AccumulatorProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BeamAccumulatorProvider.class);
  private final Map<String, Counter> counterMap;
  private final Map<String, Timer> timerMap;
  private final Map<String, Histogram> histogramMap;

  private BeamAccumulatorProvider() {
    counterMap = new ConcurrentHashMap<>();
    timerMap = new ConcurrentHashMap<>();
    histogramMap = new ConcurrentHashMap<>();
  }

  public static Factory getFactory() {
    return Factory.get();
  }

  @Override
  public Counter getCounter(String name) {
    return counterMap.computeIfAbsent(name, BeamMetricsCounter::new);
  }

  @Override
  public Histogram getHistogram(String name) {
    return histogramMap.computeIfAbsent(name, BeamMetricsHistogram::new);
  }

  @Override
  public Timer getTimer(String name) {
    return timerMap.computeIfAbsent(name, BeamMetricsTimer::new);
  }

  // ------------------------

  /** AccumulatorProvider Factory. */
  public static class Factory implements AccumulatorProvider.Factory {

    private static final BeamAccumulatorProvider.Factory INSTANCE =
        new BeamAccumulatorProvider.Factory();

    private static final AccumulatorProvider PROVIDER = new BeamAccumulatorProvider();

    private static final AtomicBoolean isLogged = new AtomicBoolean();

    private Factory() {}

    public static BeamAccumulatorProvider.Factory get() {
      return INSTANCE;
    }

    @Override
    public AccumulatorProvider create(Settings settings) {
      if (isLogged.compareAndSet(false, true)) {
        LOG.warn("Using accumulators with BeamAccumulatorProvider");
      }
      return PROVIDER;
    }
  }

  // ------------------------

  /** Implementation of Counter via {@link org.apache.beam.sdk.metrics.Counter}. */
  public static class BeamMetricsCounter extends BeamMetrics implements Counter {

    public BeamMetricsCounter(String name) {
      super(name);
    }

    @Override
    public void increment(long value) {
      Metrics.counter(this.getClass(), getName()).inc(value);
    }

    @Override
    public void increment() {
      Metrics.counter(this.getClass(), getName()).inc();
    }
  }

  /** Implementation of Histrogram via {@link org.apache.beam.sdk.metrics.Distribution}. */
  public static class BeamMetricsHistogram extends BeamMetrics implements Histogram {

    public BeamMetricsHistogram(String name) {
      super(name);
    }

    @Override
    public void add(long value) {
      Metrics.distribution(this.getClass(), getName()).update(value);
    }

    @Override
    public void add(long value, long times) {
      LongStream.range(0, times)
          .forEach(i -> Metrics.distribution(this.getClass(), getName()).update(value));
    }
  }

  /** Implementation of Timer via {@link org.apache.beam.sdk.metrics.Distribution}. */
  public static class BeamMetricsTimer extends BeamMetrics implements Timer {

    public BeamMetricsTimer(String name) {
      super(name);
    }

    @Override
    public void add(Duration duration) {
      Metrics.distribution(this.getClass(), getName()).update(duration.getSeconds());
    }
  }

  abstract static class BeamMetrics {

    private final String name;

    protected BeamMetrics(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
