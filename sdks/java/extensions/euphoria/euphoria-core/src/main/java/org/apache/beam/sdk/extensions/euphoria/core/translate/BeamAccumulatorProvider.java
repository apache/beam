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

package org.apache.beam.sdk.extensions.euphoria.core.translate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translation of accumulators to {@link Metrics}. Metric's namespace is taken from operator name.
 * So for better orientation in metrics it's recommended specify operator name with method .named().
 */
public class BeamAccumulatorProvider implements AccumulatorProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BeamAccumulatorProvider.class);
  private static final String KEY_METRIC_SEPARATOR = "::";

  private final Map<String, Counter> counterMap = new ConcurrentHashMap<>();
  private final Map<String, Histogram> histogramMap = new ConcurrentHashMap<>();

  private BeamAccumulatorProvider() {}

  public static Factory getFactory() {
    return Factory.get();
  }

  @Override
  public Counter getCounter(String name) {
    throw new UnsupportedOperationException(
        "BeamAccumulatorProvider doesn't support"
            + " getCounter(String name). Please specify namespace and name.");
  }

  @Override
  public Counter getCounter(final String namespace, final String name) {
    return counterMap.computeIfAbsent(
        getMetricKey(namespace, name), key -> new BeamMetricsCounter(namespace, name));
  }

  @Override
  public Histogram getHistogram(String name) {
    throw new UnsupportedOperationException(
        "BeamAccumulatorProvider doesn't support"
            + " getHistogram(String name). Please specify namespace and name.");
  }

  @Override
  public Histogram getHistogram(final String namespace, final String name) {
    return histogramMap.computeIfAbsent(
        getMetricKey(namespace, name), key -> new BeamMetricsHistogram(namespace, name));
  }

  @Override
  public Timer getTimer(String name) {
    throw new UnsupportedOperationException(
        "BeamAccumulatorProvider doesn't support"
            + " getTimer(String name). Please specify namespace and name.");
  }

  /**
   * Metric key for accumulator map.
   *
   * @param namespace = operator name
   * @param name of metric
   * @return metricKey = namespace + SEPARATOR + name
   */
  private static String getMetricKey(String namespace, String name) {
    return namespace.concat(KEY_METRIC_SEPARATOR).concat(name);
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
        LOG.info("Using accumulators with BeamAccumulatorProvider");
      }
      return PROVIDER;
    }
  }

  // ------------------------

  /** Implementation of Counter via {@link org.apache.beam.sdk.metrics.Counter}. */
  public static class BeamMetricsCounter extends BeamMetrics implements Counter {

    public BeamMetricsCounter(String namespace, String name) {
      super(namespace, name);
    }

    @Override
    public void increment(long value) {
      Metrics.counter(getNamespace(), getName()).inc(value);
    }

    @Override
    public void increment() {
      Metrics.counter(getNamespace(), getName()).inc();
    }
  }

  /** Implementation of Histrogram via {@link org.apache.beam.sdk.metrics.Distribution}. */
  public static class BeamMetricsHistogram extends BeamMetrics implements Histogram {

    public BeamMetricsHistogram(String namespace, String name) {
      super(namespace, name);
    }

    @Override
    public void add(long value) {
      Metrics.distribution(getNamespace(), getName()).update(value);
    }

    @Override
    public void add(long value, long times) {
      final Distribution histogram = Metrics.distribution(getNamespace(), getName());
      for (long i = 0; i < times; i++) {
        histogram.update(value);
      }
    }
  }

  abstract static class BeamMetrics {

    private final String namespace;
    private final String name;

    protected BeamMetrics(String namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public String getNamespace() {
      return namespace;
    }
  }
}
