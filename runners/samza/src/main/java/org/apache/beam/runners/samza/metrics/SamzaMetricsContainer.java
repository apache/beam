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
package org.apache.beam.runners.samza.metrics;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.MetricsRegistryMap;

/**
 * This class holds the {@link MetricsContainer}s for BEAM metrics, and update the results to Samza
 * metrics.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SamzaMetricsContainer {
  private static final String BEAM_METRICS_GROUP = "BeamMetrics";

  private final MetricsContainerStepMap metricsContainers = new MetricsContainerStepMap();
  private final MetricsRegistryMap metricsRegistry;

  public SamzaMetricsContainer(MetricsRegistryMap metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
    this.metricsRegistry.metrics().put(BEAM_METRICS_GROUP, new ConcurrentHashMap<>());
  }

  public MetricsContainer getContainer(String stepName) {
    return this.metricsContainers.getContainer(stepName);
  }

  public MetricsContainerStepMap getContainers() {
    return this.metricsContainers;
  }

  public void updateMetrics(String stepName) {
    assert metricsRegistry != null;

    final MetricResults metricResults = asAttemptedOnlyMetricResults(metricsContainers);
    final MetricQueryResults results =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(stepName).build());

    final CounterUpdater updateCounter = new CounterUpdater();
    results.getCounters().forEach(updateCounter);

    final GaugeUpdater updateGauge = new GaugeUpdater();
    results.getGauges().forEach(updateGauge);

    // TODO(https://github.com/apache/beam/issues/21043): add distribution metrics to Samza
  }

  public void updateExecutableStageBundleMetric(String metricName, long time) {
    @SuppressWarnings("unchecked")
    Gauge<Long> gauge = (Gauge<Long>) getSamzaMetricFor(metricName);
    if (gauge == null) {
      gauge = metricsRegistry.newGauge(BEAM_METRICS_GROUP, metricName, 0L);
    }
    gauge.set(time);
  }

  private class CounterUpdater implements Consumer<MetricResult<Long>> {
    @Override
    public void accept(MetricResult<Long> metricResult) {
      final String metricName = getMetricName(metricResult);
      Counter counter = (Counter) getSamzaMetricFor(metricName);
      if (counter == null) {
        counter = metricsRegistry.newCounter(BEAM_METRICS_GROUP, metricName);
      }
      counter.dec(counter.getCount());
      counter.inc(metricResult.getAttempted());
    }
  }

  private class GaugeUpdater implements Consumer<MetricResult<GaugeResult>> {
    @Override
    public void accept(MetricResult<GaugeResult> metricResult) {
      final String metricName = getMetricName(metricResult);
      @SuppressWarnings("unchecked")
      Gauge<Long> gauge = (Gauge<Long>) getSamzaMetricFor(metricName);
      if (gauge == null) {
        gauge = metricsRegistry.newGauge(BEAM_METRICS_GROUP, metricName, 0L);
      }
      gauge.set(metricResult.getAttempted().getValue());
    }
  }

  private Metric getSamzaMetricFor(String metricName) {
    return metricsRegistry.getGroup(BEAM_METRICS_GROUP).get(metricName);
  }

  private static String getMetricName(MetricResult<?> metricResult) {
    return metricResult.getKey().toString();
  }
}
