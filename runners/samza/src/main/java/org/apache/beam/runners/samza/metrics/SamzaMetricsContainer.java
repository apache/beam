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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.beam.runners.core.metrics.DefaultMetricResults;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class holds the {@link MetricsContainer}s for BEAM metrics, and update the results to Samza
 * metrics.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SamzaMetricsContainer {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaMetricsContainer.class);
  private static final String BEAM_METRICS_GROUP = "BeamMetrics";
  private static final String DELIMITER = "-";
  // global metrics container is the default container that can be used in user threads
  public static final String GLOBAL_CONTAINER_STEP_NAME = "GLOBAL_METRICS";
  public static final String USE_SHORT_METRIC_NAMES_CONFIG =
      "beam.samza.metrics.useShortMetricNames";

  private final MetricsContainerStepMap metricsContainers = new MetricsContainerStepMap();
  private final MetricsRegistryMap metricsRegistry;
  private final boolean useShortMetricNames;

  public SamzaMetricsContainer(MetricsRegistryMap metricsRegistry, Config config) {
    this.metricsRegistry = metricsRegistry;
    this.useShortMetricNames = config.getBoolean(USE_SHORT_METRIC_NAMES_CONFIG, false);
    this.metricsRegistry.metrics().put(BEAM_METRICS_GROUP, new ConcurrentHashMap<>());
    LOG.info("Creating Samza metrics container with userShortMetricName = {}", useShortMetricNames);
  }

  public MetricsContainer getContainer(String stepName) {
    return this.metricsContainers.getContainer(stepName);
  }

  public MetricsContainerStepMap getContainers() {
    return this.metricsContainers;
  }

  /** Update Beam metrics to Samza metrics for the current step and global step. */
  public void updateMetrics(String stepName) {

    assert metricsRegistry != null;

    // Since global metrics do not belong to any step, we need to update it in every step.
    final MetricResults metricResults =
        asAttemptedOnlyMetricResultsForSteps(
            metricsContainers, Arrays.asList(stepName, GLOBAL_CONTAINER_STEP_NAME));
    final MetricQueryResults results = metricResults.allMetrics();

    final CounterUpdater updateCounter = new CounterUpdater();
    results.getCounters().forEach(updateCounter);

    final GaugeUpdater updateGauge = new GaugeUpdater();
    results.getGauges().forEach(updateGauge);

    // TODO: add distribution metrics to Samza
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

  private String getMetricName(MetricResult<?> metricResult) {
    return useShortMetricNames
        ? metricResult.getName().toString()
        : metricResult.getKey().toString();
  }

  /**
   * Similar to {@link MetricsContainerStepMap#asAttemptedOnlyMetricResults}, it gets the metrics
   * results from the MetricsContainerStepMap. Instead of getting from all steps, it gets result
   * from only interested steps. Thus, it's more efficient.
   */
  private static MetricResults asAttemptedOnlyMetricResultsForSteps(
      MetricsContainerStepMap metricsContainers, List<String> steps) {
    List<MetricResult<Long>> counters = new ArrayList<>();
    List<MetricResult<GaugeResult>> gauges = new ArrayList<>();

    for (String step : steps) {
      MetricsContainerImpl container = metricsContainers.getContainer(step);
      MetricUpdates cumulative = container.getUpdates();

      // Merging counters
      for (MetricUpdates.MetricUpdate<Long> counterUpdate : cumulative.counterUpdates()) {
        counters.add(MetricResult.attempted(counterUpdate.getKey(), counterUpdate.getUpdate()));
      }

      // Merging gauges
      for (MetricUpdates.MetricUpdate<GaugeData> gaugeUpdate : cumulative.gaugeUpdates()) {
        gauges.add(
            MetricResult.attempted(gaugeUpdate.getKey(), gaugeUpdate.getUpdate().extractResult()));
      }
    }

    return new DefaultMetricResults(counters, Collections.emptyList(), gauges);
  }
}
