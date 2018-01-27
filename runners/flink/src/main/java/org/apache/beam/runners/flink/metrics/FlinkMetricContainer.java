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
package org.apache.beam.runners.flink.metrics;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for holding a {@link MetricsContainerImpl} and forwarding Beam metrics to
 * Flink accumulators and metrics.
 */
public class FlinkMetricContainer {

  public static final String ACCUMULATOR_NAME = "__metricscontainers";

  private static final Logger LOG = LoggerFactory.getLogger(FlinkMetricContainer.class);

  private static final String METRIC_KEY_SEPARATOR = "__";
  private static final String COUNTER_PREFIX = "__counter";
  private static final String DISTRIBUTION_PREFIX = "__distribution";
  private static final String GAUGE_PREFIX = "__gauge";

  private final RuntimeContext runtimeContext;
  private final Map<String, Counter> flinkCounterCache;
  private final Map<String, FlinkDistributionGauge> flinkDistributionGaugeCache;
  private final Map<String, FlinkGauge> flinkGaugeCache;
  private final MetricsAccumulator metricsAccumulator;

  public FlinkMetricContainer(RuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
    this.flinkCounterCache = new HashMap<>();
    this.flinkDistributionGaugeCache = new HashMap<>();
    this.flinkGaugeCache = new HashMap<>();

    Accumulator<MetricsContainerStepMap, MetricsContainerStepMap> metricsAccumulator =
        runtimeContext.getAccumulator(ACCUMULATOR_NAME);
    if (metricsAccumulator == null) {
      metricsAccumulator = new MetricsAccumulator();
      try {
        runtimeContext.addAccumulator(ACCUMULATOR_NAME, metricsAccumulator);
      } catch (Exception e) {
        LOG.error("Failed to create metrics accumulator.", e);
      }
    }
    this.metricsAccumulator = (MetricsAccumulator) metricsAccumulator;
  }

  MetricsContainer getMetricsContainer(String stepName) {
    return metricsAccumulator != null
        ? metricsAccumulator.getLocalValue().getContainer(stepName)
        : null;
  }

  void updateMetrics() {
    MetricResults metricResults =
        asAttemptedOnlyMetricResults(metricsAccumulator.getLocalValue());
    MetricQueryResults metricQueryResults =
        metricResults.queryMetrics(MetricsFilter.builder().build());
    updateCounters(metricQueryResults.counters());
    updateDistributions(metricQueryResults.distributions());
    updateGauge(metricQueryResults.gauges());
  }

  private void updateCounters(Iterable<MetricResult<Long>> counters) {
    for (MetricResult<Long> metricResult : counters) {
      String flinkMetricName = getFlinkMetricNameString(COUNTER_PREFIX, metricResult);

      Long update = metricResult.attempted();

      // update flink metric
      Counter counter =
          flinkCounterCache.computeIfAbsent(
              flinkMetricName, n -> runtimeContext.getMetricGroup().counter(n));
      counter.dec(counter.getCount());
      counter.inc(update);
    }
  }

  private void updateDistributions(Iterable<MetricResult<DistributionResult>> distributions) {
    for (MetricResult<DistributionResult> metricResult : distributions) {
      String flinkMetricName =
          getFlinkMetricNameString(DISTRIBUTION_PREFIX, metricResult);

      DistributionResult update = metricResult.attempted();

      // update flink metric
      FlinkDistributionGauge gauge = flinkDistributionGaugeCache.get(flinkMetricName);
      if (gauge == null) {
        gauge = runtimeContext.getMetricGroup()
            .gauge(flinkMetricName, new FlinkDistributionGauge(update));
        flinkDistributionGaugeCache.put(flinkMetricName, gauge);
      } else {
        gauge.update(update);
      }
    }
  }

  private void updateGauge(Iterable<MetricResult<GaugeResult>> gauges) {
    for (MetricResult<GaugeResult> metricResult : gauges) {
      String flinkMetricName =
          getFlinkMetricNameString(GAUGE_PREFIX, metricResult);

      GaugeResult update = metricResult.attempted();

      // update flink metric
      FlinkGauge gauge = flinkGaugeCache.get(flinkMetricName);
      if (gauge == null) {
        gauge = runtimeContext.getMetricGroup()
            .gauge(flinkMetricName, new FlinkGauge(update));
        flinkGaugeCache.put(flinkMetricName, gauge);
      } else {
        gauge.update(update);
      }
    }
  }

  private static String getFlinkMetricNameString(String prefix, MetricResult<?> metricResult) {
    return prefix
        + METRIC_KEY_SEPARATOR + metricResult.step()
        + METRIC_KEY_SEPARATOR + metricResult.name().namespace()
        + METRIC_KEY_SEPARATOR + metricResult.name().name();
  }

  /**
   * Flink {@link Gauge} for {@link DistributionResult}.
   */
  public static class FlinkDistributionGauge implements Gauge<DistributionResult> {

    DistributionResult data;

    FlinkDistributionGauge(DistributionResult data) {
      this.data = data;
    }

    void update(DistributionResult data) {
      this.data = data;
    }

    @Override
    public DistributionResult getValue() {
      return data;
    }
  }

  /**
   * Flink {@link Gauge} for {@link GaugeResult}.
   */
  public static class FlinkGauge implements Gauge<GaugeResult> {

    GaugeResult data;

    FlinkGauge(GaugeResult data) {
      this.data = data;
    }

    void update(GaugeResult update) {
      this.data = update;
    }

    @Override
    public GaugeResult getValue() {
      return data;
    }
  }
}
