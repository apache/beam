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
import static org.apache.beam.runners.core.metrics.MonitoringInfos.keyFromMonitoringInfo;
import static org.apache.beam.runners.core.metrics.MonitoringInfos.processMetric;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Metric;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.construction.metrics.MetricKey;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.function.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for holding a {@link MetricsContainerImpl} and forwarding Beam metrics to Flink
 * accumulators and metrics.
 */
public class FlinkMetricContainer {

  public static final String ACCUMULATOR_NAME = "__metricscontainers";

  private static final Logger LOG = LoggerFactory.getLogger(FlinkMetricContainer.class);

  private static final String METRIC_KEY_SEPARATOR =
      GlobalConfiguration.loadConfiguration().getString(MetricOptions.SCOPE_DELIMITER);

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

  public MetricsContainer getMetricsContainer(String stepName) {
    return metricsAccumulator != null
        ? metricsAccumulator.getLocalValue().getContainer(stepName)
        : null;
  }

  public MetricsContainer getUnboundMetricsContainer() {
    return metricsAccumulator != null
        ? metricsAccumulator.getLocalValue().getUnboundContainer()
        : null;
  }

  /**
   * Update this container with metrics from the passed {@link MonitoringInfo}s, and send updates
   * along to Flink's internal metrics framework.
   */
  public void updateMetrics(List<MonitoringInfo> monitoringInfos) {
    LOG.info("Flink updating metrics with {} monitoring infos", monitoringInfos.size());
    monitoringInfos.forEach(
        monitoringInfo -> {
          if (!monitoringInfo.hasMetric()) {
            LOG.info("Skipping metric-less MonitoringInfo: {}", monitoringInfo);
            return;
          }

          Metric metric = monitoringInfo.getMetric();

          MetricKey metricKey = keyFromMonitoringInfo(monitoringInfo);

          String ptransform = metricKey.stepName();
          MetricName metricName = metricKey.metricName();

          MetricsContainer container = getMetricsContainer(ptransform);
          if (container == null) {
            LOG.warn("Can't add monitoringinfo to null MetricsContainer: {}", monitoringInfo);
            return;
          }

          // Update Beam metrics
          processMetric(
              metric,
              update -> container.getCounter(metricName).inc(update),
              update ->
                  container
                      .getDistribution(metricName)
                      .update(update.getSum(), update.getCount(), update.getMin(), update.getMax()),
              update -> container.getGauge(metricName).set(update.getValue()));

          // Update Flink internal metrics
          processMetric(
              metric,
              counter -> updateCounter(metricKey, counter),
              distribution -> updateDistribution(metricKey, distribution),
              gauge -> updateGauge(metricKey, gauge));
        });
  }

  /**
   * Update Flink's internal metrics ({@link this#flinkCounterCache}) with the latest metrics for a
   * given step.
   */
  void updateMetrics() {
    MetricResults metricResults = asAttemptedOnlyMetricResults(metricsAccumulator.getLocalValue());
    MetricQueryResults metricQueryResults =
        metricResults.queryMetrics(MetricsFilter.builder().build());

    updateMetrics(metricQueryResults.getCounters(), this::updateCounter);
    updateMetrics(metricQueryResults.getDistributions(), this::updateDistribution);
    updateMetrics(metricQueryResults.getGauges(), this::updateGauge);
  }

  private <T> void updateMetrics(
      Iterable<MetricResult<T>> metricResults, BiConsumer<MetricKey, T> fn) {
    for (MetricResult<T> metricResult : metricResults) {
      MetricKey key = MetricKey.create(metricResult.getStep(), metricResult.getName());
      fn.accept(key, metricResult.getAttempted());
    }
  }

  private <T, FlinkT> void updateMetric(
      MetricKey key,
      Map<String, FlinkT> flinkMetricMap,
      TriFunction<String, MetricGroup, T, FlinkT> create,
      BiConsumer<FlinkT, T> update,
      T value) {
    String flinkMetricName = getFlinkMetricNameString(key);

    // update flink metric
    FlinkT metric = flinkMetricMap.get(flinkMetricName);
    if (metric == null) {
      metric = create.apply(flinkMetricName, runtimeContext.getMetricGroup(), value);
      flinkMetricMap.put(flinkMetricName, metric);
    }
    update.accept(metric, value);
  }

  private void updateCounter(MetricKey metricKey, long attempted) {
    updateMetric(
        metricKey,
        flinkCounterCache,
        (name, group, value) -> group.counter(name),
        (counter, value) -> {
          counter.dec(counter.getCount());
          counter.inc(value);
        },
        attempted);
  }

  private void updateDistribution(MetricKey metricKey, DistributionResult attempted) {
    updateMetric(
        metricKey,
        flinkDistributionGaugeCache,
        (name, group, value) -> group.gauge(name, new FlinkDistributionGauge(value)),
        FlinkDistributionGauge::update,
        attempted);
  }

  private void updateGauge(MetricKey metricKey, GaugeResult attempted) {
    updateMetric(
        metricKey,
        flinkGaugeCache,
        (name, group, value) -> group.gauge(name, new FlinkGauge(value)),
        FlinkGauge::update,
        attempted);
  }

  static String getFlinkMetricNameString(MetricKey metricKey) {
    return metricKey.toString(METRIC_KEY_SEPARATOR);
  }

  /** Flink {@link Gauge} for {@link DistributionResult}. */
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

  /** Flink {@link Gauge} for {@link GaugeResult}. */
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
