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
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

/**
 * The base helper class for holding a {@link MetricsContainerImpl} and forwarding Beam metrics to
 * Flink accumulators and metrics. The two subclasses of this base class are {@link
 * FlinkMetricContainer} and {@link FlinkMetricContainerWithoutAccumulator}. The former is used when
 * {@link org.apache.flink.api.common.functions.RuntimeContext Flink RuntimeContext} is available.
 * The latter is used otherwise.
 */
abstract class FlinkMetricContainerBase {

  private static final String METRIC_KEY_SEPARATOR =
      GlobalConfiguration.loadConfiguration().getString(MetricOptions.SCOPE_DELIMITER);

  protected final MetricsContainerStepMap metricsContainers;
  private final Map<String, Counter> flinkCounterCache;
  private final Map<String, FlinkDistributionGauge> flinkDistributionGaugeCache;
  private final Map<String, FlinkGauge> flinkGaugeCache;
  private final MetricGroup metricGroup;

  public FlinkMetricContainerBase(MetricGroup metricGroup) {
    this.flinkCounterCache = new HashMap<>();
    this.flinkDistributionGaugeCache = new HashMap<>();
    this.flinkGaugeCache = new HashMap<>();
    this.metricsContainers = new MetricsContainerStepMap();
    this.metricGroup = metricGroup;
  }

  public MetricGroup getMetricGroup() {
    return metricGroup;
  }

  public MetricsContainerImpl getMetricsContainer(String stepName) {
    return metricsContainers.getContainer(stepName);
  }

  /**
   * Update this container with metrics from the passed {@link MetricsApi.MonitoringInfo}s, and send
   * updates along to Flink's internal metrics framework.
   */
  public void updateMetrics(String stepName, List<MetricsApi.MonitoringInfo> monitoringInfos) {
    getMetricsContainer(stepName).update(monitoringInfos);
    updateMetrics(stepName);
  }

  /**
   * Update Flink's internal metrics ({@link this#flinkCounterCache}) with the latest metrics for a
   * given step.
   */
  void updateMetrics(String stepName) {
    MetricResults metricResults = asAttemptedOnlyMetricResults(metricsContainers);
    MetricQueryResults metricQueryResults =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(stepName).build());
    updateCounters(metricQueryResults.getCounters());
    updateDistributions(metricQueryResults.getDistributions());
    updateGauge(metricQueryResults.getGauges());
  }

  private void updateCounters(Iterable<MetricResult<Long>> counters) {
    for (MetricResult<Long> metricResult : counters) {
      String flinkMetricName = getFlinkMetricNameString(metricResult.getKey());

      Long update = metricResult.getAttempted();

      // update flink metric
      Counter counter =
          flinkCounterCache.computeIfAbsent(flinkMetricName, n -> getMetricGroup().counter(n));
      // Beam counters are already pre-aggregated, just update with the current value here
      counter.inc(update - counter.getCount());
    }
  }

  private void updateDistributions(Iterable<MetricResult<DistributionResult>> distributions) {
    for (MetricResult<DistributionResult> metricResult : distributions) {
      String flinkMetricName = getFlinkMetricNameString(metricResult.getKey());

      DistributionResult update = metricResult.getAttempted();

      // update flink metric
      FlinkDistributionGauge gauge = flinkDistributionGaugeCache.get(flinkMetricName);
      if (gauge == null) {
        gauge = getMetricGroup().gauge(flinkMetricName, new FlinkDistributionGauge(update));
        flinkDistributionGaugeCache.put(flinkMetricName, gauge);
      } else {
        gauge.update(update);
      }
    }
  }

  private void updateGauge(Iterable<MetricResult<GaugeResult>> gauges) {
    for (MetricResult<GaugeResult> metricResult : gauges) {
      String flinkMetricName = getFlinkMetricNameString(metricResult.getKey());

      GaugeResult update = metricResult.getAttempted();

      // update flink metric
      FlinkGauge gauge = flinkGaugeCache.get(flinkMetricName);
      if (gauge == null) {
        gauge = getMetricGroup().gauge(flinkMetricName, new FlinkGauge(update));
        flinkGaugeCache.put(flinkMetricName, gauge);
      } else {
        gauge.update(update);
      }
    }
  }

  @VisibleForTesting
  static String getFlinkMetricNameString(MetricKey metricKey) {
    MetricName metricName = metricKey.metricName();
    // We use only the MetricName here, the step name is already contained
    // in the operator name which is passed to Flink's MetricGroup to which
    // the metric with the following name will be added.
    return metricName.getNamespace() + METRIC_KEY_SEPARATOR + metricName.getName();
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
  public static class FlinkGauge implements Gauge<Long> {

    GaugeResult data;

    FlinkGauge(GaugeResult data) {
      this.data = data;
    }

    void update(GaugeResult update) {
      this.data = update;
    }

    @Override
    public Long getValue() {
      return data.getValue();
    }
  }
}
