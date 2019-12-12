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
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for holding a {@link MetricsContainerImpl} and forwarding Beam metrics to Flink
 * accumulators and metrics.
 *
 * <p>Using accumulators can be turned off because it is memory and network intensive. The
 * accumulator results are only meaningful in batch applications or testing streaming applications
 * which have a defined end. They are not essential during execution because metrics will also be
 * reported using the configured metrics reporter.
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

  public FlinkMetricContainer(RuntimeContext runtimeContext, boolean accumulatorDisabled) {
    this.runtimeContext = runtimeContext;
    this.flinkCounterCache = new HashMap<>();
    this.flinkDistributionGaugeCache = new HashMap<>();
    this.flinkGaugeCache = new HashMap<>();

    Accumulator<MetricsContainerStepMap, MetricsContainerStepMap> metricsAccumulator;
    if (accumulatorDisabled) {
      // Do not register the accumulator with Flink
      metricsAccumulator = new MetricsAccumulator();
    } else {
      metricsAccumulator = runtimeContext.getAccumulator(ACCUMULATOR_NAME);
      if (metricsAccumulator == null) {
        metricsAccumulator = new MetricsAccumulator();
        try {
          runtimeContext.addAccumulator(ACCUMULATOR_NAME, metricsAccumulator);
        } catch (UnsupportedOperationException e) {
          // Not supported in all environments, e.g. tests
        } catch (Exception e) {
          LOG.error("Failed to create metrics accumulator.", e);
        }
      }
    }
    this.metricsAccumulator = (MetricsAccumulator) metricsAccumulator;
  }

  public MetricsContainerImpl getMetricsContainer(String stepName) {
    return metricsAccumulator != null
        ? metricsAccumulator.getLocalValue().getContainer(stepName)
        : null;
  }

  /**
   * Update this container with metrics from the passed {@link MonitoringInfo}s, and send updates
   * along to Flink's internal metrics framework.
   */
  public void updateMetrics(String stepName, List<MonitoringInfo> monitoringInfos) {
    getMetricsContainer(stepName).update(monitoringInfos);
    updateMetrics(stepName);
  }

  /**
   * Update Flink's internal metrics ({@link this#flinkCounterCache}) with the latest metrics for a
   * given step.
   */
  void updateMetrics(String stepName) {
    MetricResults metricResults = asAttemptedOnlyMetricResults(metricsAccumulator.getLocalValue());
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
          flinkCounterCache.computeIfAbsent(
              flinkMetricName, n -> runtimeContext.getMetricGroup().counter(n));
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
        gauge =
            runtimeContext
                .getMetricGroup()
                .gauge(flinkMetricName, new FlinkDistributionGauge(update));
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
        gauge = runtimeContext.getMetricGroup().gauge(flinkMetricName, new FlinkGauge(update));
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
