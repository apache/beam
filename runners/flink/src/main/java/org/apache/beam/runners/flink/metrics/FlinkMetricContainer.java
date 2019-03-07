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
import static org.apache.beam.sdk.metrics.DistributionProtos.fromProto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.MetricsApi.CounterData;
import org.apache.beam.model.pipeline.v1.MetricsApi.DistributionData;
import org.apache.beam.model.pipeline.v1.MetricsApi.ExtremaData;
import org.apache.beam.model.pipeline.v1.MetricsApi.IntDistributionData;
import org.apache.beam.model.pipeline.v1.MetricsApi.Metric;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.labels.MetricLabels;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
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

  @Nullable
  public MetricsContainer getMetricsContainer(MetricLabels labels) {
    return metricsAccumulator != null
        ? metricsAccumulator.getLocalValue().getContainer(labels)
        : null;
  }

  @Nullable
  public MetricsContainer ptransformContainer(String ptransform) {
    return getMetricsContainer(MetricLabels.ptransform(ptransform));
  }

  /**
   * Update this container with metrics from the passed {@link MonitoringInfo}s, and send updates
   * along to Flink's internal metrics framework.
   */
  public void updateMetrics(String stepName, List<MonitoringInfo> monitoringInfos) {
    monitoringInfos.forEach(
        monitoringInfo -> {
          if (!monitoringInfo.hasMetric()) {
            LOG.info("Skipping metric-less MonitoringInfo: {}", monitoringInfo);
            return;
          }
          Metric metric = monitoringInfo.getMetric();

          MetricKey key = MetricKey.create(monitoringInfo.getUrn(), monitoringInfo.getLabelsMap());
          MetricName metricName = key.metricName();

          MetricsContainer container = getMetricsContainer(key.labels());

          if (metric.hasCounterData()) {
            CounterData counterData = metric.getCounterData();
            if (counterData.getValueCase() == CounterData.ValueCase.INT64_VALUE) {
              org.apache.beam.sdk.metrics.Counter counter = container.getCounter(metricName);
              counter.inc(counterData.getInt64Value());
              updateCounter(key, counterData.getInt64Value());
            } else {
              LOG.warn("Unsupported CounterData type: {}", counterData);
            }
          } else if (metric.hasDistributionData()) {
            DistributionData distributionData = metric.getDistributionData();
            if (distributionData.hasIntDistributionData()) {
              Distribution distribution = container.getDistribution(metricName);
              IntDistributionData intDistributionData = distributionData.getIntDistributionData();
              DistributionResult distributionResult = fromProto(intDistributionData);
              distribution.update(
                  distributionResult.getSum(),
                  distributionResult.getCount(),
                  distributionResult.getMin(),
                  distributionResult.getMax());
              updateDistribution(key, distributionResult);
            } else {
              LOG.warn("Unsupported DistributionData type: {}", distributionData);
            }
          } else if (metric.hasExtremaData()) {
            ExtremaData extremaData = metric.getExtremaData();
            LOG.warn("Extrema metric unsupported: {}", extremaData);
          }
        });
  }

  /**
   * Update Flink's internal metrics ({@link this#flinkCounterCache}) with the latest metrics for a
   * given step.
   */
  void updateFlinkMetrics(String stepName) {
    MetricResults metricResults = asAttemptedOnlyMetricResults(metricsAccumulator.getLocalValue());
    MetricQueryResults metricQueryResults =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(stepName).build());

    updateMetrics(metricQueryResults.getCounters(), this::updateCounter);
    updateMetrics(metricQueryResults.getDistributions(), this::updateDistribution);
    updateMetrics(metricQueryResults.getGauges(), this::updateGauge);
  }

  private <T> void updateMetrics(
      Iterable<MetricResult<T>> metricResults, BiConsumer<MetricKey, T> fn) {
    for (MetricResult<T> metricResult : metricResults) {
      MetricKey key = metricResult.getKey();
      fn.accept(key, metricResult.getAttempted());
    }
  }

  private <T, FlinkT> void updateMetric(
      MetricKey key,
      Map<String, FlinkT> flinkMetricMap,
      BiFunction<String, MetricGroup, FlinkT> create,
      BiConsumer<FlinkT, T> update,
      T value) {
    String flinkMetricName = getFlinkMetricNameString(key);

    // update flink metric
    FlinkT metric = flinkMetricMap.get(flinkMetricName);
    if (metric == null) {
      metric = create.apply(flinkMetricName, runtimeContext.getMetricGroup());
      flinkMetricMap.put(flinkMetricName, metric);
    }
    update.accept(metric, value);
  }

  private void updateCounter(MetricKey metricKey, long value) {
    updateMetric(
        metricKey,
        flinkCounterCache,
        (name, group) -> group.counter(name),
        (counter, unused) -> {
          counter.dec(counter.getCount());
          counter.inc(value);
        },
        value);
  }

  private void updateDistribution(MetricKey metricKey, DistributionResult value) {
    updateMetric(
        metricKey,
        flinkDistributionGaugeCache,
        (name, group) -> group.gauge(name, new FlinkDistributionGauge(value)),
        FlinkDistributionGauge::update,
        value);
  }

  private void updateGauge(MetricKey metricKey, GaugeResult value) {
    updateMetric(
        metricKey,
        flinkGaugeCache,
        (name, group) -> group.gauge(name, new FlinkGauge(value)),
        FlinkGauge::update,
        value);
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
