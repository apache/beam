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
package org.apache.beam.runners.jstorm.translation;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.metrics.Gauge;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.beam.runners.core.metrics.MetricKey;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsFilter;

/**
 * Class that holds a {@link MetricsContainerStepMap}, and reports metrics to JStorm engine.
 */
class MetricsReporter {

  private static final String METRIC_KEY_SEPARATOR = "__";
  private static final String COUNTER_PREFIX = "__metrics";

  private final MetricsContainerStepMap metricsContainers = new MetricsContainerStepMap();
  private final Map<String, Long> reportedCounters = Maps.newHashMap();
  private final Map<String, DistributionResult> reportedDistributions = Maps.newHashMap();
  private final MetricClient metricClient;

  public static MetricsReporter create(MetricClient metricClient) {
    return new MetricsReporter(metricClient);
  }

  /**
   * Converts JStorm metric name to {@link MetricKey}.
   */
  public static MetricKey toMetricKey(String jstormMetricName) {
    String[] nameSplits = jstormMetricName.split(METRIC_KEY_SEPARATOR);
    int length = nameSplits.length;
    String stepName = length > 2 ? nameSplits[length - 3] : "";
    String namespace = length > 1 ? nameSplits[length - 2] : "";
    String counterName = length > 0 ? nameSplits[length - 1] : "";
    return MetricKey.create(stepName, MetricName.named(namespace, counterName));
  }

  private MetricsReporter(MetricClient metricClient) {
    this.metricClient = checkNotNull(metricClient, "metricClient");
  }

  public MetricsContainer getMetricsContainer(String stepName) {
    return metricsContainers.getContainer(stepName);
  }

  public void updateMetrics() {
    MetricResults metricResults = asAttemptedOnlyMetricResults(metricsContainers);
    MetricQueryResults metricQueryResults =
        metricResults.queryMetrics(MetricsFilter.builder().build());
    updateCounters(metricQueryResults.counters());
    updateGauges(metricQueryResults.gauges());
    updateDistributions(metricQueryResults.distributions());
  }

  private void updateCounters(Iterable<MetricResult<Long>> counters) {
    for (MetricResult<Long> metricResult : counters) {
      String metricName = getMetricNameString(COUNTER_PREFIX, metricResult);
      Long updateValue = metricResult.attempted();
      Long oldValue = reportedCounters.get(metricName);

      if (oldValue == null || oldValue < updateValue) {
        AsmCounter counter = metricClient.registerCounter(metricName);
        Long incValue = (oldValue == null ? updateValue : updateValue - oldValue);
        counter.update(incValue);
        reportedCounters.put(metricName, updateValue);
      }
    }
  }

  private void updateGauges(Iterable<MetricResult<GaugeResult>> gauges) {
    for (final MetricResult<GaugeResult> gaugeResult : gauges) {
      String metricName = getMetricNameString(COUNTER_PREFIX, gaugeResult);
      metricClient.registerGauge(metricName, new Gauge<Double>() {
        @Override
        public Double getValue() {
          return (double) gaugeResult.attempted().value();
        }});
    }
  }

  private void updateDistributions(Iterable<MetricResult<DistributionResult>> distributions) {
    for (final MetricResult<DistributionResult> distributionResult : distributions) {
      String metricName = getMetricNameString(COUNTER_PREFIX, distributionResult);
      AsmHistogram histogram = metricClient.registerHistogram(metricName);
      DistributionResult distribution = distributionResult.attempted();
      if (distribution.count() == 0) {
        return;
      }
      DistributionResult oldDistribution = reportedDistributions.get(metricName);
      reportedDistributions.put(metricName, distribution);
      Long newMin;
      Long newMax;
      long restCount;
      long restSum;
      if (oldDistribution == null) {
        newMin = distribution.min();
        newMax = distribution.min() != distribution.max() ? distribution.max() : null;
        restCount = distribution.count();
        restSum = distribution.sum();
      } else {
        newMin = distribution.min() < oldDistribution.min() ? distribution.min() : null;
        newMax =
            (distribution.max() > oldDistribution.max() && distribution.min() != distribution.max())
                ? distribution.max() : null;
        restCount = distribution.count() - oldDistribution.count();
        restSum = distribution.sum() - oldDistribution.sum();
      }
      if (newMin != null) {
        histogram.update(newMin);
        restCount--;
        restSum -= newMin;
      }
      if (newMax != null) {
        histogram.update(newMax);
        restCount--;
        restSum -= newMax;
      }
      if (restCount == 0) {
        return;
      }
      long restAvg = restSum / restCount;
      long restMod = restSum % restCount;
      for (long i = 0; i < restCount; ++i) {
        if (i == 0) {
          histogram.update(restAvg + restMod);
        } else {
          histogram.update(restAvg);
        }
      }
    }
  }

  private String getMetricNameString(String prefix, MetricResult<?> metricResult) {
    return prefix
        + CommonInstance.METRIC_KEY_SEPARATOR + metricResult.step()
        + CommonInstance.METRIC_KEY_SEPARATOR + metricResult.name().namespace()
        + CommonInstance.METRIC_KEY_SEPARATOR + metricResult.name().name();
  }
}
