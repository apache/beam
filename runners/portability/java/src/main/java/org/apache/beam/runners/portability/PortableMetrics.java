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
package org.apache.beam.runners.portability;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;

public class PortableMetrics extends MetricResults {

  private static final List<String> COUNTER_METRIC_TYPES =
      ImmutableList.of("beam:metrics:sum_int_64");
  private static final List<String> DISTRIBUTION_METRIC_TYPES =
      ImmutableList.of("beam:metrics:distribution_int_64");
  private static final List<String> GAUGE_METRIC_TYPES =
      ImmutableList.of("beam:metrics:latest_int_64");

  private static final String NAMESPACE_LABEL = "NAMESPACE";
  private static final String METRIC_NAME_LABEL = "NAME";
  private static final String STEP_NAME_LABEL = "PTRANSFORM";
  private Iterable<MetricResult<Long>> counters;
  private Iterable<MetricResult<DistributionResult>> distributions;
  private Iterable<MetricResult<GaugeResult>> gauges;

  private PortableMetrics(
      Iterable<MetricResult<Long>> counters,
      Iterable<MetricResult<DistributionResult>> distributions,
      Iterable<MetricResult<GaugeResult>> gauges) {
    this.counters = counters;
    this.distributions = distributions;
    this.gauges = gauges;
  }

  public static PortableMetrics of(JobApi.MetricResults jobMetrics) {
    return convertMonitoringInfosToMetricResults(jobMetrics);
  }

  @Override
  public MetricQueryResults queryMetrics(MetricsFilter filter) {
    return MetricQueryResults.create(
        Iterables.filter(
            this.counters, (counter) -> MetricFiltering.matches(filter, counter.getKey())),
        Iterables.filter(
            this.distributions,
            (distribution) -> MetricFiltering.matches(filter, distribution.getKey())),
        Iterables.filter(this.gauges, (gauge) -> MetricFiltering.matches(filter, gauge.getKey())));
  }

  private static PortableMetrics convertMonitoringInfosToMetricResults(
      JobApi.MetricResults jobMetrics) {
    List<MetricsApi.MonitoringInfo> monitoringInfoList = new ArrayList<>();
    monitoringInfoList.addAll(jobMetrics.getAttemptedList());
    monitoringInfoList.addAll(jobMetrics.getCommittedList());
    Iterable<MetricResult<Long>> countersFromJobMetrics =
        extractCountersFromJobMetrics(monitoringInfoList);
    Iterable<MetricResult<DistributionResult>> distributionsFromMetrics =
        extractDistributionMetricsFromJobMetrics(monitoringInfoList);
    Iterable<MetricResult<GaugeResult>> gaugesFromMetrics =
        extractGaugeMetricsFromJobMetrics(monitoringInfoList);
    return new PortableMetrics(countersFromJobMetrics, distributionsFromMetrics, gaugesFromMetrics);
  }

  private static Iterable<MetricResult<DistributionResult>>
      extractDistributionMetricsFromJobMetrics(List<MetricsApi.MonitoringInfo> monitoringInfoList) {
    return monitoringInfoList.stream()
        .filter(item -> DISTRIBUTION_METRIC_TYPES.contains(item.getType()))
        .filter(item -> item.getLabelsMap().get(NAMESPACE_LABEL) != null)
        .map(PortableMetrics::convertDistributionMonitoringInfoToDistribution)
        .collect(Collectors.toList());
  }

  private static Iterable<MetricResult<GaugeResult>> extractGaugeMetricsFromJobMetrics(
      List<MetricsApi.MonitoringInfo> monitoringInfoList) {
    return monitoringInfoList.stream()
        .filter(item -> GAUGE_METRIC_TYPES.contains(item.getType()))
        .filter(item -> item.getLabelsMap().get(NAMESPACE_LABEL) != null)
        .map(PortableMetrics::convertGaugeMonitoringInfoToGauge)
        .collect(Collectors.toList());
  }

  private static MetricResult<GaugeResult> convertGaugeMonitoringInfoToGauge(
      MetricsApi.MonitoringInfo monitoringInfo) {
    Map<String, String> labelsMap = monitoringInfo.getLabelsMap();
    MetricKey key =
        MetricKey.create(
            labelsMap.get(STEP_NAME_LABEL),
            MetricName.named(labelsMap.get(NAMESPACE_LABEL), labelsMap.get(METRIC_NAME_LABEL)));
    MetricsApi.IntExtremaData extremaData =
        monitoringInfo.getMetric().getExtremaData().getIntExtremaData();
    // Get only last value of the extrema table
    Instant timestamp = Instant.ofEpochSecond(monitoringInfo.getTimestamp().getSeconds());
    if (extremaData.getIntValuesCount() > 0) {
      GaugeResult result =
          GaugeResult.create(
              extremaData.getIntValues(extremaData.getIntValuesCount() - 1), timestamp);
      return MetricResult.create(key, false, result);
    }
    return null;
  }

  private static MetricResult<DistributionResult> convertDistributionMonitoringInfoToDistribution(
      MetricsApi.MonitoringInfo monitoringInfo) {
    Map<String, String> labelsMap = monitoringInfo.getLabelsMap();
    MetricKey key =
        MetricKey.create(
            labelsMap.get(STEP_NAME_LABEL),
            MetricName.named(labelsMap.get(NAMESPACE_LABEL), labelsMap.get(METRIC_NAME_LABEL)));
    MetricsApi.IntDistributionData intDistributionData =
        monitoringInfo.getMetric().getDistributionData().getIntDistributionData();
    DistributionResult result =
        DistributionResult.create(
            intDistributionData.getSum(),
            intDistributionData.getCount(),
            intDistributionData.getMin(),
            intDistributionData.getMax());
    return MetricResult.create(key, false, result);
  }

  private static Iterable<MetricResult<Long>> extractCountersFromJobMetrics(
      List<MetricsApi.MonitoringInfo> monitoringInfoList) {
    return monitoringInfoList.stream()
        .filter(item -> COUNTER_METRIC_TYPES.contains(item.getType()))
        .filter(
            item ->
                item.getLabelsMap().get(NAMESPACE_LABEL) != null) // filter out pcollection metrics
        .map(PortableMetrics::convertCounterMonitoringInfoToCounter)
        .collect(Collectors.toList());
  }

  private static MetricResult<Long> convertCounterMonitoringInfoToCounter(
      MetricsApi.MonitoringInfo counterMonInfo) {
    Map<String, String> labelsMap = counterMonInfo.getLabelsMap();
    MetricKey key =
        MetricKey.create(
            labelsMap.get(STEP_NAME_LABEL),
            MetricName.named(labelsMap.get(NAMESPACE_LABEL), labelsMap.get(METRIC_NAME_LABEL)));
    return MetricResult.create(
        key, false, counterMonInfo.getMetric().getCounterData().getInt64Value());
  }
}
