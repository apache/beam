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

import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.DISTRIBUTION_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.LATEST_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.SET_STRING_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Gauge;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeStringSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.StringSetData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.StringSetResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PortableMetrics extends MetricResults {
  private static final String NAMESPACE_LABEL = "NAMESPACE";
  private static final String METRIC_NAME_LABEL = "NAME";
  private static final String STEP_NAME_LABEL = "PTRANSFORM";
  private Iterable<MetricResult<Long>> counters;
  private Iterable<MetricResult<DistributionResult>> distributions;
  private Iterable<MetricResult<GaugeResult>> gauges;
  private Iterable<MetricResult<StringSetResult>> stringSets;

  private PortableMetrics(
      Iterable<MetricResult<Long>> counters,
      Iterable<MetricResult<DistributionResult>> distributions,
      Iterable<MetricResult<GaugeResult>> gauges,
      Iterable<MetricResult<StringSetResult>> stringSets) {
    this.counters = counters;
    this.distributions = distributions;
    this.gauges = gauges;
    this.stringSets = stringSets;
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
        Iterables.filter(this.gauges, (gauge) -> MetricFiltering.matches(filter, gauge.getKey())),
        Iterables.filter(
            this.stringSets, (stringSet) -> MetricFiltering.matches(filter, stringSet.getKey())));
  }

  private static PortableMetrics convertMonitoringInfosToMetricResults(
      JobApi.MetricResults jobMetrics) {
    List<MetricsApi.MonitoringInfo> monitoringInfoList = new ArrayList<>();
    // TODO(https://github.com/apache/beam/issues/32001) dedup Attempted and Committed metrics
    monitoringInfoList.addAll(jobMetrics.getAttemptedList());
    monitoringInfoList.addAll(jobMetrics.getCommittedList());
    Iterable<MetricResult<Long>> countersFromJobMetrics =
        extractCountersFromJobMetrics(monitoringInfoList);
    Iterable<MetricResult<DistributionResult>> distributionsFromMetrics =
        extractDistributionMetricsFromJobMetrics(monitoringInfoList);
    Iterable<MetricResult<GaugeResult>> gaugesFromMetrics =
        extractGaugeMetricsFromJobMetrics(monitoringInfoList);
    Iterable<MetricResult<StringSetResult>> stringSetFromMetrics =
        extractStringSetMetricsFromJobMetrics(monitoringInfoList);
    return new PortableMetrics(
        countersFromJobMetrics, distributionsFromMetrics, gaugesFromMetrics, stringSetFromMetrics);
  }

  private static Iterable<MetricResult<DistributionResult>>
      extractDistributionMetricsFromJobMetrics(List<MetricsApi.MonitoringInfo> monitoringInfoList) {
    return monitoringInfoList.stream()
        .filter(item -> DISTRIBUTION_INT64_TYPE.equals(item.getType()))
        .filter(item -> item.getLabelsMap().get(NAMESPACE_LABEL) != null)
        .map(PortableMetrics::convertDistributionMonitoringInfoToDistribution)
        .collect(Collectors.toList());
  }

  private static Iterable<MetricResult<GaugeResult>> extractGaugeMetricsFromJobMetrics(
      List<MetricsApi.MonitoringInfo> monitoringInfoList) {
    return monitoringInfoList.stream()
        .filter(item -> LATEST_INT64_TYPE.equals(item.getType()))
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

    GaugeData data = decodeInt64Gauge(monitoringInfo.getPayload());
    GaugeResult result = GaugeResult.create(data.value(), data.timestamp());
    return MetricResult.create(key, false, result);
  }

  private static Iterable<MetricResult<StringSetResult>> extractStringSetMetricsFromJobMetrics(
      List<MetricsApi.MonitoringInfo> monitoringInfoList) {
    return monitoringInfoList.stream()
        .filter(item -> SET_STRING_TYPE.equals(item.getType()))
        .filter(item -> item.getLabelsMap().get(NAMESPACE_LABEL) != null)
        .map(PortableMetrics::convertStringSetMonitoringInfoToStringSet)
        .collect(Collectors.toList());
  }

  private static MetricResult<StringSetResult> convertStringSetMonitoringInfoToStringSet(
      MetricsApi.MonitoringInfo monitoringInfo) {
    Map<String, String> labelsMap = monitoringInfo.getLabelsMap();
    MetricKey key =
        MetricKey.create(
            labelsMap.get(STEP_NAME_LABEL),
            MetricName.named(labelsMap.get(NAMESPACE_LABEL), labelsMap.get(METRIC_NAME_LABEL)));

    StringSetData data = decodeStringSet(monitoringInfo.getPayload());
    StringSetResult result = StringSetResult.create(data.stringSet());
    return MetricResult.create(key, false, result);
  }

  private static MetricResult<DistributionResult> convertDistributionMonitoringInfoToDistribution(
      MetricsApi.MonitoringInfo monitoringInfo) {
    Map<String, String> labelsMap = monitoringInfo.getLabelsMap();
    MetricKey key =
        MetricKey.create(
            labelsMap.get(STEP_NAME_LABEL),
            MetricName.named(labelsMap.get(NAMESPACE_LABEL), labelsMap.get(METRIC_NAME_LABEL)));
    DistributionData data = decodeInt64Distribution(monitoringInfo.getPayload());
    DistributionResult result =
        DistributionResult.create(data.sum(), data.count(), data.min(), data.max());
    return MetricResult.create(key, false, result);
  }

  private static Iterable<MetricResult<Long>> extractCountersFromJobMetrics(
      List<MetricsApi.MonitoringInfo> monitoringInfoList) {
    return monitoringInfoList.stream()
        .filter(item -> SUM_INT64_TYPE.equals(item.getType()))
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
    return MetricResult.create(key, false, decodeInt64Counter(counterMonInfo.getPayload()));
  }
}
