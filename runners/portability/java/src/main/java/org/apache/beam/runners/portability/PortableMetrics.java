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

import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.BOUNDED_TRIE_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.DISTRIBUTION_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.LATEST_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.SET_STRING_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeBoundedTrie;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Gauge;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeStringSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.runners.core.metrics.BoundedTrieData;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.StringSetData;
import org.apache.beam.sdk.metrics.BoundedTrieResult;
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
  private Iterable<MetricResult<BoundedTrieResult>> boundedTries;

  private PortableMetrics(
      Iterable<MetricResult<Long>> counters,
      Iterable<MetricResult<DistributionResult>> distributions,
      Iterable<MetricResult<GaugeResult>> gauges,
      Iterable<MetricResult<StringSetResult>> stringSets,
      Iterable<MetricResult<BoundedTrieResult>> boundedTries) {
    this.counters = counters;
    this.distributions = distributions;
    this.gauges = gauges;
    this.stringSets = stringSets;
    this.boundedTries = boundedTries;
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
            this.stringSets, (stringSet) -> MetricFiltering.matches(filter, stringSet.getKey())),
        Iterables.filter(
            this.boundedTries,
            (boundedTries) -> MetricFiltering.matches(filter, boundedTries.getKey())),
        Collections.emptyList());
  }

  private static PortableMetrics convertMonitoringInfosToMetricResults(
      JobApi.MetricResults jobMetrics) {
    // Deduplicate attempted + committed. Committed wins.
    LinkedHashMap<String, MiAndCommitted> infoMap = new LinkedHashMap<>();

    for (MetricsApi.MonitoringInfo attempted : jobMetrics.getAttemptedList()) {
      String key = monitoringInfoKey(attempted);
      infoMap.putIfAbsent(key, new MiAndCommitted(attempted, false));
    }

    for (MetricsApi.MonitoringInfo committed : jobMetrics.getCommittedList()) {
      String key = monitoringInfoKey(committed);
      infoMap.put(key, new MiAndCommitted(committed, true));
    }

    List<MiAndCommitted> merged = new ArrayList<>(infoMap.values());

    Iterable<MetricResult<Long>> countersFromJobMetrics = extractCountersFromJobMetrics(merged);
    Iterable<MetricResult<DistributionResult>> distributionsFromMetrics =
        extractDistributionMetricsFromJobMetrics(merged);
    Iterable<MetricResult<GaugeResult>> gaugesFromMetrics =
        extractGaugeMetricsFromJobMetrics(merged);
    Iterable<MetricResult<StringSetResult>> stringSetFromMetrics =
        extractStringSetMetricsFromJobMetrics(merged);
    Iterable<MetricResult<BoundedTrieResult>> boundedTrieFromMetrics =
        extractBoundedTrieMetricsFromJobMetrics(merged);
    return new PortableMetrics(
        countersFromJobMetrics,
        distributionsFromMetrics,
        gaugesFromMetrics,
        stringSetFromMetrics,
        boundedTrieFromMetrics);
  }

  /**
   * Build a stable deduplication key for a MonitoringInfo based on type and the metric identity
   * labels.
   */
  private static String monitoringInfoKey(MetricsApi.MonitoringInfo mi) {
    StringBuilder sb = new StringBuilder();
    sb.append(mi.getType()).append('|');
    Map<String, String> labels = mi.getLabelsMap();
    // Use canonical labels that form the metric identity
    sb.append(labels.getOrDefault(STEP_NAME_LABEL, "")).append('|');
    sb.append(labels.getOrDefault(NAMESPACE_LABEL, "")).append('|');
    sb.append(labels.getOrDefault(METRIC_NAME_LABEL, ""));
    return sb.toString();
  }

  private static class MiAndCommitted {
    final MetricsApi.MonitoringInfo mi;
    final boolean committed;

    MiAndCommitted(MetricsApi.MonitoringInfo mi, boolean committed) {
      this.mi = mi;
      this.committed = committed;
    }
  }

  private static Iterable<MetricResult<DistributionResult>>
      extractDistributionMetricsFromJobMetrics(List<MiAndCommitted> monitoringInfoList) {
    return monitoringInfoList.stream()
        .map(m -> m.mi)
        .filter(item -> DISTRIBUTION_INT64_TYPE.equals(item.getType()))
        .filter(item -> item.getLabelsMap().get(NAMESPACE_LABEL) != null)
        .map(
            item -> {
              boolean isCommitted = findCommittedFlag(monitoringInfoList, item);
              return convertDistributionMonitoringInfoToDistribution(item, isCommitted);
            })
        .collect(Collectors.toList());
  }

  private static Iterable<MetricResult<GaugeResult>> extractGaugeMetricsFromJobMetrics(
      List<MiAndCommitted> monitoringInfoList) {
    return monitoringInfoList.stream()
        .map(m -> m.mi)
        .filter(item -> LATEST_INT64_TYPE.equals(item.getType()))
        .filter(item -> item.getLabelsMap().get(NAMESPACE_LABEL) != null)
        .map(
            item -> {
              boolean isCommitted = findCommittedFlag(monitoringInfoList, item);
              return convertGaugeMonitoringInfoToGauge(item, isCommitted);
            })
        .collect(Collectors.toList());
  }

  private static MetricResult<GaugeResult> convertGaugeMonitoringInfoToGauge(
      MetricsApi.MonitoringInfo monitoringInfo, boolean isCommitted) {
    Map<String, String> labelsMap = monitoringInfo.getLabelsMap();
    MetricKey key =
        MetricKey.create(
            labelsMap.get(STEP_NAME_LABEL),
            MetricName.named(labelsMap.get(NAMESPACE_LABEL), labelsMap.get(METRIC_NAME_LABEL)));

    GaugeData data = decodeInt64Gauge(monitoringInfo.getPayload());
    GaugeResult result = GaugeResult.create(data.value(), data.timestamp());
    return MetricResult.create(key, isCommitted, result);
  }

  private static Iterable<MetricResult<StringSetResult>> extractStringSetMetricsFromJobMetrics(
      List<MiAndCommitted> monitoringInfoList) {
    return monitoringInfoList.stream()
        .map(m -> m.mi)
        .filter(item -> SET_STRING_TYPE.equals(item.getType()))
        .filter(item -> item.getLabelsMap().get(NAMESPACE_LABEL) != null)
        .map(
            item -> {
              boolean isCommitted = findCommittedFlag(monitoringInfoList, item);
              return convertStringSetMonitoringInfoToStringSet(item, isCommitted);
            })
        .collect(Collectors.toList());
  }

  private static Iterable<MetricResult<BoundedTrieResult>> extractBoundedTrieMetricsFromJobMetrics(
      List<MiAndCommitted> monitoringInfoList) {
    return monitoringInfoList.stream()
        .map(m -> m.mi)
        .filter(item -> BOUNDED_TRIE_TYPE.equals(item.getType()))
        .filter(item -> item.getLabelsMap().get(NAMESPACE_LABEL) != null)
        .map(
            item -> {
              boolean isCommitted = findCommittedFlag(monitoringInfoList, item);
              return convertBoundedTrieMonitoringInfoToBoundedTrie(item, isCommitted);
            })
        .collect(Collectors.toList());
  }

  private static MetricResult<StringSetResult> convertStringSetMonitoringInfoToStringSet(
      MetricsApi.MonitoringInfo monitoringInfo, boolean isCommitted) {
    Map<String, String> labelsMap = monitoringInfo.getLabelsMap();
    MetricKey key =
        MetricKey.create(
            labelsMap.get(STEP_NAME_LABEL),
            MetricName.named(labelsMap.get(NAMESPACE_LABEL), labelsMap.get(METRIC_NAME_LABEL)));

    StringSetData data = decodeStringSet(monitoringInfo.getPayload());
    StringSetResult result = StringSetResult.create(data.stringSet());
    return MetricResult.create(key, isCommitted, result);
  }

  private static MetricResult<BoundedTrieResult> convertBoundedTrieMonitoringInfoToBoundedTrie(
      MetricsApi.MonitoringInfo monitoringInfo, boolean isCommitted) {
    Map<String, String> labelsMap = monitoringInfo.getLabelsMap();
    MetricKey key =
        MetricKey.create(
            labelsMap.get(STEP_NAME_LABEL),
            MetricName.named(labelsMap.get(NAMESPACE_LABEL), labelsMap.get(METRIC_NAME_LABEL)));

    BoundedTrieData data = decodeBoundedTrie(monitoringInfo.getPayload());
    BoundedTrieResult result = BoundedTrieResult.create(data.extractResult().getResult());
    return MetricResult.create(key, isCommitted, result);
  }

  private static MetricResult<DistributionResult> convertDistributionMonitoringInfoToDistribution(
      MetricsApi.MonitoringInfo monitoringInfo, boolean isCommitted) {
    Map<String, String> labelsMap = monitoringInfo.getLabelsMap();
    MetricKey key =
        MetricKey.create(
            labelsMap.get(STEP_NAME_LABEL),
            MetricName.named(labelsMap.get(NAMESPACE_LABEL), labelsMap.get(METRIC_NAME_LABEL)));
    DistributionData data = decodeInt64Distribution(monitoringInfo.getPayload());
    DistributionResult result =
        DistributionResult.create(data.sum(), data.count(), data.min(), data.max());
    return MetricResult.create(key, isCommitted, result);
  }

  private static Iterable<MetricResult<Long>> extractCountersFromJobMetrics(
      List<MiAndCommitted> monitoringInfoList) {
    return monitoringInfoList.stream()
        .map(m -> m.mi)
        .filter(item -> SUM_INT64_TYPE.equals(item.getType()))
        .filter(
            item ->
                item.getLabelsMap().get(NAMESPACE_LABEL) != null) // filter out pcollection metrics
        .map(
            item -> {
              boolean isCommitted = findCommittedFlag(monitoringInfoList, item);
              return convertCounterMonitoringInfoToCounter(item, isCommitted);
            })
        .collect(Collectors.toList());
  }

  private static MetricResult<Long> convertCounterMonitoringInfoToCounter(
      MetricsApi.MonitoringInfo counterMonInfo, boolean isCommitted) {
    Map<String, String> labelsMap = counterMonInfo.getLabelsMap();
    MetricKey key =
        MetricKey.create(
            labelsMap.get(STEP_NAME_LABEL),
            MetricName.named(labelsMap.get(NAMESPACE_LABEL), labelsMap.get(METRIC_NAME_LABEL)));
    return MetricResult.create(key, isCommitted, decodeInt64Counter(counterMonInfo.getPayload()));
  }

  /** Helper to retrieve the committed flag for a MonitoringInfo from the merged list. */
  private static boolean findCommittedFlag(
      List<MiAndCommitted> merged, MetricsApi.MonitoringInfo mi) {
    // Reconstruct the key and look up in the merged map entries.
    String key = monitoringInfoKey(mi);
    for (MiAndCommitted entry : merged) {
      if (monitoringInfoKey(entry.mi).equals(key)) {
        return entry.committed;
      }
    }
    return false;
  }
}
