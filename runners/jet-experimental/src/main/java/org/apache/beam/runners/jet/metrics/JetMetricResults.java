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
package org.apache.beam.runners.jet.metrics;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.MapClearedListener;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.FluentIterable;

/** Jet specific {@link MetricResults}. */
public class JetMetricResults extends MetricResults
    implements EntryAddedListener<String, MetricUpdates>, MapClearedListener {

  private final Map<MetricKey, Long> counters = new HashMap<>();
  private final Map<MetricKey, DistributionData> distributions = new HashMap<>();
  private final Map<MetricKey, GaugeData> gauges = new HashMap<>();

  private static MetricKey normalizeStepName(MetricKey key) {
    return MetricKey.create(
        JetMetricsContainer.ownerIdFromStepName(key.stepName()), key.metricName());
  }

  @Override
  public void entryAdded(EntryEvent<String, MetricUpdates> event) {
    merge(event.getValue());
  }

  @Override
  public void mapCleared(MapEvent mapEvent) {
    counters.clear();
    distributions.clear();
    gauges.clear();
  }

  private void merge(MetricUpdates metricUpdates) {
    mergeCounters(metricUpdates.counterUpdates());
    mergeDistributions(metricUpdates.distributionUpdates());
    mergeGauges(metricUpdates.gaugeUpdates());
  }

  private void mergeGauges(Iterable<MetricUpdate<GaugeData>> updates) {
    for (MetricUpdate<GaugeData> update : updates) {
      MetricKey key = normalizeStepName(update.getKey());
      GaugeData oldGauge = gauges.getOrDefault(key, GaugeData.empty());
      GaugeData updatedGauge = update.getUpdate().combine(oldGauge);
      gauges.put(key, updatedGauge);
    }
  }

  private void mergeDistributions(Iterable<MetricUpdate<DistributionData>> updates) {
    for (MetricUpdate<DistributionData> update : updates) {
      MetricKey key = normalizeStepName(update.getKey());
      DistributionData oldDistribution = distributions.getOrDefault(key, DistributionData.EMPTY);
      DistributionData updatedDistribution = update.getUpdate().combine(oldDistribution);
      distributions.put(key, updatedDistribution);
    }
  }

  private void mergeCounters(Iterable<MetricUpdate<Long>> updates) {
    for (MetricUpdate<Long> update : updates) {
      MetricKey key = normalizeStepName(update.getKey());
      Long oldValue = counters.getOrDefault(key, 0L);
      Long updatedValue = oldValue + update.getUpdate();
      counters.put(key, updatedValue);
    }
  }

  @Override
  public MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
    return new QueryResults(filter);
  }

  private class QueryResults extends MetricQueryResults {
    private final MetricsFilter filter;

    private QueryResults(MetricsFilter filter) {
      this.filter = filter;
    }

    @Override
    public Iterable<MetricResult<Long>> getCounters() {
      return FluentIterable.from(counters.entrySet())
          .filter(matchesFilter(filter))
          .transform(this::counterUpdateToResult)
          .toList();
    }

    private MetricResult<Long> counterUpdateToResult(Map.Entry<MetricKey, Long> entry) {
      MetricKey key = entry.getKey();
      Long counter = entry.getValue();
      return MetricResult.create(key, counter, counter);
    }

    @Override
    public Iterable<MetricResult<DistributionResult>> getDistributions() {
      return FluentIterable.from(distributions.entrySet())
          .filter(matchesFilter(filter))
          .transform(this::distributionUpdateToResult)
          .toList();
    }

    private MetricResult<DistributionResult> distributionUpdateToResult(
        Map.Entry<MetricKey, DistributionData> entry) {
      MetricKey key = entry.getKey();
      DistributionResult distributionResult = entry.getValue().extractResult();
      return MetricResult.create(key, distributionResult, distributionResult);
    }

    @Override
    public Iterable<MetricResult<GaugeResult>> getGauges() {
      return FluentIterable.from(gauges.entrySet())
          .filter(matchesFilter(filter))
          .transform(this::gaugeUpdateToResult)
          .toList();
    }

    private MetricResult<GaugeResult> gaugeUpdateToResult(Map.Entry<MetricKey, GaugeData> entry) {
      MetricKey key = entry.getKey();
      GaugeResult gaugeResult = entry.getValue().extractResult();
      return MetricResult.create(key, gaugeResult, gaugeResult);
    }

    private Predicate<Map.Entry<MetricKey, ?>> matchesFilter(final MetricsFilter filter) {
      return entry -> MetricFiltering.matches(filter, entry.getKey());
    }
  }
}
