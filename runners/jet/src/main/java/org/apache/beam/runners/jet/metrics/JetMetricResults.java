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

import com.hazelcast.map.IMap;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Jet specific {@link MetricResults}. */
public class JetMetricResults extends MetricResults {

  @GuardedBy("this")
  private final Counters counters = new Counters();

  @GuardedBy("this")
  private final Distributions distributions = new Distributions();

  @GuardedBy("this")
  private final Gauges gauges = new Gauges();

  @GuardedBy("this")
  private IMap<String, MetricUpdates> metricsAccumulator;

  public JetMetricResults(IMap<String, MetricUpdates> metricsAccumulator) {
    this.metricsAccumulator = metricsAccumulator;
  }

  public synchronized void freeze() {
    updateLocalMetrics(metricsAccumulator);
    this.metricsAccumulator = null;
  }

  @Override
  public synchronized MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
    if (metricsAccumulator != null) {
      updateLocalMetrics(metricsAccumulator);
    }
    return new QueryResults(
        counters.filter(filter), distributions.filter(filter), gauges.filter(filter));
  }

  private synchronized void updateLocalMetrics(IMap<String, MetricUpdates> metricsAccumulator) {
    counters.clear();
    distributions.clear();
    gauges.clear();

    for (MetricUpdates metricUpdates : metricsAccumulator.values()) {
      counters.merge(metricUpdates.counterUpdates());
      distributions.merge(metricUpdates.distributionUpdates());
      gauges.merge(metricUpdates.gaugeUpdates());
    }
  }

  private static Predicate<Map.Entry<MetricKey, ?>> matchesFilter(final MetricsFilter filter) {
    return entry -> MetricFiltering.matches(filter, entry.getKey());
  }

  private static class QueryResults extends MetricQueryResults {
    private final Iterable<MetricResult<Long>> counters;
    private final Iterable<MetricResult<DistributionResult>> distributions;
    private final Iterable<MetricResult<GaugeResult>> gauges;

    private QueryResults(
        Iterable<MetricResult<Long>> counters,
        Iterable<MetricResult<DistributionResult>> distributions,
        Iterable<MetricResult<GaugeResult>> gauges) {
      this.counters = counters;
      this.distributions = distributions;
      this.gauges = gauges;
    }

    @Override
    public Iterable<MetricResult<Long>> getCounters() {
      return counters;
    }

    @Override
    public Iterable<MetricResult<DistributionResult>> getDistributions() {
      return distributions;
    }

    @Override
    public Iterable<MetricResult<GaugeResult>> getGauges() {
      return gauges;
    }
  }

  private static class Counters {

    private final Map<MetricKey, Long> counters = new HashMap<>();

    void merge(Iterable<MetricUpdate<Long>> updates) {
      for (MetricUpdate<Long> update : updates) {
        MetricKey key = update.getKey();
        Long oldValue = counters.getOrDefault(key, 0L);
        Long updatedValue = oldValue + update.getUpdate();
        counters.put(key, updatedValue);
      }
    }

    void clear() {
      counters.clear();
    }

    Iterable<MetricResult<Long>> filter(MetricsFilter filter) {
      return FluentIterable.from(counters.entrySet())
          .filter(matchesFilter(filter))
          .transform(this::toUpdateResult)
          .toList();
    }

    private MetricResult<Long> toUpdateResult(Map.Entry<MetricKey, Long> entry) {
      MetricKey key = entry.getKey();
      Long counter = entry.getValue();
      return MetricResult.create(key, counter, counter);
    }
  }

  private static class Distributions {

    private final Map<MetricKey, DistributionData> distributions = new HashMap<>();

    void merge(Iterable<MetricUpdate<DistributionData>> updates) {
      for (MetricUpdate<DistributionData> update : updates) {
        MetricKey key = update.getKey();
        DistributionData oldDistribution = distributions.getOrDefault(key, DistributionData.EMPTY);
        DistributionData updatedDistribution = update.getUpdate().combine(oldDistribution);
        distributions.put(key, updatedDistribution);
      }
    }

    void clear() {
      distributions.clear();
    }

    Iterable<MetricResult<DistributionResult>> filter(MetricsFilter filter) {
      return FluentIterable.from(distributions.entrySet())
          .filter(matchesFilter(filter))
          .transform(this::toUpdateResult)
          .toList();
    }

    private MetricResult<DistributionResult> toUpdateResult(
        Map.Entry<MetricKey, DistributionData> entry) {
      MetricKey key = entry.getKey();
      DistributionResult distributionResult = entry.getValue().extractResult();
      return MetricResult.create(key, distributionResult, distributionResult);
    }
  }

  private static class Gauges {

    private final Map<MetricKey, GaugeData> gauges = new HashMap<>();

    void merge(Iterable<MetricUpdate<GaugeData>> updates) {
      for (MetricUpdate<GaugeData> update : updates) {
        MetricKey key = update.getKey();
        GaugeData oldGauge = gauges.getOrDefault(key, GaugeData.empty());
        GaugeData updatedGauge = update.getUpdate().combine(oldGauge);
        gauges.put(key, updatedGauge);
      }
    }

    void clear() {
      gauges.clear();
    }

    Iterable<MetricResult<GaugeResult>> filter(MetricsFilter filter) {
      return FluentIterable.from(gauges.entrySet())
          .filter(matchesFilter(filter))
          .transform(this::toUpdateResult)
          .toList();
    }

    private MetricResult<GaugeResult> toUpdateResult(Map.Entry<MetricKey, GaugeData> entry) {
      MetricKey key = entry.getKey();
      GaugeResult gaugeResult = entry.getValue().extractResult();
      return MetricResult.create(key, gaugeResult, gaugeResult);
    }
  }
}
