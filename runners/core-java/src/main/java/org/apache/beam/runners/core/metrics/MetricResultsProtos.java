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
package org.apache.beam.runners.core.metrics;

import static org.apache.beam.runners.core.metrics.MonitoringInfos.processMetric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Metric;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;

/** Convert {@link MetricResults} to and from {@link BeamFnApi.MetricResults}. */
public class MetricResultsProtos {

  private static <T> void process(
      BeamFnApi.MetricResults.Builder builder,
      MetricKey metricKey,
      T value,
      BiConsumer<SimpleMonitoringInfoBuilder, T> set,
      BiConsumer<BeamFnApi.MetricResults.Builder, MonitoringInfo> add) {
    if (value != null) {
      SimpleMonitoringInfoBuilder partial =
          new SimpleMonitoringInfoBuilder().handleMetricKey(metricKey);
      ;
      set.accept(partial, value);
      MonitoringInfo monitoringInfo = partial.build();
      if (monitoringInfo != null) {
        add.accept(builder, monitoringInfo);
      }
    }
  }

  /**
   * Add this {@link MetricResult}'s "attempted" and "committed" values to the corresponding lists
   * of {@param builder}.
   */
  private static <T> void process(
      BeamFnApi.MetricResults.Builder builder,
      MetricResult<T> metricResult,
      BiConsumer<SimpleMonitoringInfoBuilder, T> set) {
    MetricKey metricKey = metricResult.getKey();
    process(
        builder,
        metricKey,
        metricResult.getAttempted(),
        set,
        BeamFnApi.MetricResults.Builder::addAttempted);
    if (!metricResult.hasCommitted()) {
      return;
    }
    process(
        builder,
        metricKey,
        metricResult.getCommitted(),
        set,
        BeamFnApi.MetricResults.Builder::addCommitted);
  }

  // Convert between proto- and SDK-representations of MetricResults

  /** Convert a {@link MetricResults} to a {@link BeamFnApi.MetricResults}. */
  public static BeamFnApi.MetricResults toProto(MetricResults metricResults) {
    BeamFnApi.MetricResults.Builder builder = BeamFnApi.MetricResults.newBuilder();
    MetricQueryResults results = metricResults.allMetrics();
    results
        .getCounters()
        .forEach(counter -> process(builder, counter, SimpleMonitoringInfoBuilder::setInt64Value));
    results
        .getDistributions()
        .forEach(
            distribution ->
                process(
                    builder, distribution, SimpleMonitoringInfoBuilder::setIntDistributionValue));
    results
        .getGauges()
        .forEach(gauge -> process(builder, gauge, SimpleMonitoringInfoBuilder::setGaugeValue));
    return builder.build();
  }

  /**
   * Helper for converting {@link BeamFnApi.MetricResults} to {@link MetricResults}.
   *
   * <p>The former separates "attempted" and "committed" metrics, while the latter splits on
   * metric-type (counter, distribution, or gauge) at the top level, so converting basically amounts
   * to performing that pivot.
   */
  private static class PTransformMetricResultsBuilder {
    private final Map<MetricKey, MetricResult<Long>> counters = new ConcurrentHashMap<>();
    private final Map<MetricKey, MetricResult<DistributionResult>> distributions =
        new ConcurrentHashMap<>();
    private final Map<MetricKey, MetricResult<GaugeResult>> gauges = new ConcurrentHashMap<>();

    public PTransformMetricResultsBuilder(BeamFnApi.MetricResults metrics) {
      add(metrics.getAttemptedList(), false);
      add(metrics.getCommittedList(), true);
    }

    public Map<MetricKey, MetricResult<Long>> getCounters() {
      return (Map) counters;
    }

    public Map<MetricKey, MetricResult<DistributionResult>> getDistributions() {
      return (Map) distributions;
    }

    public MetricResults build() {
      return new DefaultMetricResults(
          getCounters().values(), getDistributions().values(), getGauges().values());
    }

    public Map<MetricKey, MetricResult<GaugeResult>> getGauges() {
      return (Map) gauges;
    }

    public void add(Iterable<MonitoringInfo> monitoringInfos, Boolean committed) {
      for (MonitoringInfo monitoringInfo : monitoringInfos) {
        add(monitoringInfo, committed);
      }
    }

    public void add(MonitoringInfo monitoringInfo, Boolean committed) {
      add(MetricKey.of(monitoringInfo), monitoringInfo.getMetric(), committed);
    }

    public void add(MetricKey metricKey, Metric metric, Boolean committed) {
      processMetric(
          metric,
          counter -> add(metricKey, counter, committed, counters),
          distribution -> add(metricKey, distribution, committed, distributions),
          gauge -> add(metricKey, gauge, committed, gauges));
    }

    public <T> void add(
        MetricKey key, T value, Boolean committed, Map<MetricKey, MetricResult<T>> map) {
      if (committed) {
        MetricResult<T> result = map.get(key);
        map.put(key, result.setCommitted(value));
      } else {
        map.put(key, MetricResult.attempted(key, value));
      }
    }
  }

  public static MetricResults fromProto(BeamFnApi.MetricResults metricResults) {
    return new PTransformMetricResultsBuilder(metricResults).build();
  }
}
