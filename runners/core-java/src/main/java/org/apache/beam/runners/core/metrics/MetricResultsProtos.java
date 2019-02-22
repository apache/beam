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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.runners.core.metrics.MonitoringInfos.forEachMetricType;
import static org.apache.beam.runners.core.metrics.MonitoringInfos.keyFromMonitoringInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;

/**
 * Convert between Java SDK {@link MetricResults} and corresponding protobuf {@link
 * MetricsApi.MetricResults}.
 *
 * <p>Their structures are similar, but the former distinguishes "attempted" and "committed" values
 * at the lowest level (a {@link MetricResult} contains both), while the latter separates them into
 * two lists of {@link MonitoringInfo}s at the top level ({@link
 * MetricsApi.MetricResults#getAttemptedList attempted}, {@link
 * MetricsApi.MetricResults#getCommittedList} committed}.
 *
 * <p>The proto form also holds more kinds of values, so some {@link MonitoringInfo}s may be dropped
 * from converting to the Java SDK form.
 */
public class MetricResultsProtos {

  /** Convert metric results from proto to Java SDK form. */
  public static MetricResults fromProto(MetricsApi.MetricResults metricResults) {
    return new MetricResultsBuilder(metricResults).build();
  }

  /** Convert metric results from Java SDK to proto form. */
  public static MetricsApi.MetricResults toProto(MetricResults metricResults) {
    MetricsApi.MetricResults.Builder builder = MetricsApi.MetricResults.newBuilder();
    MetricQueryResults results = metricResults.allMetrics();
    results
        .getCounters()
        .forEach(
            counter ->
                addMetricResultToBuilder(
                    builder, counter, SimpleMonitoringInfoBuilder::setInt64Value));
    results
        .getDistributions()
        .forEach(
            distribution ->
                addMetricResultToBuilder(
                    builder, distribution, SimpleMonitoringInfoBuilder::setIntDistributionValue));
    results
        .getGauges()
        .forEach(
            gauge ->
                addMetricResultToBuilder(
                    builder, gauge, SimpleMonitoringInfoBuilder::setGaugeValue));
    return builder.build();
  }

  /**
   * Add this {@link MetricResult}'s "attempted" and "committed" values to the corresponding lists
   * of {@param builder}.
   */
  private static <T> void addMetricResultToBuilder(
      MetricsApi.MetricResults.Builder builder,
      MetricResult<T> metricResult,
      BiConsumer<SimpleMonitoringInfoBuilder, T> set) {
    MetricKey metricKey = metricResult.getKey();
    addMetricToBuilder(
        builder,
        metricKey,
        metricResult.getAttempted(),
        set,
        MetricsApi.MetricResults.Builder::addAttempted);
    T committed = metricResult.getCommittedOrNull();
    if (committed != null) {
      addMetricToBuilder(
          builder,
          metricKey,
          metricResult.getCommitted(),
          set,
          MetricsApi.MetricResults.Builder::addCommitted);
    }
  }

  /**
   * Add a metric key and value (from a {@link MetricResult}) to a {@link
   * MetricsApi.MetricResults.Builder}.
   *
   * @param builder proto {@link MetricsApi.MetricResults.Builder} to add to
   * @param set set this metric's value on a {@link SimpleMonitoringInfoBuilder} (via an API that's
   *     specific to this metric's type, as provided by the caller)
   * @param add add the {@link MonitoringInfo} created from this key/value to either the "attempted"
   *     or "committed" list of the {@link MetricsApi.MetricResults.Builder builder}
   */
  private static <T> void addMetricToBuilder(
      MetricsApi.MetricResults.Builder builder,
      MetricKey metricKey,
      T value,
      BiConsumer<SimpleMonitoringInfoBuilder, T> set,
      BiConsumer<MetricsApi.MetricResults.Builder, MonitoringInfo> add) {
    if (value != null) {
      SimpleMonitoringInfoBuilder partial =
          new SimpleMonitoringInfoBuilder().setLabelsAndUrnFrom(metricKey);
      set.accept(partial, value);
      MonitoringInfo monitoringInfo = partial.build();
      if (monitoringInfo != null) {
        add.accept(builder, monitoringInfo);
      }
    }
  }

  /**
   * Helper for converting {@link MetricsApi.MetricResults} to {@link MetricResults}.
   *
   * <p>The former separates "attempted" and "committed" metrics at the highest level, while the
   * latter splits on metric-type (counter, distribution, or gauge) first, so converting basically
   * amounts pivoting those between those two.
   */
  private static class MetricResultsBuilder {
    private final Map<MetricKey, MetricResultBuilder<Long>> counters = new ConcurrentHashMap<>();
    private final Map<MetricKey, MetricResultBuilder<DistributionResult>> distributions =
        new ConcurrentHashMap<>();
    private final Map<MetricKey, MetricResultBuilder<GaugeResult>> gauges =
        new ConcurrentHashMap<>();

    /**
     * Populate metric-type-specific maps with {@link MonitoringInfo}s from a {@link
     * MetricsApi.MetricResults}.
     */
    public MetricResultsBuilder(MetricsApi.MetricResults metrics) {
      add(metrics.getAttemptedList(), false);
      add(metrics.getCommittedList(), true);
    }

    public MetricResults build() {
      return new DefaultMetricResults(build(counters), build(distributions), build(gauges));
    }

    private void add(Iterable<MonitoringInfo> monitoringInfos, Boolean committed) {
      for (MonitoringInfo monitoringInfo : monitoringInfos) {
        addMonitoringInfo(monitoringInfo, committed);
      }
    }

    /** Helper for turning a map of result-builders into a sequence of results. */
    private static <T> List<MetricResult<T>> build(Map<MetricKey, MetricResultBuilder<T>> map) {
      return map.values().stream().map(MetricResultBuilder::build).collect(toList());
    }

    private void addMonitoringInfo(MonitoringInfo monitoringInfo, Boolean committed) {
      MetricKey metricKey = keyFromMonitoringInfo(monitoringInfo);
      forEachMetricType(
          monitoringInfo.getMetric(),
          monitoringInfo.getType(),
          monitoringInfo.getTimestamp(),
          counter -> addMetricToResultMap(metricKey, counter, committed, counters),
          distribution -> addMetricToResultMap(metricKey, distribution, committed, distributions),
          gauge -> addMetricToResultMap(metricKey, gauge, committed, gauges));
    }

    private <T> void addMetricToResultMap(
        MetricKey key, T value, Boolean committed, Map<MetricKey, MetricResultBuilder<T>> map) {
      if (committed) {
        MetricResultBuilder builder = map.get(key);
        if (builder == null) {
          throw new IllegalStateException(
              String.format("No attempted value found for committed metric %s: %s", key, value));
        }
        builder.setCommitted(value);
      } else {
        map.put(key, new MetricResultBuilder(key, value));
      }
    }

    /**
     * Helper class for storing and combining {@link MetricResult#getAttempted attempted} and {@link
     * MetricResult#getCommitted committed} values, to obtain complete {@link MetricResult}s.
     *
     * <p>Used while traversing {@link MonitoringInfo} lists where they are stored separately.
     */
    private static class MetricResultBuilder<T> {
      private final MetricKey key;
      private final T attempted;
      @Nullable private T committed;

      public MetricResultBuilder(MetricKey key, T attempted) {
        this.key = key;
        this.attempted = attempted;
      }

      public void setCommitted(T committed) {
        this.committed = committed;
      }

      public MetricResult<T> build() {
        return MetricResult.create(key, committed, attempted);
      }
    }
  }
}
