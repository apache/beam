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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.util.JsonFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Metrics containers by step.
 *
 * <p>This class is not thread-safe.
 */
public class MetricsContainerStepMap implements Serializable {

  private Map<String, MetricsContainerImpl> metricsContainers;
  private MetricsContainerImpl unboundContainer = new MetricsContainerImpl(null);

  public MetricsContainerStepMap() {
    this.metricsContainers = new ConcurrentHashMap<>();
  }

  /* Returns the container that is not bound to any step name. */
  public MetricsContainerImpl getUnboundContainer() {
    return this.unboundContainer;
  }

  /** Returns the container for the given step name. */
  public MetricsContainerImpl getContainer(String stepName) {
    if (stepName == null) {
      // TODO(https://github.com/apache/beam/issues/19275): Disallow this in the future, some tests
      // rely on an empty step name today.
      return getUnboundContainer();
    }
    return metricsContainers.computeIfAbsent(
        stepName, (String name) -> new MetricsContainerImpl(name));
  }

  /**
   * Update this {@link MetricsContainerStepMap} with all values from given {@link
   * MetricsContainerStepMap}.
   */
  public void updateAll(MetricsContainerStepMap other) {
    for (Map.Entry<String, MetricsContainerImpl> container : other.metricsContainers.entrySet()) {
      getContainer(container.getKey()).update(container.getValue());
    }
    getUnboundContainer().update(other.getUnboundContainer());
  }

  /**
   * Update {@link MetricsContainerImpl} for given step in this map with all values from given
   * {@link MetricsContainerImpl}.
   */
  public void update(String step, MetricsContainerImpl container) {
    getContainer(step).update(container);
  }

  /** Reset the metric containers. */
  public void reset() {
    metricsContainers.forEach((key, value) -> value.reset());
    unboundContainer.reset();
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof MetricsContainerStepMap) {
      MetricsContainerStepMap metricsContainerStepMap = (MetricsContainerStepMap) object;
      return Objects.equals(metricsContainers, metricsContainerStepMap.metricsContainers)
          && Objects.equals(unboundContainer, metricsContainerStepMap.unboundContainer);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricsContainers, unboundContainer);
  }

  /**
   * Returns {@link MetricResults} based on given {@link MetricsContainerStepMap} of attempted
   * metrics.
   *
   * <p>This constructor is intended for runners which only support `attempted` metrics. Accessing
   * {@link MetricResult#getCommitted()} in the resulting {@link MetricResults} will result in an
   * {@link UnsupportedOperationException}.
   */
  public static MetricResults asAttemptedOnlyMetricResults(
      MetricsContainerStepMap attemptedMetricsContainers) {
    return asMetricResults(attemptedMetricsContainers, new MetricsContainerStepMap());
  }

  /**
   * Returns {@link MetricResults} based on given {@link MetricsContainerStepMap
   * MetricsContainerStepMaps} of attempted and committed metrics.
   *
   * <p>This constructor is intended for runners which support both attempted and committed metrics.
   */
  public static MetricResults asMetricResults(
      MetricsContainerStepMap attemptedMetricsContainers,
      MetricsContainerStepMap committedMetricsContainers) {
    Map<MetricKey, MetricResult<Long>> counters = new HashMap<>();
    Map<MetricKey, MetricResult<DistributionData>> distributions = new HashMap<>();
    Map<MetricKey, MetricResult<GaugeData>> gauges = new HashMap<>();
    Map<MetricKey, MetricResult<StringSetData>> sets = new HashMap<>();
    Map<MetricKey, MetricResult<HistogramData>> perWorkerHistograms = new HashMap<>();

    attemptedMetricsContainers.forEachMetricContainer(
        container -> {
          MetricUpdates cumulative = container.getCumulative();
          mergeAttemptedResults(counters, cumulative.counterUpdates(), (l, r) -> l + r);
          mergeAttemptedResults(
              distributions, cumulative.distributionUpdates(), DistributionData::combine);
          mergeAttemptedResults(gauges, cumulative.gaugeUpdates(), GaugeData::combine);
          mergeAttemptedResults(sets, cumulative.stringSetUpdates(), StringSetData::combine);
          mergeAttemptedResults(
              perWorkerHistograms, cumulative.perWorkerHistogramsUpdates(), HistogramData::combine);
        });
    committedMetricsContainers.forEachMetricContainer(
        container -> {
          MetricUpdates cumulative = container.getCumulative();
          mergeCommittedResults(counters, cumulative.counterUpdates(), (l, r) -> l + r);
          mergeCommittedResults(
              distributions, cumulative.distributionUpdates(), DistributionData::combine);
          mergeCommittedResults(gauges, cumulative.gaugeUpdates(), GaugeData::combine);
          mergeCommittedResults(sets, cumulative.stringSetUpdates(), StringSetData::combine);
          mergeCommittedResults(
              perWorkerHistograms, cumulative.perWorkerHistogramsUpdates(), HistogramData::combine);
        });

    return new DefaultMetricResults(
        counters.values(),
        distributions.values().stream()
            .map(result -> result.transform(DistributionData::extractResult))
            .collect(toList()),
        gauges.values().stream()
            .map(result -> result.transform(GaugeData::extractResult))
            .collect(toList()),
        sets.values().stream()
            .map(result -> result.transform(StringSetData::extractResult))
            .collect(toList()),
        perWorkerHistograms.values().stream()
            .map(result -> result.transform(HistogramData::extractResult))
            .collect(toList()));
  }

  /** Return the cumulative values for any metrics in this container as MonitoringInfos. */
  public Iterable<MonitoringInfo> getMonitoringInfos() {
    // Extract user metrics and store as MonitoringInfos.
    ArrayList<MonitoringInfo> monitoringInfos = new ArrayList<>();
    forEachMetricContainer(
        container -> {
          for (MonitoringInfo mi : container.getMonitoringInfos()) {
            monitoringInfos.add(mi);
          }
        });
    return monitoringInfos;
  }

  /** Return the cumulative values for any metrics in this container as MonitoringInfo data. */
  public Map<String, ByteString> getMonitoringData(ShortIdMap shortIds) {
    // Extract user metrics and store as MonitoringInfos.
    ImmutableMap.Builder<String, ByteString> builder = ImmutableMap.builder();
    forEachMetricContainer((container) -> builder.putAll(container.getMonitoringData(shortIds)));
    return builder.build();
  }

  @Override
  public String toString() {
    JobApi.MetricResults results =
        JobApi.MetricResults.newBuilder().addAllAttempted(getMonitoringInfos()).build();
    GetJobMetricsResponse response = GetJobMetricsResponse.newBuilder().setMetrics(results).build();
    try {
      JsonFormat.Printer printer = JsonFormat.printer();
      return printer.print(response);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private void forEachMetricContainer(Consumer<MetricsContainerImpl> c) {
    metricsContainers.forEach((key, value) -> c.accept(value));
    c.accept(unboundContainer);
  }

  @SuppressWarnings("ConstantConditions")
  private static <T> void mergeAttemptedResults(
      Map<MetricKey, MetricResult<T>> metricResultMap,
      Iterable<MetricUpdate<T>> updates,
      BiFunction<T, T, T> combine) {
    for (MetricUpdate<T> metricUpdate : updates) {
      MetricKey key = metricUpdate.getKey();
      metricResultMap.compute(
          key,
          (k, current) -> {
            if (current == null) {
              return MetricResult.attempted(key, metricUpdate.getUpdate());
            } else {
              return current.addAttempted(metricUpdate.getUpdate(), combine);
            }
          });
    }
  }

  private static <T> void mergeCommittedResults(
      Map<MetricKey, MetricResult<T>> metricResultMap,
      Iterable<MetricUpdate<T>> updates,
      BiFunction<T, T, T> combine) {
    for (MetricUpdate<T> metricUpdate : updates) {
      MetricKey key = metricUpdate.getKey();
      if (metricResultMap.computeIfPresent(
              key, (k, current) -> current.addCommitted(metricUpdate.getUpdate(), combine))
          == null) {
        throw new IllegalStateException(
            String.format(
                "%s: existing 'attempted' result not found for 'committed' value %s",
                key, metricUpdate.getUpdate()));
      }
    }
  }
}
