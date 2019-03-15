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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.metrics.Metric;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/**
 * Holds the metrics for a single step and uses metric cells that allow extracting the cumulative
 * value. Generally, this implementation should be used for a specific unit of commitment (bundle)
 * that wishes to report the values since the start of the bundle (e.g., for committed metrics).
 *
 * <p>This class is thread-safe. It is intended to be used with 1 (or more) threads are updating
 * metrics and at-most 1 thread is extracting updates by calling {@link #getUpdates} and {@link
 * #commitUpdates}. Outside of this it is still safe. Although races in the update extraction may
 * cause updates that don't actually have any changes, it will never lose an update.
 *
 * <p>For consistency, all threads that update metrics should finish before getting the final
 * cumulative values/updates.
 */
@Experimental(Kind.METRICS)
public class MetricsContainerImpl implements Serializable, MetricsContainer {

  @Nullable private final String stepName;

  private MetricsMap<MetricName, CounterCell> counters = new MetricsMap<>(CounterCell::new);

  private MetricsMap<MetricName, DistributionCell> distributions =
      new MetricsMap<>(DistributionCell::new);

  private MetricsMap<MetricName, GaugeCell> gauges = new MetricsMap<>(GaugeCell::new);

  /** Create a new {@link MetricsContainerImpl} associated with the given {@code stepName}. */
  public MetricsContainerImpl(@Nullable String stepName) {
    this.stepName = stepName;
  }

  /**
   * Return a {@code CounterCell} named {@code metricName}. If it doesn't exist, create a {@code
   * Metric} with the specified name.
   */
  @Override
  public CounterCell getCounter(MetricName metricName) {
    return counters.get(metricName);
  }

  /**
   * Return a {@code CounterCell} named {@code metricName}. If it doesn't exist, return {@code
   * null}.
   */
  @Nullable
  public CounterCell tryGetCounter(MetricName metricName) {
    return counters.tryGet(metricName);
  }

  /**
   * Return a {@code DistributionCell} named {@code metricName}. If it doesn't exist, create a
   * {@code Metric} with the specified name.
   */
  @Override
  public DistributionCell getDistribution(MetricName metricName) {
    return distributions.get(metricName);
  }

  /**
   * Return a {@code DistributionCell} named {@code metricName}. If it doesn't exist, return {@code
   * null}.
   */
  @Nullable
  public DistributionCell tryGetDistribution(MetricName metricName) {
    return distributions.tryGet(metricName);
  }

  /**
   * Return a {@code GaugeCell} named {@code metricName}. If it doesn't exist, create a {@code
   * Metric} with the specified name.
   */
  @Override
  public GaugeCell getGauge(MetricName metricName) {
    return gauges.get(metricName);
  }

  /**
   * Return a {@code GaugeCell} named {@code metricName}. If it doesn't exist, return {@code null}.
   */
  @Nullable
  public GaugeCell tryGetGauge(MetricName metricName) {
    return gauges.tryGet(metricName);
  }

  private <UpdateT, CellT extends MetricCell<UpdateT>>
      ImmutableList<MetricUpdate<UpdateT>> extractUpdates(MetricsMap<MetricName, CellT> cells) {
    ImmutableList.Builder<MetricUpdate<UpdateT>> updates = ImmutableList.builder();
    for (Map.Entry<MetricName, CellT> cell : cells.entries()) {
      if (cell.getValue().getDirty().beforeCommit()) {
        updates.add(
            MetricUpdate.create(
                MetricKey.create(stepName, cell.getKey()), cell.getValue().getCumulative()));
      }
    }
    return updates.build();
  }

  /**
   * Return the cumulative values for any metrics that have changed since the last time updates were
   * committed.
   */
  public MetricUpdates getUpdates() {
    return MetricUpdates.create(
        extractUpdates(counters), extractUpdates(distributions), extractUpdates(gauges));
  }

  /**
   * @param metricUpdate
   * @return The MonitoringInfo generated from the metricUpdate.
   */
  @Nullable
  private MonitoringInfo counterUpdateToMonitoringInfo(MetricUpdate<Long> metricUpdate) {
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder(true);
    MetricName metricName = metricUpdate.getKey().metricName();
    if (metricName instanceof MonitoringInfoMetricName) {
      MonitoringInfoMetricName monitoringInfoName = (MonitoringInfoMetricName) metricName;
      // Represents a specific MonitoringInfo for a specific URN.
      builder.setUrn(monitoringInfoName.getUrn());
      for (Entry<String, String> e : monitoringInfoName.getLabels().entrySet()) {
        builder.setLabel(e.getKey(), e.getValue());
      }
    } else { // Note: (metricName instanceof MetricName) is always True.
      // Represents a user counter.
      builder.setUrnForUserMetric(
          metricUpdate.getKey().metricName().getNamespace(),
          metricUpdate.getKey().metricName().getName());
      // Drop if the stepname is not set. All user counters must be
      // defined for a PTransform. They must be defined on a container bound to a step.
      if (this.stepName == null) {
        return null;
      }
      builder.setPTransformLabel(metricUpdate.getKey().stepName());
    }
    builder.setInt64Value(metricUpdate.getUpdate());
    builder.setTimestampToNow();
    return builder.build();
  }

  /** Return the cumulative values for any metrics in this container as MonitoringInfos. */
  public Iterable<MonitoringInfo> getMonitoringInfos() {
    // Extract user metrics and store as MonitoringInfos.
    ArrayList<MonitoringInfo> monitoringInfos = new ArrayList<MonitoringInfo>();
    MetricUpdates metricUpdates = this.getUpdates();

    for (MetricUpdate<Long> metricUpdate : metricUpdates.counterUpdates()) {
      MonitoringInfo mi = counterUpdateToMonitoringInfo(metricUpdate);
      if (mi != null) {
        monitoringInfos.add(counterUpdateToMonitoringInfo(metricUpdate));
      }
    }
    return monitoringInfos;
  }

  private void commitUpdates(MetricsMap<MetricName, ? extends MetricCell<?>> cells) {
    for (MetricCell<?> cell : cells.values()) {
      cell.getDirty().afterCommit();
    }
  }

  /**
   * Mark all of the updates that were retrieved with the latest call to {@link #getUpdates()} as
   * committed.
   */
  public void commitUpdates() {
    commitUpdates(counters);
    commitUpdates(distributions);
    commitUpdates(gauges);
  }

  private <UserT extends Metric, UpdateT, CellT extends MetricCell<UpdateT>>
      ImmutableList<MetricUpdate<UpdateT>> extractCumulatives(MetricsMap<MetricName, CellT> cells) {
    ImmutableList.Builder<MetricUpdate<UpdateT>> updates = ImmutableList.builder();
    for (Map.Entry<MetricName, CellT> cell : cells.entries()) {
      UpdateT update = checkNotNull(cell.getValue().getCumulative());
      updates.add(MetricUpdate.create(MetricKey.create(stepName, cell.getKey()), update));
    }
    return updates.build();
  }

  /**
   * Return the {@link MetricUpdates} representing the cumulative values of all metrics in this
   * container.
   */
  public MetricUpdates getCumulative() {
    return MetricUpdates.create(
        extractCumulatives(counters),
        extractCumulatives(distributions),
        extractCumulatives(gauges));
  }

  /** Update values of this {@link MetricsContainerImpl} by merging the value of another cell. */
  public void update(MetricsContainerImpl other) {
    updateCounters(counters, other.counters);
    updateDistributions(distributions, other.distributions);
    updateGauges(gauges, other.gauges);
  }

  private void updateCounters(
      MetricsMap<MetricName, CounterCell> current, MetricsMap<MetricName, CounterCell> updates) {
    for (Map.Entry<MetricName, CounterCell> counter : updates.entries()) {
      current.get(counter.getKey()).inc(counter.getValue().getCumulative());
    }
  }

  private void updateDistributions(
      MetricsMap<MetricName, DistributionCell> current,
      MetricsMap<MetricName, DistributionCell> updates) {
    for (Map.Entry<MetricName, DistributionCell> counter : updates.entries()) {
      current.get(counter.getKey()).update(counter.getValue().getCumulative());
    }
  }

  private void updateGauges(
      MetricsMap<MetricName, GaugeCell> current, MetricsMap<MetricName, GaugeCell> updates) {
    for (Map.Entry<MetricName, GaugeCell> counter : updates.entries()) {
      current.get(counter.getKey()).update(counter.getValue().getCumulative());
    }
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof MetricsContainerImpl) {
      MetricsContainerImpl metricsContainerImpl = (MetricsContainerImpl) object;
      return Objects.equals(stepName, metricsContainerImpl.stepName)
          && Objects.equals(counters, metricsContainerImpl.counters)
          && Objects.equals(distributions, metricsContainerImpl.distributions)
          && Objects.equals(gauges, metricsContainerImpl.gauges);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(stepName, counters, distributions, gauges);
  }
}
