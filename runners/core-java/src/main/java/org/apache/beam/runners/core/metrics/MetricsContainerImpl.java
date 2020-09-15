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

import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.DISTRIBUTION_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.LATEST_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Gauge;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metric;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class MetricsContainerImpl implements Serializable, MetricsContainer {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsContainerImpl.class);

  private final @Nullable String stepName;

  private MetricsMap<MetricName, CounterCell> counters = new MetricsMap<>(CounterCell::new);

  private MetricsMap<MetricName, DistributionCell> distributions =
      new MetricsMap<>(DistributionCell::new);

  private MetricsMap<MetricName, GaugeCell> gauges = new MetricsMap<>(GaugeCell::new);

  /** Create a new {@link MetricsContainerImpl} associated with the given {@code stepName}. */
  public MetricsContainerImpl(@Nullable String stepName) {
    this.stepName = stepName;
  }

  /** Reset the metrics. */
  public void reset() {
    reset(counters);
    reset(distributions);
    reset(gauges);
  }

  private void reset(MetricsMap<MetricName, ? extends MetricCell<?>> cells) {
    for (MetricCell<?> cell : cells.values()) {
      cell.reset();
    }
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
  public @Nullable CounterCell tryGetCounter(MetricName metricName) {
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
  public @Nullable DistributionCell tryGetDistribution(MetricName metricName) {
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
  public @Nullable GaugeCell tryGetGauge(MetricName metricName) {
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

  /** @return The MonitoringInfo generated from the metricUpdate. */
  private @Nullable MonitoringInfo counterUpdateToMonitoringInfo(MetricUpdate<Long> metricUpdate) {
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder(true);

    MetricName metricName = metricUpdate.getKey().metricName();
    if (metricName instanceof MonitoringInfoMetricName) {
      MonitoringInfoMetricName monitoringInfoName = (MonitoringInfoMetricName) metricName;
      // Represents a specific MonitoringInfo for a specific URN.
      builder.setUrn(monitoringInfoName.getUrn());
      for (Entry<String, String> e : monitoringInfoName.getLabels().entrySet()) {
        builder.setLabel(e.getKey(), e.getValue());
      }
    } else { // Represents a user counter.
      // Drop if the stepname is not set. All user counters must be
      // defined for a PTransform. They must be defined on a container bound to a step.
      if (this.stepName == null) {
        return null;
      }

      builder
          .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
          .setLabel(
              MonitoringInfoConstants.Labels.NAMESPACE,
              metricUpdate.getKey().metricName().getNamespace())
          .setLabel(
              MonitoringInfoConstants.Labels.NAME, metricUpdate.getKey().metricName().getName())
          .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, metricUpdate.getKey().stepName());
    }

    builder.setInt64SumValue(metricUpdate.getUpdate());

    return builder.build();
  }

  /**
   * @param metricUpdate
   * @return The MonitoringInfo generated from the metricUpdate.
   */
  private @Nullable MonitoringInfo distributionUpdateToMonitoringInfo(
      MetricUpdate<org.apache.beam.runners.core.metrics.DistributionData> metricUpdate) {
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
      builder
          .setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64)
          .setLabel(
              MonitoringInfoConstants.Labels.NAMESPACE,
              metricUpdate.getKey().metricName().getNamespace())
          .setLabel(
              MonitoringInfoConstants.Labels.NAME, metricUpdate.getKey().metricName().getName());

      // Drop if the stepname is not set. All user counters must be
      // defined for a PTransform. They must be defined on a container bound to a step.
      if (this.stepName == null) {
        // TODO(BEAM-7191): Consider logging a warning with a quiet logging API.
        return null;
      }
      builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, metricUpdate.getKey().stepName());
    }
    builder.setInt64DistributionValue(metricUpdate.getUpdate());
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
        monitoringInfos.add(mi);
      }
    }

    for (MetricUpdate<org.apache.beam.runners.core.metrics.DistributionData> metricUpdate :
        metricUpdates.distributionUpdates()) {
      MonitoringInfo mi = distributionUpdateToMonitoringInfo(metricUpdate);
      if (mi != null) {
        monitoringInfos.add(mi);
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

  private void updateForSumInt64Type(MonitoringInfo monitoringInfo) {
    MetricName metricName = MonitoringInfoMetricName.of(monitoringInfo);
    CounterCell counter = getCounter(metricName);
    counter.inc(decodeInt64Counter(monitoringInfo.getPayload()));
  }

  private void updateForDistributionInt64Type(MonitoringInfo monitoringInfo) {
    MetricName metricName = MonitoringInfoMetricName.of(monitoringInfo);
    Distribution distribution = getDistribution(metricName);

    DistributionData data = decodeInt64Distribution(monitoringInfo.getPayload());
    distribution.update(data.sum(), data.count(), data.min(), data.max());
  }

  private void updateForLatestInt64Type(MonitoringInfo monitoringInfo) {
    MetricName metricName = MonitoringInfoMetricName.of(monitoringInfo);
    GaugeCell gauge = getGauge(metricName);
    gauge.update(decodeInt64Gauge(monitoringInfo.getPayload()));
  }

  /** Update values of this {@link MetricsContainerImpl} by reading from {@code monitoringInfos}. */
  public void update(Iterable<MonitoringInfo> monitoringInfos) {
    for (MonitoringInfo monitoringInfo : monitoringInfos) {
      if (monitoringInfo.getPayload().isEmpty()) {
        return;
      }

      switch (monitoringInfo.getType()) {
        case SUM_INT64_TYPE:
          updateForSumInt64Type(monitoringInfo);
          break;

        case DISTRIBUTION_INT64_TYPE:
          updateForDistributionInt64Type(monitoringInfo);
          break;

        case LATEST_INT64_TYPE:
          updateForLatestInt64Type(monitoringInfo);
          break;

        default:
          LOG.warn("Unsupported metric type {}", monitoringInfo.getType());
      }
    }
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
  public boolean equals(@Nullable Object object) {
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
