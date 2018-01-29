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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.metrics.MetricKey;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.metrics.Metric;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;

/**
 * Holds the metrics for a single step and uses metric cells that allow extracting the cumulative
 * value. Generally, this implementation should be used for a specific unit of commitment (bundle)
 * that wishes to report the values since the start of the bundle (eg., for committed metrics).
 *
 * <p>This class is thread-safe. It is intended to be used with 1 (or more) threads are updating
 * metrics and at-most 1 thread is extracting updates by calling {@link #getUpdates} and
 * {@link #commitUpdates}. Outside of this it is still safe. Although races in the update extraction
 * may cause updates that don't actually have any changes, it will never lose an update.
 *
 * <p>For consistency, all threads that update metrics should finish before getting the final
 * cumulative values/updates.
 */
@Experimental(Kind.METRICS)
public class MetricsContainerImpl implements Serializable, MetricsContainer {

  private final String stepName;

  private MetricsMap<MetricName, CounterCell> counters = new MetricsMap<>(CounterCell::new);

  private MetricsMap<MetricName, DistributionCell> distributions =
      new MetricsMap<>(DistributionCell::new);

  private MetricsMap<MetricName, GaugeCell> gauges = new MetricsMap<>(GaugeCell::new);

  /**
   * Create a new {@link MetricsContainerImpl} associated with the given {@code stepName}.
   */
  public MetricsContainerImpl(String stepName) {
    this.stepName = stepName;
  }

  /**
   * Return a {@code CounterCell} named {@code metricName}. If it doesn't exist, create a
   * {@code Metric} with the specified name.
   */
  @Override
  public CounterCell getCounter(MetricName metricName) {
    return counters.get(metricName);
  }

  /**
   * Return a {@code CounterCell} named {@code metricName}. If it doesn't exist, return
   * {@code null}.
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
   * Return a {@code DistributionCell} named {@code metricName}. If it doesn't exist, return
   * {@code null}.
   */
  @Nullable
  public DistributionCell tryGetDistribution(MetricName metricName) {
    return distributions.tryGet(metricName);
  }

  /**
   * Return a {@code GaugeCell} named {@code metricName}. If it doesn't exist, create a
   * {@code Metric} with the specified name.
   */
  @Override
  public GaugeCell getGauge(MetricName metricName) {
    return gauges.get(metricName);
  }

  /**
   * Return a {@code GaugeCell} named {@code metricName}. If it doesn't exist, return
   * {@code null}.
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
        updates.add(MetricUpdate.create(MetricKey.create(stepName, cell.getKey()),
            cell.getValue().getCumulative()));
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
        extractUpdates(counters),
        extractUpdates(distributions),
        extractUpdates(gauges));
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
  ImmutableList<MetricUpdate<UpdateT>> extractCumulatives(
      MetricsMap<MetricName, CellT> cells) {
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

  /**
   * Update values of this {@link MetricsContainerImpl} by merging the value of another cell.
   */
  public void update(MetricsContainerImpl other) {
    updateCounters(counters, other.counters);
    updateDistributions(distributions, other.distributions);
    updateGauges(gauges, other.gauges);
  }

  private void updateCounters(
      MetricsMap<MetricName, CounterCell> current,
      MetricsMap<MetricName, CounterCell> updates) {
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
      MetricsMap<MetricName, GaugeCell> current,
      MetricsMap<MetricName, GaugeCell> updates) {
    for (Map.Entry<MetricName, GaugeCell> counter : updates.entries()) {
      current.get(counter.getKey()).update(counter.getValue().getCumulative());
    }
  }
}
