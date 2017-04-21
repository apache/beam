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
package org.apache.beam.sdk.metrics;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;

/**
 * Holds the metrics for a single step and unit-of-commit (bundle).
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
public class MetricsContainer {

  private final String stepName;

  private MetricsMap<MetricName, CounterCell> counters =
      new MetricsMap<>(new MetricsMap.Factory<MetricName, CounterCell>() {
        @Override
        public CounterCell createInstance(MetricName unusedKey) {
          return new CounterCell();
        }
      });

  private MetricsMap<MetricName, DistributionCell> distributions =
      new MetricsMap<>(new MetricsMap.Factory<MetricName, DistributionCell>() {
        @Override
        public DistributionCell createInstance(MetricName unusedKey) {
          return new DistributionCell();
        }
      });

  private MetricsMap<MetricName, GaugeCell> gauges =
      new MetricsMap<>(new MetricsMap.Factory<MetricName, GaugeCell>() {
        @Override
        public GaugeCell createInstance(MetricName unusedKey) {
          return new GaugeCell();
        }
      });

  /**
   * Create a new {@link MetricsContainer} associated with the given {@code stepName}.
   */
  public MetricsContainer(String stepName) {
    this.stepName = stepName;
  }

  /**
   * Return the {@link CounterCell} that should be used for implementing the given
   * {@code metricName} in this container.
   */
  public CounterCell getCounter(MetricName metricName) {
    return counters.get(metricName);
  }

  /**
   * Return the {@link DistributionCell} that should be used for implementing the given
   * {@code metricName} in this container.
   */
  public DistributionCell getDistribution(MetricName metricName) {
    return distributions.get(metricName);
  }

  /**
   * Return the {@link GaugeCell} that should be used for implementing the given
   * {@code metricName} in this container.
   */
  public GaugeCell getGauge(MetricName metricName) {
    return gauges.get(metricName);
  }

  private <UpdateT, CellT extends MetricCell<?, UpdateT>>
  ImmutableList<MetricUpdate<UpdateT>> extractUpdates(
      MetricsMap<MetricName, CellT> cells) {
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

  private void commitUpdates(MetricsMap<MetricName, ? extends MetricCell<?, ?>> cells) {
    for (MetricCell<?, ?> cell : cells.values()) {
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
  }

  private <UpdateT, CellT extends MetricCell<?, UpdateT>>
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
}
