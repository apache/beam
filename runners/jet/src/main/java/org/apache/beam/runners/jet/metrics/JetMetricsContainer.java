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

import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.map.IMap;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** Jet specific implementation of {@link MetricsContainer}. */
public class JetMetricsContainer implements MetricsContainer {

  public static String getMetricsMapName(long jobId) {
    return Util.idToString(jobId) + "_METRICS";
  }

  private final String stepName;
  private final String metricsKey;

  private final Map<MetricName, CounterImpl> counters = new HashMap<>();
  private final Map<MetricName, DistributionImpl> distributions = new HashMap<>();
  private final Map<MetricName, GaugeImpl> gauges = new HashMap<>();

  private final IMap<String, MetricUpdates> accumulator;

  public JetMetricsContainer(String stepName, String ownerId, Processor.Context context) {
    this.metricsKey = context.globalProcessorIndex() + "/" + stepName + "/" + ownerId;
    this.stepName = stepName;
    this.accumulator = context.jetInstance().getMap(getMetricsMapName(context.jobId()));
  }

  @Override
  public Counter getCounter(MetricName metricName) {
    return counters.computeIfAbsent(metricName, CounterImpl::new);
  }

  @Override
  public Distribution getDistribution(MetricName metricName) {
    return distributions.computeIfAbsent(metricName, DistributionImpl::new);
  }

  @Override
  public Gauge getGauge(MetricName metricName) {
    return gauges.computeIfAbsent(metricName, GaugeImpl::new);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  public void flush(boolean async) {
    if (counters.isEmpty() && distributions.isEmpty() && gauges.isEmpty()) {
      return;
    }

    ImmutableList<MetricUpdates.MetricUpdate<Long>> counters = extractUpdates(this.counters);
    ImmutableList<MetricUpdates.MetricUpdate<DistributionData>> distributions =
        extractUpdates(this.distributions);
    ImmutableList<MetricUpdates.MetricUpdate<GaugeData>> gauges = extractUpdates(this.gauges);
    MetricUpdates updates = new MetricUpdatesImpl(counters, distributions, gauges);

    if (async) {
      accumulator.setAsync(metricsKey, updates);
    } else {
      accumulator.set(metricsKey, updates);
    }
  }

  private <UpdateT, CellT extends AbstractMetric<UpdateT>>
      ImmutableList<MetricUpdates.MetricUpdate<UpdateT>> extractUpdates(
          Map<MetricName, CellT> cells) {
    ImmutableList.Builder<MetricUpdates.MetricUpdate<UpdateT>> updates = ImmutableList.builder();
    for (CellT cell : cells.values()) {
      UpdateT value = cell.getValue();
      if (value != null) {
        MetricKey key = MetricKey.create(stepName, cell.getName());
        MetricUpdates.MetricUpdate<UpdateT> update = MetricUpdates.MetricUpdate.create(key, value);
        updates.add(update);
      }
    }
    return updates.build();
  }

  private static class MetricUpdatesImpl extends MetricUpdates implements Serializable {

    private final Iterable<MetricUpdate<Long>> counters;
    private final Iterable<MetricUpdate<DistributionData>> distributions;
    private final Iterable<MetricUpdate<GaugeData>> gauges;

    MetricUpdatesImpl(
        Iterable<MetricUpdate<Long>> counters,
        Iterable<MetricUpdate<DistributionData>> distributions,
        Iterable<MetricUpdate<GaugeData>> gauges) {
      this.counters = counters;
      this.distributions = distributions;
      this.gauges = gauges;
    }

    @Override
    public Iterable<MetricUpdate<Long>> counterUpdates() {
      return counters;
    }

    @Override
    public Iterable<MetricUpdate<DistributionData>> distributionUpdates() {
      return distributions;
    }

    @Override
    public Iterable<MetricUpdate<GaugeData>> gaugeUpdates() {
      return gauges;
    }
  }
}
