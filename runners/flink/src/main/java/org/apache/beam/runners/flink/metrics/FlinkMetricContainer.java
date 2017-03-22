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
package org.apache.beam.runners.flink.metrics;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.GaugeData;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;

/**
 * Helper class for holding a {@link MetricsContainer} and forwarding Beam metrics to
 * Flink accumulators and metrics.
 */
public class FlinkMetricContainer {

  private static final String METRIC_KEY_SEPARATOR = "__";
  static final String COUNTER_PREFIX = "__counter";
  static final String DISTRIBUTION_PREFIX = "__distribution";
  static final String GAUGE_PREFIX = "__gauge";

  private final MetricsContainer metricsContainer;
  private final RuntimeContext runtimeContext;
  private final Map<String, Counter> flinkCounterCache;
  private final Map<String, FlinkDistributionGauge> flinkDistributionGaugeCache;
  private final Map<String, FlinkGauge> flinkGaugeCache;

  public FlinkMetricContainer(String stepName, RuntimeContext runtimeContext) {
    metricsContainer = new MetricsContainer(stepName);
    this.runtimeContext = runtimeContext;
    flinkCounterCache = new HashMap<>();
    flinkDistributionGaugeCache = new HashMap<>();
    flinkGaugeCache = new HashMap<>();
  }

  public MetricsContainer getMetricsContainer() {
    return metricsContainer;
  }

  public void updateMetrics() {
    // update metrics
    MetricUpdates updates = metricsContainer.getUpdates();
    if (updates != null) {
      updateCounters(updates.counterUpdates());
      updateDistributions(updates.distributionUpdates());
      updateGauge(updates.gaugeUpdates());
      metricsContainer.commitUpdates();
    }
  }

  private void updateCounters(Iterable<MetricUpdates.MetricUpdate<Long>> updates) {

    for (MetricUpdates.MetricUpdate<Long> metricUpdate : updates) {

      String flinkMetricName = getFlinkMetricNameString(COUNTER_PREFIX, metricUpdate.getKey());
      Long update = metricUpdate.getUpdate();

      // update flink metric
      Counter counter = flinkCounterCache.get(flinkMetricName);
      if (counter == null) {
        counter = runtimeContext.getMetricGroup().counter(flinkMetricName);
        flinkCounterCache.put(flinkMetricName, counter);
      }
      counter.dec(counter.getCount());
      counter.inc(update);

      // update flink accumulator
      Accumulator<Long, Long> accumulator = runtimeContext.getAccumulator(flinkMetricName);
      if (accumulator == null) {
        accumulator = new LongCounter(update);
        runtimeContext.addAccumulator(flinkMetricName, accumulator);
      } else {
        accumulator.resetLocal();
        accumulator.add(update);
      }
    }
  }

  private void updateDistributions(Iterable<MetricUpdates.MetricUpdate<DistributionData>> updates) {

    for (MetricUpdates.MetricUpdate<DistributionData> metricUpdate : updates) {

      String flinkMetricName =
          getFlinkMetricNameString(DISTRIBUTION_PREFIX, metricUpdate.getKey());
      DistributionData update = metricUpdate.getUpdate();

      // update flink metric
      FlinkDistributionGauge gauge = flinkDistributionGaugeCache.get(flinkMetricName);
      if (gauge == null) {
        gauge = runtimeContext.getMetricGroup()
            .gauge(flinkMetricName, new FlinkDistributionGauge(update));
        flinkDistributionGaugeCache.put(flinkMetricName, gauge);
      } else {
        gauge.update(update);
      }

      // update flink accumulator
      Accumulator<DistributionData, DistributionData> accumulator =
          runtimeContext.getAccumulator(flinkMetricName);
      if (accumulator == null) {
        accumulator = new FlinkDistributionDataAccumulator(update);
        runtimeContext.addAccumulator(flinkMetricName, accumulator);
      } else {
        accumulator.resetLocal();
        accumulator.add(update);
      }
    }
  }

  private void updateGauge(Iterable<MetricUpdates.MetricUpdate<GaugeData>> updates) {
    for (MetricUpdates.MetricUpdate<GaugeData> metricUpdate : updates) {

      String flinkMetricName =
          getFlinkMetricNameString(GAUGE_PREFIX, metricUpdate.getKey());
      GaugeData update = metricUpdate.getUpdate();

      // update flink metric
      FlinkGauge gauge = flinkGaugeCache.get(flinkMetricName);
      if (gauge == null) {
        gauge = runtimeContext.getMetricGroup()
            .gauge(flinkMetricName, new FlinkGauge(update));
        flinkGaugeCache.put(flinkMetricName, gauge);
      } else {
        gauge.update(update);
      }

      // update flink accumulator
      Accumulator<GaugeData, GaugeData> accumulator =
          runtimeContext.getAccumulator(flinkMetricName);
      if (accumulator == null) {
        accumulator = new FlinkGaugeAccumulator(update);
        runtimeContext.addAccumulator(flinkMetricName, accumulator);
      }
      accumulator.resetLocal();
      accumulator.add(update);
    }
  }

  private static String getFlinkMetricNameString(String prefix, MetricKey key) {
    return prefix
        + METRIC_KEY_SEPARATOR + key.stepName()
        + METRIC_KEY_SEPARATOR + key.metricName().namespace()
        + METRIC_KEY_SEPARATOR + key.metricName().name();
  }

  static MetricKey parseMetricKey(String flinkMetricName) {
    String[] arr = flinkMetricName.split(METRIC_KEY_SEPARATOR);
    return MetricKey.create(arr[2], MetricName.named(arr[3], arr[4]));
  }

  /**
   * Flink {@link Gauge} for {@link DistributionData}.
   */
  public static class FlinkDistributionGauge implements Gauge<DistributionData> {

    DistributionData data;

    FlinkDistributionGauge(DistributionData data) {
      this.data = data;
    }

    void update(DistributionData data) {
      this.data = data;
    }

    @Override
    public DistributionData getValue() {
      return data;
    }
  }

  /**
   * Flink {@link Gauge} for {@link GaugeData}.
   */
  public static class FlinkGauge implements Gauge<GaugeData> {

    GaugeData data;

    FlinkGauge(GaugeData data) {
      this.data = data;
    }

    void update(GaugeData update) {
      this.data = data.combine(update);
    }

    @Override
    public GaugeData getValue() {
      return data;
    }
  }

  /**
   * Flink {@link Accumulator} for {@link GaugeData}.
   */
  public static class FlinkDistributionDataAccumulator implements
      Accumulator<DistributionData, DistributionData> {

    private static final long serialVersionUID = 1L;

    private DistributionData data;

    public FlinkDistributionDataAccumulator(DistributionData data) {
      this.data = data;
    }

    @Override
    public void add(DistributionData value) {
      if (data == null) {
        this.data = value;
      } else {
        this.data = this.data.combine(value);
      }
    }

    @Override
    public DistributionData getLocalValue() {
      return data;
    }

    @Override
    public void resetLocal() {
      data = null;
    }

    @Override
    public void merge(Accumulator<DistributionData, DistributionData> other) {
      data = data.combine(other.getLocalValue());
    }

    @Override
    public Accumulator<DistributionData, DistributionData> clone() {
      try {
        super.clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }

      return new FlinkDistributionDataAccumulator(
          DistributionData.create(data.sum(), data.count(), data.min(), data.max()));
    }
  }

  /**
   * Flink {@link Accumulator} for {@link GaugeData}.
   */
  public static class FlinkGaugeAccumulator implements Accumulator<GaugeData, GaugeData> {

    private GaugeData data;

    public FlinkGaugeAccumulator(GaugeData data) {
      this.data = data;
    }

    @Override
    public void add(GaugeData value) {
      if (data == null) {
        this.data = value;
      } else {
        this.data = this.data.combine(value);
      }
    }

    @Override
    public GaugeData getLocalValue() {
      return data;
    }

    @Override
    public void resetLocal() {
      this.data = null;
    }

    @Override
    public void merge(Accumulator<GaugeData, GaugeData> other) {
      data = data.combine(other.getLocalValue());
    }

    @Override
    public Accumulator<GaugeData, GaugeData> clone() {
      try {
        super.clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }

      return new FlinkGaugeAccumulator(
          GaugeData.create(data.value()));
    }
  }

}
