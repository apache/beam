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

package org.apache.beam.runners.spark.metrics;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.GaugeData;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Spark accumulator value which holds all {@link MetricsContainer}s, aggregates and merges them.
 */
public class SparkMetricsContainer implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkMetricsContainer.class);

  private transient volatile LoadingCache<String, MetricsContainer> metricsContainers;

  private final Map<MetricKey, MetricUpdate<Long>> counters = new HashMap<>();
  private final Map<MetricKey, MetricUpdate<DistributionData>> distributions = new HashMap<>();
  private final Map<MetricKey, MetricUpdate<GaugeData>> gauges = new HashMap<>();

  public MetricsContainer getContainer(String stepName) {
    if (metricsContainers == null) {
      synchronized (this) {
        if (metricsContainers == null) {
          initializeMetricsContainers();
        }
      }
    }
    try {
      return metricsContainers.get(stepName);
    } catch (ExecutionException e) {
      LOG.error("Error while creating metrics container", e);
      return null;
    }
  }

  static Collection<MetricUpdate<Long>> getCounters() {
    SparkMetricsContainer sparkMetricsContainer = getInstance();
    sparkMetricsContainer.materialize();
    return sparkMetricsContainer.counters.values();
  }

  static Collection<MetricUpdate<DistributionData>> getDistributions() {
    SparkMetricsContainer sparkMetricsContainer = getInstance();
    sparkMetricsContainer.materialize();
    return sparkMetricsContainer.distributions.values();
  }

  static Collection<MetricUpdate<GaugeData>> getGauges() {
    return getInstance().gauges.values();
  }

  public SparkMetricsContainer update(SparkMetricsContainer other) {
    other.materialize();
    this.updateCounters(other.counters.values());
    this.updateDistributions(other.distributions.values());
    this.updateGauges(other.gauges.values());
    return this;
  }

  private static SparkMetricsContainer getInstance() {
    return MetricsAccumulator.getInstance().value();
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    // Since MetricsContainer instances are not serializable, materialize a serializable map of
    // MetricsAggregators relating to the same metrics. This is done here, when Spark serializes
    // the SparkMetricsContainer accumulator before sending results back to the driver at a point in
    // time where all the metrics updates have already been made to the MetricsContainers.
    materialize();
    out.defaultWriteObject();
  }

  /**
   * Materialize metrics. Must be called to enable this instance's data to be serialized correctly.
   * This method is idempotent.
   */
  public void materialize() {
    // Nullifying metricsContainers makes this method idempotent.
    if (metricsContainers != null) {
      for (MetricsContainer container : metricsContainers.asMap().values()) {
        MetricUpdates cumulative = container.getCumulative();
        this.updateCounters(cumulative.counterUpdates());
        this.updateDistributions(cumulative.distributionUpdates());
        this.updateGauges(cumulative.gaugeUpdates());
      }
      metricsContainers = null;
    }
  }

  private void updateCounters(Iterable<MetricUpdate<Long>> updates) {
    for (MetricUpdate<Long> update : updates) {
      MetricKey key = update.getKey();
      MetricUpdate<Long> current = counters.get(key);
      counters.put(key, current != null
          ? MetricUpdate.create(key, current.getUpdate() + update.getUpdate()) : update);
    }
  }

  private void updateDistributions(Iterable<MetricUpdate<DistributionData>> updates) {
    for (MetricUpdate<DistributionData> update : updates) {
      MetricKey key = update.getKey();
      MetricUpdate<DistributionData> current = distributions.get(key);
      distributions.put(key, current != null
          ? MetricUpdate.create(key, current.getUpdate().combine(update.getUpdate())) : update);
    }
  }

  private void updateGauges(Iterable<MetricUpdate<GaugeData>> updates) {
    for (MetricUpdate<GaugeData> update : updates) {
      MetricKey key = update.getKey();
      MetricUpdate<GaugeData> current = gauges.get(key);
      gauges.put(
          key,
          current != null
              ? MetricUpdate.create(key, current.getUpdate().combine(update.getUpdate()))
              : update);
    }
  }

  private static class MetricsContainerCacheLoader extends CacheLoader<String, MetricsContainer> {
    @SuppressWarnings("NullableProblems")
    @Override
    public MetricsContainer load(String stepName) throws Exception {
      return new MetricsContainer(stepName);
    }
  }

  private void initializeMetricsContainers() {
    metricsContainers = CacheBuilder.<String, SparkMetricsContainer>newBuilder()
        .build(new MetricsContainerCacheLoader());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, ?> metric : new SparkBeamMetric().renderAll().entrySet()) {
      sb.append(metric.getKey()).append(": ").append(metric.getValue()).append(" ");
    }
    return sb.toString();
  }
}
