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

  public MetricsContainer getContainer(String stepName) {
    if (metricsContainers == null) {
      synchronized (this) {
        if (metricsContainers == null) {
          metricsContainers = CacheBuilder.<String, SparkMetricsContainer>newBuilder()
              .build(new MetricsContainerCacheLoader());
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
    return getInstance().counters.values();
  }

  static Collection<MetricUpdate<DistributionData>> getDistributions() {
    return getInstance().distributions.values();
  }

  SparkMetricsContainer update(SparkMetricsContainer other) {
    this.updateCounters(other.counters.values());
    this.updateDistributions(other.distributions.values());
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

  private void materialize() {
    if (metricsContainers != null) {
      for (MetricsContainer container : metricsContainers.asMap().values()) {
        MetricUpdates cumulative = container.getCumulative();
        this.updateCounters(cumulative.counterUpdates());
        this.updateDistributions(cumulative.distributionUpdates());
      }
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

  private static class MetricsContainerCacheLoader extends CacheLoader<String, MetricsContainer> {
    @SuppressWarnings("NullableProblems")
    @Override
    public MetricsContainer load(String stepName) throws Exception {
      return new MetricsContainer(stepName);
    }
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
