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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.runners.spark.metrics.MetricAggregator.CounterAggregator;
import org.apache.beam.runners.spark.metrics.MetricAggregator.DistributionAggregator;
import org.apache.beam.runners.spark.metrics.MetricAggregator.SparkDistributionData;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Spark accumulator value which holds all {@link MetricsContainer}s, aggregates and merges them.
 */
public class SparkMetricsContainer implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkMetricsContainer.class);

  private transient volatile LoadingCache<String, MetricsContainer> metricsContainers;

  private final Map<MetricKey, MetricAggregator<?>> metrics = new HashMap<>();

  SparkMetricsContainer() {}

  public static Accumulator<SparkMetricsContainer> getAccumulator(JavaSparkContext jsc) {
    return MetricsAccumulator.getInstance(jsc);
  }

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

  Collection<CounterAggregator> getCounters() {
    return
        FluentIterable
            .from(metrics.values())
            .filter(IS_COUNTER)
            .transform(TO_COUNTER)
            .toList();
  }

  private static final Predicate<MetricAggregator<?>> IS_COUNTER =
      new Predicate<MetricAggregator<?>>() {
        @Override
        public boolean apply(MetricAggregator<?> input) {
          return (input instanceof CounterAggregator);
        }
      };

  private static final Function<MetricAggregator<?>, CounterAggregator> TO_COUNTER =
      new Function<MetricAggregator<?>,
          CounterAggregator>() {
        @Override
        public CounterAggregator apply(MetricAggregator<?> metricAggregator) {
          return (CounterAggregator) metricAggregator;
        }
      };

  Collection<DistributionAggregator> getDistributions() {
    return
        FluentIterable
            .from(metrics.values())
            .filter(IS_DISTRIBUTION)
            .transform(TO_DISTRIBUTION)
            .toList();
  }

  private static final Predicate<MetricAggregator<?>> IS_DISTRIBUTION =
      new Predicate<MetricAggregator<?>>() {
        @Override
        public boolean apply(MetricAggregator<?> input) {
          return (input instanceof DistributionAggregator);
        }
      };

  private static final Function<MetricAggregator<?>, DistributionAggregator> TO_DISTRIBUTION =
      new Function<MetricAggregator<?>, DistributionAggregator>() {
        @Override
        public DistributionAggregator apply(MetricAggregator<?> metricAggregator) {
          return (DistributionAggregator) metricAggregator;
        }
      };

  SparkMetricsContainer merge(SparkMetricsContainer other) {
    return
        new SparkMetricsContainer()
            .updated(this.getAggregators())
            .updated(other.getAggregators());
  }

  private Collection<MetricAggregator<?>> getAggregators() {
    return metrics.values();
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    materialize();
    out.defaultWriteObject();
  }

  private void materialize() {
    if (metricsContainers != null) {
      for (MetricsContainer container : metricsContainers.asMap().values()) {
        MetricUpdates cumulative = container.getCumulative();
        updated(Iterables.transform(cumulative.counterUpdates(), TO_COUNTER_AGGREGATOR));
        updated(Iterables.transform(cumulative.distributionUpdates(), TO_DISTRIBUTION_AGGREGATOR));
      }
    }
  }

  private static final Function<MetricUpdate<Long>, MetricAggregator<?>>
      TO_COUNTER_AGGREGATOR = new Function<MetricUpdate<Long>, MetricAggregator<?>>() {
    @SuppressWarnings("ConstantConditions")
    @Override
    public CounterAggregator
    apply(MetricUpdate<Long> update) {
      return update != null ? new CounterAggregator(new SparkMetricKey(update.getKey()),
          update.getUpdate()) : null;
    }
  };

  private static final Function<MetricUpdate<DistributionData>, MetricAggregator<?>>
      TO_DISTRIBUTION_AGGREGATOR =
      new Function<MetricUpdate<DistributionData>, MetricAggregator<?>>() {
        @SuppressWarnings("ConstantConditions")
        @Override
        public DistributionAggregator
        apply(MetricUpdate<DistributionData> update) {
          return update != null ? new DistributionAggregator(new SparkMetricKey(update.getKey()),
              new SparkDistributionData(update.getUpdate())) : null;
        }
      };

  private SparkMetricsContainer updated(Iterable<MetricAggregator<?>> updates) {
    for (MetricAggregator<?> update : updates) {
      MetricKey key = update.getKey();
      MetricAggregator<?> current = metrics.get(key);
      Object updateValue = update.getValue();
      metrics.put(new SparkMetricKey(key),
          current != null ? MetricAggregator.updated(current, updateValue) : update);
    }
    return this;
  }

  private static class MetricsContainerCacheLoader extends CacheLoader<String, MetricsContainer> {
    @SuppressWarnings("NullableProblems")
    @Override
    public MetricsContainer load(String stepName) throws Exception {
      return new MetricsContainer(stepName);
    }
  }

  private static class SparkMetricKey extends MetricKey implements Serializable {
    private final String stepName;
    private final MetricName metricName;

    SparkMetricKey(MetricKey original) {
      this.stepName = original.stepName();
      MetricName metricName = original.metricName();
      this.metricName = new SparkMetricName(metricName.namespace(), metricName.name());
    }

    @Override
    public String stepName() {
      return stepName;
    }

    @Override
    public MetricName metricName() {
      return metricName;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof MetricKey) {
        MetricKey that = (MetricKey) o;
        return (this.stepName.equals(that.stepName()))
            && (this.metricName.equals(that.metricName()));
      }
      return false;
    }

    @Override
    public int hashCode() {
      int h = 1;
      h *= 1000003;
      h ^= stepName.hashCode();
      h *= 1000003;
      h ^= metricName.hashCode();
      return h;
    }
  }

  private static class SparkMetricName extends MetricName implements Serializable {
    private final String namespace;
    private final String name;

    SparkMetricName(String namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    @Override
    public String namespace() {
      return namespace;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof MetricName) {
        MetricName that = (MetricName) o;
        return (this.namespace.equals(that.namespace()))
            && (this.name.equals(that.name()));
      }
      return false;
    }

    @Override
    public int hashCode() {
      int h = 1;
      h *= 1000003;
      h ^= namespace.hashCode();
      h *= 1000003;
      h ^= name.hashCode();
      return h;
    }
  }
}
