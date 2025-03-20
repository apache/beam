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
package org.apache.beam.runners.dataflow.worker;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.PerStepNamespaceMetrics;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.metrics.BoundedTrieCell;
import org.apache.beam.runners.core.metrics.BoundedTrieData;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeCell;
import org.apache.beam.runners.core.metrics.MetricsMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.StringSetCell;
import org.apache.beam.runners.core.metrics.StringSetData;
import org.apache.beam.sdk.metrics.BoundedTrie;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.LabeledMetricNameUtils;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.NoOpHistogram;
import org.apache.beam.sdk.metrics.StringSet;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * For Dataflow Streaming, we want to efficiently support many threads report metric updates, and a
 * single total delta being reported periodically as physical counters.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StreamingStepMetricsContainer implements MetricsContainer {

  private final String stepName;

  private static boolean enablePerWorkerMetrics = false;

  private MetricsMap<MetricName, DeltaCounterCell> counters =
      new MetricsMap<>(DeltaCounterCell::new);

  private final ConcurrentHashMap<MetricName, AtomicLong> perWorkerCounters;

  private MetricsMap<MetricName, GaugeCell> gauges = new MetricsMap<>(GaugeCell::new);

  // Stores metrics that will be later aggregted per worker.
  private MetricsMap<MetricName, GaugeCell> perWorkerGauges = new MetricsMap<>(GaugeCell::new);

  private MetricsMap<MetricName, StringSetCell> stringSets = new MetricsMap<>(StringSetCell::new);

  private MetricsMap<MetricName, DeltaDistributionCell> distributions =
      new MetricsMap<>(DeltaDistributionCell::new);

  private MetricsMap<MetricName, BoundedTrieCell> boundedTries =
      new MetricsMap<>(BoundedTrieCell::new);

  // Stores metrics that will be later aggregted per worker.
  private final ConcurrentHashMap<MetricName, LockFreeHistogram> perWorkerHistograms =
      new ConcurrentHashMap<>();

  private final Map<MetricName, Instant> perWorkerCountersByFirstStaleTime;

  private final ConcurrentHashMap<MetricName, LabeledMetricNameUtils.ParsedMetricName>
      parsedPerWorkerMetricsCache;

  // PerWorkerCounters that have been longer than this value will be removed from the underlying
  // metrics map.
  private final Duration maximumPerWorkerCounterStaleness = Duration.ofMinutes(5);

  private final Clock clock;

  private StreamingStepMetricsContainer(String stepName) {
    this.stepName = stepName;
    this.perWorkerCountersByFirstStaleTime = new ConcurrentHashMap<>();
    this.clock = Clock.systemUTC();
    this.perWorkerCounters = new ConcurrentHashMap<>();
    this.parsedPerWorkerMetricsCache = new ConcurrentHashMap<>();
  }

  public static MetricsContainerRegistry<StreamingStepMetricsContainer> createRegistry() {
    return new MetricsContainerRegistry<StreamingStepMetricsContainer>() {
      @Override
      protected StreamingStepMetricsContainer createContainer(String stepName) {
        return new StreamingStepMetricsContainer(stepName);
      }
    };
  }

  /**
   * Construct a {@code StreamingStepMetricsContainer} that supports mock clock, perWorkerCounters,
   * and perWorkerCountersByFirstStaleTime. For testing purposes only.
   */
  private StreamingStepMetricsContainer(
      String stepName,
      Map<MetricName, Instant> perWorkerCountersByFirstStaleTime,
      ConcurrentHashMap<MetricName, AtomicLong> perWorkerCounters,
      ConcurrentHashMap<MetricName, LabeledMetricNameUtils.ParsedMetricName>
          parsedPerWorkerMetricsCache,
      Clock clock) {
    this.stepName = stepName;
    this.perWorkerCountersByFirstStaleTime = perWorkerCountersByFirstStaleTime;
    this.perWorkerCounters = perWorkerCounters;
    this.parsedPerWorkerMetricsCache = parsedPerWorkerMetricsCache;
    this.clock = clock;
  }

  @VisibleForTesting
  static StreamingStepMetricsContainer forTesting(
      String stepName,
      Map<MetricName, Instant> perWorkerCountersByFirstStaleTime,
      ConcurrentHashMap<MetricName, AtomicLong> perWorkerCounters,
      ConcurrentHashMap<MetricName, LabeledMetricNameUtils.ParsedMetricName>
          parsedPerWorkerMetricsCache,
      Clock clock) {
    return new StreamingStepMetricsContainer(
        stepName,
        perWorkerCountersByFirstStaleTime,
        perWorkerCounters,
        parsedPerWorkerMetricsCache,
        clock);
  }

  @Override
  public Counter getCounter(MetricName metricName) {
    return counters.get(metricName);
  }

  @Override
  public Counter getPerWorkerCounter(MetricName metricName) {
    if (enablePerWorkerMetrics) {
      return new RemoveSafeDeltaCounterCell(metricName, perWorkerCounters);
    } else {
      return MetricsContainer.super.getPerWorkerCounter(metricName);
    }
  }

  @Override
  public Distribution getDistribution(MetricName metricName) {
    return distributions.get(metricName);
  }

  @Override
  public Gauge getGauge(MetricName metricName) {
    if (MonitoringInfoConstants.isPerWorkerMetric(metricName)) {
      return perWorkerGauges.get(metricName);
    }
    return gauges.get(metricName);
  }

  @Override
  public StringSet getStringSet(MetricName metricName) {
    return stringSets.get(metricName);
  }

  @Override
  public BoundedTrie getBoundedTrie(MetricName metricName) {
    return boundedTries.get(metricName);
  }

  @Override
  public Histogram getHistogram(MetricName metricName, HistogramData.BucketType bucketType) {
    // Currently worker only supports per worker histograms, and not regular histograms.
    if (!enablePerWorkerMetrics || !MonitoringInfoConstants.isPerWorkerMetric(metricName)) {
      return NoOpHistogram.getInstance();
    }

    LockFreeHistogram val = perWorkerHistograms.get(metricName);
    if (val != null) {
      return val;
    }

    return perWorkerHistograms.computeIfAbsent(
        metricName, name -> new LockFreeHistogram(metricName, bucketType));
  }

  public Iterable<CounterUpdate> extractUpdates() {
    // Streaming metrics are updated as delta and not cumulative.
    return counterUpdates()
        .append(distributionUpdates())
        .append(gaugeUpdates())
        .append(stringSetUpdates())
        .append(boundedTrieUpdates());
  }

  private FluentIterable<CounterUpdate> counterUpdates() {
    return FluentIterable.from(counters.entries())
        .transform(
            new Function<Entry<MetricName, DeltaCounterCell>, CounterUpdate>() {

              @SuppressFBWarnings(
                  value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
                  justification = "https://github.com/google/guava/issues/920")
              @Override
              public @Nullable CounterUpdate apply(
                  @Nonnull Map.Entry<MetricName, DeltaCounterCell> entry) {
                long value = entry.getValue().getSumAndReset();
                if (value == 0) {
                  return null;
                }

                return MetricsToCounterUpdateConverter.fromCounter(
                    MetricKey.create(stepName, entry.getKey()), false, value);
              }
            })
        .filter(Predicates.notNull());
  }

  private FluentIterable<CounterUpdate> gaugeUpdates() {
    return FluentIterable.from(gauges.entries())
        .transform(
            new Function<Entry<MetricName, GaugeCell>, CounterUpdate>() {
              @Override
              public @Nullable CounterUpdate apply(
                  @Nonnull Map.Entry<MetricName, GaugeCell> entry) {
                if (!MonitoringInfoConstants.isPerWorkerMetric(entry.getKey())) {
                  long value = entry.getValue().getCumulative().value();
                  org.joda.time.Instant timestamp = entry.getValue().getCumulative().timestamp();
                  return MetricsToCounterUpdateConverter.fromGauge(
                      MetricKey.create(stepName, entry.getKey()), value, timestamp);
                } else {
                  return null;
                }
              }
            })
        .filter(Predicates.notNull());
  }

  private FluentIterable<CounterUpdate> stringSetUpdates() {
    return FluentIterable.from(stringSets.entries())
        .transform(
            new Function<Entry<MetricName, StringSetCell>, CounterUpdate>() {
              @Override
              public @Nullable CounterUpdate apply(
                  @Nonnull Map.Entry<MetricName, StringSetCell> entry) {
                StringSetData value = entry.getValue().getAndReset();
                if (value.stringSet().isEmpty()) {
                  return null;
                }
                return MetricsToCounterUpdateConverter.fromStringSet(
                    MetricKey.create(stepName, entry.getKey()), false, value);
              }
            })
        .filter(Predicates.notNull());
  }

  private FluentIterable<CounterUpdate> boundedTrieUpdates() {
    return FluentIterable.from(boundedTries.entries())
        .transform(
            new Function<Entry<MetricName, BoundedTrieCell>, CounterUpdate>() {
              @Override
              public @Nullable CounterUpdate apply(
                  @Nonnull Map.Entry<MetricName, BoundedTrieCell> entry) {
                BoundedTrieData value = entry.getValue().getAndReset();
                if (value.isEmpty()) {
                  return null;
                }
                return MetricsToCounterUpdateConverter.fromBoundedTrie(
                    MetricKey.create(stepName, entry.getKey()), false, value);
              }
            })
        .filter(Predicates.notNull());
  }

  private FluentIterable<CounterUpdate> distributionUpdates() {
    return FluentIterable.from(distributions.entries())
        .transform(
            new Function<Entry<MetricName, DeltaDistributionCell>, CounterUpdate>() {
              @SuppressFBWarnings(
                  value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
                  justification = "https://github.com/google/guava/issues/920")
              @Override
              public @Nullable CounterUpdate apply(
                  @Nonnull Map.Entry<MetricName, DeltaDistributionCell> entry) {
                DistributionData value = entry.getValue().getAndReset();
                if (value.count() == 0) {
                  return null;
                }

                return MetricsToCounterUpdateConverter.fromDistribution(
                    MetricKey.create(stepName, entry.getKey()), false, value);
              }
            })
        .filter(Predicates.notNull());
  }

  /**
   * Returns {@link CounterUpdate} protos representing the changes to all metrics that have changed
   * since the last time it was invoked.
   */
  public static Iterable<CounterUpdate> extractMetricUpdates(
      MetricsContainerRegistry<StreamingStepMetricsContainer> metricsContainerRegistry) {
    return metricsContainerRegistry
        .getContainers()
        .transformAndConcat(StreamingStepMetricsContainer::extractUpdates);
  }

  public static void setEnablePerWorkerMetrics(Boolean enablePerWorkerMetrics) {
    StreamingStepMetricsContainer.enablePerWorkerMetrics = enablePerWorkerMetrics;
  }

  public static boolean getEnablePerWorkerMetrics() {
    return StreamingStepMetricsContainer.enablePerWorkerMetrics;
  }
  /**
   * Updates {@code perWorkerCountersByFirstStaleTime} with the current zero-valued metrics and
   * removes metrics that have been stale for longer than {@code maximumPerWorkerCounterStaleness}
   * from {@code perWorkerCounters}.
   *
   * @param currentZeroValuedCounters Current zero-valued perworker counters.
   * @param extractionTime Time {@code currentZeroValuedCounters} were discovered to be zero-valued.
   */
  private void deleteStaleCounters(
      Set<MetricName> currentZeroValuedCounters, Instant extractionTime) {
    // perWorkerCountersByFirstStaleTime should only contain metrics that are currently zero-valued.
    perWorkerCountersByFirstStaleTime.keySet().retainAll(currentZeroValuedCounters);

    // Delete metrics that have been longer than 'maximumPerWorkerCounterStaleness'.
    Set<MetricName> deletedMetricNames = new HashSet<MetricName>();
    for (Entry<MetricName, Instant> entry : perWorkerCountersByFirstStaleTime.entrySet()) {
      if (Duration.between(entry.getValue(), extractionTime)
              .compareTo(maximumPerWorkerCounterStaleness)
          > 0) {
        RemoveSafeDeltaCounterCell cell =
            new RemoveSafeDeltaCounterCell(entry.getKey(), perWorkerCounters);
        cell.deleteIfZero();
        deletedMetricNames.add(entry.getKey());
      }
    }

    // Insert new zero-valued metrics into `perWorkerCountersByFirstStaleTime`.
    currentZeroValuedCounters.forEach(
        name -> perWorkerCountersByFirstStaleTime.putIfAbsent(name, extractionTime));

    // Metrics in 'deletedMetricNames' have either been removed from 'perWorkerCounters' or are no
    // longer zero-valued.
    perWorkerCountersByFirstStaleTime.keySet().removeAll(deletedMetricNames);

    // Remove potentially deleted metric names from the cache. If these metrics are non-zero valued
    // in the future, they will automatically be added back to the cache.
    parsedPerWorkerMetricsCache.keySet().removeAll(deletedMetricNames);
  }

  /**
   * Extracts metric updates for all PerWorker metrics that have changed in this Container since the
   * last time this function was called. Additionally, deletes any PerWorker counters that have been
   * zero valued for more than {@code maximumPerWorkerCounterStaleness}.
   */
  @VisibleForTesting
  Iterable<PerStepNamespaceMetrics> extractPerWorkerMetricUpdates() {
    ImmutableMap.Builder<MetricName, Long> counters = new ImmutableMap.Builder<MetricName, Long>();
    ImmutableMap.Builder<MetricName, Long> perWorkerGaugeUpdates =
        new ImmutableMap.Builder<MetricName, Long>();
    ImmutableMap.Builder<MetricName, LockFreeHistogram.Snapshot> perWorkerHistogramUpdates =
        new ImmutableMap.Builder<MetricName, LockFreeHistogram.Snapshot>();
    HashSet<MetricName> currentZeroValuedCounters = new HashSet<MetricName>();
    perWorkerCounters.forEach(
        (k, v) -> {
          Long val = v.getAndSet(0);
          if (val == 0) {
            currentZeroValuedCounters.add(k);
            return;
          }
          counters.put(k, val);
        });

    perWorkerGauges.forEach(
        (k, v) -> {
          Long val = v.getCumulative().value();
          perWorkerGaugeUpdates.put(k, val);
          v.reset();
        });

    perWorkerHistograms.forEach(
        (k, v) -> {
          v.getSnapshotAndReset().ifPresent(snapshot -> perWorkerHistogramUpdates.put(k, snapshot));
        });

    deleteStaleCounters(currentZeroValuedCounters, Instant.now(clock));

    return MetricsToPerStepNamespaceMetricsConverter.convert(
        stepName,
        counters.build(),
        perWorkerGaugeUpdates.build(),
        perWorkerHistogramUpdates.build(),
        parsedPerWorkerMetricsCache);
  }

  /**
   * @param metricsContainerRegistry Metrics will be extracted for all containers in this registry.
   * @return An iterable of {@link PerStepNamespaceMetrics} representing the changes to all
   *     PerWorkerMetrics that have changed since the last time this function was invoked.
   */
  public static Iterable<PerStepNamespaceMetrics> extractPerWorkerMetricUpdates(
      MetricsContainerRegistry<StreamingStepMetricsContainer> metricsContainerRegistry) {
    return metricsContainerRegistry
        .getContainers()
        .transformAndConcat(StreamingStepMetricsContainer::extractPerWorkerMetricUpdates);
  }
}
