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
package org.apache.beam.runners.direct;

import static java.util.Arrays.asList;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeData;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.MetricsMap;

/**
 * Implementation of {@link MetricResults} for the Direct Runner.
 */
class DirectMetrics extends MetricResults {

  // TODO: (BEAM-723) Create a shared ExecutorService for maintenance tasks in the DirectRunner.
  private static final ExecutorService COUNTER_COMMITTER = Executors.newCachedThreadPool();

  private interface MetricAggregation<UpdateT, ResultT> {
    UpdateT zero();
    UpdateT combine(Iterable<UpdateT> updates);
    ResultT extract(UpdateT data);
  }

  /**
   * Implementation of a metric in the direct runner.
   *
   * @param <UpdateT> The type of raw data received and aggregated across updates.
   * @param <ResultT> The type of result extracted from the data.
   */
  private static class DirectMetric<UpdateT, ResultT> {
    private final MetricAggregation<UpdateT, ResultT> aggregation;

    private final AtomicReference<UpdateT> finishedCommitted;

    private final Object attemptedLock = new Object();
    @GuardedBy("attemptedLock")
    private volatile UpdateT finishedAttempted;
    @GuardedBy("attemptedLock")
    private final ConcurrentMap<CommittedBundle<?>, UpdateT> inflightAttempted =
        new ConcurrentHashMap<>();

    public DirectMetric(MetricAggregation<UpdateT, ResultT> aggregation) {
      this.aggregation = aggregation;
      finishedCommitted = new AtomicReference<>(aggregation.zero());
      finishedAttempted = aggregation.zero();
    }

    /**
     * Add the given {@code tentativeCumulative} update to the physical aggregate.
     *
     * @param bundle The bundle receiving an update.
     * @param tentativeCumulative The new cumulative value for the given bundle.
     */
    public void updatePhysical(CommittedBundle<?> bundle, UpdateT tentativeCumulative) {
      // Add (or update) the cumulatiev value for the given bundle.
      inflightAttempted.put(bundle, tentativeCumulative);
    }

    /**
     * Commit a physical value for the given {@code bundle}.
     *
     * @param bundle The bundle being committed.
     * @param finalCumulative The final cumulative value for the given bundle.
     */
    public void commitPhysical(final CommittedBundle<?> bundle, final UpdateT finalCumulative) {
      // To prevent a query from blocking the commit, we perform the commit in two steps.
      // 1. We perform a non-blocking write to the uncommitted table to make the new vaule
      //    available immediately.
      // 2. We submit a runnable that will commit the update and remove the tentative value in
      //    a synchronized block.
      inflightAttempted.put(bundle, finalCumulative);
      COUNTER_COMMITTER.submit(new Runnable() {
        @Override
        public void run() {
          synchronized (attemptedLock) {
            finishedAttempted = aggregation.combine(asList(finishedAttempted, finalCumulative));
            inflightAttempted.remove(bundle);
          }
        }
      });
    }

    /** Extract the latest values from all attempted and in-progress bundles. */
    public ResultT extractLatestAttempted() {
      ArrayList<UpdateT> updates = new ArrayList<>(inflightAttempted.size() + 1);
      // Within this block we know that will be consistent. Specifically, the only change that can
      // happen concurrently is the addition of new (larger) values to inflightAttempted.
      synchronized (attemptedLock) {
        updates.add(finishedAttempted);
        updates.addAll(inflightAttempted.values());
      }
      return aggregation.extract(aggregation.combine(updates));
    }

    /**
     * Commit a logical value for the given {@code bundle}.
     *
     * @param bundle The bundle being committed.
     * @param finalCumulative The final cumulative value for the given bundle.
     */
    public void commitLogical(final CommittedBundle<?> bundle, final UpdateT finalCumulative) {
      UpdateT current;
      do {
        current = finishedCommitted.get();
      } while (!finishedCommitted.compareAndSet(current,
          aggregation.combine(asList(current, finalCumulative))));
    }

    /** Extract the value from all successfully committed bundles. */
    public ResultT extractCommitted() {
      return aggregation.extract(finishedCommitted.get());
    }
  }

  private static final MetricAggregation<Long, Long> COUNTER =
      new MetricAggregation<Long, Long>() {
    @Override
    public Long zero() {
      return 0L;
    }

    @Override
    public Long combine(Iterable<Long> updates) {
      long value = 0;
      for (long update : updates) {
        value += update;
      }
      return value;
    }

    @Override
    public Long extract(Long data) {
      return data;
    }
  };

  private static final MetricAggregation<DistributionData, DistributionResult> DISTRIBUTION =
      new MetricAggregation<DistributionData, DistributionResult>() {
        @Override
        public DistributionData zero() {
          return DistributionData.EMPTY;
        }

        @Override
        public DistributionData combine(Iterable<DistributionData> updates) {
          DistributionData result = DistributionData.EMPTY;
          for (DistributionData update : updates) {
            result = result.combine(update);
          }
          return result;
        }

        @Override
        public DistributionResult extract(DistributionData data) {
          return data.extractResult();
        }
      };

  private static final MetricAggregation<GaugeData, GaugeResult> GAUGE =
      new MetricAggregation<GaugeData, GaugeResult>() {
        @Override
        public GaugeData zero() {
          return GaugeData.empty();
        }

        @Override
        public GaugeData combine(Iterable<GaugeData> updates) {
          GaugeData result = GaugeData.empty();
          for (GaugeData update : updates) {
            result = result.combine(update);
          }
          return result;
        }

        @Override
        public GaugeResult extract(GaugeData data) {
          return data.extractResult();
        }
      };

  /** The current values of counters in memory. */
  private MetricsMap<MetricKey, DirectMetric<Long, Long>> counters =
      new MetricsMap<>(new MetricsMap.Factory<MetricKey, DirectMetric<Long, Long>>() {
        @Override
        public DirectMetric<Long, Long> createInstance(MetricKey unusedKey) {
          return new DirectMetric<>(COUNTER);
        }
      });
  private MetricsMap<MetricKey, DirectMetric<DistributionData, DistributionResult>> distributions =
      new MetricsMap<>(
          new MetricsMap.Factory<MetricKey, DirectMetric<DistributionData, DistributionResult>>() {
        @Override
        public DirectMetric<DistributionData, DistributionResult> createInstance(
            MetricKey unusedKey) {
          return new DirectMetric<>(DISTRIBUTION);
        }
      });
  private MetricsMap<MetricKey, DirectMetric<GaugeData, GaugeResult>> gauges =
      new MetricsMap<>(
          new MetricsMap.Factory<MetricKey, DirectMetric<GaugeData, GaugeResult>>() {
            @Override
            public DirectMetric<GaugeData, GaugeResult> createInstance(
                MetricKey unusedKey) {
              return new DirectMetric<>(GAUGE);
            }
          });

  @AutoValue
  abstract static class DirectMetricQueryResults implements MetricQueryResults {
    public static MetricQueryResults create(
        Iterable<MetricResult<Long>> counters,
        Iterable<MetricResult<DistributionResult>> distributions,
        Iterable<MetricResult<GaugeResult>> gauges) {
      return new AutoValue_DirectMetrics_DirectMetricQueryResults(counters, distributions, gauges);
    }
  }

  @AutoValue
  abstract static class DirectMetricResult<T> implements MetricResult<T> {
    // need to define these here so they appear in the correct order
    // and the generated constructor is usable and consistent
    public abstract MetricName name();
    public abstract String step();
    public abstract T committed();
    public abstract T attempted();

    public static <T> MetricResult<T> create(MetricName name, String scope,
        T committed, T attempted) {
      return new AutoValue_DirectMetrics_DirectMetricResult<T>(
          name, scope, committed, attempted);
    }
  }

  @Override
  public MetricQueryResults queryMetrics(MetricsFilter filter) {
    ImmutableList.Builder<MetricResult<Long>> counterResults = ImmutableList.builder();
    for (Entry<MetricKey, DirectMetric<Long, Long>> counter : counters.entries()) {
      maybeExtractResult(filter, counterResults, counter);
    }
    ImmutableList.Builder<MetricResult<DistributionResult>> distributionResults =
        ImmutableList.builder();
    for (Entry<MetricKey, DirectMetric<DistributionData, DistributionResult>> distribution
        : distributions.entries()) {
      maybeExtractResult(filter, distributionResults, distribution);
    }
    ImmutableList.Builder<MetricResult<GaugeResult>> gaugeResults =
        ImmutableList.builder();
    for (Entry<MetricKey, DirectMetric<GaugeData, GaugeResult>> gauge
        : gauges.entries()) {
      maybeExtractResult(filter, gaugeResults, gauge);
    }

    return DirectMetricQueryResults.create(counterResults.build(), distributionResults.build(),
        gaugeResults.build());
  }

  private <ResultT> void maybeExtractResult(
      MetricsFilter filter,
      ImmutableList.Builder<MetricResult<ResultT>> resultsBuilder,
      Map.Entry<MetricKey, ? extends DirectMetric<?, ResultT>> entry) {
    if (MetricFiltering.matches(filter, entry.getKey())) {
      resultsBuilder.add(DirectMetricResult.create(
          entry.getKey().metricName(),
          entry.getKey().stepName(),
          entry.getValue().extractCommitted(),
          entry.getValue().extractLatestAttempted()));
    }
  }

  /** Apply metric updates that represent physical counter deltas to the current metric values. */
  public void updatePhysical(CommittedBundle<?> bundle, MetricUpdates updates) {
    for (MetricUpdate<Long> counter : updates.counterUpdates()) {
      counters.get(counter.getKey()).updatePhysical(bundle, counter.getUpdate());
    }
    for (MetricUpdate<DistributionData> distribution : updates.distributionUpdates()) {
      distributions.get(distribution.getKey())
          .updatePhysical(bundle, distribution.getUpdate());
    }
    for (MetricUpdate<GaugeData> gauge : updates.gaugeUpdates()) {
      gauges.get(gauge.getKey())
          .updatePhysical(bundle, gauge.getUpdate());
    }
  }

  public void commitPhysical(CommittedBundle<?> bundle, MetricUpdates updates) {
    for (MetricUpdate<Long> counter : updates.counterUpdates()) {
      counters.get(counter.getKey()).commitPhysical(bundle, counter.getUpdate());
    }
    for (MetricUpdate<DistributionData> distribution : updates.distributionUpdates()) {
      distributions.get(distribution.getKey())
          .commitPhysical(bundle, distribution.getUpdate());
    }
    for (MetricUpdate<GaugeData> gauge : updates.gaugeUpdates()) {
      gauges.get(gauge.getKey())
          .commitPhysical(bundle, gauge.getUpdate());
    }
  }

  /** Apply metric updates that represent new logical values from a bundle being committed. */
  public void commitLogical(CommittedBundle<?> bundle, MetricUpdates updates) {
    for (MetricUpdate<Long> counter : updates.counterUpdates()) {
      counters.get(counter.getKey()).commitLogical(bundle, counter.getUpdate());
    }
    for (MetricUpdate<DistributionData> distribution : updates.distributionUpdates()) {
      distributions.get(distribution.getKey())
          .commitLogical(bundle, distribution.getUpdate());
    }
    for (MetricUpdate<GaugeData> gauge : updates.gaugeUpdates()) {
      gauges.get(gauge.getKey())
          .commitLogical(bundle, gauge.getUpdate());
    }
  }
}
