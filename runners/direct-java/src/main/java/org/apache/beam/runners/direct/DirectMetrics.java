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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.core.metrics.BoundedTrieData;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.runners.core.metrics.MetricsMap;
import org.apache.beam.runners.core.metrics.StringSetData;
import org.apache.beam.sdk.metrics.BoundedTrieResult;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.StringSetResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Implementation of {@link MetricResults} for the Direct Runner. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class DirectMetrics extends MetricResults {

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

    private final Executor executor;

    private final AtomicReference<UpdateT> finishedCommitted;

    private final Object attemptedLock = new Object();

    @GuardedBy("attemptedLock")
    private volatile UpdateT finishedAttempted;

    private final ConcurrentMap<CommittedBundle<?>, UpdateT> inflightAttempted =
        new ConcurrentHashMap<>();

    public DirectMetric(MetricAggregation<UpdateT, ResultT> aggregation, Executor executor) {
      this.aggregation = aggregation;
      this.executor = executor;
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
      // Add (or update) the cumulative value for the given bundle.
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
      // 1. We perform a non-blocking write to the uncommitted table to make the new value
      //    available immediately.
      // 2. We submit a runnable that will commit the update and remove the tentative value in
      //    a synchronized block.
      inflightAttempted.put(bundle, finalCumulative);
      executor.execute(
          () -> {
            synchronized (attemptedLock) {
              finishedAttempted = aggregation.combine(asList(finishedAttempted, finalCumulative));
              inflightAttempted.remove(bundle);
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
      } while (!finishedCommitted.compareAndSet(
          current, aggregation.combine(asList(current, finalCumulative))));
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

  private static final MetricAggregation<StringSetData, StringSetResult> STRING_SET =
      new MetricAggregation<StringSetData, StringSetResult>() {
        @Override
        public StringSetData zero() {
          return StringSetData.empty();
        }

        @Override
        public StringSetData combine(Iterable<StringSetData> updates) {
          StringSetData result = StringSetData.empty();
          result = result.combine(updates);
          return result;
        }

        @Override
        public StringSetResult extract(StringSetData data) {
          return data.extractResult();
        }
      };

  private static final MetricAggregation<BoundedTrieData, BoundedTrieResult> BOUNDED_TRIE =
      new MetricAggregation<BoundedTrieData, BoundedTrieResult>() {
        @Override
        public BoundedTrieData zero() {
          return new BoundedTrieData();
        }

        @Override
        public BoundedTrieData combine(Iterable<BoundedTrieData> updates) {
          BoundedTrieData result = new BoundedTrieData();
          for (BoundedTrieData update : updates) {
            result = result.combine(update);
          }
          return result;
        }

        @Override
        public BoundedTrieResult extract(BoundedTrieData data) {
          return data.extractResult();
        }
      };

  /** The current values of counters in memory. */
  private final MetricsMap<MetricKey, DirectMetric<Long, Long>> counters;

  private final MetricsMap<MetricKey, DirectMetric<DistributionData, DistributionResult>>
      distributions;

  private final MetricsMap<MetricKey, DirectMetric<GaugeData, GaugeResult>> gauges;
  private final MetricsMap<MetricKey, DirectMetric<StringSetData, StringSetResult>> stringSet;
  private final MetricsMap<MetricKey, DirectMetric<BoundedTrieData, BoundedTrieResult>> boundedTrie;

  DirectMetrics(ExecutorService executorService) {
    this.counters = new MetricsMap<>(unusedKey -> new DirectMetric<>(COUNTER, executorService));
    this.distributions =
        new MetricsMap<>(unusedKey -> new DirectMetric<>(DISTRIBUTION, executorService));
    this.gauges = new MetricsMap<>(unusedKey -> new DirectMetric<>(GAUGE, executorService));
    this.stringSet = new MetricsMap<>(unusedKey -> new DirectMetric<>(STRING_SET, executorService));
    this.boundedTrie =
        new MetricsMap<>(unusedKey -> new DirectMetric<>(BOUNDED_TRIE, executorService));
  }

  @Override
  public MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
    ImmutableList.Builder<MetricResult<Long>> counterResults = ImmutableList.builder();
    for (Entry<MetricKey, DirectMetric<Long, Long>> counter : counters.entries()) {
      maybeExtractResult(filter, counterResults, counter);
    }
    ImmutableList.Builder<MetricResult<DistributionResult>> distributionResults =
        ImmutableList.builder();
    for (Entry<MetricKey, DirectMetric<DistributionData, DistributionResult>> distribution :
        distributions.entries()) {
      maybeExtractResult(filter, distributionResults, distribution);
    }
    ImmutableList.Builder<MetricResult<GaugeResult>> gaugeResults = ImmutableList.builder();
    for (Entry<MetricKey, DirectMetric<GaugeData, GaugeResult>> gauge : gauges.entries()) {
      maybeExtractResult(filter, gaugeResults, gauge);
    }

    ImmutableList.Builder<MetricResult<StringSetResult>> stringSetResult = ImmutableList.builder();
    for (Entry<MetricKey, DirectMetric<StringSetData, StringSetResult>> stringSet :
        stringSet.entries()) {
      maybeExtractResult(filter, stringSetResult, stringSet);
    }

    ImmutableList.Builder<MetricResult<BoundedTrieResult>> boundedTrieResult =
        ImmutableList.builder();
    for (Entry<MetricKey, DirectMetric<BoundedTrieData, BoundedTrieResult>> boundedTrie :
        boundedTrie.entries()) {
      maybeExtractResult(filter, boundedTrieResult, boundedTrie);
    }

    return MetricQueryResults.create(
        counterResults.build(),
        distributionResults.build(),
        gaugeResults.build(),
        stringSetResult.build(),
        boundedTrieResult.build(),
        Collections.emptyList());
  }

  private <ResultT> void maybeExtractResult(
      MetricsFilter filter,
      ImmutableList.Builder<MetricResult<ResultT>> resultsBuilder,
      Map.Entry<MetricKey, ? extends DirectMetric<?, ResultT>> entry) {
    if (MetricFiltering.matches(filter, entry.getKey())) {
      resultsBuilder.add(
          MetricResult.create(
              entry.getKey(),
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
      distributions.get(distribution.getKey()).updatePhysical(bundle, distribution.getUpdate());
    }
    for (MetricUpdate<GaugeData> gauge : updates.gaugeUpdates()) {
      gauges.get(gauge.getKey()).updatePhysical(bundle, gauge.getUpdate());
    }
    for (MetricUpdate<StringSetData> sSet : updates.stringSetUpdates()) {
      stringSet.get(sSet.getKey()).updatePhysical(bundle, sSet.getUpdate());
    }
    for (MetricUpdate<BoundedTrieData> bTrie : updates.boundedTrieUpdates()) {
      boundedTrie.get(bTrie.getKey()).updatePhysical(bundle, bTrie.getUpdate());
    }
  }

  public void commitPhysical(CommittedBundle<?> bundle, MetricUpdates updates) {
    for (MetricUpdate<Long> counter : updates.counterUpdates()) {
      counters.get(counter.getKey()).commitPhysical(bundle, counter.getUpdate());
    }
    for (MetricUpdate<DistributionData> distribution : updates.distributionUpdates()) {
      distributions.get(distribution.getKey()).commitPhysical(bundle, distribution.getUpdate());
    }
    for (MetricUpdate<GaugeData> gauge : updates.gaugeUpdates()) {
      gauges.get(gauge.getKey()).commitPhysical(bundle, gauge.getUpdate());
    }
    for (MetricUpdate<StringSetData> sSet : updates.stringSetUpdates()) {
      stringSet.get(sSet.getKey()).commitPhysical(bundle, sSet.getUpdate());
    }
    for (MetricUpdate<BoundedTrieData> bTrie : updates.boundedTrieUpdates()) {
      boundedTrie.get(bTrie.getKey()).commitPhysical(bundle, bTrie.getUpdate());
    }
  }

  /** Apply metric updates that represent new logical values from a bundle being committed. */
  public void commitLogical(CommittedBundle<?> bundle, MetricUpdates updates) {
    for (MetricUpdate<Long> counter : updates.counterUpdates()) {
      counters.get(counter.getKey()).commitLogical(bundle, counter.getUpdate());
    }
    for (MetricUpdate<DistributionData> distribution : updates.distributionUpdates()) {
      distributions.get(distribution.getKey()).commitLogical(bundle, distribution.getUpdate());
    }
    for (MetricUpdate<GaugeData> gauge : updates.gaugeUpdates()) {
      gauges.get(gauge.getKey()).commitLogical(bundle, gauge.getUpdate());
    }
    for (MetricUpdate<StringSetData> sSet : updates.stringSetUpdates()) {
      stringSet.get(sSet.getKey()).commitLogical(bundle, sSet.getUpdate());
    }
    for (MetricUpdate<BoundedTrieData> bTrie : updates.boundedTrieUpdates()) {
      boundedTrie.get(bTrie.getKey()).commitLogical(bundle, bTrie.getUpdate());
    }
  }
}
