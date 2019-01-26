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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.construction.metrics.MetricFiltering;
import org.apache.beam.runners.core.construction.metrics.MetricKey;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.FluentIterable;

/**
 * Metrics containers by step.
 *
 * <p>This class is not thread-safe.
 */
public class MetricsContainerStepMap implements Serializable {

  /**
   * Key class to lookup the metric container for a step or unbound to any step. This is preferable
   * to using null(which can't be used as a key). Additonally, this class could be extended to allow
   * containers for other non pTransform contexts.
   */
  @AutoValue
  abstract static class MetricsContainerKey implements Serializable {
    public static MetricsContainerKey unBounded() {
      return new AutoValue_MetricsContainerStepMap_MetricsContainerKey(null);
    }

    public static MetricsContainerKey forStep(String step) {
      return new AutoValue_MetricsContainerStepMap_MetricsContainerKey(step);
    }

    @Nullable
    public abstract String getStepName();

    @Override
    public int hashCode() {
      // Don't include name and namespace, since they are lazily set.
      return Objects.hash(getStepName());
    }

    @Override
    public boolean equals(Object o) {
      // If the object is compared with itself then return true
      if (o == this) {
        return true;
      }
      if (!(o instanceof MetricsContainerKey)) {
        return false;
      }
      MetricsContainerKey other = (MetricsContainerKey) o;
      return Objects.equals(this.getStepName(), other.getStepName());
    }
  }

  private Map<MetricsContainerKey, MetricsContainerImpl> metricsContainers;

  public MetricsContainerStepMap() {
    this.metricsContainers = new ConcurrentHashMap<>();
  }

  /* Returns the container that is not bound to any step name. */
  public MetricsContainerImpl getUnboundContainer() {
    return getContainerForKey(MetricsContainerKey.unBounded());
  }

  /** Returns the container for the given step name. */
  public MetricsContainerImpl getContainer(String stepName) {
    checkArgument(!Strings.isNullOrEmpty(stepName), "stepName must be non-empty");
    return getContainerForKey(MetricsContainerKey.forStep(stepName));
  }

  /** @return the container for the given key */
  private MetricsContainerImpl getContainerForKey(MetricsContainerKey key) {
    if (!metricsContainers.containsKey(key)) {
      metricsContainers.put(key, new MetricsContainerImpl(key.getStepName()));
    }
    return metricsContainers.get(key);
  }

  /**
   * Update this {@link MetricsContainerStepMap} with all values from given {@link
   * MetricsContainerStepMap}.
   */
  public void updateAll(MetricsContainerStepMap other) {
    for (Map.Entry<MetricsContainerKey, MetricsContainerImpl> container :
        other.metricsContainers.entrySet()) {
      getContainerForKey(container.getKey()).update(container.getValue());
    }
  }

  /**
   * Update {@link MetricsContainerImpl} for given step in this map with all values from given
   * {@link MetricsContainerImpl}.
   */
  public void update(String step, MetricsContainerImpl container) {
    getContainer(step).update(container);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetricsContainerStepMap that = (MetricsContainerStepMap) o;

    return metricsContainers.equals(that.metricsContainers);
  }

  @Override
  public int hashCode() {
    return metricsContainers.hashCode();
  }

  /**
   * Returns {@link MetricResults} based on given {@link MetricsContainerStepMap
   * MetricsContainerStepMaps} of attempted and committed metrics.
   *
   * <p>This constructor is intended for runners which support both attempted and committed metrics.
   */
  public static MetricResults asMetricResults(
      MetricsContainerStepMap attemptedMetricsContainers,
      MetricsContainerStepMap committedMetricsContainers) {
    return new MetricsContainerStepMapMetricResults(
        attemptedMetricsContainers, committedMetricsContainers);
  }

  /** Return the cumulative values for any metrics in this container as MonitoringInfos. */
  public Iterable<MonitoringInfo> getMonitoringInfos() {
    // Extract user metrics and store as MonitoringInfos.
    ArrayList<MonitoringInfo> monitoringInfos = new ArrayList<MonitoringInfo>();
    for (MetricsContainerImpl container : metricsContainers.values()) {
      for (MonitoringInfo mi : container.getMonitoringInfos()) {
        monitoringInfos.add(mi);
      }
    }
    return monitoringInfos;
  }

  /**
   * Returns {@link MetricResults} based on given {@link MetricsContainerStepMap} of attempted
   * metrics.
   *
   * <p>This constructor is intended for runners which only support `attempted` metrics. Accessing
   * {@link MetricResult#getCommitted()} in the resulting {@link MetricResults} will result in an
   * {@link UnsupportedOperationException}.
   */
  public static MetricResults asAttemptedOnlyMetricResults(
      MetricsContainerStepMap attemptedMetricsContainers) {
    return new MetricsContainerStepMapMetricResults(attemptedMetricsContainers);
  }

  private Map<MetricsContainerKey, MetricsContainerImpl> getMetricsContainers() {
    return metricsContainers;
  }

  private static class MetricsContainerStepMapMetricResults extends MetricResults {
    private final Map<MetricKey, AttemptedAndCommitted<Long>> counters = new HashMap<>();
    private final Map<MetricKey, AttemptedAndCommitted<DistributionData>> distributions =
        new HashMap<>();
    private final Map<MetricKey, AttemptedAndCommitted<GaugeData>> gauges = new HashMap<>();
    private final boolean isCommittedSupported;

    private MetricsContainerStepMapMetricResults(
        MetricsContainerStepMap attemptedMetricsContainers) {
      this(attemptedMetricsContainers, new MetricsContainerStepMap(), false);
    }

    private MetricsContainerStepMapMetricResults(
        MetricsContainerStepMap attemptedMetricsContainers,
        MetricsContainerStepMap committedMetricsContainers) {
      this(attemptedMetricsContainers, committedMetricsContainers, true);
    }

    private MetricsContainerStepMapMetricResults(
        MetricsContainerStepMap attemptedMetricsContainers,
        MetricsContainerStepMap committedMetricsContainers,
        boolean isCommittedSupported) {
      for (MetricsContainerImpl container :
          attemptedMetricsContainers.getMetricsContainers().values()) {
        MetricUpdates cumulative = container.getCumulative();
        mergeCounters(counters, cumulative.counterUpdates(), attemptedCounterUpdateFn());
        mergeDistributions(
            distributions, cumulative.distributionUpdates(), attemptedDistributionUpdateFn());
        mergeGauges(gauges, cumulative.gaugeUpdates(), attemptedGaugeUpdateFn());
      }
      for (MetricsContainerImpl container :
          committedMetricsContainers.getMetricsContainers().values()) {
        MetricUpdates cumulative = container.getCumulative();
        mergeCounters(counters, cumulative.counterUpdates(), committedCounterUpdateFn());
        mergeDistributions(
            distributions, cumulative.distributionUpdates(), committedDistributionUpdateFn());
        mergeGauges(gauges, cumulative.gaugeUpdates(), committedGaugeUpdateFn());
      }
      this.isCommittedSupported = isCommittedSupported;
    }

    private Function<MetricUpdate<DistributionData>, AttemptedAndCommitted<DistributionData>>
        attemptedDistributionUpdateFn() {
      return input -> {
        MetricKey key = input.getKey();
        return new AttemptedAndCommitted<>(
            key, input, MetricUpdate.create(key, DistributionData.EMPTY));
      };
    }

    private Function<MetricUpdate<DistributionData>, AttemptedAndCommitted<DistributionData>>
        committedDistributionUpdateFn() {
      return input -> {
        MetricKey key = input.getKey();
        return new AttemptedAndCommitted<>(
            key, MetricUpdate.create(key, DistributionData.EMPTY), input);
      };
    }

    private Function<MetricUpdate<GaugeData>, AttemptedAndCommitted<GaugeData>>
        attemptedGaugeUpdateFn() {
      return input -> {
        MetricKey key = input.getKey();
        return new AttemptedAndCommitted<>(key, input, MetricUpdate.create(key, GaugeData.empty()));
      };
    }

    private Function<MetricUpdate<GaugeData>, AttemptedAndCommitted<GaugeData>>
        committedGaugeUpdateFn() {
      return input -> {
        MetricKey key = input.getKey();
        return new AttemptedAndCommitted<>(key, MetricUpdate.create(key, GaugeData.empty()), input);
      };
    }

    private Function<MetricUpdate<Long>, AttemptedAndCommitted<Long>> attemptedCounterUpdateFn() {
      return input -> {
        MetricKey key = input.getKey();
        return new AttemptedAndCommitted<>(key, input, MetricUpdate.create(key, 0L));
      };
    }

    private Function<MetricUpdate<Long>, AttemptedAndCommitted<Long>> committedCounterUpdateFn() {
      return input -> {
        MetricKey key = input.getKey();
        return new AttemptedAndCommitted<>(key, MetricUpdate.create(key, 0L), input);
      };
    }

    @Override
    public MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
      return new QueryResults(filter);
    }

    private class QueryResults extends MetricQueryResults {
      private final MetricsFilter filter;

      private QueryResults(MetricsFilter filter) {
        this.filter = filter;
      }

      @Override
      public Iterable<MetricResult<Long>> getCounters() {
        return FluentIterable.from(counters.values())
            .filter(matchesFilter(filter))
            .transform(counterUpdateToResult())
            .toList();
      }

      @Override
      public Iterable<MetricResult<DistributionResult>> getDistributions() {
        return FluentIterable.from(distributions.values())
            .filter(matchesFilter(filter))
            .transform(distributionUpdateToResult())
            .toList();
      }

      @Override
      public Iterable<MetricResult<GaugeResult>> getGauges() {
        return FluentIterable.from(gauges.values())
            .filter(matchesFilter(filter))
            .transform(gaugeUpdateToResult())
            .toList();
      }

      private Predicate<AttemptedAndCommitted<?>> matchesFilter(final MetricsFilter filter) {
        return attemptedAndCommitted ->
            MetricFiltering.matches(filter, attemptedAndCommitted.getKey());
      }
    }

    private Function<AttemptedAndCommitted<Long>, MetricResult<Long>> counterUpdateToResult() {
      return metricResult -> {
        MetricKey key = metricResult.getKey();
        return new AccumulatedMetricResult<>(
            key.metricName(),
            key.stepName(),
            metricResult.getAttempted().getUpdate(),
            isCommittedSupported ? metricResult.getCommitted().getUpdate() : null,
            isCommittedSupported);
      };
    }

    private Function<AttemptedAndCommitted<DistributionData>, MetricResult<DistributionResult>>
        distributionUpdateToResult() {
      return metricResult -> {
        MetricKey key = metricResult.getKey();
        return new AccumulatedMetricResult<>(
            key.metricName(),
            key.stepName(),
            metricResult.getAttempted().getUpdate().extractResult(),
            isCommittedSupported ? metricResult.getCommitted().getUpdate().extractResult() : null,
            isCommittedSupported);
      };
    }

    private Function<AttemptedAndCommitted<GaugeData>, MetricResult<GaugeResult>>
        gaugeUpdateToResult() {
      return metricResult -> {
        MetricKey key = metricResult.getKey();
        return new AccumulatedMetricResult<>(
            key.metricName(),
            key.stepName(),
            metricResult.getAttempted().getUpdate().extractResult(),
            isCommittedSupported ? metricResult.getCommitted().getUpdate().extractResult() : null,
            isCommittedSupported);
      };
    }

    @SuppressWarnings("ConstantConditions")
    private void mergeCounters(
        Map<MetricKey, AttemptedAndCommitted<Long>> counters,
        Iterable<MetricUpdate<Long>> updates,
        Function<MetricUpdate<Long>, AttemptedAndCommitted<Long>> updateToAttemptedAndCommittedFn) {
      for (MetricUpdate<Long> metricUpdate : updates) {
        MetricKey key = metricUpdate.getKey();
        AttemptedAndCommitted<Long> update = updateToAttemptedAndCommittedFn.apply(metricUpdate);
        if (counters.containsKey(key)) {
          AttemptedAndCommitted<Long> current = counters.get(key);
          update =
              new AttemptedAndCommitted<>(
                  key,
                  MetricUpdate.create(
                      key, update.getAttempted().getUpdate() + current.getAttempted().getUpdate()),
                  MetricUpdate.create(
                      key, update.getCommitted().getUpdate() + current.getCommitted().getUpdate()));
        }
        counters.put(key, update);
      }
    }

    @SuppressWarnings("ConstantConditions")
    private void mergeDistributions(
        Map<MetricKey, AttemptedAndCommitted<DistributionData>> distributions,
        Iterable<MetricUpdate<DistributionData>> updates,
        Function<MetricUpdate<DistributionData>, AttemptedAndCommitted<DistributionData>>
            updateToAttemptedAndCommittedFn) {
      for (MetricUpdate<DistributionData> metricUpdate : updates) {
        MetricKey key = metricUpdate.getKey();
        AttemptedAndCommitted<DistributionData> update =
            updateToAttemptedAndCommittedFn.apply(metricUpdate);
        if (distributions.containsKey(key)) {
          AttemptedAndCommitted<DistributionData> current = distributions.get(key);
          update =
              new AttemptedAndCommitted<>(
                  key,
                  MetricUpdate.create(
                      key,
                      update
                          .getAttempted()
                          .getUpdate()
                          .combine(current.getAttempted().getUpdate())),
                  MetricUpdate.create(
                      key,
                      update
                          .getCommitted()
                          .getUpdate()
                          .combine(current.getCommitted().getUpdate())));
        }
        distributions.put(key, update);
      }
    }

    @SuppressWarnings("ConstantConditions")
    private void mergeGauges(
        Map<MetricKey, AttemptedAndCommitted<GaugeData>> gauges,
        Iterable<MetricUpdate<GaugeData>> updates,
        Function<MetricUpdate<GaugeData>, AttemptedAndCommitted<GaugeData>>
            updateToAttemptedAndCommittedFn) {
      for (MetricUpdate<GaugeData> metricUpdate : updates) {
        MetricKey key = metricUpdate.getKey();
        AttemptedAndCommitted<GaugeData> update =
            updateToAttemptedAndCommittedFn.apply(metricUpdate);
        if (gauges.containsKey(key)) {
          AttemptedAndCommitted<GaugeData> current = gauges.get(key);
          update =
              new AttemptedAndCommitted<>(
                  key,
                  MetricUpdate.create(
                      key,
                      update
                          .getAttempted()
                          .getUpdate()
                          .combine(current.getAttempted().getUpdate())),
                  MetricUpdate.create(
                      key,
                      update
                          .getCommitted()
                          .getUpdate()
                          .combine(current.getCommitted().getUpdate())));
        }
        gauges.put(key, update);
      }
    }

    /** Accumulated implementation of {@link MetricResult}. */
    private static class AccumulatedMetricResult<T> implements MetricResult<T> {
      private final MetricName name;
      private final String step;
      private final T attempted;
      private final @Nullable T committed;
      private final boolean isCommittedSupported;

      private AccumulatedMetricResult(
          MetricName name,
          String step,
          T attempted,
          @Nullable T committed,
          boolean isCommittedSupported) {
        this.name = name;
        this.step = step;
        this.attempted = attempted;
        this.committed = committed;
        this.isCommittedSupported = isCommittedSupported;
      }

      @Override
      public MetricName getName() {
        return name;
      }

      @Override
      public String getStep() {
        return step;
      }

      @Override
      public T getCommitted() {
        if (!isCommittedSupported) {
          throw new UnsupportedOperationException(
              "This runner does not currently support committed"
                  + " metrics results. Please use 'attempted' instead.");
        }
        return committed;
      }

      @Override
      public T getAttempted() {
        return attempted;
      }
    }

    /** Attempted and committed {@link MetricUpdate MetricUpdates}. */
    private static class AttemptedAndCommitted<T> {
      private final MetricKey key;
      private final MetricUpdate<T> attempted;
      private final MetricUpdate<T> committed;

      private AttemptedAndCommitted(
          MetricKey key, MetricUpdate<T> attempted, MetricUpdate<T> committed) {
        this.key = key;
        this.attempted = attempted;
        this.committed = committed;
      }

      private MetricKey getKey() {
        return key;
      }

      private MetricUpdate<T> getAttempted() {
        return attempted;
      }

      private MetricUpdate<T> getCommitted() {
        return committed;
      }
    }
  }
}
