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

import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.DISTRIBUTION_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.LATEST_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Gauge;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Distribution;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metric;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the metrics for a single step and uses metric cells that allow extracting the cumulative
 * value. Generally, this implementation should be used for a specific unit of commitment (bundle)
 * that wishes to report the values since the start of the bundle (e.g., for committed metrics).
 *
 * <p>This class is thread-safe. It is intended to be used with 1 (or more) threads are updating
 * metrics and at-most 1 thread is extracting updates by calling {@link #getUpdates} and {@link
 * #commitUpdates}. Outside of this it is still safe. Although races in the update extraction may
 * cause updates that don't actually have any changes, it will never lose an update.
 *
 * <p>For consistency, all threads that update metrics should finish before getting the final
 * cumulative values/updates.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class MetricsContainerImpl implements Serializable, MetricsContainer {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsContainerImpl.class);

  protected final @Nullable String stepName;

  private MetricsMap<MetricName, CounterCell> counters = new MetricsMap<>(CounterCell::new);

  private MetricsMap<MetricName, DistributionCell> distributions =
      new MetricsMap<>(DistributionCell::new);

  private MetricsMap<MetricName, GaugeCell> gauges = new MetricsMap<>(GaugeCell::new);

  private MetricsMap<KV<MetricName, HistogramData.BucketType>, HistogramCell> histograms =
      new MetricsMap<>(HistogramCell::new);

  /** Create a new {@link MetricsContainerImpl} associated with the given {@code stepName}. */
  public MetricsContainerImpl(@Nullable String stepName) {
    this.stepName = stepName;
  }

  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
      justification = "No bug",
      value = "SE_BAD_FIELD")
  private Map<MetricKey, Optional<String>> shortIdsByMetricKey = new ConcurrentHashMap<>();

  /** Reset the metrics. */
  public void reset() {
    reset(counters);
    reset(distributions);
    reset(gauges);
    reset(histograms);
  }

  private void reset(MetricsMap<?, ? extends MetricCell<?>> cells) {
    for (MetricCell<?> cell : cells.values()) {
      cell.reset();
    }
  }

  /**
   * Return a {@code CounterCell} named {@code metricName}. If it doesn't exist, create a {@code
   * Metric} with the specified name.
   */
  @Override
  public CounterCell getCounter(MetricName metricName) {
    return counters.get(metricName);
  }

  /**
   * Return a {@code CounterCell} named {@code metricName}. If it doesn't exist, return {@code
   * null}.
   */
  public @Nullable CounterCell tryGetCounter(MetricName metricName) {
    return counters.tryGet(metricName);
  }

  /**
   * Return a {@code DistributionCell} named {@code metricName}. If it doesn't exist, create a
   * {@code Metric} with the specified name.
   */
  @Override
  public DistributionCell getDistribution(MetricName metricName) {
    return distributions.get(metricName);
  }

  /**
   * Return a {@code DistributionCell} named {@code metricName}. If it doesn't exist, return {@code
   * null}.
   */
  public @Nullable DistributionCell tryGetDistribution(MetricName metricName) {
    return distributions.tryGet(metricName);
  }

  /**
   * Return a {@code HistogramCell} named {@code metricName}. If it doesn't exist, create a {@code
   * Metric} with the specified name.
   */
  @Override
  public HistogramCell getHistogram(MetricName metricName, HistogramData.BucketType bucketType) {
    return histograms.get(KV.of(metricName, bucketType));
  }

  /**
   * Return a {@code HistogramCell} named {@code metricName}. If it doesn't exist, return {@code
   * null}.
   */
  public @Nullable HistogramCell tryGetHistogram(
      MetricName metricName, HistogramData.BucketType bucketType) {
    return histograms.tryGet(KV.of(metricName, bucketType));
  }

  /**
   * Return a {@code GaugeCell} named {@code metricName}. If it doesn't exist, create a {@code
   * Metric} with the specified name.
   */
  @Override
  public GaugeCell getGauge(MetricName metricName) {
    return gauges.get(metricName);
  }

  /**
   * Return a {@code GaugeCell} named {@code metricName}. If it doesn't exist, return {@code null}.
   */
  public @Nullable GaugeCell tryGetGauge(MetricName metricName) {
    return gauges.tryGet(metricName);
  }

  private <UpdateT, CellT extends MetricCell<UpdateT>>
      ImmutableList<MetricUpdate<UpdateT>> extractUpdates(MetricsMap<MetricName, CellT> cells) {
    ImmutableList.Builder<MetricUpdate<UpdateT>> updates = ImmutableList.builder();
    for (Map.Entry<MetricName, CellT> cell : cells.entries()) {
      if (cell.getValue().getDirty().beforeCommit()) {
        updates.add(
            MetricUpdate.create(
                MetricKey.create(stepName, cell.getKey()), cell.getValue().getCumulative()));
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
        extractUpdates(counters), extractUpdates(distributions), extractUpdates(gauges));
  }

  /** @return The MonitoringInfo metadata from the metric. */
  private @Nullable SimpleMonitoringInfoBuilder metricToMonitoringMetadata(
      MetricKey metricKey, String typeUrn, String userUrn) {
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder(true);
    builder.setType(typeUrn);

    MetricName metricName = metricKey.metricName();
    if (metricName instanceof MonitoringInfoMetricName) {
      MonitoringInfoMetricName monitoringInfoName = (MonitoringInfoMetricName) metricName;
      // Represents a specific MonitoringInfo for a specific URN.
      builder.setUrn(monitoringInfoName.getUrn());
      for (Entry<String, String> e : monitoringInfoName.getLabels().entrySet()) {
        builder.setLabel(e.getKey(), e.getValue());
      }
    } else { // Represents a user counter.
      // Drop if the stepname is not set. All user counters must be
      // defined for a PTransform. They must be defined on a container bound to a step.
      if (this.stepName == null) {
        return null;
      }

      builder
          .setUrn(userUrn)
          .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, metricKey.metricName().getNamespace())
          .setLabel(MonitoringInfoConstants.Labels.NAME, metricKey.metricName().getName())
          .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, metricKey.stepName());
    }
    return builder;
  }

  /** @return The MonitoringInfo metadata from the counter metric. */
  private @Nullable SimpleMonitoringInfoBuilder counterToMonitoringMetadata(MetricKey metricKey) {
    return metricToMonitoringMetadata(
        metricKey,
        MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE,
        MonitoringInfoConstants.Urns.USER_SUM_INT64);
  }

  /** @return The MonitoringInfo generated from the counter metricUpdate. */
  private @Nullable MonitoringInfo counterUpdateToMonitoringInfo(MetricUpdate<Long> metricUpdate) {
    SimpleMonitoringInfoBuilder builder = counterToMonitoringMetadata(metricUpdate.getKey());
    if (builder == null) {
      return null;
    }
    builder.setInt64SumValue(metricUpdate.getUpdate());
    return builder.build();
  }

  /** @return The MonitoringInfo metadata from the distribution metric. */
  private @Nullable SimpleMonitoringInfoBuilder distributionToMonitoringMetadata(
      MetricKey metricKey) {
    return metricToMonitoringMetadata(
        metricKey,
        MonitoringInfoConstants.TypeUrns.DISTRIBUTION_INT64_TYPE,
        MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64);
  }

  /**
   * @param metricUpdate
   * @return The MonitoringInfo generated from the distribution metricUpdate.
   */
  private @Nullable MonitoringInfo distributionUpdateToMonitoringInfo(
      MetricUpdate<org.apache.beam.runners.core.metrics.DistributionData> metricUpdate) {
    SimpleMonitoringInfoBuilder builder = distributionToMonitoringMetadata(metricUpdate.getKey());
    if (builder == null) {
      return null;
    }
    builder.setInt64DistributionValue(metricUpdate.getUpdate());
    return builder.build();
  }

  /** Return the cumulative values for any metrics in this container as MonitoringInfos. */
  public Iterable<MonitoringInfo> getMonitoringInfos() {
    // Extract user metrics and store as MonitoringInfos.
    ArrayList<MonitoringInfo> monitoringInfos = new ArrayList<MonitoringInfo>();
    MetricUpdates metricUpdates = this.getUpdates();

    for (MetricUpdate<Long> metricUpdate : metricUpdates.counterUpdates()) {
      MonitoringInfo mi = counterUpdateToMonitoringInfo(metricUpdate);
      if (mi != null) {
        monitoringInfos.add(mi);
      }
    }

    for (MetricUpdate<org.apache.beam.runners.core.metrics.DistributionData> metricUpdate :
        metricUpdates.distributionUpdates()) {
      MonitoringInfo mi = distributionUpdateToMonitoringInfo(metricUpdate);
      if (mi != null) {
        monitoringInfos.add(mi);
      }
    }
    return monitoringInfos;
  }

  public Map<String, ByteString> getMonitoringData(ShortIdMap shortIds) {
    ImmutableMap.Builder<String, ByteString> builder = ImmutableMap.builder();
    MetricUpdates metricUpdates = this.getUpdates();
    for (MetricUpdate<Long> metricUpdate : metricUpdates.counterUpdates()) {
      String shortId =
          getShortId(metricUpdate.getKey(), this::counterToMonitoringMetadata, shortIds);
      if (shortId != null) {
        builder.put(shortId, encodeInt64Counter(metricUpdate.getUpdate()));
      }
    }
    for (MetricUpdate<org.apache.beam.runners.core.metrics.DistributionData> metricUpdate :
        metricUpdates.distributionUpdates()) {
      String shortId =
          getShortId(metricUpdate.getKey(), this::distributionToMonitoringMetadata, shortIds);
      if (shortId != null) {
        builder.put(shortId, encodeInt64Distribution(metricUpdate.getUpdate()));
      }
    }
    return builder.build();
  }

  private String getShortId(
      MetricKey key, Function<MetricKey, SimpleMonitoringInfoBuilder> toInfo, ShortIdMap shortIds) {
    Optional<String> shortId = shortIdsByMetricKey.get(key);
    if (shortId == null) {
      SimpleMonitoringInfoBuilder monitoringInfoBuilder = toInfo.apply(key);
      if (monitoringInfoBuilder == null) {
        shortId = Optional.empty();
      } else {
        MonitoringInfo monitoringInfo = monitoringInfoBuilder.build();
        if (monitoringInfo == null) {
          shortId = Optional.empty();
        } else {
          shortId = Optional.of(shortIds.getOrCreateShortId(monitoringInfo));
        }
      }
      shortIdsByMetricKey.put(key, shortId);
    }
    return shortId.orElse(null);
  }

  private void commitUpdates(MetricsMap<MetricName, ? extends MetricCell<?>> cells) {
    for (MetricCell<?> cell : cells.values()) {
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
    commitUpdates(gauges);
  }

  private <UserT extends Metric, UpdateT, CellT extends MetricCell<UpdateT>>
      ImmutableList<MetricUpdate<UpdateT>> extractCumulatives(MetricsMap<MetricName, CellT> cells) {
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

  /** Update values of this {@link MetricsContainerImpl} by merging the value of another cell. */
  public void update(MetricsContainerImpl other) {
    updateCounters(counters, other.counters);
    updateDistributions(distributions, other.distributions);
    updateGauges(gauges, other.gauges);
    updateHistograms(histograms, other.histograms);
  }

  private void updateForSumInt64Type(MonitoringInfo monitoringInfo) {
    MetricName metricName = MonitoringInfoMetricName.of(monitoringInfo);
    CounterCell counter = getCounter(metricName);
    counter.inc(decodeInt64Counter(monitoringInfo.getPayload()));
  }

  private void updateForDistributionInt64Type(MonitoringInfo monitoringInfo) {
    MetricName metricName = MonitoringInfoMetricName.of(monitoringInfo);
    Distribution distribution = getDistribution(metricName);

    DistributionData data = decodeInt64Distribution(monitoringInfo.getPayload());
    distribution.update(data.sum(), data.count(), data.min(), data.max());
  }

  private void updateForLatestInt64Type(MonitoringInfo monitoringInfo) {
    MetricName metricName = MonitoringInfoMetricName.of(monitoringInfo);
    GaugeCell gauge = getGauge(metricName);
    gauge.update(decodeInt64Gauge(monitoringInfo.getPayload()));
  }

  /** Update values of this {@link MetricsContainerImpl} by reading from {@code monitoringInfos}. */
  public void update(Iterable<MonitoringInfo> monitoringInfos) {
    for (MonitoringInfo monitoringInfo : monitoringInfos) {
      if (monitoringInfo.getPayload().isEmpty()) {
        return;
      }

      switch (monitoringInfo.getType()) {
        case SUM_INT64_TYPE:
          updateForSumInt64Type(monitoringInfo);
          break;

        case DISTRIBUTION_INT64_TYPE:
          updateForDistributionInt64Type(monitoringInfo);
          break;

        case LATEST_INT64_TYPE:
          updateForLatestInt64Type(monitoringInfo);
          break;

        default:
          LOG.warn("Unsupported metric type {}", monitoringInfo.getType());
      }
    }
  }

  private void updateCounters(
      MetricsMap<MetricName, CounterCell> current, MetricsMap<MetricName, CounterCell> updates) {
    for (Map.Entry<MetricName, CounterCell> counter : updates.entries()) {
      current.get(counter.getKey()).inc(counter.getValue().getCumulative());
    }
  }

  private void updateDistributions(
      MetricsMap<MetricName, DistributionCell> current,
      MetricsMap<MetricName, DistributionCell> updates) {
    for (Map.Entry<MetricName, DistributionCell> counter : updates.entries()) {
      current.get(counter.getKey()).update(counter.getValue().getCumulative());
    }
  }

  private void updateGauges(
      MetricsMap<MetricName, GaugeCell> current, MetricsMap<MetricName, GaugeCell> updates) {
    for (Map.Entry<MetricName, GaugeCell> counter : updates.entries()) {
      current.get(counter.getKey()).update(counter.getValue().getCumulative());
    }
  }

  private void updateHistograms(
      MetricsMap<KV<MetricName, HistogramData.BucketType>, HistogramCell> current,
      MetricsMap<KV<MetricName, HistogramData.BucketType>, HistogramCell> updates) {
    for (Map.Entry<KV<MetricName, HistogramData.BucketType>, HistogramCell> histogram :
        updates.entries()) {
      HistogramCell h = histogram.getValue();
      current.get(histogram.getKey()).update(h);
    }
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof MetricsContainerImpl) {
      MetricsContainerImpl metricsContainerImpl = (MetricsContainerImpl) object;
      return Objects.equals(stepName, metricsContainerImpl.stepName)
          && Objects.equals(counters, metricsContainerImpl.counters)
          && Objects.equals(distributions, metricsContainerImpl.distributions)
          && Objects.equals(gauges, metricsContainerImpl.gauges);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(stepName, counters, distributions, gauges);
  }

  /**
   * Match a MetricName with a given metric filter. If the metric filter is null, the method always
   * returns true. TODO(BEAM-10986) Consider making this use the MetricNameFilter and related
   * classes.
   */
  @VisibleForTesting
  static boolean matchMetric(MetricName metricName, @Nullable Set<String> allowedMetricUrns) {
    if (allowedMetricUrns == null) {
      return true;
    }
    if (metricName instanceof MonitoringInfoMetricName) {
      return allowedMetricUrns.contains(((MonitoringInfoMetricName) metricName).getUrn());
    }
    return false;
  }

  /** Return a string representing the cumulative values of all metrics in this container. */
  public String getCumulativeString(@Nullable Set<String> allowedMetricUrns) {
    StringBuilder message = new StringBuilder();
    for (Map.Entry<MetricName, CounterCell> cell : counters.entries()) {
      if (!matchMetric(cell.getKey(), allowedMetricUrns)) {
        continue;
      }
      message.append(cell.getKey().toString());
      message.append(" = ");
      message.append(cell.getValue().getCumulative());
      message.append("\n");
    }
    for (Map.Entry<MetricName, DistributionCell> cell : distributions.entries()) {
      if (!matchMetric(cell.getKey(), allowedMetricUrns)) {
        continue;
      }
      message.append(cell.getKey().toString());
      message.append(" = ");
      DistributionData data = cell.getValue().getCumulative();
      message.append(
          String.format(
              "{sum: %d, count: %d, min: %d, max: %d}",
              data.sum(), data.count(), data.min(), data.max()));
      message.append("\n");
    }
    for (Map.Entry<MetricName, GaugeCell> cell : gauges.entries()) {
      if (!matchMetric(cell.getKey(), allowedMetricUrns)) {
        continue;
      }
      message.append(cell.getKey().toString());
      message.append(" = ");
      GaugeData data = cell.getValue().getCumulative();
      message.append(String.format("{timestamp: %s, value: %d}", data.timestamp(), data.value()));
      message.append("\n");
    }
    for (Map.Entry<KV<MetricName, HistogramData.BucketType>, HistogramCell> cell :
        histograms.entries()) {
      if (!matchMetric(cell.getKey().getKey(), allowedMetricUrns)) {
        continue;
      }
      message.append(cell.getKey().getKey().toString());
      message.append(" = ");
      HistogramData data = cell.getValue().getCumulative();
      if (data.getTotalCount() > 0) {
        message.append(
            String.format(
                "{count: %d, p50: %f, p90: %f, p99: %f}",
                data.getTotalCount(), data.p50(), data.p90(), data.p99()));
      } else {
        message.append("{count: 0}");
      }
      message.append("\n");
    }
    return message.toString();
  }

  /**
   * Returns a MetricContainer with the delta values between two MetricsContainers. The purpose of
   * this function is to print the changes made to the metrics within a window of time. The
   * difference between the counter and histogram bucket counters are calculated between curr and
   * prev. The most recent value are used for gauges. Distribution metrics are dropped (As there is
   * meaningful way to calculate the delta). Returns curr if prev is null.
   */
  public static MetricsContainerImpl deltaContainer(
      @Nullable MetricsContainerImpl prev, MetricsContainerImpl curr) {
    if (prev == null) {
      return curr;
    }
    MetricsContainerImpl deltaContainer = new MetricsContainerImpl(curr.stepName);
    for (Map.Entry<MetricName, CounterCell> cell : curr.counters.entries()) {
      Long prevValue = prev.counters.get(cell.getKey()).getCumulative();
      Long currValue = cell.getValue().getCumulative();
      deltaContainer.counters.get(cell.getKey()).inc(currValue - prevValue);
    }
    for (Map.Entry<MetricName, GaugeCell> cell : curr.gauges.entries()) {
      // Simply take the most recent value for gauge, no need to count deltas.
      deltaContainer.gauges.get(cell.getKey()).update(cell.getValue().getCumulative());
    }
    for (Map.Entry<KV<MetricName, HistogramData.BucketType>, HistogramCell> cell :
        curr.histograms.entries()) {
      HistogramData.BucketType bt = cell.getKey().getValue();
      HistogramData prevValue = prev.histograms.get(cell.getKey()).getCumulative();
      HistogramData currValue = cell.getValue().getCumulative();
      HistogramCell deltaValueCell = deltaContainer.histograms.get(cell.getKey());
      deltaValueCell.incBottomBucketCount(
          currValue.getBottomBucketCount() - prevValue.getBottomBucketCount());
      for (int i = 0; i < bt.getNumBuckets(); i++) {
        Long bucketCountDelta = currValue.getCount(i) - prevValue.getCount(i);
        deltaValueCell.incBucketCount(i, bucketCountDelta);
      }
      deltaValueCell.incTopBucketCount(
          currValue.getTopBucketCount() - prevValue.getTopBucketCount());
    }
    return deltaContainer;
  }
}
