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
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.SET_STRING_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Gauge;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeStringSet;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Gauge;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeStringSet;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class MetricsContainerImpl implements Serializable, MetricsContainer {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsContainerImpl.class);

  protected final @Nullable String stepName;

  private final boolean isProcessWide;

  private MetricsMap<MetricName, CounterCell> counters = new MetricsMap<>(CounterCell::new);

  private MetricsMap<MetricName, DistributionCell> distributions =
      new MetricsMap<>(DistributionCell::new);

  private MetricsMap<MetricName, GaugeCell> gauges = new MetricsMap<>(GaugeCell::new);

  private MetricsMap<MetricName, StringSetCell> stringSets = new MetricsMap<>(StringSetCell::new);

  private MetricsMap<KV<MetricName, HistogramData.BucketType>, HistogramCell> histograms =
      new MetricsMap<>(HistogramCell::new);

  private MetricsContainerImpl(@Nullable String stepName, boolean isProcessWide) {
    this.stepName = stepName;
    this.isProcessWide = isProcessWide;
  }

  /**
   * Create a new {@link MetricsContainerImpl} associated with the given {@code stepName}. If
   * stepName is null, this MetricsContainer is not bound to a step.
   */
  public MetricsContainerImpl(@Nullable String stepName) {
    this(stepName, false);
  }

  /**
   * Create a new {@link MetricsContainerImpl} associated with the entire process. Used for
   * collecting processWide metrics for HarnessMonitoringInfoRequest/Response.
   */
  public static MetricsContainerImpl createProcessWideContainer() {
    return new MetricsContainerImpl(null, true);
  }

  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
      justification = "No bug",
      value = "SE_BAD_FIELD")
  private Map<MetricName, Optional<String>> shortIdsByMetricName = new ConcurrentHashMap<>();

  /** Reset the metrics. */
  public void reset() {
    if (this.isProcessWide) {
      throw new RuntimeException("Process Wide metric containers must not be reset");
    }
    counters.forEachValue(CounterCell::reset);
    distributions.forEachValue(DistributionCell::reset);
    gauges.forEachValue(GaugeCell::reset);
    histograms.forEachValue(HistogramCell::reset);
    stringSets.forEachValue(StringSetCell::reset);
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

  /**
   * Return a {@code StringSetCell} named {@code metricName}. If it doesn't exist, create a {@code
   * Metric} with the specified name.
   */
  @Override
  public StringSetCell getStringSet(MetricName metricName) {
    return stringSets.get(metricName);
  }

  /**
   * Return a {@code StringSetCell} named {@code metricName}. If it doesn't exist, return {@code
   * null}.
   */
  public @Nullable StringSetCell tryGetStringSet(MetricName metricName) {
    return stringSets.tryGet(metricName);
  }

  private <UpdateT, CellT extends MetricCell<UpdateT>>
      ImmutableList<MetricUpdate<UpdateT>> extractUpdates(MetricsMap<MetricName, CellT> cells) {
    ImmutableList.Builder<MetricUpdate<UpdateT>> updates = ImmutableList.builder();
    cells.forEach(
        (key, value) -> {
          if (value.getDirty().beforeCommit()) {
            updates.add(
                MetricUpdate.create(MetricKey.create(stepName, key), value.getCumulative()));
          }
        });
    return updates.build();
  }

  /**
   * Return the cumulative values for any metrics that have changed since the last time updates were
   * committed.
   */
  public MetricUpdates getUpdates() {
    return MetricUpdates.create(
        extractUpdates(counters),
        extractUpdates(distributions),
        extractUpdates(gauges),
        extractUpdates(stringSets));
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
      MetricUpdate<DistributionData> metricUpdate) {
    SimpleMonitoringInfoBuilder builder = distributionToMonitoringMetadata(metricUpdate.getKey());
    if (builder == null) {
      return null;
    }
    builder.setInt64DistributionValue(metricUpdate.getUpdate());
    return builder.build();
  }

  /** @return The MonitoringInfo metadata from the gauge metric. */
  private @Nullable SimpleMonitoringInfoBuilder gaugeToMonitoringMetadata(MetricKey metricKey) {
    return metricToMonitoringMetadata(
        metricKey,
        MonitoringInfoConstants.TypeUrns.LATEST_INT64_TYPE,
        MonitoringInfoConstants.Urns.USER_LATEST_INT64);
  }

  /**
   * @param metricUpdate
   * @return The MonitoringInfo generated from the distribution metricUpdate.
   */
  private @Nullable MonitoringInfo gaugeUpdateToMonitoringInfo(
      MetricUpdate<GaugeData> metricUpdate) {
    SimpleMonitoringInfoBuilder builder = gaugeToMonitoringMetadata(metricUpdate.getKey());
    if (builder == null) {
      return null;
    }
    builder.setInt64LatestValue(metricUpdate.getUpdate());
    return builder.build();
  }

  /** @return The MonitoringInfo metadata from the string set metric. */
  private @Nullable SimpleMonitoringInfoBuilder stringSetToMonitoringMetadata(MetricKey metricKey) {
    return metricToMonitoringMetadata(
        metricKey,
        MonitoringInfoConstants.TypeUrns.SET_STRING_TYPE,
        MonitoringInfoConstants.Urns.USER_SET_STRING);
  }

  /**
   * @param metricUpdate
   * @return The MonitoringInfo generated from the string set metricUpdate.
   */
  private @Nullable MonitoringInfo stringSetUpdateToMonitoringInfo(
      MetricUpdate<StringSetData> metricUpdate) {
    SimpleMonitoringInfoBuilder builder = stringSetToMonitoringMetadata(metricUpdate.getKey());
    if (builder == null) {
      return null;
    }
    builder.setStringSetValue(metricUpdate.getUpdate());
    return builder.build();
  }

  /** Return the cumulative values for any metrics in this container as MonitoringInfos. */
  @Override
  public Iterable<MonitoringInfo> getMonitoringInfos() {
    // Extract user metrics and store as MonitoringInfos.
    List<MonitoringInfo> monitoringInfos = new ArrayList<>();
    MetricUpdates metricUpdates = this.getUpdates();

    for (MetricUpdate<Long> metricUpdate : metricUpdates.counterUpdates()) {
      MonitoringInfo mi = counterUpdateToMonitoringInfo(metricUpdate);
      if (mi != null) {
        monitoringInfos.add(mi);
      }
    }

    for (MetricUpdate<DistributionData> metricUpdate : metricUpdates.distributionUpdates()) {
      MonitoringInfo mi = distributionUpdateToMonitoringInfo(metricUpdate);
      if (mi != null) {
        monitoringInfos.add(mi);
      }
    }

    for (MetricUpdate<GaugeData> metricUpdate : metricUpdates.gaugeUpdates()) {
      MonitoringInfo mi = gaugeUpdateToMonitoringInfo(metricUpdate);
      if (mi != null) {
        monitoringInfos.add(mi);
      }
    }

    for (MetricUpdate<StringSetData> metricUpdate : metricUpdates.stringSetUpdates()) {
      MonitoringInfo mi = stringSetUpdateToMonitoringInfo(metricUpdate);
      if (mi != null) {
        monitoringInfos.add(mi);
      }
    }
    return monitoringInfos;
  }

  public Map<String, ByteString> getMonitoringData(ShortIdMap shortIds) {
    ImmutableMap.Builder<String, ByteString> builder = ImmutableMap.builder();
    counters.forEach(
        (metricName, counterCell) -> {
          if (counterCell.getDirty().beforeCommit()) {
            String shortId = getShortId(metricName, this::counterToMonitoringMetadata, shortIds);
            if (shortId != null) {
              builder.put(shortId, encodeInt64Counter(counterCell.getCumulative()));
            }
          }
        });
    distributions.forEach(
        (metricName, distributionCell) -> {
          if (distributionCell.getDirty().beforeCommit()) {
            String shortId =
                getShortId(metricName, this::distributionToMonitoringMetadata, shortIds);
            if (shortId != null) {
              builder.put(shortId, encodeInt64Distribution(distributionCell.getCumulative()));
            }
          }
        });
    gauges.forEach(
        (metricName, gaugeCell) -> {
          if (gaugeCell.getDirty().beforeCommit()) {
            String shortId = getShortId(metricName, this::gaugeToMonitoringMetadata, shortIds);
            if (shortId != null) {
              builder.put(shortId, encodeInt64Gauge(gaugeCell.getCumulative()));
            }
          }
        });
    stringSets.forEach(
        (metricName, stringSetCell) -> {
          if (stringSetCell.getDirty().beforeCommit()) {
            String shortId = getShortId(metricName, this::stringSetToMonitoringMetadata, shortIds);
            if (shortId != null) {
              builder.put(shortId, encodeStringSet(stringSetCell.getCumulative()));
            }
          }
        });
    return builder.build();
  }

  private String getShortId(
      MetricName metricName,
      Function<MetricKey, SimpleMonitoringInfoBuilder> toInfo,
      ShortIdMap shortIds) {
    Optional<String> shortId = shortIdsByMetricName.get(metricName);
    if (shortId == null) {
      SimpleMonitoringInfoBuilder monitoringInfoBuilder =
          toInfo.apply(MetricKey.create(stepName, metricName));
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
      shortIdsByMetricName.put(metricName, shortId);
    }
    return shortId.orElse(null);
  }

  /**
   * Mark all of the updates that were retrieved with the latest call to {@link #getUpdates()} as
   * committed.
   */
  public void commitUpdates() {
    counters.forEachValue(counter -> counter.getDirty().afterCommit());
    distributions.forEachValue(distribution -> distribution.getDirty().afterCommit());
    gauges.forEachValue(gauge -> gauge.getDirty().afterCommit());
    stringSets.forEachValue(sSets -> sSets.getDirty().afterCommit());
  }

  private <UserT extends Metric, UpdateT, CellT extends MetricCell<UpdateT>>
      ImmutableList<MetricUpdate<UpdateT>> extractCumulatives(MetricsMap<MetricName, CellT> cells) {
    ImmutableList.Builder<MetricUpdate<UpdateT>> updates = ImmutableList.builder();
    cells.forEach(
        (key, value) -> {
          UpdateT update = checkNotNull(value.getCumulative());
          updates.add(MetricUpdate.create(MetricKey.create(stepName, key), update));
        });
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
        extractCumulatives(gauges),
        extractCumulatives(stringSets));
  }

  /** Update values of this {@link MetricsContainerImpl} by merging the value of another cell. */
  public void update(MetricsContainerImpl other) {
    updateCounters(counters, other.counters);
    updateDistributions(distributions, other.distributions);
    updateGauges(gauges, other.gauges);
    updateHistograms(histograms, other.histograms);
    updateStringSets(stringSets, other.stringSets);
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

  private void updateForStringSetType(MonitoringInfo monitoringInfo) {
    MetricName metricName = MonitoringInfoMetricName.of(monitoringInfo);
    StringSetCell stringSet = getStringSet(metricName);
    stringSet.update(decodeStringSet(monitoringInfo.getPayload()));
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

        case SET_STRING_TYPE:
          updateForStringSetType(monitoringInfo);
          break;

        default:
          LOG.warn("Unsupported metric type {}", monitoringInfo.getType());
      }
    }
  }

  private void updateCounters(
      MetricsMap<MetricName, CounterCell> current, MetricsMap<MetricName, CounterCell> updates) {
    updates.forEach((key, value) -> current.get(key).inc(value.getCumulative()));
  }

  private void updateDistributions(
      MetricsMap<MetricName, DistributionCell> current,
      MetricsMap<MetricName, DistributionCell> updates) {
    updates.forEach((key, value) -> current.get(key).update(value.getCumulative()));
  }

  private void updateGauges(
      MetricsMap<MetricName, GaugeCell> current, MetricsMap<MetricName, GaugeCell> updates) {
    updates.forEach((key, value) -> current.get(key).update(value.getCumulative()));
  }

  private void updateHistograms(
      MetricsMap<KV<MetricName, HistogramData.BucketType>, HistogramCell> current,
      MetricsMap<KV<MetricName, HistogramData.BucketType>, HistogramCell> updates) {
    updates.forEach((key, value) -> current.get(key).update(value));
  }

  private void updateStringSets(
      MetricsMap<MetricName, StringSetCell> current,
      MetricsMap<MetricName, StringSetCell> updates) {
    updates.forEach((key, value) -> current.get(key).update(value.getCumulative()));
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof MetricsContainerImpl) {
      MetricsContainerImpl metricsContainerImpl = (MetricsContainerImpl) object;
      return Objects.equals(stepName, metricsContainerImpl.stepName)
          && Objects.equals(counters, metricsContainerImpl.counters)
          && Objects.equals(distributions, metricsContainerImpl.distributions)
          && Objects.equals(gauges, metricsContainerImpl.gauges)
          && Objects.equals(stringSets, metricsContainerImpl.stringSets);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(stepName, counters, distributions, gauges, stringSets);
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
    for (Map.Entry<MetricName, StringSetCell> cell : stringSets.entries()) {
      if (!matchMetric(cell.getKey(), allowedMetricUrns)) {
        continue;
      }
      message.append(cell.getKey().toString());
      message.append(" = ");
      StringSetData data = cell.getValue().getCumulative();
      message.append(data.stringSet().toString());
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
    for (Map.Entry<MetricName, StringSetCell> cell : curr.stringSets.entries()) {
      // Simply take the most recent value for stringSets, no need to count deltas.
      deltaContainer.stringSets.get(cell.getKey()).update(cell.getValue().getCumulative());
    }
    return deltaContainer;
  }
}
