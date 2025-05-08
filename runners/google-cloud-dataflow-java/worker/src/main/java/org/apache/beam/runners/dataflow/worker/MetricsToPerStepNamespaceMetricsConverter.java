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

import com.google.api.services.dataflow.model.Base2Exponent;
import com.google.api.services.dataflow.model.BucketOptions;
import com.google.api.services.dataflow.model.DataflowGaugeValue;
import com.google.api.services.dataflow.model.DataflowHistogramValue;
import com.google.api.services.dataflow.model.Linear;
import com.google.api.services.dataflow.model.MetricValue;
import com.google.api.services.dataflow.model.OutlierStats;
import com.google.api.services.dataflow.model.PerStepNamespaceMetrics;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySinkMetrics;
import org.apache.beam.sdk.metrics.LabeledMetricNameUtils;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.HistogramData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts metric updates to {@link PerStepNamespaceMetrics} protos. Currently we only support
 * converting metrics from {@link BigQuerySinkMetrics} and from {@link KafkaSinkMetrics} with this
 * converter.
 */
public class MetricsToPerStepNamespaceMetricsConverter {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetricsToPerStepNamespaceMetricsConverter.class);

  // Avoids to introduce mandatory kafka-io dependency to Dataflow worker
  // keep in sync with org.apache.beam.sdk.io.kafka.KafkaSinkMetrics.METRICS_NAMESPACE
  public static String KAFKA_SINK_METRICS_NAMESPACE = "KafkaSink";
  private static String[] SUPPORTED_NAMESPACES = {
    KAFKA_SINK_METRICS_NAMESPACE, BigQuerySinkMetrics.METRICS_NAMESPACE
  };

  private static Optional<LabeledMetricNameUtils.ParsedMetricName> getParsedMetricName(
      MetricName metricName,
      Map<MetricName, LabeledMetricNameUtils.ParsedMetricName> parsedPerWorkerMetricsCache) {
    Optional<LabeledMetricNameUtils.ParsedMetricName> parsedMetricName =
        Optional.ofNullable(parsedPerWorkerMetricsCache.get(metricName));
    if (!parsedMetricName.isPresent()) {
      parsedMetricName = LabeledMetricNameUtils.parseMetricName(metricName.getName());
      parsedMetricName.ifPresent(
          parsedName -> parsedPerWorkerMetricsCache.put(metricName, parsedName));
    }
    return parsedMetricName;
  }

  /**
   * @param metricName The {@link MetricName} that represents this metric.
   * @return boolean If the metric is from a supported namespace.
   */
  private static boolean isNameSpaceSupported(MetricName metricName) {
    boolean isValidNameSpace =
        Stream.of(SUPPORTED_NAMESPACES).anyMatch(x -> x.equals(metricName.getNamespace()));
    if (!isValidNameSpace) {
      LOG.warn(
          "Dropping metric {} since {} is not one of the supported namespaces: {}",
          metricName,
          metricName.getNamespace(),
          Arrays.toString(SUPPORTED_NAMESPACES));
    }
    return isValidNameSpace;
  }

  /**
   * @param metricName The {@link MetricName} that represents this counter.
   * @param value The counter value.
   * @return If the conversion succeeds, {@code MetricValue} that represents this counter. Otherwise
   *     return an empty optional
   */
  private static Optional<MetricValue> convertCounterToMetricValue(
      MetricName metricName,
      Long value,
      Map<MetricName, LabeledMetricNameUtils.ParsedMetricName> parsedPerWorkerMetricsCache) {

    if (value == 0) {
      return Optional.empty();
    }

    return getParsedMetricName(metricName, parsedPerWorkerMetricsCache)
        .filter(labeledName -> !labeledName.getBaseName().isEmpty())
        .map(
            labeledName ->
                new MetricValue()
                    .setMetric(labeledName.getBaseName())
                    .setMetricLabels(labeledName.getMetricLabels())
                    .setValueInt64(value));
  }

  /**
   * @param metricName The {@link MetricName} that represents this counter.
   * @param value The counter value.
   * @return If the conversion succeeds, {@code MetricValue} that represents this counter. Otherwise
   *     returns an empty optional
   */
  private static Optional<MetricValue> convertGaugeToMetricValue(
      MetricName metricName,
      Long value,
      Map<MetricName, LabeledMetricNameUtils.ParsedMetricName> parsedPerWorkerMetricsCache) {

    Optional<LabeledMetricNameUtils.ParsedMetricName> labeledName =
        getParsedMetricName(metricName, parsedPerWorkerMetricsCache);
    if (!labeledName.isPresent() || labeledName.get().getBaseName().isEmpty()) {
      return Optional.empty();
    }

    DataflowGaugeValue gauge_value = new DataflowGaugeValue();
    gauge_value.setValue(value);

    return Optional.of(
        new MetricValue()
            .setMetric(labeledName.get().getBaseName())
            .setMetricLabels(labeledName.get().getMetricLabels())
            .setValueGauge64(gauge_value));
  }

  /**
   * Adds {@code outlierStats} to {@code outputHistogram} if {@code inputHistogram} has recorded
   * overflow or underflow values.
   *
   * @param inputHistogram
   * @param outputHistogram
   */
  private static void addOutlierStatsToHistogram(
      LockFreeHistogram.Snapshot inputHistogram, DataflowHistogramValue outputHistogram) {
    long overflowCount = inputHistogram.overflowStatistic().count();
    long underflowCount = inputHistogram.underflowStatistic().count();
    if (underflowCount == 0 && overflowCount == 0) {
      return;
    }

    OutlierStats outlierStats = new OutlierStats();
    if (underflowCount > 0) {
      outlierStats
          .setUnderflowCount(underflowCount)
          .setUnderflowMean(inputHistogram.underflowStatistic().mean());
    }
    if (overflowCount > 0) {
      outlierStats
          .setOverflowCount(overflowCount)
          .setOverflowMean(inputHistogram.overflowStatistic().mean());
    }
    outputHistogram.setOutlierStats(outlierStats);
  }

  /**
   * @param metricName The {@link MetricName} that represents this Histogram.
   * @param value The histogram value. Currently we only support converting histograms that use
   *     {@code linear} or {@code exponential} buckets.
   * @return If this conversion succeeds, a {@code MetricValue} that represents this histogram.
   *     Otherwise returns an empty optional.
   */
  private static Optional<MetricValue> convertHistogramToMetricValue(
      MetricName metricName,
      LockFreeHistogram.Snapshot inputHistogram,
      Map<MetricName, LabeledMetricNameUtils.ParsedMetricName> parsedPerWorkerMetricsCache) {
    if (inputHistogram.totalCount() == 0L) {
      return Optional.empty();
    }

    Optional<LabeledMetricNameUtils.ParsedMetricName> labeledName =
        getParsedMetricName(metricName, parsedPerWorkerMetricsCache);
    if (!labeledName.isPresent() || labeledName.get().getBaseName().isEmpty()) {
      return Optional.empty();
    }

    DataflowHistogramValue outputHistogram = new DataflowHistogramValue();
    int numberOfBuckets = inputHistogram.bucketType().getNumBuckets();

    if (inputHistogram.bucketType() instanceof HistogramData.LinearBuckets) {
      HistogramData.LinearBuckets buckets =
          (HistogramData.LinearBuckets) inputHistogram.bucketType();
      Linear linearOptions =
          new Linear()
              .setNumberOfBuckets(numberOfBuckets)
              .setWidth(buckets.getWidth())
              .setStart(buckets.getStart());
      outputHistogram.setBucketOptions(new BucketOptions().setLinear(linearOptions));
    } else if (inputHistogram.bucketType() instanceof HistogramData.ExponentialBuckets) {
      HistogramData.ExponentialBuckets buckets =
          (HistogramData.ExponentialBuckets) inputHistogram.bucketType();
      Base2Exponent expoenntialOptions =
          new Base2Exponent().setNumberOfBuckets(numberOfBuckets).setScale(buckets.getScale());
      outputHistogram.setBucketOptions(new BucketOptions().setExponential(expoenntialOptions));
    } else {
      parsedPerWorkerMetricsCache.remove(metricName);
      return Optional.empty();
    }

    outputHistogram.setCount(inputHistogram.totalCount());
    List<Long> bucketCounts = new ArrayList<>(inputHistogram.buckets().length());

    inputHistogram.buckets().forEach(val -> bucketCounts.add(val));

    // Remove trailing 0 buckets.
    for (int i = bucketCounts.size() - 1; i >= 0; i--) {
      if (bucketCounts.get(i) != 0) {
        break;
      }
      bucketCounts.remove(i);
    }

    outputHistogram.setBucketCounts(bucketCounts);

    addOutlierStatsToHistogram(inputHistogram, outputHistogram);

    return Optional.of(
        new MetricValue()
            .setMetric(labeledName.get().getBaseName())
            .setMetricLabels(labeledName.get().getMetricLabels())
            .setValueHistogram(outputHistogram));
  }

  /**
   * @param stepName The unfused stage that these metrics are associated with.
   * @param counters Counter updates to convert.
   * @param histograms Histogram updates to convert.
   * @param parsedPerWorkerMetricsCache cache of previously converted {@code ParsedMetricName}. The
   *     cache will be updated to include all valid metric names in counters and histograms.
   * @return Collection of {@code PerStepNamespaceMetrics} that represent these metric updates. Each
   *     {@code PerStepNamespaceMetrics} contains a list of {@code MetricUpdates} for a {unfused
   *     stage, metrics namespace} pair.
   */
  public static Collection<PerStepNamespaceMetrics> convert(
      String stepName,
      Map<MetricName, Long> counters,
      Map<MetricName, Long> gauges,
      Map<MetricName, LockFreeHistogram.Snapshot> histograms,
      Map<MetricName, LabeledMetricNameUtils.ParsedMetricName> parsedPerWorkerMetricsCache) {

    Map<String, PerStepNamespaceMetrics> metricsByNamespace = new HashMap<>();
    for (Entry<MetricName, Long> entry : counters.entrySet()) {
      MetricName metricName = entry.getKey();

      boolean validNameSpace = isNameSpaceSupported(metricName);
      if (!validNameSpace) {
        continue;
      }

      Optional<MetricValue> metricValue =
          convertCounterToMetricValue(metricName, entry.getValue(), parsedPerWorkerMetricsCache);
      if (!metricValue.isPresent()) {
        continue;
      }

      PerStepNamespaceMetrics stepNamespaceMetrics =
          metricsByNamespace.get(metricName.getNamespace());
      if (stepNamespaceMetrics == null) {
        stepNamespaceMetrics =
            new PerStepNamespaceMetrics()
                .setMetricValues(new ArrayList<>())
                .setOriginalStep(stepName)
                .setMetricsNamespace(metricName.getNamespace());
        metricsByNamespace.put(metricName.getNamespace(), stepNamespaceMetrics);
      }

      stepNamespaceMetrics.getMetricValues().add(metricValue.get());
    }

    for (Entry<MetricName, LockFreeHistogram.Snapshot> entry : histograms.entrySet()) {
      MetricName metricName = entry.getKey();

      boolean validNameSpace = isNameSpaceSupported(metricName);
      if (!validNameSpace) {
        continue;
      }
      Optional<MetricValue> metricValue =
          convertHistogramToMetricValue(metricName, entry.getValue(), parsedPerWorkerMetricsCache);
      if (!metricValue.isPresent()) {
        continue;
      }

      PerStepNamespaceMetrics stepNamespaceMetrics =
          metricsByNamespace.get(metricName.getNamespace());
      if (stepNamespaceMetrics == null) {
        stepNamespaceMetrics =
            new PerStepNamespaceMetrics()
                .setMetricValues(new ArrayList<>())
                .setOriginalStep(stepName)
                .setMetricsNamespace(metricName.getNamespace());
        metricsByNamespace.put(metricName.getNamespace(), stepNamespaceMetrics);
      }

      stepNamespaceMetrics.getMetricValues().add(metricValue.get());
    }

    for (Entry<MetricName, Long> entry : gauges.entrySet()) {
      MetricName metricName = entry.getKey();

      boolean validNameSpace = isNameSpaceSupported(metricName);
      if (!validNameSpace) {
        continue;
      }

      Optional<MetricValue> metricValue =
          convertGaugeToMetricValue(metricName, entry.getValue(), parsedPerWorkerMetricsCache);
      if (!metricValue.isPresent()) {
        continue;
      }

      PerStepNamespaceMetrics stepNamespaceMetrics =
          metricsByNamespace.get(metricName.getNamespace());
      if (stepNamespaceMetrics == null) {
        stepNamespaceMetrics =
            new PerStepNamespaceMetrics()
                .setMetricValues(new ArrayList<>())
                .setOriginalStep(stepName)
                .setMetricsNamespace(metricName.getNamespace());
        metricsByNamespace.put(metricName.getNamespace(), stepNamespaceMetrics);
      }
      stepNamespaceMetrics.getMetricValues().add(metricValue.get());
    }
    return metricsByNamespace.values();
  }
}
