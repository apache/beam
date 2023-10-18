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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeCell;
import org.apache.beam.runners.core.metrics.HistogramCell;
import org.apache.beam.runners.core.metrics.MetricsMap;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
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

  private static Boolean enablePerWorkerMetrics;

  private MetricsMap<MetricName, DeltaCounterCell> counters =
      new MetricsMap<>(DeltaCounterCell::new);

  private MetricsMap<MetricName, DeltaCounterCell> perWorkerCounters =
      new MetricsMap<>(DeltaCounterCell::new);

  private MetricsMap<MetricName, GaugeCell> gauges = new MetricsMap<>(GaugeCell::new);

  private MetricsMap<MetricName, DeltaDistributionCell> distributions =
      new MetricsMap<>(DeltaDistributionCell::new);

  private MetricsMap<KV<MetricName, HistogramData.BucketType>, HistogramCell> perWorkerHistograms =
      new MetricsMap<>(HistogramCell::new);

  private StreamingStepMetricsContainer(String stepName) {
    this.stepName = stepName;
  }

  public static MetricsContainerRegistry<StreamingStepMetricsContainer> createRegistry() {
    return new MetricsContainerRegistry<StreamingStepMetricsContainer>() {
      @Override
      protected StreamingStepMetricsContainer createContainer(String stepName) {
        return new StreamingStepMetricsContainer(stepName);
      }
    };
  }

  @Override
  public Counter getCounter(MetricName metricName) {
    return counters.get(metricName);
  }

  @Override
  public Counter getPerWorkerCounter(MetricName metricName) {
    if (enablePerWorkerMetrics) {
      return perWorkerCounters.get(metricName);
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
    return gauges.get(metricName);
  }

  @Override
  public Histogram getPerWorkerHistogram(
      MetricName metricName, HistogramData.BucketType bucketType) {
    if (enablePerWorkerMetrics) {
      return perWorkerHistograms.get(KV.of(metricName, bucketType));
    } else {
      return MetricsContainer.super.getPerWorkerHistogram(metricName, bucketType);
    }
  }

  public Iterable<CounterUpdate> extractUpdates() {
    return counterUpdates().append(distributionUpdates());
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
}
