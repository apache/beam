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

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates.not;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;

/**
 * An adapter between the {@link MetricsContainerStepMap} and the Dropwizard {@link Metric}
 * interface.
 */
class SparkBeamMetric extends BeamMetricSet {

  private static final String ILLEGAL_CHARACTERS = "[^A-Za-z0-9-]";

  @Override
  public Map<String, Gauge<Double>> getValue(String prefix, MetricFilter filter) {
    MetricResults metricResults =
        asAttemptedOnlyMetricResults(MetricsAccumulator.getInstance().value());
    Map<String, Gauge<Double>> metrics = new HashMap<>();
    MetricQueryResults allMetrics = metricResults.allMetrics();
    for (MetricResult<Long> metricResult : allMetrics.getCounters()) {
      putFiltered(metrics, filter, renderName(prefix, metricResult), metricResult.getAttempted());
    }
    for (MetricResult<DistributionResult> metricResult : allMetrics.getDistributions()) {
      DistributionResult result = metricResult.getAttempted();
      String baseName = renderName(prefix, metricResult);
      putFiltered(metrics, filter, baseName + ".count", result.getCount());
      putFiltered(metrics, filter, baseName + ".sum", result.getSum());
      putFiltered(metrics, filter, baseName + ".min", result.getMin());
      putFiltered(metrics, filter, baseName + ".max", result.getMax());
      putFiltered(metrics, filter, baseName + ".mean", result.getMean());
    }
    for (MetricResult<GaugeResult> metricResult : allMetrics.getGauges()) {
      putFiltered(
          metrics,
          filter,
          renderName(prefix, metricResult),
          metricResult.getAttempted().getValue());
    }
    return metrics;
  }

  @VisibleForTesting
  @SuppressWarnings("nullness") // ok to have nullable elements on stream
  static String renderName(String prefix, MetricResult<?> metricResult) {
    MetricKey key = metricResult.getKey();
    MetricName name = key.metricName();
    String step = key.stepName();
    return Streams.concat(
            Stream.of(prefix),
            Stream.of(stripSuffix(normalizePart(step))),
            Stream.of(name.getNamespace(), name.getName()).map(SparkBeamMetric::normalizePart))
        .filter(not(Strings::isNullOrEmpty))
        .collect(Collectors.joining("."));
  }

  private static @Nullable String normalizePart(@Nullable String str) {
    return str != null ? str.replaceAll(ILLEGAL_CHARACTERS, "_") : null;
  }

  private static @Nullable String stripSuffix(@Nullable String str) {
    return str != null && str.endsWith("_") ? str.substring(0, str.length() - 1) : str;
  }

  private void putFiltered(
      Map<String, Gauge<Double>> metrics, MetricFilter filter, String name, Number value) {
    Gauge<Double> metric = staticGauge(value);
    if (filter.matches(name, metric)) {
      metrics.put(name, metric);
    }
  }
}
