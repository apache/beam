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

import com.codahale.metrics.Metric;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;


/**
 * An adapter between the {@link MetricsContainerStepMap} and Codahale's {@link Metric} interface.
 */
class SparkBeamMetric implements Metric {
  private static final String ILLEGAL_CHARACTERS = "[^A-Za-z0-9\\._-]";
  private static final String ILLEGAL_CHARACTERS_AND_PERIOD = "[^A-Za-z0-9_-]";

  Map<String, ?> renderAll() {
    Map<String, Object> metrics = new HashMap<>();
    MetricResults metricResults =
        asAttemptedOnlyMetricResults(
            MetricsAccumulator.getInstance().value());
    MetricQueryResults metricQueryResults =
        metricResults.queryMetrics(MetricsFilter.builder().build());
    for (MetricResult<Long> metricResult : metricQueryResults.getCounters()) {
      metrics.put(renderName(metricResult), metricResult.getAttempted());
    }
    for (MetricResult<DistributionResult> metricResult : metricQueryResults.getDistributions()) {
      DistributionResult result = metricResult.getAttempted();
      metrics.put(renderName(metricResult) + ".count", result.getCount());
      metrics.put(renderName(metricResult) + ".sum", result.getSum());
      metrics.put(renderName(metricResult) + ".min", result.getMin());
      metrics.put(renderName(metricResult) + ".max", result.getMax());
      metrics.put(renderName(metricResult) + ".mean", result.getMean());
    }
    for (MetricResult<GaugeResult> metricResult : metricQueryResults.getGauges()) {
      metrics.put(renderName(metricResult), metricResult.getAttempted().getValue());
    }
    return metrics;
  }

  @VisibleForTesting
  String renderName(MetricResult<?> metricResult) {
    String renderedStepName = metricResult.getStep().replaceAll(ILLEGAL_CHARACTERS_AND_PERIOD, "_");
    if (renderedStepName.endsWith("_")) {
      renderedStepName = renderedStepName.substring(0, renderedStepName.length() - 1);
    }
    MetricName metricName = metricResult.getName();
    return (renderedStepName + "." + metricName.getNamespace() + "." + metricName.getName())
        .replaceAll(ILLEGAL_CHARACTERS, "_");
  }
}
