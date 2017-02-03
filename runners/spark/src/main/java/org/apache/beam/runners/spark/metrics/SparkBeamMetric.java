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

import com.codahale.metrics.Metric;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;


/**
 * An adapter between the {@link SparkMetricsContainer} and Codahale's {@link Metric} interface.
 */
class SparkBeamMetric implements Metric {
  private static final String ILLEGAL_CHARACTERS = "[^A-Za-z0-9\\._-]";

  private final SparkMetricResults metricResults = new SparkMetricResults();

  Map<String, ?> renderAll() {
    Map<String, Object> metrics = new HashMap<>();
    MetricQueryResults metricQueryResults =
        metricResults.queryMetrics(MetricsFilter.builder().build());
    for (MetricResult<Long> metricResult : metricQueryResults.counters()) {
      metrics.put(renderName(metricResult), metricResult.attempted());
    }
    for (MetricResult<DistributionResult> metricResult : metricQueryResults.distributions()) {
      DistributionResult result = metricResult.attempted();
      metrics.put(renderName(metricResult) + ".count", result.count());
      metrics.put(renderName(metricResult) + ".sum", result.sum());
      metrics.put(renderName(metricResult) + ".min", result.min());
      metrics.put(renderName(metricResult) + ".max", result.max());
      metrics.put(renderName(metricResult) + ".mean", result.mean());
    }
    return metrics;
  }

  private String renderName(MetricResult<?> metricResult) {
    MetricName metricName = metricResult.name();
    String rendered = metricResult.step() + "." + metricName.namespace() + "." + metricName.name();
    return rendered.replaceAll(ILLEGAL_CHARACTERS, "_");
  }
}
