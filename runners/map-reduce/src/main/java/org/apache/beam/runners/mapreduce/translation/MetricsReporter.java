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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.beam.runners.core.construction.metrics.MetricKey;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Class that holds a {@link MetricsContainerStepMap}, and reports metrics to MapReduce framework.
 */
public class MetricsReporter {

  private static final String METRIC_KEY_SEPARATOR = "__";
  private static final String METRIC_PREFIX = "__metrics";

  private final TaskAttemptContext context;
  private final MetricsContainerStepMap metricsContainers;
  private final Map<String, Long> reportedCounters = Maps.newHashMap();

  MetricsReporter(TaskAttemptContext context) {
    this.context = checkNotNull(context, "context");
    this.metricsContainers = new MetricsContainerStepMap();
  }

  public MetricsContainer getMetricsContainer(String stepName) {
    return metricsContainers.getContainer(stepName);
  }

  public void updateMetrics() {
    MetricResults metricResults = asAttemptedOnlyMetricResults(metricsContainers);
    MetricQueryResults metricQueryResults =
        metricResults.queryMetrics(MetricsFilter.builder().build());
    updateCounters(metricQueryResults.counters());
  }

  private void updateCounters(Iterable<MetricResult<Long>> counters) {
    for (MetricResult<Long> metricResult : counters) {
      String reportedCounterKey = reportedCounterKey(metricResult);
      Long updateValue = metricResult.attempted();
      Long oldValue = reportedCounters.get(reportedCounterKey);

      if (oldValue == null || oldValue < updateValue) {
        Long incValue = (oldValue == null ? updateValue : updateValue - oldValue);
        context.getCounter(groupName(metricResult), metricResult.name().name())
            .increment(incValue);
        reportedCounters.put(reportedCounterKey, updateValue);
      }
    }
  }

  private String groupName(MetricResult<?> metricResult) {
    return METRIC_PREFIX
        + METRIC_KEY_SEPARATOR + metricResult.step()
        + METRIC_KEY_SEPARATOR + metricResult.name().namespace();
  }

  private String reportedCounterKey(MetricResult<?> metricResult) {
    return metricResult.step()
        + METRIC_KEY_SEPARATOR + metricResult.name().namespace()
        + METRIC_KEY_SEPARATOR + metricResult.name().name();
  }

  public static MetricKey toMetricKey(String groupName, String counterName) {
    String[] nameSplits = groupName.split(METRIC_KEY_SEPARATOR);
    int length = nameSplits.length;
    String stepName = length > 1 ? nameSplits[length - 2] : "";
    String namespace = length > 0 ? nameSplits[length - 1] : "";
    return MetricKey.create(stepName, MetricName.named(namespace, counterName));
  }
}
