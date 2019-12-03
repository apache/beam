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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.metrics.MetricsSink;

/**
 * This sink just stores in a static field the first counter (if it exists) attempted value. This is
 * useful for tests.
 */
public class TestMetricsSink implements MetricsSink {

  private static MetricQueryResults metricQueryResults;
  private static final String SYSTEM_METRIC_PREFIX = "metric:";

  public TestMetricsSink(MetricsOptions pipelineOptions) {}

  public static long getCounterValue(String counterName) {
    for (MetricResult<Long> metricResult : metricQueryResults.getCounters()) {
      if (metricResult.getName().getName().equals(counterName)) {
        return metricResult.getAttempted();
      }
    }
    return 0L;
  }

  public static List<MetricResult<Long>> getSystemCounters() {
    List<MetricResult<Long>> counters =
        StreamSupport.stream(metricQueryResults.getCounters().spliterator(), false)
            .filter(result -> result.getKey().metricName().getName().contains(SYSTEM_METRIC_PREFIX))
            .collect(Collectors.toList());
    return counters;
  }

  public static void clear() {
    metricQueryResults = null;
  }

  @Override
  public void writeMetrics(MetricQueryResults metricQueryResult) throws Exception {
    metricQueryResults = metricQueryResult;
  }
}
