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
package org.apache.beam.runners.dataflow.worker.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.runners.dataflow.worker.StreamingStepMetricsContainer;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySinkMetrics;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.HistogramData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StageInfoTest {
  @Test
  public void testTranslateKnownPerWorkerCounters_validCounters() throws Exception {
    StageInfo stageInfo = StageInfo.create("user_name", "system_name");
    StreamingStepMetricsContainer metricsContainer =
        stageInfo.metricsContainerRegistry().getContainer("s1");
    MetricsEnvironment.setCurrentContainer(metricsContainer);
    StreamingStepMetricsContainer.setEnablePerWorkerMetrics(true);

    BigQuerySinkMetrics.throttledTimeCounter(BigQuerySinkMetrics.RpcMethod.APPEND_ROWS).inc(100);
    stageInfo.extractPerWorkerMetricValues();

    assertThat(stageInfo.throttledMsecs().getAggregate(), equalTo(100L));
  }

  /**
   * Test the scenario where there is a metric with a known metric name - {@code ThrottledTime} -
   * that is not a counter. {@code translateKnownPerWorkerCounters} should handle this gracefully.
   */
  @Test
  public void testTranslateKnownPerWorkerCounters_malformedCounters() throws Exception {
    StageInfo stageInfo = StageInfo.create("user_name", "system_name");
    StreamingStepMetricsContainer metricsContainer =
        stageInfo.metricsContainerRegistry().getContainer("s1");
    MetricsEnvironment.setCurrentContainer(metricsContainer);
    StreamingStepMetricsContainer.setEnablePerWorkerMetrics(true);

    MetricName name =
        BigQuerySinkMetrics.throttledTimeCounter(BigQuerySinkMetrics.RpcMethod.APPEND_ROWS)
            .getName();

    HistogramData.BucketType linearBuckets = HistogramData.LinearBuckets.of(0, 10.0, 10);
    metricsContainer.getPerWorkerHistogram(name, linearBuckets).update(10.0);

    stageInfo.extractPerWorkerMetricValues();
    assertThat(stageInfo.throttledMsecs().getAggregate(), equalTo(0L));
  }
}
