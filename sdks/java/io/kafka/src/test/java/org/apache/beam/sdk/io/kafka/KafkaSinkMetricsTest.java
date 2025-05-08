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
package org.apache.beam.sdk.io.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link KafkaSinkMetrics}. */
// TODO:Naireen - Refactor to remove duplicate code between the Kafka and BigQuery sinks
@RunWith(JUnit4.class)
public class KafkaSinkMetricsTest {
  @Test
  public void testCreatingHistogram() throws Exception {
    Histogram histogram =
        KafkaSinkMetrics.createRPCLatencyHistogram(KafkaSinkMetrics.RpcMethod.POLL, "topic1");

    MetricName histogramName =
        MetricName.named(
            "KafkaSink",
            "RpcLatency*rpc_method:POLL;topic_name:topic1;",
            ImmutableMap.of("PER_WORKER_METRIC", "true"));
    assertThat(histogram.getName(), equalTo(histogramName));
    assertTrue(
        histogram
            .getName()
            .getLabels()
            .containsKey(MonitoringInfoConstants.Labels.PER_WORKER_METRIC));
  }

  @Test
  public void testCreatingBacklogGauge() throws Exception {
    Gauge gauge =
        KafkaSinkMetrics.createBacklogGauge(
            KafkaSinkMetrics.getMetricGaugeName("topic", /*partitionId*/ 0));

    MetricName gaugeName =
        MetricName.named(
            "KafkaSink",
            "EstimatedBacklogSize*partition_id:0;topic_name:topic;",
            ImmutableMap.of("PER_WORKER_METRIC", "true"));

    assertThat(gauge.getName(), equalTo(gaugeName));
    assertTrue(
        gauge.getName().getLabels().containsKey(MonitoringInfoConstants.Labels.PER_WORKER_METRIC));
  }
}
