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

import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.MetricName;
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
        MetricName.named("KafkaSink", "RpcLatency*rpc_method:POLL;topic_name:topic1;");
    assertThat(histogram.getName(), equalTo(histogramName));
  }
}
