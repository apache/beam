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

package org.apache.beam.runners.extensions.metrics;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Instant;
import org.junit.Test;

/** Test class for MetricsHttpSink. */
public class MetricsHttpSinkTest {

  @Test
  public void testSerializerWithCommittedSupported() throws Exception {
    MetricQueryResults metricQueryResults = new CustomMetricQueryResults(true);
    MetricsHttpSink metricsHttpSink = new MetricsHttpSink(PipelineOptionsFactory.create());
    String serializeMetrics = metricsHttpSink.serializeMetrics(metricQueryResults);
    assertEquals(
        "Error in serialization",
        "{\"counters\":[{\"attempted\":20,\"committed\":10,\"name\":{\"name\":\"n1\","
            + "\"namespace\":\"ns1\"},\"step\":\"s1\"}],\"distributions\":[{\"attempted\":"
            + "{\"count\":4,\"max\":9,\"mean\":6.25,\"min\":3,\"sum\":25},\"committed\":"
            + "{\"count\":2,\"max\":8,\"mean\":5.0,\"min\":5,\"sum\":10},\"name\":{\"name\":\"n2\","
            + "\"namespace\":\"ns1\"},\"step\":\"s2\"}],\"gauges\":[{\"attempted\":{\"timestamp\":"
            + "\"1970-01-05T00:04:22.800Z\",\"value\":120},\"committed\":{\"timestamp\":"
            + "\"1970-01-05T00:04:22.800Z\",\"value\":100},\"name\":{\"name\":\"n3\",\"namespace\":"
            + "\"ns1\"},\"step\":\"s3\"}]}",
        serializeMetrics);
  }

  @Test
  public void testSerializerWithCommittedUnSupported() throws Exception {
    MetricQueryResults metricQueryResults = new CustomMetricQueryResults(false);
    MetricsHttpSink metricsHttpSink = new MetricsHttpSink(PipelineOptionsFactory.create());
    String serializeMetrics = metricsHttpSink.serializeMetrics(metricQueryResults);
    assertEquals(
        "Error in serialization",
        "{\"counters\":[{\"attempted\":20,\"name\":{\"name\":\"n1\","
            + "\"namespace\":\"ns1\"},\"step\":\"s1\"}],\"distributions\":[{\"attempted\":"
            + "{\"count\":4,\"max\":9,\"mean\":6.25,\"min\":3,\"sum\":25},\"name\":{\"name\":\"n2\""
            + ",\"namespace\":\"ns1\"},\"step\":\"s2\"}],\"gauges\":[{\"attempted\":{\"timestamp\":"
            + "\"1970-01-05T00:04:22.800Z\",\"value\":120},\"name\":{\"name\":\"n3\",\"namespace\":"
            + "\"ns1\"},\"step\":\"s3\"}]}",
        serializeMetrics);
  }
}
