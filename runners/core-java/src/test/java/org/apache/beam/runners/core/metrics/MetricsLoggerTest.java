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

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.HistogramData;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

public class MetricsLoggerTest {

  @Test
  public void testGeneratedLogMessageShowsDeltas() {
    MetricName cName =
        MonitoringInfoMetricName.named(
            MonitoringInfoConstants.Urns.ELEMENT_COUNT,
            Collections.singletonMap("name", "counter"));
    HistogramData.BucketType bucketType = HistogramData.LinearBuckets.of(0, 2, 5);
    MetricName hName =
        MonitoringInfoMetricName.named(
            MonitoringInfoConstants.Urns.ELEMENT_COUNT,
            Collections.singletonMap("name", "histogram"));

    MetricsLogger logger = new MetricsLogger(null);
    logger.getCounter(cName).inc(2L);
    // Set buckets counts to: [0,1,1,,0,0,...]
    logger.getHistogram(hName, bucketType).update(1);
    logger.getHistogram(hName, bucketType).update(3);

    Set<String> allowedMetricUrns = new HashSet<String>();
    allowedMetricUrns.add(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    String msg = logger.generateLogMessage("My Headder", allowedMetricUrns, 0);
    assertThat(msg, CoreMatchers.containsString("beam:metric:element_count:v1 {name=counter} = 2"));
    assertThat(
        msg,
        CoreMatchers.containsString(
            "{name=histogram} = {count: 2, p50: 2.000000, p90: 3.600000, p99: 3.960000}"));

    logger.getCounter(cName).inc(3L);
    // Set buckets counts to: [0,5,6,0,0,0]
    // Which means a delta of: [0,4,5,0,0,0]
    for (int i = 0; i < 4; i++) {
      logger.getHistogram(hName, bucketType).update(1);
    }
    for (int i = 0; i < 5; i++) {
      logger.getHistogram(hName, bucketType).update(3);
    }
    msg = logger.generateLogMessage("My Header: ", allowedMetricUrns, 0);
    assertThat(msg, CoreMatchers.containsString("beam:metric:element_count:v1 {name=counter} = 3"));
    assertThat(
        msg,
        CoreMatchers.containsString(
            "{name=histogram} = {count: 9, p50: 2.200000, p90: 3.640000, p99: 3.964000}"));

    logger.getCounter(cName).inc(4L);
    // Set buckets counts to: [0,8,10,0,0,0]
    // Which means a delta of: [0,3,4,0,0,0]
    for (int i = 0; i < 3; i++) {
      logger.getHistogram(hName, bucketType).update(1);
    }
    for (int i = 0; i < 4; i++) {
      logger.getHistogram(hName, bucketType).update(3);
    }
    msg = logger.generateLogMessage("My Header: ", allowedMetricUrns, 0);
    assertThat(
        msg,
        CoreMatchers.containsString(
            "{name=histogram} = {count: 7, p50: 2.250000, p90: 3.650000, p99: 3.965000}"));
  }
}
