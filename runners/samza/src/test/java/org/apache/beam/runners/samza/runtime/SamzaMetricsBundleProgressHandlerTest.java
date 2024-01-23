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
package org.apache.beam.runners.samza.runtime;

import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.DISTRIBUTION_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.LATEST_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.DistributionCell;
import org.apache.beam.runners.core.metrics.GaugeCell;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class SamzaMetricsBundleProgressHandlerTest {

  public static final String EXPECTED_NAMESPACE = "namespace";
  public static final String EXPECTED_COUNTER_NAME = "counterName";
  private MetricsRegistryMap metricsRegistryMap;
  private SamzaMetricsContainer samzaMetricsContainer;

  private SamzaMetricsBundleProgressHandler samzaMetricsBundleProgressHandler;
  private String stepName = "stepName";
  Map<String, String> transformIdToUniqueName = new HashMap<>();

  @Before
  public void setup() {
    metricsRegistryMap = new MetricsRegistryMap();
    samzaMetricsContainer = new SamzaMetricsContainer(metricsRegistryMap);
    samzaMetricsBundleProgressHandler =
        new SamzaMetricsBundleProgressHandler(
            stepName, samzaMetricsContainer, transformIdToUniqueName);
  }

  @Test
  public void testCounter() {
    // Hex for 123
    byte[] payload = "\173".getBytes(Charset.defaultCharset());

    MetricsApi.MonitoringInfo monitoringInfo =
        MetricsApi.MonitoringInfo.newBuilder()
            .setType(SUM_INT64_TYPE)
            .setPayload(ByteString.copyFrom(payload))
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, EXPECTED_NAMESPACE)
            .putLabels(MonitoringInfoConstants.Labels.NAME, EXPECTED_COUNTER_NAME)
            .build();
    BeamFnApi.ProcessBundleResponse response =
        BeamFnApi.ProcessBundleResponse.newBuilder().addMonitoringInfos(monitoringInfo).build();

    // Execute
    samzaMetricsBundleProgressHandler.onCompleted(response);

    // Verify
    MetricName metricName = MetricName.named(EXPECTED_NAMESPACE, EXPECTED_COUNTER_NAME);
    CounterCell counter =
        (CounterCell) samzaMetricsContainer.getContainer(stepName).getCounter(metricName);

    assertEquals(counter.getCumulative(), (Long) 123L);
  }

  @Test
  public void testGauge() {
    // TimeStamp = 0, Value = 123
    byte[] payload = "\000\173".getBytes(Charset.defaultCharset());

    MetricsApi.MonitoringInfo monitoringInfo =
        MetricsApi.MonitoringInfo.newBuilder()
            .setType(LATEST_INT64_TYPE)
            .setPayload(ByteString.copyFrom(payload))
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, EXPECTED_NAMESPACE)
            .putLabels(MonitoringInfoConstants.Labels.NAME, EXPECTED_COUNTER_NAME)
            .build();
    BeamFnApi.ProcessBundleResponse response =
        BeamFnApi.ProcessBundleResponse.newBuilder().addMonitoringInfos(monitoringInfo).build();

    // Execute
    samzaMetricsBundleProgressHandler.onCompleted(response);

    // Verify
    MetricName metricName = MetricName.named(EXPECTED_NAMESPACE, EXPECTED_COUNTER_NAME);
    GaugeCell gauge = (GaugeCell) samzaMetricsContainer.getContainer(stepName).getGauge(metricName);

    assertEquals(123L, gauge.getCumulative().value());
    assertTrue(
        gauge.getCumulative().timestamp().isBefore(Instant.now().plus(Duration.millis(500))));
    assertTrue(
        gauge.getCumulative().timestamp().isAfter(Instant.now().minus(Duration.millis(500))));
  }

  @Test
  public void testDistribution() {
    // Count = 123, sum = 124, min = 125, max = 126
    byte[] payload = "\173\174\175\176".getBytes(Charset.defaultCharset());

    MetricsApi.MonitoringInfo monitoringInfo =
        MetricsApi.MonitoringInfo.newBuilder()
            .setType(DISTRIBUTION_INT64_TYPE)
            .setPayload(ByteString.copyFrom(payload))
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, EXPECTED_NAMESPACE)
            .putLabels(MonitoringInfoConstants.Labels.NAME, EXPECTED_COUNTER_NAME)
            .build();
    BeamFnApi.ProcessBundleResponse response =
        BeamFnApi.ProcessBundleResponse.newBuilder().addMonitoringInfos(monitoringInfo).build();

    // Execute
    samzaMetricsBundleProgressHandler.onCompleted(response);

    // Verify
    MetricName metricName = MetricName.named(EXPECTED_NAMESPACE, EXPECTED_COUNTER_NAME);
    DistributionCell gauge =
        (DistributionCell) samzaMetricsContainer.getContainer(stepName).getDistribution(metricName);

    assertEquals(123L, gauge.getCumulative().count());
    assertEquals(124L, gauge.getCumulative().sum());
    assertEquals(125L, gauge.getCumulative().min());
    assertEquals(126L, gauge.getCumulative().max());
  }

  @Test
  public void testEmptyPayload() {

    byte[] emptyPayload = "".getBytes(Charset.defaultCharset());

    MetricsApi.MonitoringInfo emptyMonitoringInfo =
        MetricsApi.MonitoringInfo.newBuilder()
            .setType(SUM_INT64_TYPE)
            .setPayload(ByteString.copyFrom(emptyPayload))
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, EXPECTED_NAMESPACE)
            .putLabels(MonitoringInfoConstants.Labels.NAME, EXPECTED_COUNTER_NAME)
            .build();
    // Hex for 123
    byte[] payload = "\173".getBytes(Charset.defaultCharset());

    MetricsApi.MonitoringInfo monitoringInfo =
        MetricsApi.MonitoringInfo.newBuilder()
            .setType(SUM_INT64_TYPE)
            .setPayload(ByteString.copyFrom(payload))
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, EXPECTED_NAMESPACE)
            .putLabels(MonitoringInfoConstants.Labels.NAME, EXPECTED_COUNTER_NAME)
            .build();
    BeamFnApi.ProcessBundleResponse response =
        BeamFnApi.ProcessBundleResponse.newBuilder()
            .addMonitoringInfos(emptyMonitoringInfo)
            .addMonitoringInfos(monitoringInfo)
            .addMonitoringInfos(emptyMonitoringInfo)
            .build();

    // Execute
    samzaMetricsBundleProgressHandler.onCompleted(response);

    // Verify
    MetricName metricName = MetricName.named(EXPECTED_NAMESPACE, EXPECTED_COUNTER_NAME);
    CounterCell counter =
        (CounterCell) samzaMetricsContainer.getContainer(stepName).getCounter(metricName);

    assertEquals(counter.getCumulative(), (Long) 123L);
  }
}
