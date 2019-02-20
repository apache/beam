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
package org.apache.beam.runners.flink.metrics;

import static org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN;
import static org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder.PTRANSFORM_LABEL;
import static org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder.USER_METRIC_URN_PREFIX;
import static org.apache.beam.runners.flink.metrics.FlinkMetricContainer.getFlinkMetricNameString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.IntDistributionData;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Metric;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.DistributionCell;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer.FlinkDistributionGauge;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link FlinkMetricContainer}. */
public class FlinkMetricContainerTest {

  @Mock private RuntimeContext runtimeContext;
  @Mock private MetricGroup metricGroup;

  @Before
  public void beforeTest() {
    MockitoAnnotations.initMocks(this);
    when(runtimeContext.<MetricsContainerStepMap, MetricsContainerStepMap>getAccumulator(
            anyString()))
        .thenReturn(new MetricsAccumulator());
    when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
  }

  @Test
  public void testMetricNameGeneration() {
    MetricKey key = MetricKey.create("step", MetricName.named("namespace", "name"));
    String name = getFlinkMetricNameString(key);
    assertThat(name, is("step.namespace.name"));
  }

  @Test
  public void testCounter() {
    SimpleCounter flinkCounter = new SimpleCounter();
    when(metricGroup.counter("step.namespace.name")).thenReturn(flinkCounter);

    FlinkMetricContainer container = new FlinkMetricContainer(runtimeContext);
    MetricsContainer step = container.getMetricsContainer("step");
    MetricName metricName = MetricName.named("namespace", "name");
    Counter counter = step.getCounter(metricName);
    counter.inc();
    counter.inc();

    assertThat(flinkCounter.getCount(), is(0L));
    container.updateMetrics();
    assertThat(flinkCounter.getCount(), is(2L));
  }

  @Test
  public void testGauge() {
    FlinkMetricContainer.FlinkGauge flinkGauge =
        new FlinkMetricContainer.FlinkGauge(GaugeResult.empty());
    when(metricGroup.gauge(eq("step.namespace.name"), anyObject())).thenReturn(flinkGauge);

    FlinkMetricContainer container = new FlinkMetricContainer(runtimeContext);
    MetricsContainer step = container.getMetricsContainer("step");
    MetricName metricName = MetricName.named("namespace", "name");
    Gauge gauge = step.getGauge(metricName);

    assertThat(flinkGauge.getValue(), is(GaugeResult.empty()));
    // first set will install the mocked gauge
    container.updateMetrics();
    gauge.set(1);
    gauge.set(42);
    container.updateMetrics();
    assertThat(flinkGauge.getValue().getValue(), is(42L));
  }

  @Test
  public void testMonitoringInfoUpdate() {
    FlinkMetricContainer container = new FlinkMetricContainer(runtimeContext);
    MetricsContainer step = container.getMetricsContainer("step");

    SimpleCounter userCounter = new SimpleCounter();
    when(metricGroup.counter("step.ns1.metric1")).thenReturn(userCounter);

    SimpleCounter elemCounter = new SimpleCounter();
    when(metricGroup.counter("step.pcoll.beam.metric.element_count.v1")).thenReturn(elemCounter);

    SimpleMonitoringInfoBuilder userCountBuilder = new SimpleMonitoringInfoBuilder();
    userCountBuilder.setUrnForUserMetric("ns1", "metric1");
    userCountBuilder.setInt64Value(111);
    userCountBuilder.setPTransformLabel("step");
    MonitoringInfo userCountMonitoringInfo = userCountBuilder.build();
    assertNotNull(userCountMonitoringInfo);

    SimpleMonitoringInfoBuilder elemCountBuilder = new SimpleMonitoringInfoBuilder();
    elemCountBuilder.setUrn(ELEMENT_COUNT_URN);
    elemCountBuilder.setInt64Value(222);
    elemCountBuilder.setPTransformLabel("step");
    elemCountBuilder.setPCollectionLabel("pcoll");
    MonitoringInfo elemCountMonitoringInfo = elemCountBuilder.build();
    assertNotNull(elemCountMonitoringInfo);

    assertThat(userCounter.getCount(), is(0L));
    assertThat(elemCounter.getCount(), is(0L));
    container.updateMetrics(ImmutableList.of(userCountMonitoringInfo, elemCountMonitoringInfo));
    assertThat(userCounter.getCount(), is(111L));
    assertThat(elemCounter.getCount(), is(222L));
  }

  @Test
  public void testSupportMultipleMonitoringInfoTypes() {
    FlinkMetricContainer flinkContainer = new FlinkMetricContainer(runtimeContext);
    MetricsContainer container = flinkContainer.getMetricsContainer("step");

    MonitoringInfo counter1 =
        MonitoringInfo.newBuilder()
            .setUrn(USER_METRIC_URN_PREFIX + "ns1:counter1")
            .putLabels(PTRANSFORM_LABEL, "step")
            .setMetric(Metric.newBuilder().setCounter(111))
            .build();

    MonitoringInfo counter2 =
        MonitoringInfo.newBuilder()
            .setUrn(USER_METRIC_URN_PREFIX + "ns2:counter2")
            .putLabels(PTRANSFORM_LABEL, "step")
            .setMetric(Metric.newBuilder().setCounter(222))
            .build();

    MonitoringInfo distribution =
        MonitoringInfo.newBuilder()
            .setUrn(USER_METRIC_URN_PREFIX + "ns3:distribution")
            .putLabels(PTRANSFORM_LABEL, "step")
            .setMetric(
                Metric.newBuilder()
                    .setDistribution(
                        IntDistributionData.newBuilder()
                            .setSum(30)
                            .setCount(10)
                            .setMin(1)
                            .setMax(5)))
            .build();

    // Mock out the counters that Flink returns; the distribution gets created by
    // FlinkMetricContainer, not by Flink itself, so we verify it in a different way below

    SimpleCounter flinkCounter1 = new SimpleCounter();
    SimpleCounter flinkCounter2 = new SimpleCounter();

    DistributionData distributionData = DistributionData.create(30, 10, 1, 5);
    DistributionResult distributionResult = distributionData.extractResult();
    FlinkDistributionGauge distributionGauge = new FlinkDistributionGauge(distributionResult);

    when(metricGroup.counter("step.ns1.counter1")).thenReturn(flinkCounter1);
    when(metricGroup.counter("step.ns2.counter2")).thenReturn(flinkCounter2);
    when(metricGroup.gauge(
            eq("step.ns3.distribution"),
            argThat(
                new ArgumentMatcher<FlinkDistributionGauge>() {
                  @Override
                  public boolean matches(Object argument) {
                    DistributionResult actual = ((FlinkDistributionGauge) argument).getValue();
                    return actual.equals(distributionResult);
                  }
                })))
        .thenReturn(distributionGauge);

    flinkContainer.updateMetrics(ImmutableList.of(counter1, counter2, distribution));

    verify(metricGroup).counter(eq("step.ns1.counter1"));
    verify(metricGroup).counter(eq("step.ns2.counter2"));

    // Verify that the counters injected into flink has the right value
    assertThat(flinkCounter1.getCount(), is(111L));
    assertThat(flinkCounter2.getCount(), is(222L));

    // Verify the counter in the java SDK MetricsContainer
    long count1 =
        ((CounterCell) container.getCounter(MetricName.named("ns1", "counter1"))).getCumulative();
    assertThat(count1, is(111L));

    long count2 =
        ((CounterCell) container.getCounter(MetricName.named("ns2", "counter2"))).getCumulative();
    assertThat(count2, is(222L));

    // Verify that the Java SDK MetricsContainer holds the same information
    DistributionData actualDistributionData =
        ((DistributionCell) container.getDistribution(MetricName.named("ns3", "distribution")))
            .getCumulative();
    assertThat(actualDistributionData, is(distributionData));
  }

  @Test
  public void testDistribution() {
    FlinkMetricContainer.FlinkDistributionGauge flinkGauge =
        new FlinkMetricContainer.FlinkDistributionGauge(DistributionResult.IDENTITY_ELEMENT);
    when(metricGroup.gauge(eq("step.namespace.name"), anyObject())).thenReturn(flinkGauge);

    FlinkMetricContainer container = new FlinkMetricContainer(runtimeContext);
    MetricsContainer step = container.getMetricsContainer("step");
    MetricName metricName = MetricName.named("namespace", "name");
    Distribution distribution = step.getDistribution(metricName);

    assertThat(flinkGauge.getValue(), is(DistributionResult.IDENTITY_ELEMENT));
    // first set will install the mocked distribution
    container.updateMetrics();
    distribution.update(42);
    distribution.update(-23);
    distribution.update(0);
    distribution.update(1);
    container.updateMetrics();
    assertThat(flinkGauge.getValue().getMax(), is(42L));
    assertThat(flinkGauge.getValue().getMin(), is(-23L));
    assertThat(flinkGauge.getValue().getCount(), is(4L));
    assertThat(flinkGauge.getValue().getSum(), is(20L));
    assertThat(flinkGauge.getValue().getMean(), is(5.0));
  }
}
