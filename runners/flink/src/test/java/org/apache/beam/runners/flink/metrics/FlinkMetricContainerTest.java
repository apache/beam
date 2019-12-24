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

import static org.apache.beam.runners.flink.metrics.FlinkMetricContainer.getFlinkMetricNameString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.model.pipeline.v1.MetricsApi.CounterData;
import org.apache.beam.model.pipeline.v1.MetricsApi.DoubleDistributionData;
import org.apache.beam.model.pipeline.v1.MetricsApi.IntDistributionData;
import org.apache.beam.model.pipeline.v1.MetricsApi.Metric;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.DistributionCell;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link FlinkMetricContainer}. */
@RunWith(Parameterized.class)
public class FlinkMetricContainerTest {

  @Parameter public boolean useMetricAccumulator;

  @Mock private RuntimeContext runtimeContext;
  @Mock private MetricGroup metricGroup;

  FlinkMetricContainer container;

  @Parameters(name = "useMetricAccumulator: {0}")
  public static Object[] data() {
    return new Object[] {true, false};
  }

  @Before
  public void beforeTest() {
    MockitoAnnotations.initMocks(this);
    when(runtimeContext.<MetricsContainerStepMap, MetricsContainerStepMap>getAccumulator(
            anyString()))
        .thenReturn(new MetricsAccumulator());
    when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
    container = new FlinkMetricContainer(runtimeContext, !useMetricAccumulator);
  }

  @Test
  public void testMetricNameGeneration() {
    MetricKey key = MetricKey.create("step", MetricName.named("namespace", "name"));
    String name = getFlinkMetricNameString(key);
    assertThat(name, is("namespace.name"));
  }

  @Test
  public void testCounter() {
    SimpleCounter flinkCounter = new SimpleCounter();
    when(metricGroup.counter("namespace.name")).thenReturn(flinkCounter);

    MetricsContainer step = container.getMetricsContainer("step");
    MetricName metricName = MetricName.named("namespace", "name");
    Counter counter = step.getCounter(metricName);
    counter.inc();
    counter.inc();

    assertThat(flinkCounter.getCount(), is(0L));
    container.updateMetrics("step");
    assertThat(flinkCounter.getCount(), is(2L));
  }

  @Test
  public void testGauge() {
    FlinkMetricContainer.FlinkGauge flinkGauge =
        new FlinkMetricContainer.FlinkGauge(GaugeResult.empty());
    when(metricGroup.gauge(eq("namespace.name"), anyObject())).thenReturn(flinkGauge);

    MetricsContainer step = container.getMetricsContainer("step");
    MetricName metricName = MetricName.named("namespace", "name");
    Gauge gauge = step.getGauge(metricName);

    assertThat(flinkGauge.getValue(), is(-1L));
    // first set will install the mocked gauge
    container.updateMetrics("step");
    gauge.set(1);
    gauge.set(42);
    container.updateMetrics("step");
    assertThat(flinkGauge.getValue(), is(42L));
  }

  @Test
  public void testMonitoringInfoUpdate() {
    SimpleCounter userCounter = new SimpleCounter();
    when(metricGroup.counter("ns1.metric1")).thenReturn(userCounter);

    SimpleCounter pCollectionCounter = new SimpleCounter();
    when(metricGroup.counter("pcoll.metric:element_count:v1")).thenReturn(pCollectionCounter);

    SimpleCounter pTransformCounter = new SimpleCounter();
    when(metricGroup.counter("anyPTransform.myMetric")).thenReturn(pTransformCounter);

    MonitoringInfo userCountMonitoringInfo =
        new SimpleMonitoringInfoBuilder()
            .setUrn(MonitoringInfoConstants.Urns.USER_COUNTER)
            .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "ns1")
            .setLabel(MonitoringInfoConstants.Labels.NAME, "metric1")
            .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "anyPTransform")
            .setInt64Value(111)
            .build();
    assertNotNull(userCountMonitoringInfo);

    MonitoringInfo pCollectionScoped =
        new SimpleMonitoringInfoBuilder()
            .setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT)
            .setInt64Value(222)
            .setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, "pcoll")
            .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "anyPTransform")
            .build();
    assertNotNull(pCollectionScoped);

    MonitoringInfo transformScoped =
        new SimpleMonitoringInfoBuilder()
            .setUrn(MonitoringInfoConstants.Urns.START_BUNDLE_MSECS)
            .setInt64Value(333)
            .setLabel(MonitoringInfoConstants.Labels.NAME, "myMetric")
            .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "anyPTransform")
            .build();
    assertNotNull(transformScoped);

    assertThat(userCounter.getCount(), is(0L));
    assertThat(pCollectionCounter.getCount(), is(0L));
    assertThat(pTransformCounter.getCount(), is(0L));
    container.updateMetrics(
        "step", ImmutableList.of(userCountMonitoringInfo, pCollectionScoped, transformScoped));
    assertThat(userCounter.getCount(), is(111L));
    assertThat(pCollectionCounter.getCount(), is(222L));
    assertThat(pTransformCounter.getCount(), is(333L));
  }

  @Test
  public void testDropUnexpectedMonitoringInfoTypes() {
    MetricsContainerImpl step = container.getMetricsContainer("step");

    MonitoringInfo intCounter =
        MonitoringInfo.newBuilder()
            .setUrn(MonitoringInfoConstants.Urns.USER_COUNTER)
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, "ns1")
            .putLabels(MonitoringInfoConstants.Labels.NAME, "int_counter")
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "step")
            .setMetric(
                Metric.newBuilder().setCounterData(CounterData.newBuilder().setInt64Value(111)))
            .build();

    MonitoringInfo doubleCounter =
        MonitoringInfo.newBuilder()
            .setUrn(MonitoringInfoConstants.Urns.USER_COUNTER)
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, "ns2")
            .putLabels(MonitoringInfoConstants.Labels.NAME, "double_counter")
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "step")
            .setMetric(
                Metric.newBuilder().setCounterData(CounterData.newBuilder().setDoubleValue(222)))
            .build();

    MonitoringInfo intDistribution =
        MonitoringInfo.newBuilder()
            .setUrn(MonitoringInfoConstants.Urns.USER_COUNTER)
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, "ns3")
            .putLabels(MonitoringInfoConstants.Labels.NAME, "int_distribution")
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "step")
            .setMetric(
                Metric.newBuilder()
                    .setDistributionData(
                        MetricsApi.DistributionData.newBuilder()
                            .setIntDistributionData(
                                IntDistributionData.newBuilder()
                                    .setSum(30)
                                    .setCount(10)
                                    .setMin(1)
                                    .setMax(5))))
            .build();

    MonitoringInfo doubleDistribution =
        MonitoringInfo.newBuilder()
            .setUrn(MonitoringInfoConstants.Urns.USER_COUNTER)
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, "ns4")
            .putLabels(MonitoringInfoConstants.Labels.NAME, "double_distribution")
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "step")
            .setMetric(
                Metric.newBuilder()
                    .setDistributionData(
                        MetricsApi.DistributionData.newBuilder()
                            .setDoubleDistributionData(
                                DoubleDistributionData.newBuilder()
                                    .setSum(30)
                                    .setCount(10)
                                    .setMin(1)
                                    .setMax(5))))
            .build();

    // Mock out the counter that Flink returns; the distribution gets created by
    // FlinkMetricContainer, not by Flink itself, so we verify it in a different way below
    SimpleCounter counter = new SimpleCounter();
    when(metricGroup.counter("ns1.int_counter")).thenReturn(counter);

    container.updateMetrics(
        "step", ImmutableList.of(intCounter, doubleCounter, intDistribution, doubleDistribution));

    // Flink's MetricGroup should only have asked for one counter (the integer-typed one) to be
    // created (the double-typed one is dropped currently)
    verify(metricGroup).counter(eq("ns1.int_counter"));

    // Verify that the counter injected into flink has the right value
    assertThat(counter.getCount(), is(111L));

    // Verify the counter in the java SDK MetricsContainer
    long count =
        ((CounterCell) step.tryGetCounter(MonitoringInfoMetricName.of(intCounter))).getCumulative();
    assertThat(count, is(111L));

    // The one Flink distribution that gets created is a FlinkDistributionGauge; here we verify its
    // initial (and in this test, final) value
    verify(metricGroup)
        .gauge(
            eq("ns3.int_distribution"),
            argThat(
                new ArgumentMatcher<FlinkDistributionGauge>() {
                  @Override
                  public boolean matches(FlinkDistributionGauge argument) {
                    DistributionResult actual = ((FlinkDistributionGauge) argument).getValue();
                    DistributionResult expected = DistributionResult.create(30, 10, 1, 5);
                    return actual.equals(expected);
                  }
                }));

    // Verify that the Java SDK MetricsContainer holds the same information
    DistributionData distributionData =
        ((DistributionCell) step.getDistribution(MonitoringInfoMetricName.of(intDistribution)))
            .getCumulative();
    assertThat(distributionData, is(DistributionData.create(30, 10, 1, 5)));
  }

  @Test
  public void testDistribution() {
    FlinkMetricContainer.FlinkDistributionGauge flinkGauge =
        new FlinkMetricContainer.FlinkDistributionGauge(DistributionResult.IDENTITY_ELEMENT);
    when(metricGroup.gauge(eq("namespace.name"), anyObject())).thenReturn(flinkGauge);

    MetricsContainer step = container.getMetricsContainer("step");
    MetricName metricName = MetricName.named("namespace", "name");
    Distribution distribution = step.getDistribution(metricName);

    assertThat(flinkGauge.getValue(), is(DistributionResult.IDENTITY_ELEMENT));
    // first set will install the mocked distribution
    container.updateMetrics("step");
    distribution.update(42);
    distribution.update(-23);
    distribution.update(0);
    distribution.update(1);
    container.updateMetrics("step");
    assertThat(flinkGauge.getValue().getMax(), is(42L));
    assertThat(flinkGauge.getValue().getMin(), is(-23L));
    assertThat(flinkGauge.getValue().getCount(), is(4L));
    assertThat(flinkGauge.getValue().getSum(), is(20L));
    assertThat(flinkGauge.getValue().getMean(), is(5.0));
  }
}
