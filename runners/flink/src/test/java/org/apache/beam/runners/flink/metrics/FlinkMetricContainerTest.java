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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
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
    MetricResult mock = Mockito.mock(MetricResult.class);
    when(mock.getStep()).thenReturn("step");
    MetricName metricName = MetricName.named("namespace", "name");
    when(mock.getName()).thenReturn(metricName);

    String name = FlinkMetricContainer.getFlinkMetricNameString(mock);
    assertThat(name, is("namespace.name"));
  }

  @Test
  public void testCounter() {
    SimpleCounter flinkCounter = new SimpleCounter();
    when(metricGroup.counter("namespace.name")).thenReturn(flinkCounter);

    FlinkMetricContainer container = new FlinkMetricContainer(runtimeContext);
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

    FlinkMetricContainer container = new FlinkMetricContainer(runtimeContext);
    MetricsContainer step = container.getMetricsContainer("step");
    MetricName metricName = MetricName.named("namespace", "name");
    Gauge gauge = step.getGauge(metricName);

    assertThat(flinkGauge.getValue(), is(GaugeResult.empty()));
    // first set will install the mocked gauge
    container.updateMetrics("step");
    gauge.set(1);
    gauge.set(42);
    container.updateMetrics("step");
    assertThat(flinkGauge.getValue().getValue(), is(42L));
  }

  @Test
  public void testDistribution() {
    FlinkMetricContainer.FlinkDistributionGauge flinkGauge =
        new FlinkMetricContainer.FlinkDistributionGauge(DistributionResult.IDENTITY_ELEMENT);
    when(metricGroup.gauge(eq("namespace.name"), anyObject())).thenReturn(flinkGauge);

    FlinkMetricContainer container = new FlinkMetricContainer(runtimeContext);
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
