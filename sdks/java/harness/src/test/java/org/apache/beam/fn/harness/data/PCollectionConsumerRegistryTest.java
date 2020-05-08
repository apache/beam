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
package org.apache.beam.fn.harness.data;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import org.apache.beam.fn.harness.HandlesSplits;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Labels;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** Tests for {@link PCollectionConsumerRegistryTest}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MetricsEnvironment.class)
public class PCollectionConsumerRegistryTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void singleConsumer() throws Exception {
    final String pCollectionA = "pCollectionA";
    final String pTransformIdA = "pTransformIdA";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);

    consumers.register(pCollectionA, pTransformIdA, consumerA1);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(pCollectionA);

    WindowedValue<String> element = valueInGlobalWindow("elem");
    int numElements = 20;
    for (int i = 0; i < numElements; i++) {
      wrapperConsumer.accept(element);
    }

    // Check that the underlying consumers are each invoked per element.
    verify(consumerA1, times(numElements)).accept(element);
    assertThat(consumers.keySet(), contains(pCollectionA));

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, pCollectionA);
    builder.setInt64SumValue(numElements);
    MonitoringInfo expected = builder.build();

    // Clear the timestamp before comparison.
    MonitoringInfo result =
        Iterables.find(
            metricsContainerRegistry.getMonitoringInfos(),
            monitoringInfo -> monitoringInfo.containsLabels(Labels.PCOLLECTION));
    assertEquals(expected, result);
  }

  @Test
  public void singleConsumerException() throws Exception {
    final String pCollectionA = "pCollectionA";
    final String pTransformId = "pTransformId";
    final String message = "testException";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    FnDataReceiver<WindowedValue<String>> consumer = mock(FnDataReceiver.class);

    consumers.register(pCollectionA, pTransformId, consumer);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(pCollectionA);
    doThrow(new Exception(message)).when(consumer).accept(any());

    expectedException.expectMessage(message);
    expectedException.expect(Exception.class);
    wrapperConsumer.accept(valueInGlobalWindow("elem"));
  }

  /**
   * Test that the counter increments only once when multiple consumers of same pCollection read the
   * same element.
   */
  @Test
  public void multipleConsumersSamePCollection() throws Exception {
    final String pCollectionA = "pCollectionA";
    final String pTransformIdA = "pTransformIdA";
    final String pTransformIdB = "pTransformIdB";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

    consumers.register(pCollectionA, pTransformIdA, consumerA1);
    consumers.register(pCollectionA, pTransformIdB, consumerA2);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(pCollectionA);

    WindowedValue<String> element = valueInGlobalWindow("elem");
    int numElements = 20;
    for (int i = 0; i < numElements; i++) {
      wrapperConsumer.accept(element);
    }

    // Check that the underlying consumers are each invoked per element.
    verify(consumerA1, times(numElements)).accept(element);
    verify(consumerA2, times(numElements)).accept(element);
    assertThat(consumers.keySet(), contains(pCollectionA));

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, pCollectionA);
    builder.setInt64SumValue(numElements);
    MonitoringInfo expected = builder.build();

    // Clear the timestamp before comparison.
    MonitoringInfo result =
        Iterables.find(
            metricsContainerRegistry.getMonitoringInfos(),
            monitoringInfo -> monitoringInfo.containsLabels(Labels.PCOLLECTION));
    assertEquals(expected, result);
  }

  @Test
  public void multipleConsumersSamePCollectionException() throws Exception {
    final String pCollectionA = "pCollectionA";
    final String pTransformId = "pTransformId";
    final String message = "testException";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

    consumers.register(pCollectionA, pTransformId, consumerA1);
    consumers.register(pCollectionA, pTransformId, consumerA2);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(pCollectionA);
    doThrow(new Exception(message)).when(consumerA2).accept(any());

    expectedException.expectMessage(message);
    expectedException.expect(Exception.class);
    wrapperConsumer.accept(valueInGlobalWindow("elem"));
  }

  @Test
  public void throwsOnRegisteringAfterMultiplexingConsumerWasInitialized() throws Exception {
    final String pCollectionA = "pCollectionA";
    final String pTransformId = "pTransformId";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

    consumers.register(pCollectionA, pTransformId, consumerA1);
    consumers.getMultiplexingConsumer(pCollectionA);

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("cannot be register()-d after");
    consumers.register(pCollectionA, pTransformId, consumerA2);
  }

  @Test
  public void testScopedMetricContainerInvokedUponAcceptingElement() throws Exception {
    mockStatic(MetricsEnvironment.class);
    final String pCollectionA = "pCollectionA";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

    consumers.register("pCollectionA", "pTransformA", consumerA1);
    consumers.register("pCollectionA", "pTransformB", consumerA2);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(pCollectionA);

    WindowedValue<String> element = valueInGlobalWindow("elem");
    wrapperConsumer.accept(element);

    // Verify that static scopedMetricsContainer is called with pTransformA's container.
    PowerMockito.verifyStatic(MetricsEnvironment.class, times(1));
    MetricsEnvironment.scopedMetricsContainer(metricsContainerRegistry.getContainer("pTransformA"));

    // Verify that static scopedMetricsContainer is called with pTransformB's container.
    PowerMockito.verifyStatic(MetricsEnvironment.class, times(1));
    MetricsEnvironment.scopedMetricsContainer(metricsContainerRegistry.getContainer("pTransformB"));
  }

  @Test
  public void testScopedMetricContainerInvokedUponAccept() throws Exception {
    mockStatic(MetricsEnvironment.class, withSettings().verboseLogging());
    final String pCollectionA = "pCollectionA";
    final String pTransformIdA = "pTransformIdA";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    FnDataReceiver<WindowedValue<String>> consumer =
        mock(FnDataReceiver.class, withSettings().verboseLogging());

    consumers.register(pCollectionA, pTransformIdA, consumer);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(pCollectionA);

    WindowedValue<String> element = WindowedValue.valueInGlobalWindow("elem");
    wrapperConsumer.accept(element);

    verify(consumer, times(1)).accept(element);

    // Verify that static scopedMetricsContainer is called with unbound container.
    PowerMockito.verifyStatic(MetricsEnvironment.class, times(1));
    MetricsEnvironment.scopedMetricsContainer(metricsContainerRegistry.getUnboundContainer());
  }

  @Test
  public void testHandlesSplitsPassedToOriginalConsumer() throws Exception {
    final String pCollectionA = "pCollectionA";
    final String pTransformIdA = "pTransformIdA";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    SplittingReceiver consumerA1 = mock(SplittingReceiver.class);

    consumers.register(pCollectionA, pTransformIdA, consumerA1);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(pCollectionA);

    assertTrue(wrapperConsumer instanceof HandlesSplits);

    ((HandlesSplits) wrapperConsumer).getProgress();
    verify(consumerA1).getProgress();

    ((HandlesSplits) wrapperConsumer).trySplit(0.3);
    verify(consumerA1).trySplit(0.3);
  }

  private abstract static class SplittingReceiver<T> implements FnDataReceiver<T>, HandlesSplits {}
}
