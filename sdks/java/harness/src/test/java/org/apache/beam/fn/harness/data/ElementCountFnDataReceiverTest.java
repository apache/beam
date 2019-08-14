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

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** Tests for {@link ElementCountFnDataReceiver}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MetricsEnvironment.class)
public class ElementCountFnDataReceiverTest {
  /**
   * Test that the elements are counted, and a MonitoringInfo can be extracted from a
   * metricsContainer, if it is in scope.
   *
   * @throws Exception
   */
  @Test
  public void testCountsElements() throws Exception {
    final String pCollectionA = "pCollectionA";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();

    FnDataReceiver<WindowedValue<String>> consumer = mock(FnDataReceiver.class);
    ElementCountFnDataReceiver<String> wrapperConsumer =
        new ElementCountFnDataReceiver(consumer, pCollectionA, metricsContainerRegistry);
    WindowedValue<String> element = WindowedValue.valueInGlobalWindow("elem");
    int numElements = 20;
    for (int i = 0; i < numElements; i++) {
      wrapperConsumer.accept(element);
    }
    verify(consumer, times(numElements)).accept(element);

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, pCollectionA);
    builder.setInt64Value(numElements);
    MonitoringInfo expected = builder.build();

    // Clear the timestamp before comparison.
    MonitoringInfo first = metricsContainerRegistry.getMonitoringInfos().iterator().next();
    MonitoringInfo result = SimpleMonitoringInfoBuilder.copyAndClearTimestamp(first);
    assertEquals(expected, result);
  }

  @Test
  public void testScopedMetricContainerInvokedUponAccept() throws Exception {
    mockStatic(MetricsEnvironment.class, withSettings().verboseLogging());
    final String pCollectionA = "pCollectionA";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();

    FnDataReceiver<WindowedValue<String>> consumer =
        mock(FnDataReceiver.class, withSettings().verboseLogging());
    ElementCountFnDataReceiver<String> wrapperConsumer =
        new ElementCountFnDataReceiver(consumer, pCollectionA, metricsContainerRegistry);
    WindowedValue<String> element = WindowedValue.valueInGlobalWindow("elem");
    wrapperConsumer.accept(element);

    verify(consumer, times(1)).accept(element);

    // Verify that static scopedMetricsContainer is called with unbound container.
    PowerMockito.verifyStatic(MetricsEnvironment.class, times(1));
    MetricsEnvironment.scopedMetricsContainer(metricsContainerRegistry.getUnboundContainer());
  }
}
