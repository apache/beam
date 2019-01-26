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

import java.io.Closeable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Test;

/** Tests for {@link ElementCountFnDataReceiver}. */
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

    MetricsContainerStepMap metricContainerRegistry = new MetricsContainerStepMap();

    FnDataReceiver<WindowedValue<String>> consumer = mock(FnDataReceiver.class);
    ElementCountFnDataReceiver<String> wrapperConsumer =
        new ElementCountFnDataReceiver(consumer, pCollectionA, metricContainerRegistry);
    WindowedValue<String> element = WindowedValue.valueInGlobalWindow("elem");
    int numElements = 20;
    for (int i = 0; i < numElements; i++) {
      wrapperConsumer.accept(element);
    }

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN);
    builder.setPCollectionLabel(pCollectionA);
    builder.setInt64Value(numElements);
    MonitoringInfo expected = builder.build();

    // Clear the timestamp before compairison.
    MonitoringInfo first = metricContainerRegistry.getMonitoringInfos().iterator().next();
    MonitoringInfo result = SimpleMonitoringInfoBuilder.clearTimestamp(first);
    assertEquals(expected, result);
  }
}
