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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.Closeable;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMapEnvironment;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** Tests for {@link MetricsPCollectionConsumerRegistryTest}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MetricsEnvironment.class)
public class MetricsPCollectionConsumerRegistryTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void throwsOnRegisteringAfterMultiplexingConsumerWasInitialized() throws Exception {
    try (Closeable close = MetricsContainerStepMapEnvironment.setupMetricEnvironment()) {
      final String pCollectionA = "pCollectionA";
      final String pTransformId = "pTransformId";

      MetricsPCollectionConsumerRegistry consumers = new MetricsPCollectionConsumerRegistry();

      FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
      FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

      consumers.register(pCollectionA, pTransformId, consumerA1);
      consumers.getMultiplexingConsumer(pCollectionA);

      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("cannot be register()-d after");
      consumers.register(pCollectionA, pTransformId, consumerA2);
    }
  }

  @Test
  public void testScopedMetricContainerInvokedUponAcceptingElement() throws Exception {
    try (Closeable close = MetricsContainerStepMapEnvironment.setupMetricEnvironment()) {
      mockStatic(MetricsEnvironment.class);
      final String pCollectionA = "pCollectionA";

      MetricsContainerStepMap metricsContainerRegistry =
          MetricsContainerStepMapEnvironment.getCurrent();
      MetricsPCollectionConsumerRegistry consumers = new MetricsPCollectionConsumerRegistry();
      FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
      FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

      consumers.register("pCollectionA", "pTransformA", consumerA1);
      consumers.register("pCollectionA", "pTransformB", consumerA2);

      FnDataReceiver<WindowedValue<String>> wrapperConsumer =
          (FnDataReceiver<WindowedValue<String>>)
              (FnDataReceiver) consumers.getMultiplexingConsumer(pCollectionA);

      WindowedValue<String> element = WindowedValue.valueInGlobalWindow("elem");
      wrapperConsumer.accept(element);

      // Verify that static scopedMetricsContainer is called with pTransformA's container.
      PowerMockito.verifyStatic(times(1));
      MetricsEnvironment.scopedMetricsContainer(
          metricsContainerRegistry.getContainer("pTransformA"));

      // Verify that static scopedMetricsContainer is called with pTransformB's container.
      PowerMockito.verifyStatic(times(1));
      MetricsEnvironment.scopedMetricsContainer(
          metricsContainerRegistry.getContainer("pTransformB"));
    }
  }
}
