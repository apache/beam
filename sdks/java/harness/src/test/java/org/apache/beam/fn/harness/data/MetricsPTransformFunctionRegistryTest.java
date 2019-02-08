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
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** Tests for {@link MetricsPTransformFunctionRegistry}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MetricsEnvironment.class)
public class MetricsPTransformFunctionRegistryTest {

  @Test
  public void testScopedMetricContainerInvokedUponRunningFunctions() throws Exception {
    try (Closeable close = MetricsContainerStepMapEnvironment.setupMetricEnvironment()) {
      mockStatic(MetricsEnvironment.class);
      MetricsContainerStepMap metricsContainerRegistry =
          MetricsContainerStepMapEnvironment.getCurrent();
      MetricsPTransformFunctionRegistry testObject =
          new MetricsPTransformFunctionRegistry(SimpleMonitoringInfoBuilder.START_BUNDLE_MSECS_URN);

      ThrowingRunnable runnableA = mock(ThrowingRunnable.class);
      ThrowingRunnable runnableB = mock(ThrowingRunnable.class);
      testObject.register("pTransformA", runnableA);
      testObject.register("pTransformB", runnableB);

      for (ThrowingRunnable func : testObject.getFunctions()) {
        func.run();
      }

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
