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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.fn.harness.control.ExecutionStateSampler;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTracker;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTrackerStatus;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsEnvironment.MetricsEnvironmentState;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mockito;

/** Tests for {@link PTransformFunctionRegistry}. */
@RunWith(JUnit4.class)
public class PTransformFunctionRegistryTest {

  private ExecutionStateSampler sampler;

  @Before
  public void setUp() {
    sampler = new ExecutionStateSampler(PipelineOptionsFactory.create(), System::currentTimeMillis);
  }

  @After
  public void tearDown() {
    sampler.stop();
  }

  @Test
  public void testStateTrackerRecordsStateTransitions() throws Exception {
    ExecutionStateTracker executionStateTracker = sampler.create();
    PTransformFunctionRegistry testObject =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class),
            MetricsEnvironment::setCurrentContainer,
            new ShortIdMap(),
            executionStateTracker,
            Urns.START_BUNDLE_MSECS);

    final AtomicBoolean runnableAWasCalled = new AtomicBoolean();
    final AtomicBoolean runnableBWasCalled = new AtomicBoolean();
    ThrowingRunnable runnableA =
        new ThrowingRunnable() {
          @Override
          public void run() throws Exception {
            runnableAWasCalled.set(true);
            ExecutionStateTrackerStatus executionStateTrackerStatus =
                executionStateTracker.getStatus();
            assertNotNull(executionStateTrackerStatus);
            assertEquals(Thread.currentThread(), executionStateTrackerStatus.getTrackedThread());
            assertEquals("pTransformA", executionStateTrackerStatus.getPTransformId());
          }
        };
    ThrowingRunnable runnableB =
        new ThrowingRunnable() {
          @Override
          public void run() throws Exception {
            runnableBWasCalled.set(true);
            ExecutionStateTrackerStatus executionStateTrackerStatus =
                executionStateTracker.getStatus();
            assertNotNull(executionStateTrackerStatus);
            assertEquals(Thread.currentThread(), executionStateTrackerStatus.getTrackedThread());
            assertEquals("pTransformB", executionStateTrackerStatus.getPTransformId());
          }
        };
    testObject.register("pTransformA", "pTranformAName", runnableA);
    testObject.register("pTransformB", "pTranformBName", runnableB);

    executionStateTracker.start("testBundleId");
    for (ThrowingRunnable func : testObject.getFunctions()) {
      func.run();
    }
    executionStateTracker.reset();

    assertTrue(runnableAWasCalled.get());
    assertTrue(runnableBWasCalled.get());
  }

  @Test
  public void testMetricsUponRunningFunctions() throws Exception {
    MetricsEnvironmentState metricsEnvironmentState = mock(MetricsEnvironmentState.class);
    ExecutionStateTracker executionStateTracker = sampler.create();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PTransformFunctionRegistry testObject =
        new PTransformFunctionRegistry(
            metricsContainerRegistry,
            metricsEnvironmentState,
            new ShortIdMap(),
            executionStateTracker,
            Urns.START_BUNDLE_MSECS);

    ThrowingRunnable runnableA = mock(ThrowingRunnable.class);
    ThrowingRunnable runnableB = mock(ThrowingRunnable.class);
    testObject.register("pTransformA", "pTranformAName", runnableA);
    testObject.register("pTransformB", "pTranformBName", runnableB);

    // Test both cases; when there is an existing container and where there is no container
    MetricsContainer oldContainer = mock(MetricsContainer.class);
    when(metricsEnvironmentState.activate(metricsContainerRegistry.getContainer("pTransformA")))
        .thenReturn(oldContainer);
    when(metricsEnvironmentState.activate(metricsContainerRegistry.getContainer("pTransformB")))
        .thenReturn(null);

    executionStateTracker.start("testBundleId");
    for (ThrowingRunnable func : testObject.getFunctions()) {
      func.run();
    }
    executionStateTracker.reset();

    // Verify that metrics environment state is updated with pTransformA's container, then reset to
    // the oldContainer, then pTransformB's container and then reset to null.
    InOrder inOrder = Mockito.inOrder(metricsEnvironmentState);
    inOrder
        .verify(metricsEnvironmentState)
        .activate(metricsContainerRegistry.getContainer("pTransformA"));
    inOrder.verify(metricsEnvironmentState).activate(oldContainer);
    inOrder
        .verify(metricsEnvironmentState)
        .activate(metricsContainerRegistry.getContainer("pTransformB"));
    inOrder.verify(metricsEnvironmentState).activate(null);
    inOrder.verifyNoMoreInteractions();
  }
}
