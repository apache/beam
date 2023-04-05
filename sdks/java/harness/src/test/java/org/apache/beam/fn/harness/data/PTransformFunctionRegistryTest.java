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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.fn.harness.control.ExecutionStateSampler;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTracker;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTrackerStatus;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PTransformFunctionRegistry}. */
@RunWith(JUnit4.class)
public class PTransformFunctionRegistryTest {
  private static final Counter TEST_USER_COUNTER = Metrics.counter("foo", "bar");

  private ExecutionStateSampler sampler;

  @Before
  public void setUp() {
    sampler = new ExecutionStateSampler(PipelineOptionsFactory.create(), System::currentTimeMillis);
  }

  @After
  public void tearDown() {
    MetricsEnvironment.setCurrentContainer(null);
    sampler.stop();
  }

  @Test
  public void testStateTrackerRecordsStateTransitions() throws Exception {
    ExecutionStateTracker executionStateTracker = sampler.create();
    MetricsEnvironment.setCurrentContainer(executionStateTracker.getMetricsContainer());
    PTransformFunctionRegistry testObject =
        new PTransformFunctionRegistry(
            new ShortIdMap(), executionStateTracker, Urns.START_BUNDLE_MSECS);

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
    ExecutionStateTracker executionStateTracker = sampler.create();
    MetricsEnvironment.setCurrentContainer(executionStateTracker.getMetricsContainer());
    PTransformFunctionRegistry testObject =
        new PTransformFunctionRegistry(
            new ShortIdMap(), executionStateTracker, Urns.START_BUNDLE_MSECS);

    testObject.register("pTransformA", "pTranformAName", () -> TEST_USER_COUNTER.inc());
    testObject.register("pTransformB", "pTranformBName", () -> TEST_USER_COUNTER.inc(2));

    // Test both cases; when there is an existing container and where there is no container
    executionStateTracker.start("testBundleId");
    for (ThrowingRunnable func : testObject.getFunctions()) {
      func.run();
    }
    TEST_USER_COUNTER.inc(3);

    // Verify that metrics environment state is updated with pTransform's counters including the
    // unbound container when outside the scope of the function
    assertEquals(
        1L,
        (long)
            executionStateTracker
                .getMetricsContainerRegistry()
                .getContainer("pTransformA")
                .getCounter(TEST_USER_COUNTER.getName())
                .getCumulative());
    assertEquals(
        2L,
        (long)
            executionStateTracker
                .getMetricsContainerRegistry()
                .getContainer("pTransformB")
                .getCounter(TEST_USER_COUNTER.getName())
                .getCumulative());
    assertEquals(
        3L,
        (long)
            executionStateTracker
                .getMetricsContainerRegistry()
                .getUnboundContainer()
                .getCounter(TEST_USER_COUNTER.getName())
                .getCumulative());

    executionStateTracker.reset();
  }
}
