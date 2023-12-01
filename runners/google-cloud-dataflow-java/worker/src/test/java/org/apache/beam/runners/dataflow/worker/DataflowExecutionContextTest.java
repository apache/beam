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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.core.metrics.ExecutionStateTracker.PROCESS_STATE_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.auto.service.AutoService;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.Map;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext.StreamingModeExecutionState;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.NoopProfileScope;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ContextActivationObserverRegistry. */
@RunWith(JUnit4.class)
public class DataflowExecutionContextTest {

  /** This type is used for testing the automatic registration mechanism. */
  private static class AutoRegistrationClass implements ContextActivationObserver {
    private static boolean WAS_CALLED = false;

    @Override
    public void close() throws IOException {
      AutoRegistrationClass.WAS_CALLED = true;
    }

    @Override
    public Closeable activate(ExecutionStateTracker e) {
      return this;
    }
  }

  /** This type is used for testing the automatic registration mechanism. */
  private static class AutoRegistrationClassNotActive implements ContextActivationObserver {
    private static boolean WAS_CALLED = false;

    @Override
    public void close() throws IOException {
      AutoRegistrationClassNotActive.WAS_CALLED = true;
    }

    @Override
    public Closeable activate(ExecutionStateTracker e) {
      return this;
    }
  }

  /**
   * A {@link ContextActivationObserver.Registrar} to demonstrate default {@link
   * ContextActivationObserver} registration.
   */
  @AutoService(ContextActivationObserver.Registrar.class)
  public static class RegisteredTestContextActivationObserverRegistrar
      implements ContextActivationObserver.Registrar {
    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public ContextActivationObserver getContextActivationObserver() {
      return new AutoRegistrationClass();
    }
  }

  /**
   * A {@link ContextActivationObserver.Registrar} to demonstrate disabling {@link
   * ContextActivationObserver} registration.
   */
  @AutoService(ContextActivationObserver.Registrar.class)
  public static class DisabledContextActivationObserverRegistrar
      implements ContextActivationObserver.Registrar {
    @Override
    public boolean isEnabled() {
      return false;
    }

    @Override
    public ContextActivationObserver getContextActivationObserver() {
      return new AutoRegistrationClassNotActive();
    }
  }

  @Test
  public void testContextActivationObserverActivation() throws Exception {
    BatchModeExecutionContext executionContext =
        BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "testStage");
    Closeable c = executionContext.getExecutionStateTracker().activate();
    c.close();
    // AutoRegistrationClass's variable was modified to 'true'.
    assertTrue(AutoRegistrationClass.WAS_CALLED);

    // AutoRegistrationClassNotActive class is not registered as registrar for the same is disabled.
    assertFalse(AutoRegistrationClassNotActive.WAS_CALLED);
  }

  @Test
  public void testDataflowExecutionStateTrackerRecordsActiveMessageMetadata() throws IOException {
    DataflowExecutionContext.DataflowExecutionStateTracker tracker =
        new DataflowExecutionContext.DataflowExecutionStateTracker(
            ExecutionStateSampler.instance(), null, null, PipelineOptionsFactory.create(), "");
    StreamingModeExecutionState state =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(),
            PROCESS_STATE_NAME,
            null,
            NoopProfileScope.NOOP,
            null);

    Closeable closure = tracker.enterState(state);

    // After entering a process state, we should have an active message tracked.
    ActiveMessageMetadata expectedMetadata =
        new ActiveMessageMetadata(NameContextsForTests.nameContextForTest().userName(), 1l);
    Assert.assertEquals(
        expectedMetadata.userStepName, tracker.getActiveMessageMetadata().userStepName);

    closure.close();

    // Once the state closes, the active message should get cleared.
    Assert.assertEquals(null, tracker.getActiveMessageMetadata());
  }

  @Test
  public void testDataflowExecutionStateTrackerRecordsCompletedProcessingTimes()
      throws IOException {
    DataflowExecutionContext.DataflowExecutionStateTracker tracker =
        new DataflowExecutionContext.DataflowExecutionStateTracker(
            ExecutionStateSampler.instance(), null, null, PipelineOptionsFactory.create(), "");

    // Enter a processing state
    StreamingModeExecutionState state =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(),
            PROCESS_STATE_NAME,
            null,
            NoopProfileScope.NOOP,
            null);
    tracker.enterState(state);
    // Enter a new processing state
    StreamingModeExecutionState newState =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(),
            PROCESS_STATE_NAME,
            null,
            NoopProfileScope.NOOP,
            null);
    tracker.enterState(newState);

    // The first completed state should be recorded and the new state should be active.
    Map<String, IntSummaryStatistics> gotProcessingTimes = tracker.getProcessingTimesByStep();
    Assert.assertEquals(1, gotProcessingTimes.size());
    Assert.assertEquals(
        new HashSet<>(Arrays.asList(NameContextsForTests.nameContextForTest().userName())),
        gotProcessingTimes.keySet());
    ActiveMessageMetadata expectedMetadata =
        new ActiveMessageMetadata(NameContextsForTests.nameContextForTest().userName(), 1l);
    Assert.assertEquals(
        expectedMetadata.userStepName, tracker.getActiveMessageMetadata().userStepName);
  }
}
