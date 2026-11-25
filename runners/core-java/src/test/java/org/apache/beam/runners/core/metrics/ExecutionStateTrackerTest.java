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
package org.apache.beam.runners.core.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link ExecutionStateTracker}. */
public class ExecutionStateTrackerTest {

  private MillisProvider clock;
  private ExecutionStateSampler sampler;

  @Before
  public void setUp() {
    clock = mock(MillisProvider.class);
    sampler = ExecutionStateSampler.newForTest(clock);
  }

  private static class TestExecutionState extends ExecutionState {

    private long totalMillis = 0;
    private int activateCount = 0;

    public TestExecutionState(String stateName) {
      super(stateName);
    }

    @Override
    public void onActivate(boolean pushing) {
      activateCount++;
    }

    @Override
    public void takeSample(long millisSinceLastSample) {
      totalMillis += millisSinceLastSample;
    }

    @Override
    public void reportLull(Thread trackedThread, long millis) {}
  }

  private final TestExecutionState testExecutionState = new TestExecutionState("activity");

  @Test
  public void testReset() throws Exception {
    ExecutionStateTracker tracker = createTracker();
    try (Closeable c1 = tracker.activate(new Thread())) {
      try (Closeable c2 = tracker.enterState(testExecutionState)) {
        sampler.doSampling(400);
        assertThat(testExecutionState.totalMillis, equalTo(400L));
      }
    }

    tracker.reset();
    assertThat(tracker.getTrackedThread(), equalTo(null));
    assertThat(tracker.getCurrentState(), equalTo(null));
    assertThat(tracker.getNumTransitions(), equalTo(0L));
    assertThat(tracker.getMillisSinceLastTransition(), equalTo(0L));
    assertThat(tracker.getTransitionsAtLastSample(), equalTo(0L));
    assertThat(tracker.getNextLullReportMs(), equalTo(TimeUnit.MINUTES.toMillis(5)));
  }

  @Test
  public void testDirectTransitionAndSubclassGuard() {
    ExecutionStateTracker tracker = createTracker();
    assertTrue(tracker.supportsDirectTransition());

    ExecutionStateTracker subclass = new ExecutionStateTracker(sampler) {};
    assertFalse(subclass.supportsDirectTransition());
    assertFalse(spy(tracker).supportsDirectTransition());
    assertFalse(mock(ExecutionStateTracker.class).supportsDirectTransition());

    TestExecutionState stateA = new TestExecutionState("A");
    TestExecutionState stateB = new TestExecutionState("B");

    ExecutionState prevA = tracker.enterStateDirect(stateA);
    assertThat(prevA, nullValue());
    assertThat(tracker.getCurrentState(), sameInstance(stateA));
    assertThat(tracker.getNumTransitions(), equalTo(1L));
    assertThat(stateA.activateCount, equalTo(1));

    ExecutionState prevB = tracker.enterStateDirect(stateB);
    assertThat(prevB, sameInstance(stateA));
    assertThat(tracker.getCurrentState(), sameInstance(stateB));
    assertThat(tracker.getNumTransitions(), equalTo(2L));
    assertThat(stateB.activateCount, equalTo(1));

    tracker.exitStateDirect(prevB);
    assertThat(tracker.getCurrentState(), sameInstance(stateA));
    assertThat(tracker.getNumTransitions(), equalTo(3L));
    assertThat(stateA.activateCount, equalTo(2));

    tracker.exitStateDirect(prevA);
    assertThat(tracker.getCurrentState(), nullValue());
    assertThat(tracker.getNumTransitions(), equalTo(4L));
  }

  @Test
  public void testEnterStateDirectRollsBackOnActivateException() {
    ExecutionStateTracker tracker = createTracker();
    TestExecutionState baseState = new TestExecutionState("base");
    ExecutionState failingState =
        new TestExecutionState("failing") {
          @Override
          public void onActivate(boolean pushing) {
            throw new IllegalStateException("activate failed");
          }
        };

    ExecutionState prevBase = tracker.enterStateDirect(baseState);
    assertThat(tracker.getCurrentState(), sameInstance(baseState));
    assertThat(tracker.getNumTransitions(), equalTo(1L));

    try {
      tracker.enterStateDirect(failingState);
      fail("Expected IllegalStateException from onActivate");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage(), equalTo("activate failed"));
    }

    assertThat(tracker.getCurrentState(), sameInstance(baseState));
    assertThat(tracker.getNumTransitions(), equalTo(1L));

    tracker.exitStateDirect(prevBase);
    assertThat(tracker.getCurrentState(), nullValue());
    assertThat(tracker.getNumTransitions(), equalTo(2L));
  }

  @Test
  public void testResetThenDirectTransition() {
    ExecutionStateTracker tracker = createTracker();
    TestExecutionState stateA = new TestExecutionState("A");
    tracker.enterStateDirect(stateA);
    assertThat(tracker.getCurrentState(), sameInstance(stateA));
    assertThat(tracker.getNumTransitions(), equalTo(1L));

    tracker.reset();
    assertThat(tracker.getCurrentState(), nullValue());
    assertThat(tracker.getNumTransitions(), equalTo(0L));

    ExecutionState prev = tracker.enterStateDirect(stateA);
    assertThat(prev, nullValue());
    assertThat(tracker.getCurrentState(), sameInstance(stateA));
    assertThat(tracker.getNumTransitions(), equalTo(1L));

    tracker.exitStateDirect(prev);
    assertThat(tracker.getCurrentState(), nullValue());
    assertThat(tracker.getNumTransitions(), equalTo(2L));
  }

  private ExecutionStateTracker createTracker() {
    return new ExecutionStateTracker(sampler);
  }
}
