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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

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

    public TestExecutionState(String stateName) {
      super(stateName);
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

  private ExecutionStateTracker createTracker() {
    return new ExecutionStateTracker(sampler);
  }
}
