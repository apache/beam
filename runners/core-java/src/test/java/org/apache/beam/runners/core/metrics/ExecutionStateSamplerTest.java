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

/** Tests for {@link org.apache.beam.runners.core.metrics.ExecutionStateSampler}. */
public class ExecutionStateSamplerTest {

  private MillisProvider clock;
  private ExecutionStateSampler sampler;

  @Before
  public void setUp() {
    clock = mock(MillisProvider.class);
    sampler = ExecutionStateSampler.newForTest(clock);
  }

  private static class TestExecutionState extends ExecutionState {

    private long totalMillis = 0;
    private boolean lullReported = false;

    public TestExecutionState(String stateName) {
      super(stateName);
    }

    @Override
    public void takeSample(long millisSinceLastSample) {
      totalMillis += millisSinceLastSample;
    }

    @Override
    public void reportLull(Thread trackedThread, long millis) {
      lullReported = true;
    }
  }

  private final TestExecutionState step1act1 = new TestExecutionState("activity1");
  private final TestExecutionState step1act2 = new TestExecutionState("activity2");
  private final TestExecutionState step2act1 = new TestExecutionState("activity1");

  @Test
  public void testOneThreadSampling() throws Exception {
    ExecutionStateTracker tracker = createTracker();
    try (Closeable c1 = tracker.activate(new Thread())) {
      try (Closeable c2 = tracker.enterState(step1act1)) {
        sampler.doSampling(400);
        assertThat(step1act1.totalMillis, equalTo(400L));

        sampler.doSampling(200);
        assertThat(step1act1.totalMillis, equalTo(400L + 200L));
      }

      sampler.doSampling(300); // no current state
      assertThat(step1act1.totalMillis, equalTo(400L + 200L));
      assertThat(step1act1.lullReported, equalTo(false));
    }
  }

  @Test
  public void testMultipleThreads() throws Exception {
    ExecutionStateTracker tracker1 = createTracker();
    ExecutionStateTracker tracker2 = createTracker();
    try (Closeable t1 = tracker1.activate(new Thread())) {
      try (Closeable t2 = tracker2.activate(new Thread())) {
        Closeable c1 = tracker1.enterState(step1act1);
        sampler.doSampling(101);
        Closeable c2 = tracker2.enterState(step2act1);
        sampler.doSampling(102);
        Closeable c3 = tracker1.enterState(step1act2);
        sampler.doSampling(203);
        c3.close();
        sampler.doSampling(104);
        c1.close();
        sampler.doSampling(105);
        c2.close();
      }
    }

    assertThat(step1act1.totalMillis, equalTo(101L + 102L + 104L));
    assertThat(step1act2.totalMillis, equalTo(203L));
    assertThat(step2act1.totalMillis, equalTo(102L + 203L + 104L + 105L));

    assertThat(step1act1.lullReported, equalTo(false));
    assertThat(step1act2.lullReported, equalTo(false));
    assertThat(step2act1.lullReported, equalTo(false));
  }

  @Test
  public void testLullDetectionOccurs() throws Exception {
    ExecutionStateTracker tracker1 = createTracker();
    try (Closeable t1 = tracker1.activate(new Thread())) {
      try (Closeable c = tracker1.enterState(step1act1)) {
        sampler.doSampling(TimeUnit.MINUTES.toMillis(6));
      }
    }

    assertThat(step1act1.lullReported, equalTo(true));
  }

  @Test
  public void testReset() throws Exception {
    sampler.lastSampleTimeMillis = 100L;
    sampler.reset();
    assertThat(sampler.lastSampleTimeMillis, equalTo(0L));
  }

  private ExecutionStateTracker createTracker() {
    return new ExecutionStateTracker(sampler);
  }
}
