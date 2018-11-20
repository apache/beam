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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.DataflowElementExecutionTracker;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ExecutionStateTracker.ExecutionState;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link ExecutionStateSampler}. */
public class ExecutionStateSamplerTest {
  private PipelineOptions options;
  private MillisProvider clock;
  private ExecutionStateSampler sampler;
  private CounterSet counterSet;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.create();
    clock = mock(MillisProvider.class);
    sampler = ExecutionStateSampler.newForTest(clock);
    counterSet = new CounterSet();
  }

  private static class TestExecutionState extends ExecutionState {

    private long totalMillis = 0;
    private boolean lullReported = false;

    public TestExecutionState(NameContext stepName, String stateName) {
      super(stepName, stateName);
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

  private final NameContext step1 =
      NameContext.create("stage", "originalStep1", "systemStep1", "userStep1");
  private final NameContext step2 =
      NameContext.create("stage", "originalStep2", "systemStep2", "userStep2");

  private final TestExecutionState step1act1 = new TestExecutionState(step1, "activity1");
  private final TestExecutionState step1act2 = new TestExecutionState(step1, "activity2");
  private final TestExecutionState step1Process =
      new TestExecutionState(step1, ExecutionStateTracker.PROCESS_STATE_NAME);
  private final TestExecutionState step2act1 = new TestExecutionState(step2, "activity1");

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
  public void testReportsElementExecutionTime() throws IOException {
    enableTimePerElementExperiment();
    ExecutionStateTracker tracker = createTracker();

    try (Closeable c1 = tracker.activate(new Thread())) {
      try (Closeable c2 = tracker.enterState(step1Process)) {}
      sampler.doSampling(30);
      // Execution time split evenly between executions: IDLE, step1, IDLE
    }
    assertProcessingTimeCounter(step1, 10, 4);
  }

  /** {@link ExecutionStateSampler} should take one last sample when a tracker is deactivated. */
  @Test
  public void testTakesSampleOnDeactivate() throws IOException {
    enableTimePerElementExperiment();
    ExecutionStateTracker tracker = createTracker();

    try (Closeable c1 = tracker.activate(new Thread())) {
      try (Closeable c2 = tracker.enterState(step1Process)) {
        sampler.doSampling(100);
        assertThat(step1Process.totalMillis, equalTo(100L));
      }
    }

    sampler.doSampling(100);
    assertThat(step1Process.totalMillis, equalTo(100L));
  }

  private void enableTimePerElementExperiment() {
    options
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(
            Lists.newArrayList(DataflowElementExecutionTracker.TIME_PER_ELEMENT_EXPERIMENT));
  }

  private void assertProcessingTimeCounter(NameContext step, int millis, int bucketOffset) {
    CounterName counterName = ElementExecutionTracker.COUNTER_NAME.withOriginalName(step);
    Counter<?, CounterFactory.CounterDistribution> counter =
        (Counter<?, CounterFactory.CounterDistribution>) counterSet.getExistingCounter(counterName);
    assertNotNull(counter);

    CounterFactory.CounterDistribution distribution = counter.getAggregate();
    assertThat(
        distribution,
        equalTo(
            CounterFactory.CounterDistribution.builder()
                .minMax(millis, millis)
                .count(1)
                .sum(millis)
                .sumOfSquares(millis * millis)
                .buckets(bucketOffset, Lists.newArrayList(1L))
                .build()));
  }

  private ExecutionStateTracker createTracker() {
    return new ExecutionStateTracker(
        sampler, DataflowElementExecutionTracker.create(counterSet, options));
  }
}
