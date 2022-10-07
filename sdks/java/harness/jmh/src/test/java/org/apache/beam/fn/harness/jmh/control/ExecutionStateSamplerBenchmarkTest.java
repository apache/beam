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
package org.apache.beam.fn.harness.jmh.control;

import org.apache.beam.fn.harness.jmh.control.ExecutionStateSamplerBenchmark.HarnessStateSampler;
import org.apache.beam.fn.harness.jmh.control.ExecutionStateSamplerBenchmark.HarnessStateTracker;
import org.apache.beam.fn.harness.jmh.control.ExecutionStateSamplerBenchmark.RunnersCoreStateSampler;
import org.apache.beam.fn.harness.jmh.control.ExecutionStateSamplerBenchmark.RunnersCoreStateTracker;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.openjdk.jmh.infra.Blackhole;

/** Tests for {@link ExecutionStateSamplerBenchmark}. */
@RunWith(JUnit4.class)
public class ExecutionStateSamplerBenchmarkTest {
  private Blackhole blackhole;

  @Before
  public void before() {
    blackhole =
        new Blackhole(
            "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
  }

  @Test
  public void testTinyBundleRunnersCoreStateSampler() throws Exception {
    RunnersCoreStateSampler state = new RunnersCoreStateSampler();
    RunnersCoreStateTracker threadState = new RunnersCoreStateTracker();
    state.setup();
    threadState.setup(state);
    new ExecutionStateSamplerBenchmark()
        .testTinyBundleRunnersCoreStateSampler(threadState, blackhole);
    state.tearDown();
  }

  @Test
  public void testLargeBundleRunnersCoreStateSampler() throws Exception {
    RunnersCoreStateSampler state = new RunnersCoreStateSampler();
    RunnersCoreStateTracker threadState = new RunnersCoreStateTracker();
    state.setup();
    threadState.setup(state);
    new ExecutionStateSamplerBenchmark()
        .testLargeBundleRunnersCoreStateSampler(threadState, blackhole);
    state.tearDown();
  }

  @Test
  public void testTinyBundleHarnessStateSampler() throws Exception {
    HarnessStateSampler state = new HarnessStateSampler();
    HarnessStateTracker threadState = new HarnessStateTracker();
    threadState.setup(state);
    new ExecutionStateSamplerBenchmark().testTinyBundleHarnessStateSampler(threadState, blackhole);
    state.tearDown();
    threadState.tearDown();
  }

  @Test
  public void testLargeBundleHarnessStateSampler() throws Exception {
    HarnessStateSampler state = new HarnessStateSampler();
    HarnessStateTracker threadState = new HarnessStateTracker();
    threadState.setup(state);
    new ExecutionStateSamplerBenchmark().testLargeBundleHarnessStateSampler(threadState, blackhole);
    state.tearDown();
    threadState.tearDown();
  }
}
