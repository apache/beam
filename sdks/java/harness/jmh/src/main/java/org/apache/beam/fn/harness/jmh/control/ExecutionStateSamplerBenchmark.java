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

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Labels;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.SimpleExecutionState;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p43p2.com.google.protobuf.ByteString;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/** Benchmarks for sampling execution state. */
public class ExecutionStateSamplerBenchmark {
  private static final String PTRANSFORM = "benchmarkPTransform";

  @State(Scope.Benchmark)
  public static class RunnersCoreStateSampler {
    public final ExecutionStateSampler sampler = ExecutionStateSampler.newForTest();
    public final ExecutionStateTracker tracker = new ExecutionStateTracker(sampler);
    public final SimpleExecutionState state1 =
        new SimpleExecutionState(
            "process",
            Urns.PROCESS_BUNDLE_MSECS,
            new HashMap<>(Collections.singletonMap(Labels.PTRANSFORM, PTRANSFORM)));
    public final SimpleExecutionState state2 =
        new SimpleExecutionState(
            "process",
            Urns.PROCESS_BUNDLE_MSECS,
            new HashMap<>(Collections.singletonMap(Labels.PTRANSFORM, PTRANSFORM)));
    public final SimpleExecutionState state3 =
        new SimpleExecutionState(
            "process",
            Urns.PROCESS_BUNDLE_MSECS,
            new HashMap<>(Collections.singletonMap(Labels.PTRANSFORM, PTRANSFORM)));

    @Setup(Level.Trial)
    public void setup() {
      sampler.start();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      sampler.stop();
      // Print out the total millis so that JVM doesn't optimize code away.
      System.out.println(
          state1.getTotalMillis()
              + ", "
              + state2.getTotalMillis()
              + ", "
              + state3.getTotalMillis());
    }
  }

  @State(Scope.Benchmark)
  public static class HarnessStateSampler {
    public final org.apache.beam.fn.harness.control.ExecutionStateSampler sampler =
        new org.apache.beam.fn.harness.control.ExecutionStateSampler(
            PipelineOptionsFactory.create(), System::currentTimeMillis);
    public final org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTracker
        tracker = sampler.create();
    public final org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionState state1 =
        tracker.create("1", PTRANSFORM, PTRANSFORM + "Name", "1");
    public final org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionState state2 =
        tracker.create("2", PTRANSFORM, PTRANSFORM + "Name", "2");
    public final org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionState state3 =
        tracker.create("3", PTRANSFORM, PTRANSFORM + "Name", "3");

    @TearDown(Level.Trial)
    public void tearDown() {
      sampler.stop();
      Map<String, ByteString> monitoringData = new HashMap<>();
      tracker.updateFinalMonitoringData(monitoringData);
      // Print out the total millis so that JVM doesn't optimize code away.
      System.out.println(monitoringData);
    }
  }

  @Benchmark
  @Threads(1)
  public void testTinyBundleRunnersCoreStateSampler(RunnersCoreStateSampler state)
      throws Exception {
    state.tracker.activate();
    for (int i = 0; i < 3; ) {
      Closeable close1 = state.tracker.enterState(state.state1);
      Closeable close2 = state.tracker.enterState(state.state2);
      Closeable close3 = state.tracker.enterState(state.state3);
      // trival code that is being sampled for this state
      i += 1;
      close3.close();
      close2.close();
      close1.close();
    }
    state.tracker.reset();
  }

  @Benchmark
  @Threads(1)
  public void testTinyBundleHarnessStateSampler(HarnessStateSampler state) throws Exception {
    state.tracker.start("processBundleId");
    for (int i = 0; i < 3; ) {
      state.state1.activate();
      state.state2.activate();
      state.state3.activate();
      // trival code that is being sampled for this state
      i += 1;
      state.state3.deactivate();
      state.state2.deactivate();
      state.state1.deactivate();
    }
    state.tracker.reset();
  }

  @Benchmark
  @Threads(1)
  public void testLargeBundleRunnersCoreStateSampler(RunnersCoreStateSampler state)
      throws Exception {
    state.tracker.activate();
    for (int i = 0; i < 1000; ) {
      Closeable close1 = state.tracker.enterState(state.state1);
      Closeable close2 = state.tracker.enterState(state.state2);
      Closeable close3 = state.tracker.enterState(state.state3);
      // trival code that is being sampled for this state
      i += 1;
      close3.close();
      close2.close();
      close1.close();
    }
    state.tracker.reset();
  }

  @Benchmark
  @Threads(1)
  public void testLargeBundleHarnessStateSampler(HarnessStateSampler state) throws Exception {
    state.tracker.start("processBundleId");
    for (int i = 0; i < 1000; ) {
      state.state1.activate();
      state.state2.activate();
      state.state3.activate();
      // trival code that is being sampled for this state
      i += 1;
      state.state3.deactivate();
      state.state2.deactivate();
      state.state1.deactivate();
    }
    state.tracker.reset();
  }
}
