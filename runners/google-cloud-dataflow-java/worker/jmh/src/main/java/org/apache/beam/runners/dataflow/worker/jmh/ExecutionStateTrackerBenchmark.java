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
package org.apache.beam.runners.dataflow.worker.jmh;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.worker.BatchModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Microbenchmarks for {@link ExecutionStateTracker} and {@link DataflowOperationContext} state
 * transitions on the per-element hot path.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ExecutionStateTrackerBenchmark {

  private static final class SimpleExecutionState extends ExecutionState {
    SimpleExecutionState(String stateName) {
      super(stateName);
    }

    @Override
    public void takeSample(long millisSinceLastSample) {}

    @Override
    public void reportLull(Thread trackedThread, long millis) {}
  }

  @State(Scope.Thread)
  public static class BatchTrackerState {
    DataflowOperationContext op1;
    DataflowOperationContext op2;
    DataflowOperationContext op3;
    DataflowOperationContext op4;
    Closeable batchActivation;

    @Setup
    public void setUp() {
      PipelineOptions options = PipelineOptionsFactory.create();
      BatchModeExecutionContext execContext = BatchModeExecutionContext.forTesting(options, "S01");
      batchActivation = execContext.getExecutionStateTracker().activate();
      op1 = execContext.createOperationContext(NameContext.create("S01", "s1", "s1", "Step1"));
      op2 = execContext.createOperationContext(NameContext.create("S01", "s2", "s2", "Step2"));
      op3 = execContext.createOperationContext(NameContext.create("S01", "s3", "s3", "Step3"));
      op4 = execContext.createOperationContext(NameContext.create("S01", "s4", "s4", "Step4"));
    }

    @TearDown
    public void tearDown() throws Exception {
      if (batchActivation != null) {
        batchActivation.close();
      }
    }
  }

  @State(Scope.Thread)
  public static class StreamingTrackerState {
    DataflowOperationContext op1;
    DataflowOperationContext op2;
    DataflowOperationContext op3;
    DataflowOperationContext op4;
    Closeable streamingActivation;

    @Setup
    public void setUp() {
      DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      options.setStreaming(true);
      BatchModeExecutionContext execContext = BatchModeExecutionContext.forTesting(options, "S01");
      streamingActivation = execContext.getExecutionStateTracker().activate();
      op1 =
          execContext.createOperationContext(
              NameContext.create("S01", "s1", "s1", "Query1/Query1/justBids/IsBid/ParDo(Anon)"));
      op2 =
          execContext.createOperationContext(
              NameContext.create("S01", "s2", "s2", "Query1/Query1/justBids/AsBid"));
      op3 =
          execContext.createOperationContext(
              NameContext.create("S01", "s3", "s3", "Query1/Query1/Query1.ToEuros"));
      op4 =
          execContext.createOperationContext(
              NameContext.create("S01", "s4", "s4", "Query1.DevNull"));
    }

    @TearDown
    public void tearDown() throws Exception {
      if (streamingActivation != null) {
        streamingActivation.close();
      }
    }
  }

  @State(Scope.Thread)
  public static class PlainTrackerState {
    ExecutionStateTracker plainTracker;
    ExecutionState plainState1;
    ExecutionState plainState2;
    Closeable plainActivation;

    @Setup
    public void setUp() {
      plainTracker = new ExecutionStateTracker(ExecutionStateSampler.newForTest());
      plainActivation = plainTracker.activate();
      plainState1 = new SimpleExecutionState("process-1");
      plainState2 = new SimpleExecutionState("process-2");
    }

    @TearDown
    public void tearDown() throws Exception {
      if (plainActivation != null) {
        plainActivation.close();
      }
    }
  }

  @Benchmark
  public void enterProcess_batchTracker(BatchTrackerState state) throws Exception {
    try (Closeable c = state.op1.enterProcess()) {
      // Simulate minimal work in state
    }
  }

  @Benchmark
  public void enterState_plainExecutionStateTracker(PlainTrackerState state) throws Exception {
    try (Closeable c1 = state.plainTracker.enterState(state.plainState1)) {
      try (Closeable c2 = state.plainTracker.enterState(state.plainState2)) {
        // Nested transition
      }
    }
  }

  @Benchmark
  public void enterProcess_chained4Steps(BatchTrackerState state) throws Exception {
    try (Closeable c1 = state.op1.enterProcess()) {
      try (Closeable c2 = state.op2.enterProcess()) {
        try (Closeable c3 = state.op3.enterProcess()) {
          try (Closeable c4 = state.op4.enterProcess()) {
            // 4-step fused stage transition
          }
        }
      }
    }
  }

  @Benchmark
  public void enterProcess_streamingTracker(StreamingTrackerState state) throws Exception {
    try (Closeable c = state.op1.enterProcess()) {
      // Simulate minimal work in streaming state
    }
  }

  @Benchmark
  public void enterProcess_streamingChained4Steps(StreamingTrackerState state) throws Exception {
    try (Closeable c1 = state.op1.enterProcess()) {
      try (Closeable c2 = state.op2.enterProcess()) {
        try (Closeable c3 = state.op3.enterProcess()) {
          try (Closeable c4 = state.op4.enterProcess()) {
            // 4-step streaming fused stage transition
          }
        }
      }
    }
  }
}
