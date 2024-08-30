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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.fn.harness.control.Metrics;
import org.apache.beam.fn.harness.control.Metrics.BundleCounter;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.MonitoringInfoEncodings;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/** Benchmarks for updating metrics objects during processing. */
public class MetricsBenchmark {
  private static final MetricName TEST_NAME = MetricName.named("testNamespace", "testName");
  private static final String TEST_ID = "testId";

  @State(Scope.Benchmark)
  public static class BundleProcessingThreadCounterState {
    public BundleCounter bundleCounter = Metrics.bundleProcessingThreadCounter(TEST_ID, TEST_NAME);

    @TearDown(Level.Trial)
    public void check() {
      Map<String, ByteString> reported = new HashMap<>();
      bundleCounter.updateFinalMonitoringData(reported);
      checkState(reported.containsKey(TEST_ID));
      checkState(MonitoringInfoEncodings.decodeInt64Counter(reported.get(TEST_ID)) > 0);
      bundleCounter.reset();
    }
  }

  @State(Scope.Benchmark)
  public static class CounterCellState {
    public CounterCell counterCell = new CounterCell(TEST_NAME);

    @TearDown(Level.Trial)
    public void check() {
      checkState(counterCell.getCumulative() > 0);
      counterCell.reset();
    }
  }

  @Benchmark
  @Threads(1)
  public void testCounterCellMutation(CounterCellState counterState) throws Exception {
    counterState.counterCell.inc();
  }

  @Benchmark
  @Threads(1)
  public void testCounterCellReset(CounterCellState counterState) throws Exception {
    counterState.counterCell.inc();
    counterState.counterCell.reset();
    counterState.counterCell.inc();
  }

  @Benchmark
  @Threads(1)
  public void testBundleProcessingThreadCounterMutation(
      BundleProcessingThreadCounterState counterState) throws Exception {
    counterState.bundleCounter.inc();
  }

  @Benchmark
  @Threads(1)
  public void testBundleProcessingThreadCounterReset(
      BundleProcessingThreadCounterState counterState) throws Exception {
    counterState.bundleCounter.inc();
    counterState.bundleCounter.reset();
    counterState.bundleCounter.inc();
  }
}
