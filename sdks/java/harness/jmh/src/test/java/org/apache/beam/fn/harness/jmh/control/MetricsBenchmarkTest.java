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

import org.apache.beam.fn.harness.jmh.control.MetricsBenchmark.BundleProcessingThreadCounterState;
import org.apache.beam.fn.harness.jmh.control.MetricsBenchmark.CounterCellState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetricsBenchmarkTest {
  @Test
  public void testBundleProcessingThreadCounterMutation() throws Exception {
    BundleProcessingThreadCounterState state = new BundleProcessingThreadCounterState();
    new MetricsBenchmark().testBundleProcessingThreadCounterMutation(state);
    state.check();
  }

  @Test
  public void testBundleProcessingThreadCounterReset() throws Exception {
    BundleProcessingThreadCounterState state = new BundleProcessingThreadCounterState();
    new MetricsBenchmark().testBundleProcessingThreadCounterReset(state);
    state.check();
  }

  @Test
  public void testCounterCellMutation() throws Exception {
    CounterCellState state = new CounterCellState();
    new MetricsBenchmark().testCounterCellMutation(state);
    state.check();
  }

  @Test
  public void testCounterCellReset() throws Exception {
    CounterCellState state = new CounterCellState();
    new MetricsBenchmark().testCounterCellReset(state);
    state.check();
  }
}
