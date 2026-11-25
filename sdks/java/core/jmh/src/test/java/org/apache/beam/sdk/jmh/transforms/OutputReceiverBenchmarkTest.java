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
package org.apache.beam.sdk.jmh.transforms;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.jmh.transforms.OutputReceiverBenchmark.CoderState;
import org.apache.beam.sdk.jmh.transforms.OutputReceiverBenchmark.ReceiverState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.openjdk.jmh.infra.Blackhole;

/** Smoke tests for {@link OutputReceiverBenchmark}. */
@RunWith(JUnit4.class)
public class OutputReceiverBenchmarkTest {

  private static final String[] TEMPLATE_KINDS = {
    "GLOBAL_MIN",
    "GLOBAL_TIMESTAMPED",
    "SINGLE_INTERVAL",
    "MULTI_INTERVAL",
    "MIXED_POLYMORPHIC",
    "BUILDER_TEMPLATE"
  };

  private static final String[] WINDOW_KINDS = {"GLOBAL_MIN", "SINGLE_INTERVAL", "MULTI_INTERVAL"};

  @Test
  public void testReceiverBenchmarks() {
    Blackhole bh =
        new Blackhole(
            "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    OutputReceiverBenchmark benchmark = new OutputReceiverBenchmark();
    for (String kind : TEMPLATE_KINDS) {
      ReceiverState state = new ReceiverState();
      state.templateKind = kind;
      state.setUp(bh);
      benchmark.windowedReceiverOutputTagged(state);
      benchmark.windowedReceiverOutputUntagged(state);
      benchmark.windowedReceiverOutputViaBuilder(state);
      benchmark.windowedReceiverOutputWithTimestamp(state);
      benchmark.windowedReceiverOutputWithTimestampViaBuilder(state);
      benchmark.windowedReceiverOutputWindowedValue(state);
      benchmark.windowedReceiverOutputWindowedValueViaBuilder(state);
      benchmark.rowReceiverOutput(state);
      benchmark.rowReceiverOutputViaBuilder(state);
      benchmark.rowReceiverOutputWithTimestamp(state);
      benchmark.windowedReceiverPerElementConstruction(state);
    }
  }

  @Test
  public void testCoderBenchmarks() throws Exception {
    OutputReceiverBenchmark benchmark = new OutputReceiverBenchmark();
    for (String kind : WINDOW_KINDS) {
      CoderState state = new CoderState();
      state.windowKind = kind;
      state.setUp();
      assertTrue(benchmark.fullWindowedValueCoderEncode(state) >= 0);
      assertNotNull(benchmark.fullWindowedValueCoderDecode(state));
      assertTrue(benchmark.fullWindowedValueCoderByteSizeObserver(state) > 0);
    }
  }
}
