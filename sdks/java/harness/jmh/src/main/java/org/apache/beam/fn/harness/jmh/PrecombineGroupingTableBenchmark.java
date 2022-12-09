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
package org.apache.beam.fn.harness.jmh;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.PrecombineGroupingTable;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

public class PrecombineGroupingTableBenchmark {
  private static final int TOTAL_VALUES = 1_000_000;

  @State(Scope.Benchmark)
  public static class SumIntegerBinaryCombine {
    final Combine.BinaryCombineIntegerFn sumInts = Sum.ofIntegers();
    final PipelineOptions options = PipelineOptionsFactory.create();
    List<WindowedValue<KV<String, Integer>>> elements;

    @Param({"true", "false"})
    public String globallyWindowed;

    @Param({"uniform", "normal", "hotKey", "uniqueKeys"})
    public String distribution;

    @Setup(Level.Trial)
    public void setUp() {
      // Use a stable seed to ensure consistency across benchmark runs
      Random random = new Random(-2134890234);
      elements = new ArrayList<>();
      switch (distribution) {
        case "uniform":
          for (int i = 0; i < TOTAL_VALUES; ++i) {
            int key = random.nextInt(1000);
            elements.add(WindowedValue.valueInGlobalWindow(KV.of(Integer.toString(key), key)));
          }
          break;
        case "normal":
          for (int i = 0; i < TOTAL_VALUES; ++i) {
            int key = (int) (random.nextGaussian() * 1000);
            elements.add(WindowedValue.valueInGlobalWindow(KV.of(Integer.toString(key), key)));
          }
          break;
        case "hotKey":
          for (int i = 0; i < TOTAL_VALUES; ++i) {
            int key;
            if (random.nextBoolean()) {
              key = 0;
            } else {
              key = random.nextInt(1000);
            }
            elements.add(WindowedValue.valueInGlobalWindow(KV.of(Integer.toString(key), key)));
          }
          break;
        case "uniqueKeys":
          for (int i = 0; i < TOTAL_VALUES; ++i) {
            elements.add(WindowedValue.valueInGlobalWindow(KV.of(Integer.toString(i), i)));
          }
          Collections.shuffle(elements, random);
          break;
        default:
      }
    }
  }

  @Benchmark
  public void sumIntegerBinaryCombine(SumIntegerBinaryCombine table, Blackhole blackhole)
      throws Exception {
    PrecombineGroupingTable<String, Integer, int[]> groupingTable =
        PrecombineGroupingTable.combiningAndSampling(
            table.options,
            Caches.eternal(),
            table.sumInts,
            StringUtf8Coder.of(),
            .001,
            Boolean.valueOf(table.globallyWindowed));
    for (int i = 0, size = table.elements.size(); i < size; ++i) {
      groupingTable.put(table.elements.get(i), blackhole::consume);
    }
    groupingTable.flush(blackhole::consume);
  }
}
