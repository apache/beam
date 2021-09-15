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
package org.apache.beam.runners.flink.batch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkTestPipeline;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.flink.api.java.DataSet;
import org.junit.Assert;
import org.junit.Test;

public class ReshuffleTest {

  private static class WithBundleIdFn extends DoFn<String, String> {

    private String uuid;

    @StartBundle
    public void startBundle() {
      uuid = UUID.randomUUID().toString();
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(ctx.element() + "@" + uuid);
    }
  }

  /**
   * In case of multiple chained {@link DataSet#rebalance()} operations, Flink {@link
   * org.apache.flink.optimizer.Optimizer} can ignore the latter if it thinks that input channel is
   * already {@link
   * org.apache.flink.runtime.operators.shipping.ShipStrategyType#PARTITION_FORCED_REBALANCE forced
   * rebalanced}. This test makes sure that multiple "chained" {@link Reshuffle} transforms are not
   * optimized, when there is a ParDo operation, that can change data set characteristics, in
   * between them.
   */
  @Test
  public void testEqualDistributionOnReshuffleAcrossMultipleStages() {
    final int numElements = 10_000;
    final int parallelism = 3;
    final int numReshuffles = 2;
    final Pipeline p = FlinkTestPipeline.createForBatch();
    p.getOptions().as(FlinkPipelineOptions.class).setParallelism(parallelism);
    final List<String> input = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      input.add("el_" + i);
    }
    final PCollection<String> result =
        p.apply(Create.of(input))
            .apply(ParDo.of(new WithBundleIdFn()))
            .apply(Reshuffle.viaRandomKey())
            .apply(ParDo.of(new WithBundleIdFn()))
            .apply(Reshuffle.viaRandomKey())
            .apply(ParDo.of(new WithBundleIdFn()));

    PAssert.that(result)
        .satisfies(
            it -> {
              final Map<String, Integer> histo = new HashMap<>();
              for (String item : it) {
                final String[] parts = item.split("@");
                Assert.assertEquals(4, parts.length);
                // First shuffle.
                histo.merge(String.join("->", Arrays.copyOfRange(parts, 1, 3)), 1, Integer::sum);
                // Second shuffle.
                histo.merge(String.join("->", Arrays.copyOfRange(parts, 2, 4)), 1, Integer::sum);
              }
              Assert.assertEquals(
                  numElements * numReshuffles, histo.values().stream().mapToInt(v -> v).sum());
              Assert.assertEquals(parallelism * parallelism * numReshuffles, histo.size());
              return null;
            });

    p.run().waitUntilFinish();
  }
}
