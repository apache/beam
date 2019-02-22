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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for beam to spark {@link org.apache.beam.sdk.transforms.Combine} translation. */
@RunWith(JUnit4.class)
public class CombineTest implements Serializable {
  private static Pipeline pipeline;

  @BeforeClass
  public static void beforeClass() {
    PipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    pipeline = Pipeline.create(options);
  }

  @Test
  public void testCombineGlobally() {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    input.apply(
        Combine.globally(new IntegerCombineFn()));
    pipeline.run();
  }

  @Test
  public void testCombinePerKey(){
    List<KV<Integer, Integer>> elems = new ArrayList<>();
    elems.add(KV.of(1, 1));
    elems.add(KV.of(1, 3));
    elems.add(KV.of(1, 5));
    elems.add(KV.of(2, 2));
    elems.add(KV.of(2, 4));
    elems.add(KV.of(2, 6));

    PCollection<KV<Integer, Integer>> input = pipeline.apply(Create.of(elems));
    input.apply(Combine.perKey(new IntegerCombineFn()));
    pipeline.run();
  }

  private static class IntegerCombineFn extends Combine.CombineFn<Integer, Integer, Integer> {

    @Override
    public Integer createAccumulator() {
      return 0;
    }

    @Override
    public Integer addInput(Integer accumulator, Integer input) {
      return accumulator + input;
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators) {
      Integer result = 0;
      for (Integer value : accumulators) {
        result += value;
      }
      return result;
    }

    @Override
    public Integer extractOutput(Integer accumulator) {
      return accumulator;
    }
  }
}
