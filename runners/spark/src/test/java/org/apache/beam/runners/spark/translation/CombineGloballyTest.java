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

package org.apache.beam.runners.spark.translation;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * Combine globally test.
 */
public class CombineGloballyTest {

  private static final String[] WORDS_ARRAY = {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  @Test
  public void test() throws Exception {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);
    PCollection<String> inputWords = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));
    PCollection<String> output = inputWords.apply(Combine.globally(new WordMerger()));

    EvaluationResult res = (EvaluationResult) p.run();
    assertEquals("hi there,hi,hi sue bob,hi sue,,bob hi",
        Iterables.getOnlyElement(res.get(output)));
  }

  /**
   * Word merger combine function used in the test.
   */
  public static class WordMerger extends Combine.CombineFn<String, StringBuilder, String> {

    @Override
    public StringBuilder createAccumulator() {
      // return null to differentiate from an empty string
      return null;
    }

    @Override
    public StringBuilder addInput(StringBuilder accumulator, String input) {
      return combine(accumulator, input);
    }

    @Override
    public StringBuilder mergeAccumulators(Iterable<StringBuilder> accumulators) {
      StringBuilder sb = new StringBuilder();
      for (StringBuilder accum : accumulators) {
        if (accum != null) {
          sb.append(accum);
        }
      }
      return sb;
    }

    @Override
    public String extractOutput(StringBuilder accumulator) {
      return accumulator != null ? accumulator.toString() : "";
    }

    private static StringBuilder combine(StringBuilder accum, String datum) {
      if (accum == null) {
        return new StringBuilder(datum);
      } else {
        accum.append(",").append(datum);
        return accum;
      }
    }
  }
}
