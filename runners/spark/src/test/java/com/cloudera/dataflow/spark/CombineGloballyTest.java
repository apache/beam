/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CombineGloballyTest {

  private static final String[] WORDS_ARRAY = {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  @Test
  public void test() throws Exception {
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> inputWords = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());
    PCollection<String> output = inputWords.apply(Combine.globally(new WordMerger()));

    EvaluationResult res = SparkPipelineRunner.create().run(p);
    assertEquals("hi there,hi,hi sue bob,hi sue,,bob hi", Iterables.getOnlyElement(res.get(output)));
    res.close();
  }

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
      return accumulator.toString();
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
