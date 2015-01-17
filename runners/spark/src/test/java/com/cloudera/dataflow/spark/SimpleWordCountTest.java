/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimpleWordCountTest {
  private static final String[] WORDS_ARRAY = {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
  private static final Set<String> EXPECTED_COUNT_SET =
      ImmutableSet.of("hi: 5", "there: 1", "sue: 2", "bob: 2");

  @Test
  public void testRun() throws Exception {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    PCollection<String> inputWords = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());
    PCollection<String> output = inputWords.apply(new CountWords());
    EvaluationResult res = SparkPipelineRunner.create().run(p);
    Set<String> actualCountSet = new HashSet<>();
    for (String s : res.get(output)) {
      actualCountSet.add(s);
    }

    Assert.assertEquals(String.format("Actual counts of words [%s] does not equal expected " +
                "count[%s].",
            actualCountSet, EXPECTED_COUNT_SET),
        EXPECTED_COUNT_SET, actualCountSet);
    res.close();
  }

  /**
   * A DoFn that tokenizes lines of text into individual words.
   */
  static class ExtractWordsFn extends DoFn<String, String> {
    private Aggregator<Long> emptyLines;

    @Override
    public void startBundle(Context c) {
      emptyLines = c.createAggregator("emptyLines", new Sum.SumLongFn());
    }

    @Override
    public void processElement(ProcessContext c) {
      // Split the line into words.
      String[] words = c.element().split("[^a-zA-Z']+");

      // Keep track of the number of lines without any words encountered while tokenizing.
      // This aggregator is visible in the monitoring UI when run using DataflowPipelineRunner.
      if (words.length == 0) {
        emptyLines.addValue(1L);
      }

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  /**
   * A DoFn that converts a Word and Count into a printable string.
   */
  private static class FormatCountsFn extends DoFn<KV<String, Long>, String> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().getKey() + ": " + c.element().getValue());
    }
  }

  private static class CountWords extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> apply(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      // Format each word and count into a printable string.

      return wordCounts.apply(ParDo.of(new FormatCountsFn()));
    }

  }
}
