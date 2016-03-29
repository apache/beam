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

package org.apache.beam.runners.spark;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableSet;
import org.apache.beam.runners.spark.translation.SparkPipelineOptionsFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class SimpleWordCountTest {
  private static final String[] WORDS_ARRAY = {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
  private static final Set<String> EXPECTED_COUNT_SET =
      ImmutableSet.of("hi: 5", "there: 1", "sue: 2", "bob: 2");

  @Test
  public void testRun() throws Exception {
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    options.setRunner(SparkPipelineRunner.class);
    Pipeline p = Pipeline.create(options);
    PCollection<String> inputWords = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder
        .of());
    PCollection<String> output = inputWords.apply(new CountWords());

    DataflowAssert.that(output).containsInAnyOrder(EXPECTED_COUNT_SET);

    EvaluationResult res = SparkPipelineRunner.create().run(p);
    res.close();
  }

  /**
   * A DoFn that tokenizes lines of text into individual words.
   */
  static class ExtractWordsFn extends DoFn<String, String> {
    private static final Pattern WORD_BOUNDARY = Pattern.compile("[^a-zA-Z']+");
    private final Aggregator<Long, Long> emptyLines =
        createAggregator("emptyLines", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) {
      // Split the line into words.
      String[] words = WORD_BOUNDARY.split(c.element());

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

  public static class CountWords extends PTransform<PCollection<String>, PCollection<String>> {
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
