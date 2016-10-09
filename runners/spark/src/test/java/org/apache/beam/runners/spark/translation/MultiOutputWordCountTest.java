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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.util.Set;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.ApproximateUnique;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Multi-output word count test.
 */
public class MultiOutputWordCountTest {

  private static final TupleTag<String> upper = new TupleTag<>();
  private static final TupleTag<String> lower = new TupleTag<>();
  private static final TupleTag<KV<String, Long>> lowerCnts = new TupleTag<>();
  private static final TupleTag<KV<String, Long>> upperCnts = new TupleTag<>();

  private static final Set<String> EXPECTED_LOWER_COUNTS =
      ImmutableSet.of("are: 2", "some: 3", "words: 3", "more: 2", "to: 1", "count: 1", "and: 2",
      "even: 1", "others: 1");

  @Test
  public void testRun() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);
    PCollection<String> regex = p.apply(Create.of("[^a-zA-Z']+"));
    PCollection<String> w1 = p.apply(Create.of("Here are some words to count", "and some others"));
    PCollection<String> w2 = p.apply(Create.of("Here are some more words", "and even more words"));
    PCollectionList<String> list = PCollectionList.of(w1).and(w2);

    PCollection<String> union = list.apply(Flatten.<String>pCollections());
    PCollectionView<String> regexView = regex.apply(View.<String>asSingleton());
    CountWords countWords = new CountWords(regexView);
    PCollectionTuple luc = union.apply(countWords);
    PCollection<Long> unique = luc.get(lowerCnts).apply(
        ApproximateUnique.<KV<String, Long>>globally(16));

    EvaluationResult res = (EvaluationResult) p.run();
    PAssert.that(luc.get(lowerCnts).apply(ParDo.of(new FormatCountsFn())))
        .containsInAnyOrder(EXPECTED_LOWER_COUNTS);
    Iterable<KV<String, Long>> actualUpper = res.get(luc.get(upperCnts));
    Assert.assertEquals("Here", actualUpper.iterator().next().getKey());
    Iterable<Long> actualUniqCount = res.get(unique);
    Assert.assertEquals(9, (long) actualUniqCount.iterator().next());
    int actualTotalWords = res.getAggregatorValue("totalWords", Integer.class);
    Assert.assertEquals(18, actualTotalWords);
    int actualMaxWordLength = res.getAggregatorValue("maxWordLength", Integer.class);
    Assert.assertEquals(6, actualMaxWordLength);
    AggregatorValues<Integer> aggregatorValues = res.getAggregatorValues(countWords
        .getTotalWordsAggregator());
    Assert.assertEquals(18, Iterables.getOnlyElement(aggregatorValues.getValues()).intValue());
  }

  /**
   * A {@link DoFn} that tokenizes lines of text into individual words.
   */
  static class ExtractWordsFn extends DoFn<String, String> {

    private final Aggregator<Integer, Integer> totalWords = createAggregator("totalWords",
        new Sum.SumIntegerFn());
    private final Aggregator<Integer, Integer> maxWordLength = createAggregator("maxWordLength",
        new Max.MaxIntegerFn());
    private final PCollectionView<String> regex;

    ExtractWordsFn(PCollectionView<String> regex) {
      this.regex = regex;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] words = c.element().split(c.sideInput(regex));
      for (String word : words) {
        totalWords.addValue(1);
        if (!word.isEmpty()) {
          maxWordLength.addValue(word.length());
          if (Character.isLowerCase(word.charAt(0))) {
            c.output(word);
          } else {
            c.sideOutput(upper, word);
          }
        }
      }
    }
  }

  /**
   * Count words {@link PTransform} used in the test.
   */
  public static class CountWords extends PTransform<PCollection<String>, PCollectionTuple> {

    private final PCollectionView<String> regex;
    private final ExtractWordsFn extractWordsFn;

    public CountWords(PCollectionView<String> regex) {
      this.regex = regex;
      this.extractWordsFn = new ExtractWordsFn(regex);
    }

    @Override
    public PCollectionTuple apply(PCollection<String> lines) {
      // Convert lines of text into individual words.
      PCollectionTuple lowerUpper = lines
          .apply(ParDo.of(extractWordsFn)
              .withSideInputs(regex)
              .withOutputTags(lower, TupleTagList.of(upper)));
      lowerUpper.get(lower).setCoder(StringUtf8Coder.of());
      lowerUpper.get(upper).setCoder(StringUtf8Coder.of());
      PCollection<KV<String, Long>> lowerCounts = lowerUpper.get(lower).apply(Count
          .<String>perElement());
      PCollection<KV<String, Long>> upperCounts = lowerUpper.get(upper).apply(Count
          .<String>perElement());
      return PCollectionTuple
          .of(lowerCnts, lowerCounts)
          .and(upperCnts, upperCounts);
    }

    Aggregator<Integer, Integer> getTotalWordsAggregator() {
      return extractWordsFn.totalWords;
    }
  }

  private static class FormatCountsFn extends DoFn<KV<String, Long>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().getKey() + ": " + c.element().getValue());
    }
  }
}
