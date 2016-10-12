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

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.examples.WordCount;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Test;

/**
 * Windowed word count test.
 */
public class WindowedWordCountTest {
  private static final String[] WORDS_ARRAY = {
      "hi there", "hi", "hi sue bob", "hi sue", "", "bob hi"};
  private static final Long[] TIMESTAMPS_ARRAY = {
      60000L, 60000L, 60000L, 179000L, 179000L, 179000L};
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
  private static final List<Long> TIMESTAMPS = Arrays.asList(TIMESTAMPS_ARRAY);

  private static final List<String> EXPECTED_FIXED_SEPARATE_COUNT_SET =
      ImmutableList.of("hi: 3", "there: 1", "sue: 1", "bob: 1", "hi: 2", "sue: 1", "bob: 1");

  @Test
  public void testFixed() throws Exception {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(opts);
    PCollection<String> inputWords =
        p.apply(Create.timestamped(WORDS, TIMESTAMPS)).setCoder(StringUtf8Coder.of());
    PCollection<String> windowedWords =
        inputWords.apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<String> output = windowedWords.apply(new WordCount.CountWords())
        .apply(MapElements.via(new WordCount.FormatAsTextFn()));

    PAssert.that(output).containsInAnyOrder(EXPECTED_FIXED_SEPARATE_COUNT_SET);

    p.run();
  }

  private static final List<String> EXPECTED_FIXED_SAME_COUNT_SET =
      ImmutableList.of("hi: 5", "there: 1", "sue: 2", "bob: 2");

  @Test
  public void testFixed2() throws Exception {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(opts);
    PCollection<String> inputWords = p.apply(Create.timestamped(WORDS, TIMESTAMPS)
        .withCoder(StringUtf8Coder.of()));
    PCollection<String> windowedWords = inputWords
        .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(5))));

    PCollection<String> output = windowedWords.apply(new WordCount.CountWords())
        .apply(MapElements.via(new WordCount.FormatAsTextFn()));

    PAssert.that(output).containsInAnyOrder(EXPECTED_FIXED_SAME_COUNT_SET);

    p.run();
  }

  private static final List<String> EXPECTED_SLIDING_COUNT_SET =
      ImmutableList.of("hi: 3", "there: 1", "sue: 1", "bob: 1", "hi: 5", "there: 1", "sue: 2",
      "bob: 2", "hi: 2", "sue: 1", "bob: 1");

  @Test
  public void testSliding() throws Exception {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(opts);
    PCollection<String> inputWords = p.apply(Create.timestamped(WORDS, TIMESTAMPS)
        .withCoder(StringUtf8Coder.of()));
    PCollection<String> windowedWords = inputWords
        .apply(Window.<String>into(SlidingWindows.of(Duration.standardMinutes(2))
        .every(Duration.standardMinutes(1))));

    PCollection<String> output = windowedWords.apply(new WordCount.CountWords())
        .apply(MapElements.via(new WordCount.FormatAsTextFn()));

    PAssert.that(output).containsInAnyOrder(EXPECTED_SLIDING_COUNT_SET);

    p.run();
  }

}
