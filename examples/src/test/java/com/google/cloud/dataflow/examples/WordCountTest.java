/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples;

import com.google.cloud.dataflow.examples.WordCount.CountWords;
import com.google.cloud.dataflow.examples.WordCount.ExtractWordsFn;
import com.google.cloud.dataflow.examples.WordCount.FormatAsTextFn;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests of WordCount.
 */
@RunWith(JUnit4.class)
public class WordCountTest {

  /** Example test that tests a specific DoFn. */
  @Test
  public void testExtractWordsFn() {
    DoFnTester<String, String> extractWordsFn =
        DoFnTester.of(new ExtractWordsFn());

    Assert.assertThat(extractWordsFn.processBatch(" some  input  words "),
                      CoreMatchers.hasItems("some", "input", "words"));
    Assert.assertThat(extractWordsFn.processBatch(" "),
                      CoreMatchers.<String>hasItems());
    Assert.assertThat(extractWordsFn.processBatch(" some ", " input", " words"),
                      CoreMatchers.hasItems("some", "input", "words"));
  }

  static final String[] WORDS_ARRAY = new String[] {
    "hi there", "hi", "hi sue bob",
    "hi sue", "", "bob hi"};

  static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  static final String[] COUNTS_ARRAY = new String[] {
      "hi: 5", "there: 1", "sue: 2", "bob: 2"};

  /** Example test that tests a PTransform by using an in-memory input and inspecting the output. */
  @Test
  @Category(RunnableOnService.class)
  public void testCountWords() throws Exception {
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));

    PCollection<String> output = input.apply(new CountWords())
      .apply(MapElements.via(new FormatAsTextFn()));

    DataflowAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);
    p.run();
  }
}
