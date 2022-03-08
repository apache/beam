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
package org.apache.beam.examples;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.examples.WordCount.CountWords;
import org.apache.beam.examples.WordCount.ExtractWordsFn;
import org.apache.beam.examples.WordCount.FormatAsTextFn;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of WordCount. */
@RunWith(JUnit4.class)
public class WordCountTest {

  /** Example test that tests a specific {@link DoFn}. */
  @Test
  public void testExtractWordsFn() throws Exception {
    List<String> words = Arrays.asList(" some  input  words ", " ", " cool ", " foo", " bar");
    PCollection<String> output =
        p.apply(Create.of(words).withCoder(StringUtf8Coder.of()))
            .apply(ParDo.of(new ExtractWordsFn()));
    PAssert.that(output).containsInAnyOrder("some", "input", "words", "cool", "foo", "bar");
    p.run().waitUntilFinish();
  }

  static final String[] WORDS_ARRAY =
      new String[] {
        "hi there", "hi", "hi sue bob",
        "hi sue", "", "bob hi"
      };

  static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  static final String[] COUNTS_ARRAY = new String[] {"hi: 5", "there: 1", "sue: 2", "bob: 2"};

  @Rule public TestPipeline p = TestPipeline.create();

  /** Example test that tests a PTransform by using an in-memory input and inspecting the output. */
  @Test
  @Category(ValidatesRunner.class)
  public void testCountWords() throws Exception {
    PCollection<String> input = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));

    PCollection<String> output =
        input.apply(new CountWords()).apply(MapElements.via(new FormatAsTextFn()));

    PAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);
    p.run().waitUntilFinish();
  }
}
