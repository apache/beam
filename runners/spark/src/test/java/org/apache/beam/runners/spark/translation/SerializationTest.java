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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * Serialization test.
 */
public class SerializationTest {

  /**
   * Simple String holder.
   */
  public static class StringHolder { // not serializable
    private final String string;

    public StringHolder(String string) {
      this.string = string;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StringHolder that = (StringHolder) o;
      return string.equals(that.string);
    }

    @Override
    public int hashCode() {
      return string.hashCode();
    }

    @Override
    public String toString() {
      return string;
    }
  }

  /**
   * Simple String holder with UTF-8 encoding.
   */
  public static class StringHolderUtf8Coder extends AtomicCoder<StringHolder> {

    private final StringUtf8Coder stringUtf8Coder = StringUtf8Coder.of();

    @Override
    public void encode(StringHolder value,
                       OutputStream outStream,
                       Context context) throws IOException {
      stringUtf8Coder.encode(value.toString(), outStream, context);
    }

    @Override
    public StringHolder decode(InputStream inStream, Context context) throws IOException {
      return new StringHolder(stringUtf8Coder.decode(inStream, context));
    }

    public static Coder<StringHolder> of() {
      return new StringHolderUtf8Coder();
    }
  }

  private static final String[] WORDS_ARRAY = {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};
  private static final List<StringHolder> WORDS = Lists.transform(
      Arrays.asList(WORDS_ARRAY), new Function<String, StringHolder>() {
        @Override public StringHolder apply(String s) {
          return new StringHolder(s);
        }
      });
  private static final Set<StringHolder> EXPECTED_COUNT_SET =
      ImmutableSet.copyOf(Lists.transform(
          Arrays.asList("hi: 5", "there: 1", "sue: 2", "bob: 2"),
          new Function<String, StringHolder>() {
            @Override
            public StringHolder apply(String s) {
              return new StringHolder(s);
            }
          }));

  @Test
  public void testRun() throws Exception {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);
    PCollection<StringHolder> inputWords =
        p.apply(Create.of(WORDS).withCoder(StringHolderUtf8Coder.of()));
    PCollection<StringHolder> output = inputWords.apply(new CountWords());

    PAssert.that(output).containsInAnyOrder(EXPECTED_COUNT_SET);

    p.run();
  }

  /**
   * A {@link DoFn} that tokenizes lines of text into individual words.
   */
  static class ExtractWordsFn extends DoFn<StringHolder, StringHolder> {
    private static final Pattern WORD_BOUNDARY = Pattern.compile("[^a-zA-Z']+");
    private final Aggregator<Long, Long> emptyLines =
        createAggregator("emptyLines", new Sum.SumLongFn());

    @ProcessElement
    public void processElement(ProcessContext c) {
      // Split the line into words.
      String[] words = WORD_BOUNDARY.split(c.element().toString());

      // Keep track of the number of lines without any words encountered while tokenizing.
      // This aggregator is visible in the monitoring UI when run using DataflowRunner.
      if (words.length == 0) {
        emptyLines.addValue(1L);
      }

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(new StringHolder(word));
        }
      }
    }
  }

  /**
   * A {@link DoFn} that converts a Word and Count into a printable string.
   */
  private static class FormatCountsFn extends DoFn<KV<StringHolder, Long>, StringHolder> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new StringHolder(c.element().getKey() + ": " + c.element().getValue()));
    }
  }

  private static class CountWords
      extends PTransform<PCollection<StringHolder>, PCollection<StringHolder>> {
    @Override
    public PCollection<StringHolder> apply(PCollection<StringHolder> lines) {

      // Convert lines of text into individual words.
      PCollection<StringHolder> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<StringHolder, Long>> wordCounts =
          words.apply(Count.<StringHolder>perElement());

      // Format each word and count into a printable string.

      return wordCounts.apply(ParDo.of(new FormatCountsFn()));
    }

  }
}
