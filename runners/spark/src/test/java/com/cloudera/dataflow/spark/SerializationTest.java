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
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class SerializationTest {

  public static class StringHolder { // not serializable
    private String string;

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

  public static class StringHolderUtf8Coder extends AtomicCoder<StringHolder> {

    private StringUtf8Coder stringUtf8Coder = StringUtf8Coder.of();

    @Override
    public void encode(StringHolder value, OutputStream outStream, Context context) throws IOException {
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
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    options.setRunner(SparkPipelineRunner.class);
    Pipeline p = Pipeline.create(options);
    PCollection<StringHolder> inputWords =
        p.apply(Create.of(WORDS).withCoder(StringHolderUtf8Coder.of()));
    PCollection<StringHolder> output = inputWords.apply(new CountWords());

    DataflowAssert.that(output).containsInAnyOrder(EXPECTED_COUNT_SET);

    EvaluationResult res = SparkPipelineRunner.create().run(p);
    res.close();
  }

  /**
   * A DoFn that tokenizes lines of text into individual words.
   */
  static class ExtractWordsFn extends DoFn<StringHolder, StringHolder> {
    private final Aggregator<Long, Long> emptyLines =
        createAggregator("emptyLines", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) {
      // Split the line into words.
      String[] words = c.element().toString().split("[^a-zA-Z']+");

      // Keep track of the number of lines without any words encountered while tokenizing.
      // This aggregator is visible in the monitoring UI when run using DataflowPipelineRunner.
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
   * A DoFn that converts a Word and Count into a printable string.
   */
  private static class FormatCountsFn extends DoFn<KV<StringHolder, Long>, StringHolder> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(new StringHolder(c.element().getKey() + ": " + c.element().getValue()));
    }
  }

  private static class CountWords extends PTransform<PCollection<StringHolder>, PCollection<StringHolder>> {
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
