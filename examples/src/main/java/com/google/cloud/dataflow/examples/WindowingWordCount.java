/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An example that counts words in Shakespeare. For a detailed walkthrough of this
 * example see:
 *   https://cloud.google.com/dataflow/java-sdk/wordcount-example
 *
 * To execute this pipeline locally, specify general pipeline configuration:
 *   --project=<PROJECT ID>
 * and example configuration:
 *   --output=[<LOCAL FILE> | gs://<OUTPUT PATH>]
 *
 * To execute this pipeline using the Dataflow service, specify pipeline configuration:
 *   --project=<PROJECT ID> --stagingLocation=gs://<STAGING DIRECTORY>
 *   --runner=BlockingDataflowPipelineRunner
 * and example configuration:
 *   --output=gs://<OUTPUT PATH>
 *
 * The input file defaults to gs://dataflow-samples/shakespeare/kinglear.txt and can be
 * overridden with --input.
 */
public class WindowingWordCount {

  /** A DoFn that tokenizes lines of text into individual words with timestamp. */
  static class ExtractWordsWithTimestampFn extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) {
      String[] words = c.element().split("[^a-zA-Z']+");
      for (String word : words) {
        if (!word.isEmpty()) {
          c.outputWithTimestamp(word, new Instant(System.currentTimeMillis()));
        }
      }
    }
  }

  /** A DoFn that converts a Word and Count into a printable string. */
  static class FormatCountsFn extends DoFn<KV<String, Long>, String> {
    @Override
    public void processElement(ProcessContext c) {
      String output = "Element: " + c.element().getKey()
          + " Value: " + c.element().getValue()
          + " Timestamp: " + c.timestamp()
          + " Windows: (" + c.windows() + ")";
      c.output(output);
    }
  }

  /**
   * A PTransform that converts a PCollection containing lines of text into a PCollection of
   * formatted word counts.
   * <p>
   * Although this pipeline fragment could be inlined, bundling it as a PTransform allows for easy
   * reuse, modular testing, and an improved monitoring experience.
   */
  public static class CountWords extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> apply(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsWithTimestampFn()));

      PCollection<String> windowedWords = words.apply(
          Window.<String>into(FixedWindows.of(Duration.millis(1))));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          windowedWords.apply(Count.<String>perElement());

      // Format each word and count into a printable string.
      PCollection<String> results = wordCounts.apply(
          ParDo.of(new FormatCountsFn()));

      return results;
    }
  }

  private interface Options extends PipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to")
    String getOutput();
    void setOutput(String value);

    /**
     * By default (numShards == 0), the system will choose the shard count.
     * Most programs will not need this option.
     */
    @Description("Number of output shards (0 if the system should choose automatically)")
    @Default.Integer(0)
    int getNumShards();
    void setNumShard(int value);
  }

  private static String getOutputLocation(Options options) {
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    if (options.getOutput() != null) {
      return options.getOutput();
    } else if (dataflowOptions.getStagingLocation() != null) {
      return GcsPath.fromUri(dataflowOptions.getStagingLocation())
          .resolve("counts.txt").toString();
    } else {
      throw new IllegalArgumentException("Must specify --output or --stagingLocation");
    }
  }



  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
     .apply(new CountWords())
     .apply(TextIO.Write.named("WriteCounts")
         .to(getOutputLocation(options))
         .withNumShards(options.getNumShards()));

    p.run();
  }
}
