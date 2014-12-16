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
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * An example that counts words in Shakespeare. For a detailed walkthrough of this
 * example see:
 *   https://cloud.google.com/dataflow/java-sdk/wordcount-example
 *
 * <p> Concepts: Reading/writing text files; counting a PCollection; user-defined PTransforms
 *
 * <p> To execute this pipeline locally, specify general pipeline configuration:
 *   --project=<PROJECT ID>
 * and a local output file or output prefix on GCS:
 *   --output=[<LOCAL FILE> | gs://<OUTPUT PREFIX>]
 *
 * <p> To execute this pipeline using the Dataflow service, specify pipeline configuration:
 *   --project=<PROJECT ID>
 *   --stagingLocation=gs://<STAGING DIRECTORY>
 *   --runner=BlockingDataflowPipelineRunner
 * and an output prefix on GCS:
 *   --output=gs://<OUTPUT PREFIX>
 *
 * <p> The input file defaults to gs://dataflow-samples/shakespeare/kinglear.txt and can be
 * overridden with --input.
 */
public class WordCount {

  /** A DoFn that tokenizes lines of text into individual words. */
  static class ExtractWordsFn extends DoFn<String, String> {
    private Aggregator<Long> emptyLines;

    @Override
    public void startBundle(Context c) {
      emptyLines = c.createAggregator("emptyLines", new Sum.SumLongFn());
    }

    @Override
    public void processElement(ProcessContext c) {
      // Keep track of the number of empty lines. (When using the [Blocking]DataflowPipelineRunner,
      // Aggregators are shown in the monitoring UI.)
      if (c.element().trim().isEmpty()) {
        emptyLines.addValue(1L);
      }

      // Split the line into words.
      String[] words = c.element().split("[^a-zA-Z']+");

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  /** A DoFn that converts a Word and Count into a printable string. */
  static class FormatCountsFn extends DoFn<KV<String, Long>, String> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().getKey() + ": " + c.element().getValue());
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
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      // Format each word and count into a printable string.
      PCollection<String> results = wordCounts.apply(
          ParDo.of(new FormatCountsFn()));

      return results;
    }
  }

  /**
   * Options supported by {@link WordCount}.
   * <p>
   * Inherits standard configuration options.
   */
  public static interface Options extends PipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to")
    @Default.InstanceFactory(OutputFactory.class)
    String getOutput();
    void setOutput(String value);

    /**
     * Returns gs://${STAGING_LOCATION}/"counts.txt" as the default destination.
     */
    public static class OutputFactory implements DefaultValueFactory<String> {
      @Override
      public String create(PipelineOptions options) {
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        if (dataflowOptions.getStagingLocation() != null) {
          return GcsPath.fromUri(dataflowOptions.getStagingLocation())
              .resolve("counts.txt").toString();
        } else {
          throw new IllegalArgumentException("Must specify --output or --stagingLocation");
        }
      }
    }

     /**
     * By default (numShards == 0), the system will choose the shard count.
     * Most programs will not need this option.
     */
    @Description("Number of output shards (0 if the system should choose automatically)")
    int getNumShards();
    void setNumShards(int value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
     .apply(new CountWords())
     .apply(TextIO.Write.named("WriteCounts")
         .to(options.getOutput())
         .withNumShards(options.getNumShards()));

    p.run();
  }
}

