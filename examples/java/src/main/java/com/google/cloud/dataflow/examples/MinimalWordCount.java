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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.KV;


/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link MinimalWordCount}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally
 * the {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting a PCollection
 *   4. Writing data to Cloud Storage as text files
 * </pre>
 *
 * <p>To execute this pipeline, first edit the code to set your project ID, the staging
 * location, and the output location. The specified GCS bucket(s) must already exist.
 *
 * <p>Then, run the pipeline as described in the README. It will be deployed and run using the
 * Dataflow service. No args are required to run the pipeline. You can see the results in your
 * output bucket in the GCS browser.
 */
public class MinimalWordCount {

  public static void main(String[] args) {
    // Create a DataflowPipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the associated Cloud Platform project and the location
    // in Google Cloud Storage to stage files.
    DataflowPipelineOptions options = PipelineOptionsFactory.create()
      .as(DataflowPipelineOptions.class);
    options.setRunner(BlockingDataflowPipelineRunner.class);
    // CHANGE 1/3: Your project ID is required in order to run your pipeline on the Google Cloud.
    options.setProject("SET_YOUR_PROJECT_ID_HERE");
    // CHANGE 2/3: Your Google Cloud Storage path is required for staging local files.
    options.setStagingLocation("gs://SET_YOUR_BUCKET_NAME_HERE/AND_STAGING_DIRECTORY");

    // Create the Pipeline object with the options we defined above.
    Pipeline p = Pipeline.create(options);

    // Apply the pipeline's transforms.

    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
    // of input text files. TextIO.Read returns a PCollection where each element is one line from
    // the input text (a set of Shakespeare's texts).
    p.apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/*"))
     // Concept #2: Apply a ParDo transform to our PCollection of text lines. This ParDo invokes a
     // DoFn (defined in-line) on each element that tokenizes the text line into individual words.
     // The ParDo returns a PCollection<String>, where each element is an individual word in
     // Shakespeare's collected texts.
     .apply(ParDo.named("ExtractWords").of(new DoFn<String, String>() {
                       @Override
                       public void processElement(ProcessContext c) {
                         for (String word : c.element().split("[^a-zA-Z']+")) {
                           if (!word.isEmpty()) {
                             c.output(word);
                           }
                         }
                       }
                     }))
     // Concept #3: Apply the Count transform to our PCollection of individual words. The Count
     // transform returns a new PCollection of key/value pairs, where each key represents a unique
     // word in the text. The associated value is the occurrence count for that word.
     .apply(Count.<String>perElement())
     // Apply a MapElements transform that formats our PCollection of word counts into a printable
     // string, suitable for writing to an output file.
     .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                       @Override
                       public String apply(KV<String, Long> input) {
                         return input.getKey() + ": " + input.getValue();
                       }
                     }))
     // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
     // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
     // formatted strings) to a series of text files in Google Cloud Storage.
     // CHANGE 3/3: The Google Cloud Storage path is required for outputting the results to.
     .apply(TextIO.Write.to("gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX"));

    // Run the pipeline.
    p.run();
  }
}
