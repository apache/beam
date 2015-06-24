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
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;


/**
 * An example that counts words in Shakespeare.
 *
 * <p> This class, {@link MinimalWordCount}, is the first in a series of three successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p> Next, see the {@link WordCount} pipeline, and then the {@link WindowedWordCount} pipeline,
 * for more detailed examples that introduce additional concepts.
 *
 * <p> Concepts:
 * <ol>
 *   <li>Reading data from text files.</li>
 *   <li>Specifying 'inline' transforms.</li>
 *   <li>Counting a PCollection.</li>
 *   <li>Writing data to Cloud Storage as text files.</li>
 * </ol>
 *
 * <p> To execute this pipeline, first edit the code to set your project name and Google Cloud
 * Storage values. The specified GCS bucket(s) must already exist.
 *
 * <p> Then, run the pipeline as described in the README. It will be deployed and run using the
 * Dataflow service. No args are required to run the pipeline. You can see the results in your
 * output bucket in the GCS browser.
 */


public class MinimalWordCount {

  public static void main(String[] args) {

    // Create a DataflowPipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the associated Cloud Platform project and a location in
    // Google Cloud Storage to stage files.
    DataflowPipelineOptions options = PipelineOptionsFactory.create()
      .as(DataflowPipelineOptions.class);
    options.setRunner(BlockingDataflowPipelineRunner.class);
    // TODO: CHANGE THE FOLLOWING TWO SETTINGS.
    // Your project name is required in order to run your pipeline on the Google Cloud.
    options.setProject("SET-YOUR-PROJECT-NAME-HERE");
    // Your Google Cloud Storage path for staging local files.
    options.setStagingLocation("gs://SET-YOUR-BUCKET-NAME-HERE");

    // Create the Pipeline object with the options we defined above.
    Pipeline p = Pipeline.create(options);

    // Apply the pipeline's transforms.

    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
    // of input text files. TextIO.Read returns a PCollection where each element is one line from
    // the input text (a set of Shakespeare's texts).
    p.apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/*"))
     // Concept #2:  Apply a ParDo transform to our PCollection of text lines. This ParDo invokes a
     // DoFn (defined in-line) on each element that tokenizes the text line into individual words.
     // The ParDo returns a PCollection<String>, where each element is an individual word in
     // Shakespeare's collected texts.
     .apply(ParDo.named("ExtractWords").of(new DoFn<String, String>() {
                       private static final long serialVersionUID = 0;
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
     // Apply another ParDo transform that formats our PCollection of word counts into a printable
     // string, suitable for writing to an output file.
     .apply(ParDo.named("FormatResults").of(new DoFn<KV<String, Long>, String>() {
                       private static final long serialVersionUID = 0;
                       @Override
                       public void processElement(ProcessContext c) {
                         c.output(c.element().getKey() + ": " + c.element().getValue());
                       }
                     }))
     // TODO: SPECIFY YOUR OUTPUT GCS PATH
     // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
     // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
     // formatted strings) to a series of text files in Google Cloud Storage.
     .apply(TextIO.Write.to("gs://YOUR-OUTPUT-BUCKET/AND-PREFIX"));

    // Run the pipeline.
    p.run();
  }
}
