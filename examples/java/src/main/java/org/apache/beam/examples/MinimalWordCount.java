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

// beam-playground:
//   name: MinimalWordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   default_example: true
//   context_line: 78
//   categories:
//     - Combiners
//     - Filtering
//     - IO
//     - Core Transforms
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - filter
//     - flatmap
//     - strings

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link MinimalWordCount}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public class MinimalWordCount {

  public static void main(String[] args) {

    // [START create_pipeline_options]
    // Create a PipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the runner you wish to use. This example
    // will run with the DirectRunner by default, based on the class path configured
    // in its dependencies.
    PipelineOptions options = PipelineOptionsFactory.create();
    // [END create_pipeline_options]

    // In order to run your pipeline, you need to make following runner specific changes:
    //
    // CHANGE 1/3: Select a Beam runner, such as BlockingDataflowRunner
    // or FlinkRunner.
    // CHANGE 2/3: Specify runner-required options.
    // For BlockingDataflowRunner, set project and temp location as follows:
    //   DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    //   dataflowOptions.setRunner(BlockingDataflowRunner.class);
    //   dataflowOptions.setProject("SET_YOUR_PROJECT_ID_HERE");
    //   dataflowOptions.setTempLocation("gs://SET_YOUR_BUCKET_NAME_HERE/AND_TEMP_DIRECTORY");
    // For FlinkRunner, set the runner as follows. See {@code FlinkPipelineOptions}
    // for more details.
    //   options.as(FlinkPipelineOptions.class)
    //      .setRunner(FlinkRunner.class);

    // [START create_pipeline]
    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);
    // [END create_pipeline]

    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
    // of input text files. TextIO.Read returns a PCollection where each element is one line from
    // the input text (a set of Shakespeare's texts).

    // This example reads from a public dataset containing the text of King Lear.
    // [START read_input]
    p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
        // [END read_input]

        // Concept #2: Apply a FlatMapElements transform the PCollection of text lines.
        // This transform splits the lines in PCollection<String>, where each element is an
        // individual word in Shakespeare's collected texts.
        // [START extract_words]
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
        // [END extract_words]

        // We use a Filter transform to avoid empty word
        // [START remove_empty_words]
        .apply(Filter.by((String word) -> !word.isEmpty()))
        // [END remove_empty_words]

        // Concept #3: Apply the Count transform to our PCollection of individual words. The Count
        // transform returns a new PCollection of key/value pairs, where each key represents a
        // unique word in the text. The associated value is the occurrence count for that word.
        // [START count_words]
        .apply(Count.perElement())
        // [END count_words]

        // Apply a MapElements transform that formats our PCollection of word counts into a
        // printable string, suitable for writing to an output file.
        // [START format_output]
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()))
        // [END format_output]

        // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
        // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
        // formatted strings) to a series of text files.
        //
        // By default, it will write to a set of files with names like wordcounts-00001-of-00005
        // [START write_output]
        .apply(TextIO.write().to("wordcounts"));
    // [END write_output]

    // [START run_pipeline]
    p.run().waitUntilFinish();
    // [END run_pipeline]
  }
}
