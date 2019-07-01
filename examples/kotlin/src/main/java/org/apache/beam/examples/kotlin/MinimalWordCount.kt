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
package org.apache.beam.examples.kotlin

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors

/**
 * An example that counts words in Shakespeare.
 *
 *
 * This class, [MinimalWordCount], is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 *
 * Next, see the [WordCount] pipeline, then the [DebuggingWordCount], and finally the
 * [WindowedWordCount] pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 *
 * Concepts:
 *
 * <pre>
 * 1. Reading data from text files
 * 2. Specifying 'inline' transforms
 * 3. Counting items in a PCollection
 * 4. Writing data to text files
</pre> *
 *
 *
 * No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public object MinimalWordCount {

    @JvmStatic
    fun main(args: Array<String>) {

        // Create a PipelineOptions object. This object lets us set various execution
        // options for our pipeline, such as the runner you wish to use. This example
        // will run with the DirectRunner by default, based on the class path configured
        // in its dependencies.
        val options = PipelineOptionsFactory.create()

        // In order to run your pipeline, you need to make following runner specific changes:
        //
        // CHANGE 1/3: Select a Beam runner, such as BlockingDataflowRunner
        // or FlinkRunner.
        // CHANGE 2/3: Specify runner-required options.
        // For BlockingDataflowRunner, set project and temp location as follows:
        //   val dataflowOptions : DataflowPipelineOptions = options.as(DataflowPipelineOptions::class.java)
        //   dataflowOptions.runner = BlockingDataflowRunner::class.java
        //   dataflowOptions.project = "SET_YOUR_PROJECT_ID_HERE"
        //   dataflowOptions.tempLocation = "gs://SET_YOUR_BUCKET_NAME_HERE/AND_TEMP_DIRECTORY"
        // For FlinkRunner, set the runner as follows. See {@code FlinkPipelineOptions}
        // for more details.
        //   options.as(FlinkPipelineOptions::class.java)
        //      .setRunner(FlinkRunner::class.java)

        // Create the Pipeline object with the options we defined above
        val p = Pipeline.create(options)

        // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
        // of input text files. TextIO.Read returns a PCollection where each element is one line from
        // the input text (a set of Shakespeare's texts).

        // This example reads a public data set consisting of the complete works of Shakespeare.
        p.apply<PCollection<String>>(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))

                // Concept #2: Apply a FlatMapElements transform the PCollection of text lines.
                // This transform splits the lines in PCollection<String>, where each element is an
                // individual word in Shakespeare's collected texts.
                .apply(
                        FlatMapElements.into(TypeDescriptors.strings())
                                .via(ProcessFunction<String, List<String>> { input -> input.split("[^\\p{L}]+").toList() })
                )
                // We use a Filter transform to avoid empty word
                .apply(Filter.by(SerializableFunction<String, Boolean> { input -> !input.isEmpty() }))
                // Concept #3: Apply the Count transform to our PCollection of individual words. The Count
                // transform returns a new PCollection of key/value pairs, where each key represents a
                // unique word in the text. The associated value is the occurrence count for that word.
                .apply(Count.perElement<String>())
                // Apply a MapElements transform that formats our PCollection of word counts into a
                // printable string, suitable for writing to an output file.
                .apply(
                        MapElements.into(TypeDescriptors.strings())
                                .via(ProcessFunction<KV<String, Long>, String> { input -> "${input.key} : ${input.value}" })
                )
                // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
                // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
                // formatted strings) to a series of text files.
                //
                // By default, it will write to a set of files with names like wordcounts-00001-of-00005
                .apply(TextIO.write().to("wordcounts"))

        p.run().waitUntilFinish()
    }
}
