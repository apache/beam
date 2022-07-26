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

import org.apache.beam.examples.kotlin.common.ExampleUtils
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone

/**
 * An example that counts words in Shakespeare and includes Beam best practices.
 *
 *
 * This class, [WordCount], is the second in a series of four successively more detailed
 * 'word count' examples. You may first want to take a look at [MinimalWordCount]. After
 * you've looked at this example, then see the [DebuggingWordCount] pipeline, for introduction
 * of additional concepts.
 *
 *
 * For a detailed walkthrough of this example, see [
 * https://beam.apache.org/get-started/wordcount-example/ ](https://beam.apache.org/get-started/wordcount-example/)
 *
 *
 * Basic concepts, also in the MinimalWordCount example: Reading text files; counting a
 * PCollection; writing to text files
 *
 *
 * New Concepts:
 *
 * <pre>
 * 1. Executing a Pipeline both locally and using the selected runner
 * 2. Using ParDo with static DoFns defined out-of-line
 * 3. Building a composite transform
 * 4. Defining your own pipeline options
</pre> *
 *
 *
 * Concept #1: you can execute this pipeline either locally or using by selecting another runner.
 * These are now command-line options and not hard-coded as they were in the MinimalWordCount
 * example.
 *
 *
 * To change the runner, specify:
 *
 * <pre>`--runner=YOUR_SELECTED_RUNNER
`</pre> *
 *
 *
 * To execute this pipeline, specify a local output file (if using the `DirectRunner`) or
 * output prefix on a supported distributed file system.
 *
 * <pre>`--output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
`</pre> *
 *
 *
 * The input file defaults to a public data set containing the text of King Lear, by William
 * Shakespeare. You can override it and choose your own input with `--inputFile`.
 */
public object WordCount {

    /**
     * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
     * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it to
     * a ParDo in the pipeline.
     */
    public class ExtractWordsFn : DoFn<String, String>() {
        private val emptyLines = Metrics.counter(ExtractWordsFn::class.java, "emptyLines")
        private val lineLenDist = Metrics.distribution(ExtractWordsFn::class.java, "lineLenDistro")

        @ProcessElement
        fun processElement(@Element element: String, receiver: DoFn.OutputReceiver<String>) {
            lineLenDist.update(element.length.toLong())
            if (element.trim(' ').isEmpty()) {
                emptyLines.inc()
            }

            // Split the line into words.
            val words = element.split(ExampleUtils.TOKENIZER_PATTERN.toRegex()).toTypedArray()

            // Output each word encountered into the output PCollection.
            for (word in words) {
                if (word.isNotEmpty()) {
                    receiver.output(word)
                }
            }
        }
    }

    /** A SimpleFunction that converts a Word and Count into a printable string.  */
    public class FormatAsTextFn : SimpleFunction<KV<String, Long>, String>() {
        override fun apply(input: KV<String, Long>) = "${input.key} : ${input.value}"
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     *
     *
     * Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
     * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
     * modular testing, and an improved monitoring experience.
     */
    public class CountWords : PTransform<PCollection<String>, PCollection<KV<String, Long>>>() {
        override fun expand(lines: PCollection<String>): PCollection<KV<String, Long>> {

            // Convert lines of text into individual words.
            val words = lines.apply(ParDo.of(ExtractWordsFn()))

            // Count the number of times each word occurs.
            val wordCounts = words.apply(Count.perElement())

            return wordCounts
        }
    }

    /**
     * Options supported by [WordCount].
     *
     *
     * Concept #4: Defining your own configuration options. Here, you can add your own arguments to
     * be processed by the command-line parser, and specify default values for them. You can then
     * access the options values in your pipeline code.
     *
     *
     * Inherits standard configuration options.
     */
    public interface WordCountOptions : PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @get:Description("Path of the file to read from")
        @get:Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        var inputFile: String

        /** Set this required option to specify where to write the output.  */
        @get:Description("Path of the file to write to")
        @get:Required
        var output: String
    }

    @JvmStatic
    fun runWordCount(options: WordCountOptions) {
        val p = Pipeline.create(options)

        // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
        // static FormatAsTextFn() to the ParDo transform.
        p.apply("ReadLines", TextIO.read().from(options.inputFile))
                .apply(CountWords())
                .apply(MapElements.via(FormatAsTextFn()))
                .apply<PDone>("WriteCounts", TextIO.write().to(options.output))

        p.run().waitUntilFinish()
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val options = (PipelineOptionsFactory.fromArgs(*args).withValidation() as WordCountOptions)
        runWordCount(options)
    }
}
