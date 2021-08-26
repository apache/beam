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
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.slf4j.LoggerFactory
import java.util.regex.Pattern

/**
 * An example that verifies word counts in Shakespeare and includes Beam best practices.
 *
 *
 * This class, [DebuggingWordCount], is the third in a series of four successively more
 * detailed 'word count' examples. You may first want to take a look at [MinimalWordCount] and
 * [WordCount]. After you've looked at this example, then see the [WindowedWordCount]
 * pipeline, for introduction of additional concepts.
 *
 *
 * Basic concepts, also in the MinimalWordCount and WordCount examples: Reading text files;
 * counting a PCollection; executing a Pipeline both locally and using a selected runner; defining
 * DoFns.
 *
 *
 * New Concepts:
 *
 * <pre>
 * 1. Logging using SLF4J, even in a distributed environment
 * 2. Creating a custom metric (runners have varying levels of support)
 * 3. Testing your Pipeline via PAssert
</pre> *
 *
 *
 * To execute this pipeline locally, specify general pipeline configuration:
 *
 * <pre>`--project=YOUR_PROJECT_ID
`</pre> *
 *
 *
 * To change the runner, specify:
 *
 * <pre>`--runner=YOUR_SELECTED_RUNNER
`</pre> *
 *
 *
 * The input file defaults to a public data set containing the text of of King Lear, by William
 * Shakespeare. You can override it and choose your own input with `--inputFile`.
 */
public object DebuggingWordCount {
    /** A DoFn that filters for a specific key based upon a regular expression.  */
    public class FilterTextFn(pattern: String) : DoFn<KV<String, Long>, KV<String, Long>>() {

        private val filter: Pattern = Pattern.compile(pattern)

        /**
         * Concept #2: A custom metric can track values in your pipeline as it runs. Each runner
         * provides varying levels of support for metrics, and may expose them in a dashboard, etc.
         */
        private val matchedWords = Metrics.counter(FilterTextFn::class.java, "matchedWords")

        private val unmatchedWords = Metrics.counter(FilterTextFn::class.java, "unmatchedWords")

        @ProcessElement
        fun processElement(c: ProcessContext) {
            if (filter.matcher(c.element().key).matches()) {
                // Log at the "DEBUG" level each element that we match. When executing this pipeline
                // these log lines will appear only if the log level is set to "DEBUG" or lower.
                LOG.debug("Matched: ${c.element().key}")
                matchedWords.inc()
                c.output(c.element())
            } else {
                // Log at the "TRACE" level each element that is not matched. Different log levels
                // can be used to control the verbosity of logging providing an effective mechanism
                // to filter less important information.
                LOG.trace("Did not match: ${c.element().key}")
                unmatchedWords.inc()
            }
        }

        companion object {
            /**
             * Concept #1: The logger below uses the fully qualified class name of FilterTextFn as the
             * logger. Depending on your SLF4J configuration, log statements will likely be qualified by
             * this name.
             *
             *
             * Note that this is entirely standard SLF4J usage. Some runners may provide a default SLF4J
             * configuration that is most appropriate for their logging integration.
             */
            private val LOG = LoggerFactory.getLogger(FilterTextFn::class.java)
        }
    }

    /**
     * Options supported by [DebuggingWordCount].
     *
     *
     * Inherits standard configuration options and all options defined in [ ].
     */
    public interface WordCountOptions : WordCount.WordCountOptions {

        @get:Description("Regex filter pattern to use in DebuggingWordCount. " + "Only words matching this pattern will be counted.")
        @get:Default.String("Flourish|stomach")
        var filterPattern: String
    }

    @JvmStatic
    fun runDebuggingWordCount(options: WordCountOptions) {
        val p = Pipeline.create(options)

        val filteredWords = p.apply("ReadLines", TextIO.read().from(options.inputFile))
                .apply(WordCount.CountWords())
                .apply(ParDo.of(FilterTextFn(options.filterPattern)))

        /*
     * Concept #3: PAssert is a set of convenient PTransforms in the style of
     * Hamcrest's collection matchers that can be used when writing Pipeline level tests
     * to validate the contents of PCollections. PAssert is best used in unit tests
     * with small data sets but is demonstrated here as a teaching tool.
     *
     * <p>Below we verify that the set of filtered words matches our expected counts. Note
     * that PAssert does not provide any output and that successful completion of the
     * Pipeline implies that the expectations were met. Learn more at
     * https://beam.apache.org/documentation/pipelines/test-your-pipeline/ on how to test
     * your Pipeline and see {@link DebuggingWordCountTest} for an example unit test.
     */
        val expectedResults = listOf(KV.of("Flourish", 3L), KV.of("stomach", 1L))
        PAssert.that(filteredWords).containsInAnyOrder(expectedResults)

        p.run().waitUntilFinish()
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val options = (PipelineOptionsFactory.fromArgs(*args).withValidation() as WordCountOptions)
        runDebuggingWordCount(options)
    }
}
