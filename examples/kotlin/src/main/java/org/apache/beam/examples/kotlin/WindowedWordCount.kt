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

import org.apache.beam.examples.kotlin.common.ExampleBigQueryTableOptions
import org.apache.beam.examples.kotlin.common.ExampleOptions
import org.apache.beam.examples.kotlin.common.WriteOneFilePerWindow
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.*
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.PDone
import org.joda.time.Duration
import org.joda.time.Instant
import java.io.IOException
import java.util.concurrent.ThreadLocalRandom

/**
 * An example that counts words in text, and can run over either unbounded or bounded input
 * collections.
 *
 *
 * This class, [WindowedWordCount], is the last in a series of four successively more
 * detailed 'word count' examples. First take a look at [MinimalWordCount], [WordCount],
 * and [DebuggingWordCount].
 *
 *
 * Basic concepts, also in the MinimalWordCount, WordCount, and DebuggingWordCount examples:
 * Reading text files; counting a PCollection; writing to GCS; executing a Pipeline both locally and
 * using a selected runner; defining DoFns; user-defined PTransforms; defining PipelineOptions.
 *
 *
 * New Concepts:
 *
 * <pre>
 * 1. Unbounded and bounded pipeline input modes
 * 2. Adding timestamps to data
 * 3. Windowing
 * 4. Re-using PTransforms over windowed PCollections
 * 5. Accessing the window of an element
 * 6. Writing data to per-window text files
</pre> *
 *
 *
 * By default, the examples will run with the `DirectRunner`. To change the runner,
 * specify:
 *
 * <pre>`--runner=YOUR_SELECTED_RUNNER
`</pre> *
 *
 * See examples/kotlin/README.md for instructions about how to configure different runners.
 *
 *
 * To execute this pipeline locally, specify a local output file (if using the `DirectRunner`) or output prefix on a supported distributed file system.
 *
 * <pre>`--output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
`</pre> *
 *
 *
 * The input file defaults to a public data set containing the text of King Lear, by William
 * Shakespeare. You can override it and choose your own input with `--inputFile`.
 *
 *
 * By default, the pipeline will do fixed windowing, on 10-minute windows. You can change this
 * interval by setting the `--windowSize` parameter, e.g. `--windowSize=15` for
 * 15-minute windows.
 *
 *
 * The example will try to cancel the pipeline on the signal to terminate the process (CTRL-C).
 */
public object WindowedWordCount {
    const val WINDOW_SIZE = 10 // Default window duration in minutes

    /**
     * Concept #2: A DoFn that sets the data element timestamp. This is a silly method, just for this
     * example, for the bounded data case.
     *
     *
     * Imagine that many ghosts of Shakespeare are all typing madly at the same time to recreate
     * his masterworks. Each line of the corpus will get a random associated timestamp somewhere in a
     * 2-hour period.
     */
    public class AddTimestampFn(private val minTimestamp: Instant, private val maxTimestamp: Instant) : DoFn<String, String>() {

        @ProcessElement
        fun processElement(@Element element: String, receiver: DoFn.OutputReceiver<String>) {
            val randomTimestamp = Instant(
                    ThreadLocalRandom.current()
                            .nextLong(minTimestamp.millis, maxTimestamp.millis))

            /*
       * Concept #2: Set the data element with that timestamp.
       */
            receiver.outputWithTimestamp(element, Instant(randomTimestamp))
        }
    }

    /** A [DefaultValueFactory] that returns the current system time.  */
    public class DefaultToCurrentSystemTime : DefaultValueFactory<Long> {
        override fun create(options: PipelineOptions) = System.currentTimeMillis()
    }

    /** A [DefaultValueFactory] that returns the minimum timestamp plus one hour.  */
    public class DefaultToMinTimestampPlusOneHour : DefaultValueFactory<Long> {
        override fun create(options: PipelineOptions) = (options as Options).minTimestampMillis!! + Duration.standardHours(1).millis
    }

    /**
     * Options for [WindowedWordCount].
     *
     *
     * Inherits standard example configuration options, which allow specification of the runner, as
     * well as the [WordCount.WordCountOptions] support for specification of the input and
     * output files.
     */
    public interface Options : WordCount.WordCountOptions, ExampleOptions, ExampleBigQueryTableOptions {
        @get:Description("Fixed window duration, in minutes")
        @get:Default.Integer(WINDOW_SIZE)
        var windowSize: Int?

        @get:Description("Minimum randomly assigned timestamp, in milliseconds-since-epoch")
        @get:Default.InstanceFactory(DefaultToCurrentSystemTime::class)
        var minTimestampMillis: Long?

        @get:Description("Maximum randomly assigned timestamp, in milliseconds-since-epoch")
        @get:Default.InstanceFactory(DefaultToMinTimestampPlusOneHour::class)
        var maxTimestampMillis: Long?

        @get:Description("Fixed number of shards to produce per window")
        var numShards: Int?
    }

    @Throws(IOException::class)
    @JvmStatic
    fun runWindowedWordCount(options: Options) {
        val output = options.output
        val minTimestamp = Instant(options.minTimestampMillis)
        val maxTimestamp = Instant(options.maxTimestampMillis)

        val pipeline = Pipeline.create(options)

        /*
     * Concept #1: the Beam SDK lets us run the same pipeline with either a bounded or
     * unbounded input source.
     */
        val input = pipeline
                /* Read from the GCS file. */
                .apply(TextIO.read().from(options.inputFile))
                // Concept #2: Add an element timestamp, using an artificial time just to show
                // windowing.
                // See AddTimestampFn for more detail on this.
                .apply(ParDo.of(AddTimestampFn(minTimestamp, maxTimestamp)))

        /*
     * Concept #3: Window into fixed windows. The fixed window size for this example defaults to 1
     * minute (you can change this with a command-line option). See the documentation for more
     * information on how fixed windows work, and for information on the other types of windowing
     * available (e.g., sliding windows).
     */
        val windowedWords = input.apply(
                Window.into<String>(FixedWindows.of(Duration.standardMinutes(options.windowSize!!.toLong()))))

        /*
     * Concept #4: Re-use our existing CountWords transform that does not have knowledge of
     * windows over a PCollection containing windowed values.
     */
        val wordCounts = windowedWords.apply(WordCount.CountWords())

        /*
     * Concept #5: Format the results and write to a sharded file partitioned by window, using a
     * simple ParDo operation. Because there may be failures followed by retries, the
     * writes must be idempotent, but the details of writing to files is elided here.
     */
        wordCounts
                .apply(MapElements.via(WordCount.FormatAsTextFn()))
                .apply<PDone>(WriteOneFilePerWindow(output, options.numShards))

        val result = pipeline.run()
        try {
            result.waitUntilFinish()
        } catch (exc: Exception) {
            result.cancel()
        }

    }

    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val options = (PipelineOptionsFactory.fromArgs(*args).withValidation() as Options)
        runWindowedWordCount(options)
    }
}
