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
package org.apache.beam.examples

import org.apache.beam.examples.kotlin.WindowedWordCount
import org.apache.beam.examples.kotlin.common.ExampleUtils
import org.apache.beam.examples.kotlin.common.WriteOneFilePerWindow.PerWindowFiles
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.io.FileBasedSink
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.testing.SerializableMatcher
import org.apache.beam.sdk.testing.StreamingIT
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.TestPipelineOptions
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.util.*
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Description
import org.hamcrest.Matchers.equalTo
import org.hamcrest.TypeSafeMatcher
import org.joda.time.Duration
import org.joda.time.Instant
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.rules.TestName
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.util.*
import java.util.concurrent.ThreadLocalRandom

/** End-to-end integration test of [WindowedWordCount].  */
@RunWith(JUnit4::class)
class WindowedWordCountITKotlin {

    @Rule
    var testName = TestName()

    /** Options for the [WindowedWordCount] Integration Test.  */
    interface WindowedWordCountITOptions : WindowedWordCount.Options, TestPipelineOptions, StreamingOptions

    @Test
    @Throws(Exception::class)
    fun testWindowedWordCountInBatchDynamicSharding() {
        val options = batchOptions()
        // This is the default value, but make it explicit.
        options.numShards = null
        testWindowedWordCountPipeline(options)
    }

    @Test
    @Throws(Exception::class)
    fun testWindowedWordCountInBatchStaticSharding() {
        val options = batchOptions()
        options.numShards = 3
        testWindowedWordCountPipeline(options)
    }

    // TODO: add a test with streaming and dynamic sharding after resolving
    // https://issues.apache.org/jira/browse/BEAM-1438

    @Test
    @Category(StreamingIT::class)
    @Throws(Exception::class)
    fun testWindowedWordCountInStreamingStaticSharding() {
        val options = streamingOptions()
        options.numShards = 3
        testWindowedWordCountPipeline(options)
    }

    @Throws(Exception::class)
    private fun defaultOptions(): WindowedWordCountITOptions {
        val options = TestPipeline.testingPipelineOptions().`as`(WindowedWordCountITOptions::class.java)
        options.inputFile = DEFAULT_INPUT
        options.testTimeoutSeconds = 1200L

        options.minTimestampMillis = 0L
        options.minTimestampMillis = Duration.standardHours(1).millis
        options.windowSize = 10

        options.output = FileSystems.matchNewResource(options.tempRoot, true)
                .resolve(
                        String.format(
                                "WindowedWordCountITKotlin.%s-%tFT%<tH:%<tM:%<tS.%<tL+%s",
                                testName.methodName, Date(), ThreadLocalRandom.current().nextInt()),
                        StandardResolveOptions.RESOLVE_DIRECTORY)
                .resolve("output", StandardResolveOptions.RESOLVE_DIRECTORY)
                .resolve("results", StandardResolveOptions.RESOLVE_FILE)
                .toString()
        return options
    }

    @Throws(Exception::class)
    private fun streamingOptions(): WindowedWordCountITOptions {
        val options = defaultOptions()
        options.isStreaming = true
        return options
    }

    @Throws(Exception::class)
    private fun batchOptions(): WindowedWordCountITOptions {
        val options = defaultOptions()
        // This is the default value, but make it explicit
        options.isStreaming = false
        return options
    }

    @Throws(Exception::class)
    private fun testWindowedWordCountPipeline(options: WindowedWordCountITOptions) {

        val output = FileBasedSink.convertToFileResourceIfPossible(options.output)
        val filenamePolicy = PerWindowFiles(output)

        val expectedOutputFiles = Lists.newArrayListWithCapacity<ShardedFile>(6)

        for (startMinute in ImmutableList.of(0, 10, 20, 30, 40, 50)) {
            val windowStart = Instant(options.minTimestampMillis).plus(Duration.standardMinutes(startMinute.toLong()))
            val filePrefix = filenamePolicy.filenamePrefixForWindow(
                    IntervalWindow(windowStart, windowStart.plus(Duration.standardMinutes(10))))
            expectedOutputFiles.add(
                    NumberedShardedFile(
                            output
                                    .currentDirectory
                                    .resolve(filePrefix, StandardResolveOptions.RESOLVE_FILE)
                                    .toString() + "*"))
        }

        val inputFile = ExplicitShardedFile(setOf(options.inputFile))

        // For this integration test, input is tiny and we can build the expected counts
        val expectedWordCounts = TreeMap<String, Long>()
        for (line in inputFile.readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff())) {
            val words = line.split(ExampleUtils.TOKENIZER_PATTERN.toRegex()).toTypedArray()

            for (word in words) {
                if (word.isNotEmpty()) {
                    expectedWordCounts[word] = MoreObjects.firstNonNull(expectedWordCounts[word], 0L) + 1L
                }
            }
        }

        options.onSuccessMatcher = WordCountsMatcher(expectedWordCounts, expectedOutputFiles)

        WindowedWordCount.runWindowedWordCount(options)
    }

    /**
     * A matcher that bakes in expected word counts, so they can be read directly via some other
     * mechanism, and compares a sharded output file with the result.
     */
    private class WordCountsMatcher(
            private val expectedWordCounts: SortedMap<String, Long>, private val outputFiles: List<ShardedFile>) : TypeSafeMatcher<PipelineResult>(), SerializableMatcher<PipelineResult> {
        private var actualCounts: SortedMap<String, Long>? = null

        public override fun matchesSafely(pipelineResult: PipelineResult): Boolean {
            try {
                // Load output data
                val outputLines = ArrayList<String>()
                for (outputFile in outputFiles) {
                    outputLines.addAll(
                            outputFile.readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff()))
                }

                // Since the windowing is nondeterministic we only check the sums
                actualCounts = TreeMap()
                for (line in outputLines) {
                    val splits = line.split(": ".toRegex()).toTypedArray()
                    val word = splits[0]
                    val count = java.lang.Long.parseLong(splits[1])
                    (actualCounts as java.util.Map<String, Long>).merge(word, count) { a, b -> a + b }
                }
                return actualCounts == expectedWordCounts
            } catch (e: Exception) {
                throw RuntimeException(
                        String.format("Failed to read from sharded output: %s due to exception", outputFiles), e)
            }

        }

        override fun describeTo(description: Description) {
            equalTo(expectedWordCounts).describeTo(description)
        }

        public override fun describeMismatchSafely(pResult: PipelineResult, description: Description) {
            equalTo(expectedWordCounts).describeMismatch(actualCounts, description)
        }
    }

    companion object {

        private const val DEFAULT_INPUT = "gs://apache-beam-samples/shakespeare/sonnets.txt"
        private const val MAX_READ_RETRIES = 4
        private val DEFAULT_SLEEP_DURATION = Duration.standardSeconds(10L)
        internal val BACK_OFF_FACTORY = FluentBackoff.DEFAULT
                .withInitialBackoff(DEFAULT_SLEEP_DURATION)
                .withMaxRetries(MAX_READ_RETRIES)

        @BeforeClass
        fun setUp() {
            PipelineOptionsFactory.register(TestPipelineOptions::class.java)
        }
    }
}
