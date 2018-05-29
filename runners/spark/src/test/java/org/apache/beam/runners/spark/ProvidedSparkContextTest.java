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

package org.apache.beam.runners.spark;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.spark.examples.WordCount;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

/**
 * Provided Spark Context tests.
 */
public class ProvidedSparkContextTest {
    private static final String[] WORDS_ARRAY = {
            "hi there", "hi", "hi sue bob",
            "hi sue", "", "bob hi"};
    private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
    private static final Set<String> EXPECTED_COUNT_SET =
            ImmutableSet.of("hi: 5", "there: 1", "sue: 2", "bob: 2");
    private static final String PROVIDED_CONTEXT_EXCEPTION =
            "The provided Spark context was not created or was stopped";

    /**
     * Provide a context and call pipeline run.
     * @throws Exception
     */
    @Test
    public void testWithProvidedContext() throws Exception {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "Existing_Context");
        testWithValidProvidedContext(jsc);
        // A provided context must not be stopped after execution
        assertFalse(jsc.sc().isStopped());
        jsc.stop();
    }

    /**
     * Provide a context and call pipeline run.
     * @throws Exception
     */
    @Test
    public void testWithNullContext() throws Exception {
        testWithInvalidContext(null);
    }

    /**
     * A SparkRunner with a stopped provided Spark context cannot run pipelines.
     * @throws Exception
     */
    @Test
    public void testWithStoppedProvidedContext() throws Exception {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "Existing_Context");
        // Stop the provided Spark context directly
        jsc.stop();
        testWithInvalidContext(jsc);
    }

    private void testWithValidProvidedContext(JavaSparkContext jsc) throws Exception {
        SparkContextOptions options = getSparkContextOptions(jsc);

        Pipeline p = Pipeline.create(options);
        PCollection<String> inputWords = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder
                .of()));
        PCollection<String> output = inputWords.apply(new WordCount.CountWords())
                .apply(MapElements.via(new WordCount.FormatAsTextFn()));

        PAssert.that(output).containsInAnyOrder(EXPECTED_COUNT_SET);

        // Run test from pipeline
        PipelineResult result = p.run();

        TestPipeline.verifyPAssertsSucceeded(p, result);
    }

    private void testWithInvalidContext(JavaSparkContext jsc) {
        SparkContextOptions options = getSparkContextOptions(jsc);

        Pipeline p = Pipeline.create(options);
        PCollection<String> inputWords = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder
                .of()));
        inputWords.apply(new WordCount.CountWords())
                .apply(MapElements.via(new WordCount.FormatAsTextFn()));

        try {
            p.run().waitUntilFinish();
            fail("Should throw an exception when The provided Spark context is null or stopped");
        } catch (RuntimeException e) {
            assert(e.getMessage().contains(PROVIDED_CONTEXT_EXCEPTION));
        }
    }

    private static SparkContextOptions getSparkContextOptions(JavaSparkContext jsc) {
        final SparkContextOptions options = PipelineOptionsFactory.as(SparkContextOptions.class);
        options.setRunner(TestSparkRunner.class);
        options.setUsesProvidedSparkContext(true);
        options.setProvidedSparkContext(jsc);
        options.setEnableSparkMetricSinks(false);
        return options;
    }
}
