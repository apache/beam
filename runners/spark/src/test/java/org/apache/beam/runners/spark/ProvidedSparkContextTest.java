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
import static org.junit.Assert.assertThrows;

import org.apache.beam.runners.spark.examples.WordCount;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Provided Spark Context tests.
 *
 * <p>Note: These tests are run sequentially ordered by their name to reuse the Spark context and
 * speed up testing.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ProvidedSparkContextTest {
  private static final String[] WORDS_ARRAY = {
    "hi there", "hi", "hi sue bob",
    "hi sue", "", "bob hi"
  };
  private static final ImmutableList<String> WORDS = ImmutableList.copyOf(WORDS_ARRAY);
  private static final ImmutableSet<String> EXPECTED_COUNT_SET =
      ImmutableSet.of("hi: 5", "there: 1", "sue: 2", "bob: 2");
  private static final String PROVIDED_CONTEXT_EXCEPTION =
      "The provided Spark context was not created or was stopped";

  @ClassRule
  public static SparkContextOptionsRule contextOptionsRule = new SparkContextOptionsRule();

  /** Provide a context and call pipeline run. */
  @Test
  public void testAWithProvidedContext() throws Exception {
    Pipeline p = createPipeline();
    PipelineResult result = p.run(); // Run test from pipeline
    result.waitUntilFinish();
    TestPipeline.verifyPAssertsSucceeded(p, result);
    // A provided context must not be stopped after execution
    assertFalse(contextOptionsRule.getSparkContext().sc().isStopped());
  }

  /** A SparkRunner with a stopped provided Spark context cannot run pipelines. */
  @Test
  public void testBWithStoppedProvidedContext() {
    // Stop the provided Spark context
    contextOptionsRule.getSparkContext().sc().stop();
    assertThrows(
        PROVIDED_CONTEXT_EXCEPTION,
        RuntimeException.class,
        () -> createPipeline().run().waitUntilFinish());
  }

  /** Provide a context and call pipeline run. */
  @Test
  public void testCWithNullContext() {
    contextOptionsRule.getOptions().setProvidedSparkContext(null);
    assertThrows(
        PROVIDED_CONTEXT_EXCEPTION,
        RuntimeException.class,
        () -> createPipeline().run().waitUntilFinish());
  }

  private Pipeline createPipeline() {
    Pipeline p = Pipeline.create(contextOptionsRule.getOptions());
    PCollection<String> inputWords = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));
    PCollection<String> output =
        inputWords
            .apply(new WordCount.CountWords())
            .apply(MapElements.via(new WordCount.FormatAsTextFn()));
    // Run test from pipeline
    PAssert.that(output).containsInAnyOrder(EXPECTED_COUNT_SET);
    return p;
  }
}
