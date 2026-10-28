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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.TestUnboundedSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End to end tests for {@link PAssert} and {@link Create} on Spark 4 Structured Streaming. */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class PAssertStreamingTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String TAG_GLOBAL = "passert-global";
  private static final String TAG_UNEVEN = "passert-uneven";
  private static final String TAG_WINDOWED = "passert-windowed";
  private static final String TAG_EMPTY = "passert-empty";
  private static final String TAG_FAIL = "passert-fail";

  private SparkStructuredStreamingPipelineOptions options() throws IOException {
    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(tempFolder);
    options.setTestMode(true);
    return options;
  }

  @After
  public void tearDown() {
    TestUnboundedSource.forget(TAG_GLOBAL);
    TestUnboundedSource.forget(TAG_UNEVEN);
    TestUnboundedSource.forget(TAG_WINDOWED);
    TestUnboundedSource.forget(TAG_EMPTY);
    TestUnboundedSource.forget(TAG_FAIL);
  }

  @Test
  public void testContainsInAnyOrderGlobalWindow() throws Exception {
    PCollection<String> out =
        pipeline.apply("Read", Read.from(new TestUnboundedSource(TAG_GLOBAL, 2, 4)));
    PAssert.that(out).containsInAnyOrder(TestUnboundedSource.elements(TAG_GLOBAL, 2, 4));
    pipeline.run(options());
  }

  @Test
  public void testContainsInAnyOrderUnevenShards() throws Exception {
    int[] shardCounts = {1, 4};
    PCollection<String> out =
        pipeline.apply("Read", Read.from(new TestUnboundedSource(TAG_UNEVEN, shardCounts)));
    PAssert.that(out).containsInAnyOrder(TestUnboundedSource.elements(TAG_UNEVEN, shardCounts));
    SparkStructuredStreamingPipelineOptions options = options();
    options.setMaxRecordsPerBatch(2L);
    pipeline.run(options);
  }

  @Test
  public void testContainsInAnyOrderFixedWindows() throws Exception {
    PCollection<String> out =
        pipeline
            .apply("Read", Read.from(new TestUnboundedSource(TAG_WINDOWED, 1, 4)))
            .apply("Window", Window.into(FixedWindows.of(Duration.millis(2000))));
    PAssert.that(out).containsInAnyOrder(TestUnboundedSource.elements(TAG_WINDOWED, 1, 4));
    pipeline.run(options());
  }

  @Test
  public void testEmpty() throws Exception {
    PCollection<String> out =
        pipeline.apply("Read", Read.from(new TestUnboundedSource(TAG_EMPTY, 1, 0)));
    PAssert.that(out).empty();
    pipeline.run(options());
  }

  @Test
  public void testFailingAssertionFailsPipeline() throws Exception {
    PCollection<String> out =
        pipeline.apply("Read", Read.from(new TestUnboundedSource(TAG_FAIL, 1, 2)));
    PAssert.that(out).containsInAnyOrder("wrong");
    SparkStructuredStreamingPipelineOptions options = options();
    Pipeline.PipelineExecutionException thrown =
        assertThrows(Pipeline.PipelineExecutionException.class, () -> pipeline.run(options));
    assertTrue(
        "Expected PAssert AssertionError in causal chain of " + thrown,
        Throwables.getCausalChain(thrown).stream()
            .anyMatch(
                t ->
                    t instanceof AssertionError
                        && t.getMessage() != null
                        && t.getMessage().contains("wrong")));
  }

  @Test
  public void testCreateSingleElement() throws Exception {
    PCollection<String> out = pipeline.apply(Create.of("only"));
    PAssert.that(out).containsInAnyOrder("only");
    pipeline.run(options());
  }

  @Test
  public void testCreateMultipleElements() throws Exception {
    PCollection<String> out = pipeline.apply(Create.of("one", "two", "three"));
    PAssert.that(out).containsInAnyOrder("one", "two", "three");
    pipeline.run(options());
  }
}
