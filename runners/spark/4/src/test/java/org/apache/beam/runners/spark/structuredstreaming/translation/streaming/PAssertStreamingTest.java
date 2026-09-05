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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end tests verifying {@link PAssert} evaluations against the Spark 4 Structured Streaming
 * runner.
 *
 * <p>PAssert regroups elements and verifies expectations once a window closes upon watermark
 * passage. In streaming, this relies on the end-of-stream sentinel row emitted when unbounded
 * readers reach exhaustion to advance Spark's data-driven watermark past the global window end.
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class PAssertStreamingTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  private static final Instant BASE = new Instant(0);

  private static class DoubleFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void process(@Element Integer element, OutputReceiver<Integer> out) {
      out.output(element * 2);
    }
  }

  @Test
  public void testPAssertInGlobalWindow() throws Exception {
    List<TimestampedValue<Integer>> elements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      elements.add(TimestampedValue.of(i, BASE.plus(Duration.standardSeconds(i))));
    }

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> output =
        pipeline
            .apply(
                "ReadUnbounded",
                Read.from(
                    new StreamingTestUtils.ListBackedUnboundedSource<>(
                        elements, VarIntCoder.of(), true)))
            .apply("Double", ParDo.of(new DoubleFn()));

    PAssert.that(output).containsInAnyOrder(0, 2, 4, 6, 8, 10, 12, 14, 16, 18);

    StreamingTestUtils.run(pipeline);
  }

  @Test
  public void testPAssertInFixedWindows() throws Exception {
    List<TimestampedValue<Integer>> elements = new ArrayList<>();
    // Window 1: [0s, 5s) -> elements 0, 1, 2 (doubled: 0, 2, 4)
    elements.add(TimestampedValue.of(0, BASE.plus(Duration.standardSeconds(1))));
    elements.add(TimestampedValue.of(1, BASE.plus(Duration.standardSeconds(2))));
    elements.add(TimestampedValue.of(2, BASE.plus(Duration.standardSeconds(4))));
    // Window 2: [5s, 10s) -> elements 3, 4 (doubled: 6, 8)
    elements.add(TimestampedValue.of(3, BASE.plus(Duration.standardSeconds(6))));
    elements.add(TimestampedValue.of(4, BASE.plus(Duration.standardSeconds(8))));

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> output =
        pipeline
            .apply(
                "ReadUnbounded",
                Read.from(
                    new StreamingTestUtils.ListBackedUnboundedSource<>(
                        elements, VarIntCoder.of(), true)))
            .apply("FixedWindows", Window.into(FixedWindows.of(Duration.standardSeconds(5))))
            .apply("Double", ParDo.of(new DoubleFn()));

    PAssert.that(output).containsInAnyOrder(0, 2, 4, 6, 8);

    StreamingTestUtils.run(pipeline);
  }

  @Test
  public void testPAssertFailureThrows() throws Exception {
    List<TimestampedValue<Integer>> elements = new ArrayList<>();
    elements.add(TimestampedValue.of(1, BASE.plus(Duration.standardSeconds(1))));

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> output =
        pipeline
            .apply(
                "ReadUnbounded",
                Read.from(
                    new StreamingTestUtils.ListBackedUnboundedSource<>(
                        elements, VarIntCoder.of(), true)))
            .apply("Double", ParDo.of(new DoubleFn()));

    // Deliberately incorrect expectation: output is [2], expected is [999].
    PAssert.that(output).containsInAnyOrder(999);

    Exception e = assertThrows(Exception.class, () -> StreamingTestUtils.run(pipeline));
    Throwable root = e;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    org.junit.Assert.assertTrue(
        "Expected AssertionError at root of cause chain, got: " + root,
        root instanceof AssertionError);
  }

  @Test
  public void testPAssertFailureThrowsInFixedWindows() throws Exception {
    List<TimestampedValue<Integer>> elements = new ArrayList<>();
    elements.add(TimestampedValue.of(1, BASE.plus(Duration.standardSeconds(1))));

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> output =
        pipeline
            .apply(
                "ReadUnbounded",
                Read.from(
                    new StreamingTestUtils.ListBackedUnboundedSource<>(
                        elements, VarIntCoder.of(), true)))
            .apply("FixedWindows", Window.into(FixedWindows.of(Duration.standardSeconds(5))))
            .apply("Double", ParDo.of(new DoubleFn()));

    // Deliberately incorrect expectation: output is [2], expected is [999]. This proves the
    // arrival side watermark clamp does not mask genuine failures in the rewindowed path.
    PAssert.that(output).containsInAnyOrder(999);

    Exception e = assertThrows(Exception.class, () -> StreamingTestUtils.run(pipeline));
    Throwable root = e;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    org.junit.Assert.assertTrue(
        "Expected AssertionError at root of cause chain, got: " + root,
        root instanceof AssertionError);
  }
}
