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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.TestUnboundedSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * State transitions for a streaming pipeline.
 *
 * <p>Pipelines observe RUNNING, DONE once idle, CANCELLED on cancel, and FAILED on query failure.
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class StreamingPipelineLifecycleTest implements Serializable {

  /** Session shared across tests. */
  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  /** How long to wait for a query to start before failing. */
  private static final long QUERY_START_TIMEOUT_MILLIS = 60_000L;

  @After
  public void tearDown() {
    TestUnboundedSource.forget("lifecycle-done");
    TestUnboundedSource.forget("lifecycle-cancel");
    TestUnboundedSource.forget("lifecycle-healthy");
    TestUnboundedSource.forget("lifecycle-poison");
  }

  /** Blocks until at least one streaming query is active on the shared session. */
  private static void awaitQueryStarted() throws InterruptedException {
    long deadline = System.currentTimeMillis() + QUERY_START_TIMEOUT_MILLIS;
    while (SESSION.getSession().streams().active().length == 0) {
      assertTrue(
          "no streaming query started within " + QUERY_START_TIMEOUT_MILLIS + "ms",
          System.currentTimeMillis() < deadline);
      Thread.sleep(50L);
    }
  }

  @Test
  public void idlePipelineGoesFromRunningToDoneOnceIdle() throws Exception {
    String tag = "lifecycle-done";
    String collectorId = StreamingTestUtils.newCollectorId(tag);
    StreamingTestUtils.clear(collectorId);

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("ReadUnbounded", Read.from(new TestUnboundedSource(tag, 1, 10)))
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = pipeline.run();
    assertEquals(PipelineResult.State.RUNNING, result.getState());

    PipelineResult.State finalState = StreamingTestUtils.waitUntilFinish(result);
    assertEquals(PipelineResult.State.DONE, finalState);
    assertEquals(PipelineResult.State.DONE, result.getState());

    Set<String> collected = new HashSet<>(StreamingTestUtils.<String>getCollected(collectorId));
    assertEquals(TestUnboundedSource.elements(tag, 1, 10), collected);
  }

  @Test
  public void cancelStopsTheQueryAndReportsCancelled() throws Exception {
    String tag = "lifecycle-cancel";
    String collectorId = StreamingTestUtils.newCollectorId(tag);
    StreamingTestUtils.clear(collectorId);

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    // Idle stop disabled so the query stops only from explicit cancel.
    options.setStreamingStopAfterIdleBatches(-1);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("ReadUnbounded", Read.from(new TestUnboundedSource(tag, 1, 10)))
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = pipeline.run();
    assertEquals(PipelineResult.State.RUNNING, result.getState());

    awaitQueryStarted();

    PipelineResult.State cancelledState = result.cancel();
    assertEquals(PipelineResult.State.CANCELLED, cancelledState);
    assertEquals(PipelineResult.State.CANCELLED, result.getState());

    long deadline = System.currentTimeMillis() + QUERY_START_TIMEOUT_MILLIS;
    while (SESSION.getSession().streams().active().length > 0) {
      assertTrue(
          "the streaming query was still active " + QUERY_START_TIMEOUT_MILLIS + "ms after cancel",
          System.currentTimeMillis() < deadline);
      Thread.sleep(50L);
    }
  }

  /** A failure in any leaf query surfaces through waitUntilFinish. */
  @Test
  public void failingLeafQueryFailsThePipelineAndStopsHealthySibling() throws Exception {
    String healthyTag = "lifecycle-healthy";
    String poisonTag = "lifecycle-poison";
    String collectorId = StreamingTestUtils.newCollectorId(healthyTag);
    StreamingTestUtils.clear(collectorId);

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    // Idle stop disabled so the healthy query stops only when the sibling failure stops it.
    options.setStreamingStopAfterIdleBatches(-1);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("ReadHealthy", Read.from(new TestUnboundedSource(healthyTag, 1, 10)))
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));
    pipeline
        .apply("ReadPoisoned", Read.from(new TestUnboundedSource(poisonTag, 1, 10)))
        .apply("Throw", ParDo.of(new ThrowOnElementDoFn(5)));

    PipelineResult result = pipeline.run();

    assertThrows(RuntimeException.class, () -> StreamingTestUtils.waitUntilFinish(result));
    assertEquals(PipelineResult.State.FAILED, result.getState());

    long deadline = System.currentTimeMillis() + QUERY_START_TIMEOUT_MILLIS;
    while (SESSION.getSession().streams().active().length > 0) {
      assertTrue(
          "a sibling query was still active "
              + QUERY_START_TIMEOUT_MILLIS
              + "ms after the pipeline failed",
          System.currentTimeMillis() < deadline);
      Thread.sleep(50L);
    }
  }

  /** Throws on one specific element index, passes every other element through. */
  private static final class ThrowOnElementDoFn extends DoFn<String, String> {
    private final int poisonIndex;

    ThrowOnElementDoFn(int poisonIndex) {
      this.poisonIndex = poisonIndex;
    }

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> out) {
      if (TestUnboundedSource.indexOf(element) == poisonIndex) {
        throw new IllegalStateException("poison index " + poisonIndex);
      }
      out.output(element);
    }
  }
}
