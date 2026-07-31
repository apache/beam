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
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.ParDo;
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
 * {@link PipelineResult.State} transitions for a streaming pipeline: {@code RUNNING} immediately
 * after {@code run()}, {@code DONE} once a naturally idle pipeline's idle-stop listener stops it,
 * and {@code CANCELLED} after an explicit {@code cancel()}.
 *
 * <p>Both tests here set {@code testMode(false)} on top of {@link
 * StreamingTestUtils#streamingOptions}, unlike every other test in this package: {@code
 * SparkStructuredStreamingRunner#run()} calls {@code result.waitUntilFinish()} itself before
 * returning when {@code testMode} is {@code true} (see its implementation), which would make {@code
 * run()} block past the point these tests want to observe {@code RUNNING}. Note that {@code
 * SparkSessionRule#configure} sets {@code testMode(true)}, so the override has to come after it.
 *
 * <p>Not tested here: that a streaming pipeline is rejected when run against Spark 3. That is
 * {@code PipelineTranslatorFactory#create} in the shared base module
 * (runners/spark/src/main/java/.../translation/PipelineTranslatorFactory.java) throwing {@code
 * UnsupportedOperationException}, and this test module only ever compiles and runs against the
 * Spark 4 classpath (this module's shadow copy of that same file dispatches to the real streaming
 * translator instead). Exercising the rejection needs a Spark 3 dependency this module deliberately
 * does not have; the right place for that check is a test in {@code runners/spark/src/test/...}
 * against the shared base module alone.
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class StreamingPipelineLifecycleTest implements Serializable {

  /**
   * These pipelines host no {@code transformWithState} operator, but they do need {@code
   * useActiveSparkSession} so that a cancelled or idle-stopped query does not take the shared
   * session down with it: {@code SparkStructuredStreamingRunner#sparkStopFn} only stops the session
   * on a terminal state when the session was <em>not</em> provided from outside. Configuring the
   * session the same way as the rest of the suite, including the module default of {@code
   * spark.kryo.registrationRequired=true}, keeps a single session shared across the whole streaming
   * test run.
   */
  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  private static final Instant BASE = new Instant(0);

  /** How long to wait for a query to actually start before giving up on it. */
  private static final long QUERY_START_TIMEOUT_MILLIS = 60_000L;

  private List<TimestampedValue<Integer>> tenElements() {
    List<TimestampedValue<Integer>> elements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      elements.add(TimestampedValue.of(i, BASE.plus(Duration.standardSeconds(i))));
    }
    return elements;
  }

  /**
   * Blocks until at least one streaming query is active on the shared session. Translation happens
   * asynchronously on the runner's submission thread, so {@code run()} returns before any query
   * exists, and {@code cancel()} before that point would find a {@code null} evaluation context and
   * silently have nothing to stop.
   */
  private static void awaitQueryStarted() throws InterruptedException {
    long deadline = System.currentTimeMillis() + QUERY_START_TIMEOUT_MILLIS;
    while (SESSION.getSession().streams().active().length == 0) {
      assertTrue(
          "no streaming query started within " + QUERY_START_TIMEOUT_MILLIS + "ms",
          System.currentTimeMillis() < deadline);
      Thread.sleep(50L);
    }
  }

  @Test(timeout = 300_000)
  public void idlePipelineGoesFromRunningToDoneOnceIdle() throws Exception {
    String collectorId = StreamingTestUtils.newCollectorId("lifecycle-done");
    StreamingTestUtils.clear(collectorId);

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    // Observe RUNNING ourselves instead of letting run() block until finished, see class javadoc.
    options.setTestMode(false);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "ReadUnbounded",
            Read.from(
                new StreamingTestUtils.ListBackedUnboundedSource<>(
                    tenElements(), VarIntCoder.of())))
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = pipeline.run();
    assertEquals(PipelineResult.State.RUNNING, result.getState());

    PipelineResult.State finalState = result.waitUntilFinish();
    assertEquals(PipelineResult.State.DONE, finalState);
    assertEquals(PipelineResult.State.DONE, result.getState());

    // DONE has to mean the idle-stop listener stopped a query that had actually drained its input,
    // not that the query fell over early, so check the data came through too.
    List<Integer> collected =
        new ArrayList<>(StreamingTestUtils.<Integer>getCollected(collectorId));
    Collections.sort(collected);
    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      expected.add(i);
    }
    assertEquals(expected, collected);
  }

  @Test(timeout = 300_000)
  public void cancelStopsTheQueryAndReportsCancelled() throws Exception {
    String collectorId = StreamingTestUtils.newCollectorId("lifecycle-cancel");
    StreamingTestUtils.clear(collectorId);

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    options.setTestMode(false);
    // Disabled so the query only ever stops because of the explicit cancel() below, not because it
    // happened to go idle first.
    options.setStreamingStopAfterIdleBatches(-1);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "ReadUnbounded",
            Read.from(
                new StreamingTestUtils.ListBackedUnboundedSource<>(
                    tenElements(), VarIntCoder.of())))
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = pipeline.run();
    assertEquals(PipelineResult.State.RUNNING, result.getState());

    awaitQueryStarted();

    PipelineResult.State cancelledState = result.cancel();
    assertEquals(PipelineResult.State.CANCELLED, cancelledState);
    assertEquals(PipelineResult.State.CANCELLED, result.getState());

    // cancel() has to have actually stopped the query, not just relabelled the result: with
    // idle-stop disabled this query would otherwise run until the JUnit timeout.
    long deadline = System.currentTimeMillis() + QUERY_START_TIMEOUT_MILLIS;
    while (SESSION.getSession().streams().active().length > 0) {
      assertTrue(
          "the streaming query was still active " + QUERY_START_TIMEOUT_MILLIS + "ms after cancel",
          System.currentTimeMillis() < deadline);
      Thread.sleep(50L);
    }
  }
}
