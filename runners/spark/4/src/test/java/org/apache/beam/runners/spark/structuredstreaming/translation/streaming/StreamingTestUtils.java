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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;
import org.junit.rules.TemporaryFolder;

/**
 * Shared test utilities for the Spark 4 streaming translators.
 *
 * <p>Collectors are static so they work in local mode only.
 *
 * <p>The {@link #run} helper bounds each query at five minutes and cancels on expiry.
 *
 * <p>Tests poll with deadlines instead of {@code @Test(timeout)}, JUnit runs a timed test in a
 * separate thread group and Spark's static thread pools inherit it.
 */
public final class StreamingTestUtils {

  private StreamingTestUtils() {}

  /** Driver side, per collector id accumulation of every element a {@link CollectDoFn} saw. */
  private static final Map<String, List<Object>> COLLECTORS = new ConcurrentHashMap<>();

  /**
   * Appends every element to a static collector named {@code collectorId}, then passes it through.
   * Safe to use concurrently. Works in Spark local mode only.
   */
  public static final class CollectDoFn<T> extends DoFn<T, T> {
    private final String collectorId;

    public CollectDoFn(String collectorId) {
      this.collectorId = Preconditions.checkNotNull(collectorId);
    }

    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> out) {
      append(collectorId, element);
      out.output(element);
    }
  }

  private static void append(String collectorId, Object value) {
    COLLECTORS
        .computeIfAbsent(collectorId, unused -> Collections.synchronizedList(new ArrayList<>()))
        .add(value);
  }

  /** Returns a snapshot of everything collected so far under {@code collectorId}. */
  @SuppressWarnings("unchecked")
  public static <T> List<T> getCollected(String collectorId) {
    List<Object> values = COLLECTORS.get(collectorId);
    if (values == null) {
      return Collections.emptyList();
    }
    synchronized (values) {
      return (List<T>) new ArrayList<>(values);
    }
  }

  /** Discards everything collected so far under {@code collectorId}. */
  public static void clear(String collectorId) {
    COLLECTORS.remove(collectorId);
  }

  /** Collector id that will not collide with other tests or runs. */
  public static String newCollectorId(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  /** Upper bound on the wall clock time one streaming pipeline may take. */
  public static final Duration FINISH_TIMEOUT = Duration.standardMinutes(5);

  /**
   * Runs {@code pipeline} and returns on a terminal state. Cancels the pipeline and fails after
   * {@link #FINISH_TIMEOUT}.
   */
  public static PipelineResult run(Pipeline pipeline) {
    PipelineResult result = pipeline.run();
    waitUntilFinish(result);
    return result;
  }

  /**
   * Waits at most {@link #FINISH_TIMEOUT} for {@code result} to reach a terminal state. Fails and
   * cancels if still running at the deadline.
   */
  public static PipelineResult.State waitUntilFinish(PipelineResult result) {
    PipelineResult.State state = result.waitUntilFinish(FINISH_TIMEOUT);
    if (state == null || !state.isTerminal()) {
      try {
        result.cancel();
      } catch (IOException | RuntimeException e) {
        // Best effort cancellation.
      }
      throw new AssertionError(
          "pipeline did not finish within " + FINISH_TIMEOUT + ", last state " + state);
    }
    return state;
  }

  /**
   * Streaming options for tests: runner on the active session, streaming mode, stop after 3 idle
   * batches, 200 ms trigger, the given checkpoint directory. Test mode is off, {@code run()}
   * returns at once and tests wait through {@link #run} or {@link #waitUntilFinish}.
   */
  public static SparkStructuredStreamingPipelineOptions streamingOptions(
      TemporaryFolder checkpointDir) throws IOException {
    return streamingOptions(checkpointDir.newFolder("checkpoint").getAbsolutePath());
  }

  /** Same as {@link #streamingOptions(TemporaryFolder)} with an explicit checkpoint path. */
  public static SparkStructuredStreamingPipelineOptions streamingOptions(String checkpointPath) {
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.as(SparkStructuredStreamingPipelineOptions.class);
    options.setRunner(SparkStructuredStreamingRunner.class);
    options.setUseActiveSparkSession(true);
    options.setTestMode(false);
    options.setStreaming(true);
    options.setStreamingStopAfterIdleBatches(3);
    options.setMaxBatchDurationMillis(200);
    options.setCheckpointDir(checkpointPath);
    return options;
  }
}
