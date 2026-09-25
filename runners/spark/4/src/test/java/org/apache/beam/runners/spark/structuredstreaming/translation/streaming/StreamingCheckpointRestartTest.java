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

import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.BeamReaderCache;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.TestUnboundedSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
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
 * Verifies restarted pipelines resume from durable checkpoint marks.
 *
 * <p>Two pipelines run sequentially against the same checkpoint directory. Wiping the reader cache
 * in between forces the second run to restore from the durable marks.
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class StreamingCheckpointRestartTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  private static final int ELEMENT_COUNT = 10;
  private static final String TAG = "checkpoint-restart";

  @After
  public void tearDown() {
    TestUnboundedSource.forget(TAG);
  }

  @Test
  public void restartedPipelineResumesFromDurableCheckpointMarks() throws Exception {
    String checkpointPath = checkpointDir.newFolder("checkpoint").getAbsolutePath();

    String collectorA = StreamingTestUtils.newCollectorId("checkpoint-restart-a");
    String collectorB = StreamingTestUtils.newCollectorId("checkpoint-restart-b");
    StreamingTestUtils.clear(collectorA);
    StreamingTestUtils.clear(collectorB);

    // First run reads all elements from a fresh checkpoint directory.
    runPipeline(checkpointPath, collectorA, TAG);

    Set<String> collectedA = StreamingTestUtils.collected(collectorA);
    assertEquals(
        "first run must read every element",
        TestUnboundedSource.elements(TAG, 1, ELEMENT_COUNT),
        collectedA);

    // The source checkpoint lives under the location the translator handed to Spark.
    File sourceRoot = new File(new File(checkpointPath, "0"), "sources/0");
    assertTrue("expected source checkpoint directory " + sourceRoot, sourceRoot.isDirectory());

    int createdBeforeSecondRun = TestUnboundedSource.created(TAG);

    // Invalidate cached readers and marks to force restore from durable files.
    BeamReaderCache.invalidateAll();

    // Second run resumes against the same checkpoint directory.
    runPipeline(checkpointPath, collectorB, TAG);

    Set<String> collectedB = StreamingTestUtils.collected(collectorB);
    assertTrue(
        "second run must not re-emit elements the first run committed",
        Collections.disjoint(collectedA, collectedB));
    assertTrue(
        "readers must be recreated during the second run",
        TestUnboundedSource.created(TAG) > createdBeforeSecondRun);
  }

  private PipelineResult runPipeline(String checkpointPath, String collectorId, String tag) {
    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointPath);
    // One element per split per micro batch to spread elements across batches.
    options.setMaxRecordsPerBatch(1L);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("ReadUnbounded", Read.from(new TestUnboundedSource(tag, 1, ELEMENT_COUNT)))
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));
    return StreamingTestUtils.run(pipeline);
  }
}
