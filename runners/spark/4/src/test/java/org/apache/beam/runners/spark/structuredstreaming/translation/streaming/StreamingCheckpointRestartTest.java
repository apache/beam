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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingRunner;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.BeamReaderCache;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.UnboundedSourceDataset;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
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
 * End to end proof that a Spark 4 structured streaming pipeline restarted against the same
 * checkpoint location resumes an {@link org.apache.beam.sdk.io.UnboundedSource.UnboundedReader}
 * from its durable checkpoint marks instead of re-reading the source from scratch.
 *
 * <p>Two independent {@link Pipeline}s are run one after the other against the same checkpoint
 * directory, with identical read transform names so they derive the same deterministic {@code
 * sourceId} (see {@link UnboundedSourceDataset#sourceId}). {@link BeamReaderCache#invalidateAll} is
 * called between the two runs to simulate a fresh JVM: every in-memory reader and checkpoint mark
 * is dropped, forcing the second run's readers to fall back to the durable marks persisted by
 * {@link org.apache.beam.runners.spark.structuredstreaming.io.streaming.BeamCheckpointFiles} during
 * the first run.
 *
 * <p>{@link StreamingTestUtils.ListBackedUnboundedSource} now carries a positional checkpoint mark,
 * see {@link StreamingTestUtils.ListBackedUnboundedSource.Mark}, so a reader created from a
 * restored mark resumes right after the last element it had emitted rather than always starting at
 * the beginning of its split.
 *
 * <p>The second run's assertion is deliberately weak, per the at-least-once semantics documented on
 * {@code BeamCheckpointFiles} and {@code BeamReaderCache}: Spark may replay the last micro-batch
 * that was in flight when the query stopped, so a handful of already-seen elements MAY reappear.
 * What must never happen is the second run re-reading the <em>whole</em> range, which is what a
 * regression back to a fresh, non-durable source id or in-memory-only marks would look like.
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class StreamingCheckpointRestartTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  private static final Instant BASE = new Instant(0);
  private static final int ELEMENT_COUNT = 10;
  private static final String READ_TRANSFORM_NAME = "ReadUnbounded";

  @Test(timeout = 300_000)
  public void restartedPipelineResumesFromDurableCheckpointMarks() throws Exception {
    String checkpointPath = checkpointDir.newFolder("checkpoint").getAbsolutePath();

    String collectorA = StreamingTestUtils.newCollectorId("checkpoint-restart-a");
    String collectorB = StreamingTestUtils.newCollectorId("checkpoint-restart-b");
    StreamingTestUtils.clear(collectorA);
    StreamingTestUtils.clear(collectorB);

    // First run: read the whole 0..9 range once, from a fresh checkpoint location.
    PipelineResult first = runPipeline(checkpointPath, collectorA);
    first.waitUntilFinish();

    List<Integer> collectedA =
        new ArrayList<>(StreamingTestUtils.<Integer>getCollected(collectorA));
    Collections.sort(collectedA);
    assertEquals(
        "first run (pipeline state=" + first.getState() + ") must read every element",
        fullRange(),
        collectedA);

    // The durable layout pinned by the first run must exist: the pinned split list and at least
    // one persisted checkpoint mark, both keyed by the deterministic source id of "ReadUnbounded".
    String sourceId = UnboundedSourceDataset.sourceId(READ_TRANSFORM_NAME);
    File sourceRoot = findSourceRoot(new File(checkpointPath, "0"), sourceId);
    assertNotNull(
        "expected a beam-source-" + sourceId + " directory under " + checkpointPath + "/0",
        sourceRoot);
    File splitsFile = new File(sourceRoot, "splits");
    assertTrue("pinned splits file must exist: " + splitsFile, splitsFile.isFile());
    assertTrue(
        "at least one split's marks directory must contain a persisted mark file",
        hasAnyMarkFile(new File(sourceRoot, "marks")));

    // Simulate a fresh JVM: drop every in-memory reader and checkpoint mark, forcing the next
    // reader to fall back to the durable marks just asserted above.
    BeamReaderCache.invalidateAll();

    // Second run: identical transform name, identical checkpoint location, identical source
    // content. A correct restart must resume from the durable marks rather than re-reading
    // everything.
    PipelineResult second = runPipeline(checkpointPath, collectorB);
    second.waitUntilFinish();

    List<Integer> collectedB =
        new ArrayList<>(StreamingTestUtils.<Integer>getCollected(collectorB));
    Set<Integer> seenB = new HashSet<>(collectedB);
    assertFalse(
        "a restarted run (pipeline state="
            + second.getState()
            + ") must not re-read the whole range from scratch, saw "
            + collectedB,
        seenB.containsAll(fullRange()));
  }

  private PipelineResult runPipeline(String checkpointPath, String collectorId) throws IOException {
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.as(SparkStructuredStreamingPipelineOptions.class);
    options.setRunner(SparkStructuredStreamingRunner.class);
    options.setTestMode(true);
    options.setStreaming(true);
    options.setStreamingStopAfterIdleBatches(3);
    options.setMaxBatchDurationMillis(200);
    // One element per split per micro-batch: the 10 elements are spread across several
    // micro-batches instead of a single one, so even a replayed in-flight batch can only ever
    // carry a small tail of the range, never all of it.
    options.setMaxRecordsPerMicroBatch(1);
    options.setCheckpointDir(checkpointPath);
    SESSION.configure(options);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            READ_TRANSFORM_NAME,
            Read.from(
                new StreamingTestUtils.ListBackedUnboundedSource<>(elements(), VarIntCoder.of())))
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));
    return pipeline.run();
  }

  private static List<TimestampedValue<Integer>> elements() {
    List<TimestampedValue<Integer>> elements = new ArrayList<>(ELEMENT_COUNT);
    for (int i = 0; i < ELEMENT_COUNT; i++) {
      elements.add(TimestampedValue.of(i, BASE.plus(Duration.standardSeconds(i))));
    }
    return elements;
  }

  private static List<Integer> fullRange() {
    List<Integer> range = new ArrayList<>(ELEMENT_COUNT);
    for (int i = 0; i < ELEMENT_COUNT; i++) {
      range.add(i);
    }
    return range;
  }

  /** Recursively searches {@code root} for a directory named {@code beam-source-<sourceId>}. */
  private static @Nullable File findSourceRoot(File root, String sourceId) throws IOException {
    if (!root.isDirectory()) {
      return null;
    }
    String target = "beam-source-" + sourceId;
    try (Stream<Path> paths = Files.walk(root.toPath())) {
      return paths
          .filter(Files::isDirectory)
          .filter(path -> target.equals(path.getFileName().toString()))
          .findFirst()
          .map(Path::toFile)
          .orElse(null);
    }
  }

  /** {@code true} if any split subdirectory of {@code marksRoot} contains a mark file. */
  private static boolean hasAnyMarkFile(File marksRoot) {
    File[] splitDirs = marksRoot.listFiles();
    if (splitDirs == null) {
      return false;
    }
    for (File splitDir : splitDirs) {
      File[] markFiles = splitDir.listFiles();
      if (markFiles != null && markFiles.length > 0) {
        return true;
      }
    }
    return false;
  }
}
