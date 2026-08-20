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
package org.apache.beam.runners.spark.structuredstreaming.io.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the durable checkpoint recovery pieces of the Spark 4 micro-batch source, the {@link
 * BeamCheckpointFiles} layout, the epoch fast forward of {@link BeamMicroBatchStream} and the
 * deterministic source id of {@link UnboundedSourceDataset}.
 */
@RunWith(JUnit4.class)
public class BeamCheckpointRecoveryTest {

  private static final String SOURCE_ID = "read-source-cafe0123";

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testSplitsRoundTrip() throws Exception {
    String checkpointLocation = temp.newFolder("splits").getAbsolutePath();
    List<String> splits = Arrays.asList("c3BsaXQtMA==", "c3BsaXQtMQ==", "c3BsaXQtMg==");

    assertNull(
        "no splits pinned yet", BeamCheckpointFiles.readSplits(checkpointLocation, SOURCE_ID));

    BeamCheckpointFiles.writeSplits(checkpointLocation, SOURCE_ID, splits);
    assertEquals(splits, BeamCheckpointFiles.readSplits(checkpointLocation, SOURCE_ID));

    File root = new File(checkpointLocation, "beam-source-" + SOURCE_ID);
    assertEquals(
        "only the final splits file may remain",
        new HashSet<>(Arrays.asList("splits")),
        fileNames(root));
  }

  @Test
  public void testMarkRetentionAndRecovery() throws Exception {
    String checkpointLocation = temp.newFolder("marks").getAbsolutePath();
    for (long epoch = 1; epoch <= 4; epoch++) {
      BeamCheckpointFiles.writeMark(
          checkpointLocation, SOURCE_ID, 0, epoch, new TestMark((int) epoch));
    }

    File marksDir = new File(new File(checkpointLocation, "beam-source-" + SOURCE_ID), "marks/0");
    assertEquals(
        "retention must keep the two highest epochs only",
        new HashSet<>(Arrays.asList("3", "4")),
        fileNames(marksDir));

    assertEquals(4, restoredPosition(checkpointLocation, 0, 4));
    assertEquals(3, restoredPosition(checkpointLocation, 0, 3));
    // No exact file for epoch 7, the largest epoch not exceeding it is 4.
    assertEquals(4, restoredPosition(checkpointLocation, 0, 7));
    // Epochs 1 and 2 were deleted by retention, nothing at or below 2 is left.
    assertNull(BeamCheckpointFiles.readMark(checkpointLocation, SOURCE_ID, 0, 2));
    // A different split has no marks at all.
    assertNull(BeamCheckpointFiles.readMark(checkpointLocation, SOURCE_ID, 1, 4));
  }

  @Test
  public void testEpochFastForwardsPastDeserializedOffset() throws Exception {
    BeamMicroBatchStream stream = stream(temp.newFolder("ff-offset").getAbsolutePath());
    stream.deserializeOffset("{\"epoch\":7}");
    BeamOffset next = (BeamOffset) stream.latestOffset();
    assertTrue("latestOffset must move past the replayed epoch 7, got " + next, next.epoch() > 7L);
  }

  @Test
  public void testEpochFastForwardsPastPlannedOffsets() throws Exception {
    BeamMicroBatchStream stream = stream(temp.newFolder("ff-plan").getAbsolutePath());
    InputPartition[] partitions =
        stream.planInputPartitions(new BeamOffset(3L), new BeamOffset(9L));
    assertTrue("at least one partition expected", partitions.length > 0);
    BeamOffset next = (BeamOffset) stream.latestOffset();
    assertTrue("latestOffset must move past the planned epoch 9, got " + next, next.epoch() > 9L);
  }

  @Test
  public void testSourceIdIsDeterministicAndFilesystemSafe() {
    String name = "Read from PubSub/PubsubUnboundedSource (with: spaces & colons)";
    String id = UnboundedSourceDataset.sourceId(name);

    assertEquals("same name must give the same id", id, UnboundedSourceDataset.sourceId(name));
    assertNotEquals(
        "different names must give different ids",
        UnboundedSourceDataset.sourceId("Read A"),
        UnboundedSourceDataset.sourceId("Read B"));
    assertTrue("id must be filesystem safe: " + id, id.matches("[A-Za-z0-9._-]+"));

    String longName = String.join("/", java.util.Collections.nCopies(50, "NestedTransform"));
    String longId = UnboundedSourceDataset.sourceId(longName);
    assertTrue("id length must stay bounded: " + longId.length(), longId.length() <= 73);
    assertNotEquals(
        "truncated names must still differ through the hash",
        longId,
        UnboundedSourceDataset.sourceId(longName + "/Tail"));
  }

  private static int restoredPosition(String checkpointLocation, int splitId, long startEpoch) {
    CheckpointMark mark =
        BeamCheckpointFiles.readMark(checkpointLocation, SOURCE_ID, splitId, startEpoch);
    assertNotNull("expected a durable mark at or before epoch " + startEpoch, mark);
    return ((TestMark) mark).position;
  }

  /**
   * Lists the visible files of {@code dir}, asserting no {@code .tmp} file was left behind and
   * ignoring the hidden checksum sidecars of Hadoop's local filesystem.
   */
  private static Set<String> fileNames(File dir) {
    String[] names = dir.list();
    assertNotNull("expected directory " + dir, names);
    Set<String> visible = new HashSet<>();
    for (String name : names) {
      assertFalse("no temporary file may remain: " + name, name.endsWith(".tmp"));
      if (!name.startsWith(".")) {
        visible.add(name);
      }
    }
    return visible;
  }

  /** Builds a stream over a real source, driver side only, no Spark session required. */
  private static BeamMicroBatchStream stream(String checkpointLocation) {
    Map<String, String> options = new HashMap<>();
    options.put(
        BeamStreamingSource.OPT_SOURCE, BeamStreamingSource.encode(CountingSource.unbounded()));
    options.put(
        BeamStreamingSource.OPT_CODER,
        BeamStreamingSource.encode(
            WindowedValues.getFullCoder(
                org.apache.beam.sdk.coders.VarLongCoder.of(), GlobalWindow.Coder.INSTANCE)));
    options.put(
        BeamStreamingSource.OPT_PIPELINE_OPTIONS,
        BeamStreamingSource.encode(
            new SerializablePipelineOptions(PipelineOptionsFactory.create())));
    options.put(BeamStreamingSource.OPT_SOURCE_ID, SOURCE_ID);
    options.put(BeamStreamingSource.OPT_NUM_SPLITS, "2");
    return new BeamMicroBatchStream(new CaseInsensitiveStringMap(options), checkpointLocation);
  }

  /** A trivial serializable checkpoint mark carrying a read position. */
  private static class TestMark implements CheckpointMark, Serializable {
    private static final long serialVersionUID = 1L;

    private final int position;

    TestMark(int position) {
      this.position = position;
    }

    @Override
    public void finalizeCheckpoint() {}
  }
}
