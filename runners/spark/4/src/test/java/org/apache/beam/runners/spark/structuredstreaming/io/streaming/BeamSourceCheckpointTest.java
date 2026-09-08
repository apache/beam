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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamSourceCheckpoint} on the local file system, no Spark session. */
@RunWith(JUnit4.class)
public class BeamSourceCheckpointTest {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private File root;
  private BeamSourceCheckpoint checkpoint;

  @Before
  public void setUp() {
    root = new File(temp.getRoot(), "sources/0");
    checkpoint = fresh();
  }

  /** Splits round trip once, a second pin fails, a fresh location has none. */
  @Test
  public void testSplitsArePinnedOnce() throws Exception {
    assertNull("fresh location must have no splits", checkpoint.readSplits());

    List<? extends UnboundedSource<Long, ?>> splits =
        CountingSource.unbounded().split(2, PipelineOptionsFactory.create());
    assertEquals(2, splits.size());
    checkpoint.writeSplits(splits);

    List<UnboundedSource<?, ?>> restored = fresh().readSplits();
    assertNotNull(restored);
    assertEquals(splits.size(), restored.size());
    for (int i = 0; i < splits.size(); i++) {
      assertEquals(splits.get(i).getClass(), restored.get(i).getClass());
    }

    assertThrows(IOException.class, () -> checkpoint.writeSplits(splits));
    assertThrows(IOException.class, () -> fresh().writeSplits(splits));
    assertEquals(2, fresh().readSplits().size());
    assertCleanTree();
  }

  /** Marks round trip byte for byte, the latest write at an epoch wins, absent marks are null. */
  @Test
  public void testMarkRoundTrip() throws Exception {
    byte[] first = {0, 1, -1, 127, -128, 42};
    byte[] second = {7};

    assertNull(checkpoint.readMark(0, 1));
    checkpoint.writeMark(0, 1, first);
    assertArrayEquals(first, checkpoint.readMark(0, 1));
    assertArrayEquals(first, fresh().readMark(0, 1));

    checkpoint.writeMark(0, 1, second);
    assertArrayEquals(second, checkpoint.readMark(0, 1));
    assertArrayEquals(second, fresh().readMark(0, 1));

    checkpoint.writeMark(0, 2, new byte[0]);
    assertArrayEquals(new byte[0], checkpoint.readMark(0, 2));

    assertNull("missing epoch", checkpoint.readMark(0, 3));
    assertNull("missing split", checkpoint.readMark(1, 1));
    assertNull("missing split, fresh instance", fresh().readMark(1, 1));
    assertCleanTree();
  }

  /** Purging keeps epochs at or above the floor, is idempotent and ignores absent splits. */
  @Test
  public void testPurgeMarksBelow() throws Exception {
    for (long epoch = 1; epoch <= 5; epoch++) {
      checkpoint.writeMark(0, epoch, bytes(epoch));
    }
    for (long epoch = 1; epoch <= 3; epoch++) {
      checkpoint.writeMark(1, epoch, bytes(epoch));
    }

    checkpoint.purgeMarksBelow(0, 3);
    assertEquals(epochs(3, 4, 5), markEpochs(0));
    assertEquals("other split untouched", epochs(1, 2, 3), markEpochs(1));

    checkpoint.purgeMarksBelow(0, 3);
    assertEquals("idempotent", epochs(3, 4, 5), markEpochs(0));

    checkpoint.purgeMarksBelow(0, 1);
    assertEquals("lower floor deletes nothing", epochs(3, 4, 5), markEpochs(0));

    checkpoint.purgeMarksBelow(0, 5);
    assertEquals(epochs(5), markEpochs(0));
    assertArrayEquals(bytes(5), checkpoint.readMark(0, 5));

    checkpoint.purgeMarksBelow(0, 6);
    assertEquals(epochs(), markEpochs(0));

    checkpoint.purgeMarksBelow(7, 3);
    checkpoint.purgeMarksBelow(7, 3);
    assertEquals(epochs(), markEpochs(7));
    assertCleanTree();
  }

  /** A fresh instance without an in memory floor purges the same files as the writer. */
  @Test
  public void testPurgeWithoutInMemoryFloor() throws Exception {
    for (long epoch = 1; epoch <= 5; epoch++) {
      checkpoint.writeMark(0, epoch, bytes(epoch));
    }
    checkpoint.purgeMarksBelow(0, 2);
    assertEquals(epochs(2, 3, 4, 5), markEpochs(0));

    BeamSourceCheckpoint other = fresh();
    other.purgeMarksBelow(0, 4);
    assertEquals(epochs(4, 5), markEpochs(0));
    other.purgeMarksBelow(0, 4);
    assertEquals(epochs(4, 5), markEpochs(0));

    // The writer's floor is 2, epochs 2 and 3 are already gone.
    checkpoint.purgeMarksBelow(0, 5);
    assertEquals(epochs(5), markEpochs(0));

    fresh().purgeMarksBelow(0, 5);
    assertEquals(epochs(5), markEpochs(0));
    assertCleanTree();
  }

  private BeamSourceCheckpoint fresh() {
    return new BeamSourceCheckpoint(root.getAbsolutePath(), new Configuration());
  }

  private static byte[] bytes(long epoch) {
    return new byte[] {(byte) epoch, (byte) (epoch * 3)};
  }

  private static Set<Long> epochs(long... epochs) {
    return Arrays.stream(epochs).boxed().collect(Collectors.toCollection(TreeSet::new));
  }

  /** Numeric file names under {@code marks/<split>}, empty if the directory is absent. */
  private Set<Long> markEpochs(int split) {
    File dir = new File(root, "marks/" + split);
    String[] names = dir.list();
    Set<Long> epochs = new TreeSet<>();
    if (names == null) {
      return epochs;
    }
    for (String name : names) {
      if (!name.startsWith(".")) {
        epochs.add(Long.parseLong(name));
      }
    }
    return epochs;
  }

  /** No temp files and no hidden files other than Hadoop's checksum sidecars remain. */
  private void assertCleanTree() throws IOException {
    List<String> offenders = new ArrayList<>();
    try (Stream<Path> paths = Files.walk(temp.getRoot().toPath())) {
      paths
          .map(path -> path.getFileName().toString())
          .filter(name -> name.endsWith(".tmp") || (name.startsWith(".") && !name.endsWith(".crc")))
          .forEach(offenders::add);
    }
    if (!offenders.isEmpty()) {
      fail("unexpected files under " + temp.getRoot() + ": " + offenders);
    }
  }
}
