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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable state of the Beam micro-batch source, stored next to Spark's own streaming state under
 * {@code <checkpointLocation>/beam-source-<sourceId>/}.
 *
 * <p>Two kinds of state are kept per source. The pinned split list under {@code <root>/splits}
 * records the sub sources produced by the first run, Beam sources do not guarantee deterministic
 * splitting and the split index is part of the reader cache key, so every later run must reuse the
 * first run's splits. The checkpoint marks under {@code <root>/marks/<splitId>/<epoch>} record the
 * read position of one split at the end of the micro-batch that ends at that epoch, which is also
 * the position at the start of any batch whose start offset equals that epoch.
 *
 * <p>Every file is written to a {@code .tmp} sibling first and then renamed into place, so a
 * partially written file is never observed under its final name.
 *
 * <p>The Hadoop {@link FileSystem} serving the checkpoint location is resolved from a default
 * {@link Configuration}. On executors this means the Hadoop configuration comes from classpath
 * defaults rather than from the Spark session, a known limitation of this helper.
 */
public final class BeamCheckpointFiles {

  private static final Logger LOG = LoggerFactory.getLogger(BeamCheckpointFiles.class);

  private static final String ROOT_PREFIX = "beam-source-";
  private static final String SPLITS_FILE = "splits";
  private static final String MARKS_DIR = "marks";
  private static final String TMP_SUFFIX = ".tmp";

  /** Number of most recent mark files retained per split. */
  private static final int RETAINED_MARKS = 2;

  private BeamCheckpointFiles() {}

  /**
   * Reads the pinned split list of {@code sourceId}, or returns {@code null} if no list has been
   * pinned yet.
   */
  public static @Nullable List<String> readSplits(String checkpointLocation, String sourceId)
      throws IOException {
    Path path = new Path(root(checkpointLocation, sourceId), SPLITS_FILE);
    FileSystem fs = fileSystem(path);
    if (!fs.exists(path)) {
      return null;
    }
    @SuppressWarnings("unchecked")
    List<String> splits = (List<String>) deserialize(read(fs, path), "pinned split list " + path);
    return splits;
  }

  /** Pins the split list of {@code sourceId} so later runs reuse exactly these splits. */
  public static void writeSplits(String checkpointLocation, String sourceId, List<String> splitsB64)
      throws IOException {
    Path path = new Path(root(checkpointLocation, sourceId), SPLITS_FILE);
    FileSystem fs = fileSystem(path);
    writeAtomically(fs, path, SerializableUtils.serializeToByteArray(new ArrayList<>(splitsB64)));
    LOG.info("Pinned {} split(s) of Beam source {} at {}.", splitsB64.size(), sourceId, path);
  }

  /**
   * Writes the checkpoint mark of one split at the end of the batch ending at {@code endEpoch} and
   * then, best effort, deletes mark files older than the two most recent epochs.
   *
   * @throws IOException if the mark is not {@link Serializable} or the write fails
   */
  public static void writeMark(
      String checkpointLocation, String sourceId, int splitId, long endEpoch, CheckpointMark mark)
      throws IOException {
    if (!(mark instanceof Serializable)) {
      throw new IOException(
          "Checkpoint mark "
              + mark.getClass().getName()
              + " is not Serializable, it cannot be persisted for durable recovery.");
    }
    Path dir = marksDir(checkpointLocation, sourceId, splitId);
    FileSystem fs = fileSystem(dir);
    writeAtomically(
        fs,
        new Path(dir, Long.toString(endEpoch)),
        SerializableUtils.serializeToByteArray((Serializable) mark));
    deleteOldMarks(fs, dir);
  }

  /**
   * Restores the durable checkpoint mark of one split for a batch starting at {@code startEpoch}.
   *
   * <p>The mark written under exactly {@code startEpoch} is preferred, if it is absent the mark
   * with the largest epoch not exceeding {@code startEpoch} is used. Returns {@code null}, meaning
   * a fresh start, when no such mark exists or reading fails.
   */
  public static @Nullable CheckpointMark readMark(
      String checkpointLocation, String sourceId, int splitId, long startEpoch) {
    Path dir = marksDir(checkpointLocation, sourceId, splitId);
    try {
      FileSystem fs = fileSystem(dir);
      if (!fs.exists(dir)) {
        return null;
      }
      long epoch = Long.MIN_VALUE;
      if (fs.exists(new Path(dir, Long.toString(startEpoch)))) {
        epoch = startEpoch;
      } else {
        for (FileStatus status : fs.listStatus(dir)) {
          @Nullable Long candidate = parseEpoch(status.getPath().getName());
          if (candidate != null && candidate <= startEpoch && candidate > epoch) {
            epoch = candidate;
          }
        }
      }
      if (epoch == Long.MIN_VALUE) {
        return null;
      }
      Path path = new Path(dir, Long.toString(epoch));
      CheckpointMark mark =
          (CheckpointMark) deserialize(read(fs, path), "durable checkpoint mark " + path);
      LOG.info(
          "Restored durable checkpoint mark of Beam source {} split {} at epoch {} "
              + "(requested epoch {}).",
          sourceId,
          splitId,
          epoch,
          startEpoch);
      return mark;
    } catch (IOException e) {
      LOG.warn(
          "Failed to read a durable checkpoint mark of Beam source {} split {} at epoch {}, "
              + "the reader starts without one.",
          sourceId,
          splitId,
          startEpoch,
          e);
      return null;
    }
  }

  private static Path root(String checkpointLocation, String sourceId) {
    return new Path(checkpointLocation, ROOT_PREFIX + sourceId);
  }

  private static Path marksDir(String checkpointLocation, String sourceId, int splitId) {
    return new Path(
        new Path(root(checkpointLocation, sourceId), MARKS_DIR), String.valueOf(splitId));
  }

  private static FileSystem fileSystem(Path path) throws IOException {
    return path.getFileSystem(new Configuration());
  }

  /** Writes {@code bytes} to a {@code .tmp} sibling of {@code target} and renames it into place. */
  private static void writeAtomically(FileSystem fs, Path target, byte[] bytes) throws IOException {
    Path tmp = new Path(target.getParent(), target.getName() + TMP_SUFFIX);
    try (FSDataOutputStream out = fs.create(tmp, true)) {
      out.write(bytes);
    }
    // On HDFS a rename onto an existing target fails, a mark rewritten by a task retry is the
    // same position, so an existing target counts as success.
    if (!fs.rename(tmp, target) && !fs.exists(target)) {
      throw new IOException("Failed to rename " + tmp + " to " + target);
    }
    if (fs.exists(tmp)) {
      fs.delete(tmp, false);
    }
  }

  private static byte[] read(FileSystem fs, Path path) throws IOException {
    try (FSDataInputStream in = fs.open(path)) {
      return ByteStreams.toByteArray(in);
    }
  }

  private static Object deserialize(byte[] bytes, String description) {
    return SerializableUtils.deserializeFromByteArray(bytes, description);
  }

  /** Best effort deletion of mark files older than the {@value #RETAINED_MARKS} highest epochs. */
  private static void deleteOldMarks(FileSystem fs, Path dir) {
    try {
      List<Long> epochs = new ArrayList<>();
      for (FileStatus status : fs.listStatus(dir)) {
        @Nullable Long epoch = parseEpoch(status.getPath().getName());
        if (epoch != null) {
          epochs.add(epoch);
        }
      }
      Collections.sort(epochs);
      for (int i = 0; i < epochs.size() - RETAINED_MARKS; i++) {
        fs.delete(new Path(dir, Long.toString(epochs.get(i))), false);
      }
    } catch (IOException e) {
      LOG.warn("Failed to delete outdated checkpoint marks under {}.", dir, e);
    }
  }

  private static @Nullable Long parseEpoch(String fileName) {
    try {
      return Long.valueOf(fileName);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
