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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.execution.streaming.CheckpointFileManager;
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable state of one Beam unbounded source under the per source checkpoint location Spark hands
 * to {@code toMicroBatchStream}.
 *
 * <p>{@code <location>/splits} pins the split list, written once by the driver. {@code
 * <location>/marks/<epoch>/<splitId>} holds the coded checkpoint mark of a split at the end of the
 * batch ending at that epoch. The epoch Spark last committed is read from Spark's own {@code
 * commits} and {@code offsets} logs two levels up. All IO goes through Spark's {@link
 * CheckpointFileManager}, writes are atomic renames.
 */
public final class BeamSourceCheckpoint {

  private static final Logger LOG = LoggerFactory.getLogger(BeamSourceCheckpoint.class);

  private static final String SPLITS_FILE = "splits";
  private static final String MARKS_DIR = "marks";
  private static final String SPARK_COMMITS_DIR = "commits";
  private static final String SPARK_OFFSETS_DIR = "offsets";
  private static final String SERIALIZED_VOID_OFFSET = "-";

  /** A purge further than this above the last one lists the directory instead of probing epochs. */
  private static final long MAX_BLIND_PURGE_RANGE = 1_000L;

  private final String location;
  private final CheckpointFileManager fm;
  private final Path root;
  private final Path splitsPath;
  private final Path marksRoot;

  /** Every mark epoch strictly below the value is known to be deleted, -1 for unknown. */
  private volatile long purgeFloor = -1L;

  public BeamSourceCheckpoint(String checkpointLocation, Configuration hadoopConf) {
    this.location = checkpointLocation;
    this.root = new Path(checkpointLocation);
    this.fm = CheckpointFileManager.create(root, hadoopConf);
    this.splitsPath = new Path(root, SPLITS_FILE);
    this.marksRoot = new Path(root, MARKS_DIR);
  }

  public String location() {
    return location;
  }

  /** The pinned split list, or null if none was pinned yet. */
  public @Nullable List<UnboundedSource<?, ?>> readSplits() throws IOException {
    if (!fm.exists(splitsPath)) {
      return null;
    }
    @SuppressWarnings("unchecked") // written by writeSplits as an ArrayList of sources
    List<UnboundedSource<?, ?>> splits =
        (List<UnboundedSource<?, ?>>)
            SerializableUtils.deserializeFromByteArray(read(splitsPath), "splits at " + splitsPath);
    return splits;
  }

  /** Pins the split list, fails if one is pinned already. */
  public void writeSplits(List<? extends UnboundedSource<?, ?>> splits) throws IOException {
    fm.mkdirs(root);
    if (fm.exists(splitsPath)) {
      throw new IOException("Split list already pinned at " + splitsPath);
    }
    write(splitsPath, SerializableUtils.serializeToByteArray(new ArrayList<>(splits)), false);
    LOG.info("Pinned {} split(s) at {}.", splits.size(), splitsPath);
  }

  /** Creates the mark directory of {@code epoch}, the driver calls this once per batch. */
  public void prepareEpoch(long epoch) throws IOException {
    fm.mkdirs(epochDir(epoch));
  }

  /**
   * Writes the mark, creating the epoch directory if a manager without parent creation needs it.
   */
  public void writeMark(int splitId, long epoch, byte[] codedMark) throws IOException {
    Path path = markPath(splitId, epoch);
    try {
      write(path, codedMark, true);
    } catch (FileNotFoundException e) {
      fm.mkdirs(epochDir(epoch));
      write(path, codedMark, true);
    }
  }

  /** The coded mark of a split at an epoch, or null if absent. */
  public byte @Nullable [] readMark(int splitId, long epoch) throws IOException {
    Path path = markPath(splitId, epoch);
    if (!fm.exists(path)) {
      return null;
    }
    return read(path);
  }

  /**
   * The end epoch of this source in the last batch Spark committed, or -1 if there is none or the
   * logs cannot be read. The location is {@code <root>/sources/<index>}, the batch id is the
   * highest entry of {@code <root>/commits} and its epoch is line {@code index} after the version
   * and metadata lines of {@code <root>/offsets/<id>}.
   */
  public long readSparkCommittedEpoch() {
    try {
      Path sparkRoot = root.getParent().getParent();
      int sourceIndex = Integer.parseInt(root.getName());
      Path commits = new Path(sparkRoot, SPARK_COMMITS_DIR);
      if (!fm.exists(commits)) {
        return -1L;
      }
      long batchId = -1L;
      for (FileStatus status : fm.list(commits)) {
        batchId = Math.max(batchId, parseEpoch(status.getPath().getName()));
      }
      if (batchId < 0) {
        return -1L;
      }
      Path offsets = new Path(new Path(sparkRoot, SPARK_OFFSETS_DIR), Long.toString(batchId));
      List<String> lines =
          Arrays.asList(new String(read(offsets), StandardCharsets.UTF_8).split("\n", -1));
      String line = lines.get(2 + sourceIndex).trim();
      return line.equals(SERIALIZED_VOID_OFFSET) ? -1L : Long.parseLong(line);
    } catch (IOException | RuntimeException e) {
      LOG.warn("Failed to read the epoch Spark committed for {}.", location, e);
      return -1L;
    }
  }

  /**
   * Deletes the marks of every epoch strictly below {@code epoch}, one recursive delete per epoch
   * directory. Lists the marks directory once, later calls delete the range above the previous
   * floor only. Idempotent.
   */
  public void purgeMarksBelow(long epoch) throws IOException {
    long floor = purgeFloor;
    if (floor >= 0 && epoch - floor > MAX_BLIND_PURGE_RANGE) {
      floor = -1L;
    }
    if (floor < 0) {
      if (fm.exists(marksRoot)) {
        for (FileStatus status : fm.list(marksRoot)) {
          long existing = parseEpoch(status.getPath().getName());
          if (existing >= 0 && existing < epoch) {
            fm.delete(status.getPath());
          }
        }
      }
      purgeFloor = epoch;
      return;
    }
    for (long e = floor; e < epoch; e++) {
      fm.delete(epochDir(e));
    }
    if (epoch > floor) {
      purgeFloor = epoch;
    }
  }

  private Path epochDir(long epoch) {
    return new Path(marksRoot, Long.toString(epoch));
  }

  private Path markPath(int splitId, long epoch) {
    return new Path(epochDir(epoch), Integer.toString(splitId));
  }

  private byte[] read(Path path) throws IOException {
    try (FSDataInputStream in = fm.open(path)) {
      return ByteStreams.toByteArray(in);
    }
  }

  private void write(Path path, byte[] bytes, boolean overwrite) throws IOException {
    CancellableFSDataOutputStream out = fm.createAtomic(path, overwrite);
    try {
      out.write(bytes);
      out.close();
    } catch (IOException | RuntimeException e) {
      out.cancel();
      throw e;
    }
  }

  /** The epoch encoded in a mark directory name, or -1 for anything else. */
  private static long parseEpoch(String name) {
    try {
      return Long.parseLong(name);
    } catch (NumberFormatException e) {
      return -1L;
    }
  }
}
