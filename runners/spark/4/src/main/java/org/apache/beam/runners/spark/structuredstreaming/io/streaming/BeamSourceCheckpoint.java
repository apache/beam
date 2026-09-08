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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
 * <location>/marks/<splitId>/<epoch>} holds the coded checkpoint mark of a split at the end of the
 * batch ending at that epoch. All IO goes through Spark's {@link CheckpointFileManager}, writes are
 * atomic renames.
 */
public final class BeamSourceCheckpoint {

  private static final Logger LOG = LoggerFactory.getLogger(BeamSourceCheckpoint.class);

  private static final String SPLITS_FILE = "splits";
  private static final String MARKS_DIR = "marks";

  /** A purge further than this above the last one lists the directory instead of probing epochs. */
  private static final long MAX_BLIND_PURGE_RANGE = 1_000L;

  private final String location;
  private final CheckpointFileManager fm;
  private final Path root;
  private final Path splitsPath;
  private final Path marksRoot;

  /** Splits whose mark directory this instance already created. */
  private final Set<Integer> preparedSplits = ConcurrentHashMap.newKeySet();

  /** Per split, every mark epoch strictly below the value is known to be deleted. */
  private final ConcurrentMap<Integer, Long> purgeFloors = new ConcurrentHashMap<>();

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

  public void writeMark(int splitId, long epoch, byte[] codedMark) throws IOException {
    if (preparedSplits.add(splitId)) {
      fm.mkdirs(marksDir(splitId));
    }
    write(markPath(splitId, epoch), codedMark, true);
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
   * Deletes every mark of a split with an epoch strictly below {@code epoch}. Lists the directory
   * once per split, later calls delete the range above the previous floor only. Idempotent.
   */
  public void purgeMarksBelow(int splitId, long epoch) throws IOException {
    Long floor = purgeFloors.get(splitId);
    if (floor != null && epoch - floor > MAX_BLIND_PURGE_RANGE) {
      floor = null;
    }
    if (floor == null) {
      Path dir = marksDir(splitId);
      if (!fm.exists(dir)) {
        return;
      }
      for (FileStatus status : fm.list(dir)) {
        long existing = parseEpoch(status.getPath().getName());
        if (existing >= 0 && existing < epoch) {
          fm.delete(status.getPath());
        }
      }
      purgeFloors.put(splitId, epoch);
      return;
    }
    for (long e = floor; e < epoch; e++) {
      fm.delete(markPath(splitId, e));
    }
    if (epoch > floor) {
      purgeFloors.put(splitId, epoch);
    }
  }

  private Path marksDir(int splitId) {
    return new Path(marksRoot, Integer.toString(splitId));
  }

  private Path markPath(int splitId, long epoch) {
    return new Path(marksDir(splitId), Long.toString(epoch));
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

  /** The epoch encoded in a mark file name, or -1 for anything else. */
  private static long parseEpoch(String name) {
    try {
      return Long.parseLong(name);
    } catch (NumberFormatException e) {
      return -1L;
    }
  }
}
