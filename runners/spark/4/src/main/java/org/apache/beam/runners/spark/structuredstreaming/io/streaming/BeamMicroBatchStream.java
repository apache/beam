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
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MicroBatchStream} over a Beam {@link UnboundedSource}.
 *
 * <p>Offsets are opaque epoch counters, see {@link BeamOffset}. {@link #latestOffset()} always
 * reports a value greater than the previous one so Spark keeps scheduling micro-batches, even ones
 * that turn out to be empty. Termination of a streaming pipeline is therefore never driven by the
 * offsets, it is driven by the lifecycle owner (the idle batch listener of the evaluation context,
 * or an explicit {@code StreamingQuery.stop()}).
 *
 * <p>The wrapped source is split exactly once, on the driver, and the resulting sub sources are
 * pinned to the checkpoint location so every micro-batch of every run plans the same, stable set of
 * partitions. Splits must be stable across micro-batches and across restarts because the executor
 * side reader cache and the durable checkpoint marks are keyed by split index, and Beam sources do
 * not guarantee deterministic splitting. A restarted stream therefore loads the split list written
 * by the first run instead of splitting again.
 *
 * <p>On a restart Spark replays offsets from its offset log through {@link #deserializeOffset} and
 * {@link #planInputPartitions}. The epoch counter fast forwards past every epoch seen there, so
 * {@link #latestOffset()} never emits an offset smaller than one already committed to the log.
 */
public class BeamMicroBatchStream implements MicroBatchStream {

  private static final Logger LOG = LoggerFactory.getLogger(BeamMicroBatchStream.class);

  private final String sourceB64;
  private final String coderB64;
  private final String pipelineOptionsB64;
  private final String sourceId;
  private final String checkpointLocation;
  private final int desiredNumSplits;
  private final long maxRecordsPerBatch;
  private final long maxBatchDurationMillis;

  private long epoch;
  private @Nullable List<String> splitsB64;

  BeamMicroBatchStream(CaseInsensitiveStringMap options, String checkpointLocation) {
    this.sourceB64 = BeamStreamingSource.required(options, BeamStreamingSource.OPT_SOURCE);
    this.coderB64 = BeamStreamingSource.required(options, BeamStreamingSource.OPT_CODER);
    this.pipelineOptionsB64 =
        BeamStreamingSource.required(options, BeamStreamingSource.OPT_PIPELINE_OPTIONS);
    this.sourceId = BeamStreamingSource.required(options, BeamStreamingSource.OPT_SOURCE_ID);
    this.checkpointLocation = checkpointLocation;
    this.desiredNumSplits = Math.max(1, options.getInt(BeamStreamingSource.OPT_NUM_SPLITS, 1));
    this.maxRecordsPerBatch = options.getLong(BeamStreamingSource.OPT_MAX_RECORDS, -1L);
    this.maxBatchDurationMillis =
        Math.max(1L, options.getLong(BeamStreamingSource.OPT_MAX_BATCH_DURATION_MILLIS, 500L));
  }

  @Override
  public Offset initialOffset() {
    return BeamOffset.ZERO;
  }

  @Override
  public synchronized Offset latestOffset() {
    return new BeamOffset(++epoch);
  }

  @Override
  public Offset deserializeOffset(String json) {
    BeamOffset offset = BeamOffset.fromJson(json);
    fastForwardEpoch(offset.epoch());
    return offset;
  }

  @Override
  public void commit(Offset end) {
    LOG.debug("Committed epoch offset {} of Beam source {}.", end, sourceId);
  }

  @Override
  public void stop() {
    LOG.info("Stopping Beam micro-batch stream for source {}.", sourceId);
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    long startEpoch = ((BeamOffset) start).epoch();
    long endEpoch = ((BeamOffset) end).epoch();
    fastForwardEpoch(endEpoch);
    List<String> splits = splits();
    InputPartition[] partitions = new InputPartition[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      partitions[i] =
          new BeamInputPartition(
              splits.get(i),
              coderB64,
              pipelineOptionsB64,
              sourceId,
              i,
              checkpointLocation,
              startEpoch,
              endEpoch,
              maxRecordsPerBatch,
              maxBatchDurationMillis);
    }
    return partitions;
  }

  /**
   * Raises the epoch counter to {@code seen} if it is behind, keeping {@link #latestOffset()} ahead
   * of every offset Spark already logged before a restart.
   */
  private synchronized void fastForwardEpoch(long seen) {
    if (seen > epoch) {
      LOG.info(
          "Fast forwarding the epoch of Beam source {} from {} to {} seen in Spark's offset log.",
          sourceId,
          epoch,
          seen);
      epoch = seen;
    }
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new BeamPartitionReaderFactory();
  }

  /**
   * Returns the pinned split list of this source, loading it from the checkpoint location if a
   * previous run pinned one, and splitting the source and pinning the result otherwise.
   */
  private synchronized List<String> splits() {
    if (splitsB64 != null) {
      return splitsB64;
    }
    List<String> pinned;
    try {
      pinned = BeamCheckpointFiles.readSplits(checkpointLocation, sourceId);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to read the pinned split list of Beam source " + sourceId, e);
    }
    if (pinned != null) {
      LOG.info(
          "Restored {} pinned split(s) of Beam source {} from {}.",
          pinned.size(),
          sourceId,
          checkpointLocation);
      splitsB64 = pinned;
      return pinned;
    }
    UnboundedSource<?, ?> source = BeamStreamingSource.decode(sourceB64, "UnboundedSource");
    SerializablePipelineOptions serializableOptions =
        BeamStreamingSource.decode(pipelineOptionsB64, "PipelineOptions");
    PipelineOptions options = serializableOptions.get();
    List<? extends UnboundedSource<?, ?>> split;
    try {
      split = source.split(desiredNumSplits, options);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to split UnboundedSource " + source.getClass().getCanonicalName(), e);
    }
    if (split.isEmpty()) {
      split = Collections.singletonList(source);
    }
    List<String> encoded = new ArrayList<>(split.size());
    for (UnboundedSource<?, ?> s : split) {
      encoded.add(BeamStreamingSource.encode(s));
    }
    LOG.info(
        "Split Beam source {} into {} partition(s) (desired {}).",
        sourceId,
        encoded.size(),
        desiredNumSplits);
    try {
      BeamCheckpointFiles.writeSplits(checkpointLocation, sourceId, encoded);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to pin the split list of Beam source " + sourceId, e);
    }
    splitsB64 = encoded;
    return encoded;
  }
}
