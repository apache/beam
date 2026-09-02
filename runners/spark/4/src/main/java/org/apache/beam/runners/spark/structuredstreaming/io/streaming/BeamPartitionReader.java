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
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.BeamReaderCache.CachedReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads one split of a Beam {@link UnboundedSource} for the duration of one Spark micro-batch.
 *
 * <p>The batch ends as soon as either {@code maxRecordsPerBatch} elements were emitted (a limit
 * below 1 means unlimited) or {@code maxBatchDurationMillis} of wall clock time elapsed, whichever
 * comes first. When the source has no data available the reader polls with a short sleep until the
 * deadline, so an idle source produces an empty micro-batch rather than blocking the query.
 *
 * <p>The underlying Beam reader is not closed at the end of the batch, it stays in {@link
 * BeamReaderCache} and the next micro-batch continues from the same position. See that class for
 * the failure recovery caveats.
 *
 * @param <T> the element type of the wrapped source
 */
@SuppressWarnings({
  "nullness" // the current row is only read between a true next() and the following one
})
public class BeamPartitionReader<T> implements PartitionReader<InternalRow> {

  private static final Logger LOG = LoggerFactory.getLogger(BeamPartitionReader.class);

  /** Sleep between two unsuccessful advance attempts while the batch deadline has not passed. */
  private static final long POLL_INTERVAL_MILLIS = 10L;

  private final String cacheKey;
  private final CachedReader<T> cached;
  private final Coder<WindowedValue<T>> windowedValueCoder;
  private final String checkpointLocation;
  private final String sourceId;
  private final int splitId;
  private final long endEpoch;
  private final long maxRecordsPerBatch;
  private final long maxBatchDurationMillis;

  private long recordsRead;
  private long deadlineMillis = -1L;
  private @Nullable InternalRow current;

  BeamPartitionReader(BeamInputPartition partition) {
    UnboundedSource<T, ?> source =
        BeamStreamingSource.decode(partition.sourceB64(), "UnboundedSource split");
    this.windowedValueCoder =
        BeamStreamingSource.decode(partition.coderB64(), "WindowedValue coder");
    SerializablePipelineOptions options =
        BeamStreamingSource.decode(partition.pipelineOptionsB64(), "PipelineOptions");
    this.checkpointLocation = partition.checkpointLocation();
    this.sourceId = partition.sourceId();
    this.splitId = partition.splitId();
    this.endEpoch = partition.endEpoch();
    this.maxRecordsPerBatch = partition.maxRecordsPerBatch();
    this.maxBatchDurationMillis = partition.maxBatchDurationMillis();
    this.cacheKey = BeamReaderCache.key(checkpointLocation, sourceId, splitId);
    long startEpoch = partition.startEpoch();
    this.cached =
        BeamReaderCache.getOrCreate(
            cacheKey,
            source,
            options.get(),
            () -> BeamCheckpointFiles.readMark(checkpointLocation, sourceId, splitId, startEpoch));
  }

  @Override
  public boolean next() throws IOException {
    if (deadlineMillis < 0) {
      deadlineMillis = System.currentTimeMillis() + maxBatchDurationMillis;
    }
    while (true) {
      if (maxRecordsPerBatch > 0 && recordsRead >= maxRecordsPerBatch) {
        current = null;
        return false;
      }
      long remaining = deadlineMillis - System.currentTimeMillis();
      if (remaining <= 0) {
        current = null;
        return false;
      }
      if (cached.startOrAdvance()) {
        recordsRead++;
        current = toRow();
        return true;
      }
      Uninterruptibles.sleepUninterruptibly(
          Math.min(remaining, POLL_INTERVAL_MILLIS), java.util.concurrent.TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public InternalRow get() {
    if (current == null) {
      throw new IllegalStateException("No current row, next() did not return true.");
    }
    return current;
  }

  /**
   * Ends the micro-batch. The Beam reader deliberately stays open in {@link BeamReaderCache}, only
   * its checkpoint mark is remembered, persisted for durable recovery, and finalized.
   */
  @Override
  public void close() {
    current = null;
    try {
      UnboundedSource.CheckpointMark mark = cached.reader().getCheckpointMark();
      BeamReaderCache.rememberCheckpointMark(cacheKey, mark);
      persistMark(mark);
      mark.finalizeCheckpoint();
    } catch (Exception e) {
      LOG.warn("Failed to finalize the checkpoint mark of Beam reader {}.", cacheKey, e);
    }
    LOG.debug("Beam reader {} emitted {} record(s) in this micro-batch.", cacheKey, recordsRead);
  }

  /**
   * Best effort persistence of {@code mark} under the checkpoint location. An IO failure only
   * degrades recovery after a restart, the in memory path in {@link BeamReaderCache} still works,
   * so the batch is never failed here.
   */
  private void persistMark(UnboundedSource.CheckpointMark mark) {
    try {
      BeamCheckpointFiles.writeMark(checkpointLocation, sourceId, splitId, endEpoch, mark);
    } catch (Exception e) {
      LOG.warn(
          "Failed to persist the checkpoint mark of Beam reader {} at epoch {}, recovery after a "
              + "restart will fall back to an older mark or to a fresh start.",
          cacheKey,
          endEpoch,
          e);
    }
  }

  private InternalRow toRow() {
    Instant timestamp = cached.reader().getCurrentTimestamp();
    WindowedValue<T> windowedValue =
        WindowedValues.timestampedValueInGlobalWindow(cached.reader().getCurrent(), timestamp);
    byte[] payload;
    try {
      payload = CoderUtils.encodeToByteArray(windowedValueCoder, windowedValue);
    } catch (CoderException e) {
      throw new IllegalStateException("Failed to encode element read from a Beam source.", e);
    }
    // Spark stores TimestampType as microseconds since the epoch.
    return new GenericInternalRow(new Object[] {payload, timestamp.getMillis() * 1000L});
  }
}
