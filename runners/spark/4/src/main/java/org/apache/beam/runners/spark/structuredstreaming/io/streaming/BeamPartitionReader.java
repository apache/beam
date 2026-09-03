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
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.BeamReaderCache.CachedReader;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads one split of a Beam {@link UnboundedSource} for one micro-batch.
 *
 * <p>The batch ends at the record quota or at the deadline. The reader then writes its checkpoint
 * mark durably at the end epoch and stays in {@link BeamReaderCache} for the next batch. A failed
 * mark write fails the task. An attempt Spark killed or failed writes nothing and its reader is
 * dropped, the retry restores from the durable mark at the start epoch.
 *
 * @param <T> the element type of the split
 */
public class BeamPartitionReader<T> implements PartitionReader<InternalRow> {

  private static final Logger LOG = LoggerFactory.getLogger(BeamPartitionReader.class);

  private static final Duration INITIAL_BACKOFF = Duration.millis(10);

  private final String key;
  private final UnboundedSource<T, ?> split;
  private final Coder<WindowedValue<T>> coder;
  private final BeamSourceCheckpoint checkpoint;
  private final CachedReader<T> cached;
  private final int splitId;
  private final long endEpoch;
  private final long maxRecords;
  private final long maxBatchDurationMillis;

  private long recordsRead;
  private long deadlineMillis = -1L;
  private boolean batchEnded;
  private @Nullable InternalRow current;

  BeamPartitionReader(BeamInputPartition<T> partition) throws IOException {
    this.split = partition.split();
    this.coder = partition.coder();
    this.splitId = partition.splitId();
    this.endEpoch = partition.endEpoch();
    this.maxRecords = partition.maxRecords();
    this.maxBatchDurationMillis = partition.maxBatchDurationMillis();
    PipelineOptions options = partition.options().value().get();
    Configuration conf = partition.hadoopConf().value().value();
    this.checkpoint = new BeamSourceCheckpoint(partition.checkpointLocation(), conf);
    this.key = BeamReaderCache.key(partition.checkpointLocation(), splitId);
    long startEpoch = partition.startEpoch();
    this.cached =
        BeamReaderCache.acquire(
            key,
            startEpoch,
            split,
            options,
            partition.readerIdleTimeoutMillis(),
            () -> checkpoint.readMark(splitId, startEpoch));
  }

  @Override
  public boolean next() throws IOException {
    if (deadlineMillis < 0) {
      deadlineMillis = System.currentTimeMillis() + maxBatchDurationMillis;
    }
    BackOff backOff = null;
    while (true) {
      if (maxRecords > 0 && recordsRead >= maxRecords) {
        return endOfBatch(false);
      }
      long remaining = deadlineMillis - System.currentTimeMillis();
      if (remaining <= 0) {
        return endOfBatch(false);
      }
      if (cached.startOrAdvance()) {
        recordsRead++;
        current = toRow();
        return true;
      }
      if (backOff == null) {
        backOff = backOff(remaining);
      }
      try {
        if (!BackOffUtils.next(Sleeper.DEFAULT, backOff)) {
          return endOfBatch(false);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return endOfBatch(true);
      }
    }
  }

  @Override
  public InternalRow get() {
    InternalRow row = current;
    if (row == null) {
      throw new IllegalStateException("No current row, next() did not return true.");
    }
    return row;
  }

  @Override
  public void close() throws IOException {
    endBatch(attemptDiscarded());
    current = null;
  }

  private boolean endOfBatch(boolean discarded) throws IOException {
    endBatch(discarded);
    current = null;
    return false;
  }

  /**
   * Ends the batch once. A discarded attempt drops the reader and writes nothing. A reader that was
   * never started has not moved, its start mark is written forward, an empty file standing for a
   * fresh start.
   */
  private void endBatch(boolean discarded) throws IOException {
    if (batchEnded) {
      return;
    }
    batchEnded = true;
    if (discarded) {
      LOG.info("Attempt for Beam reader {} was discarded, dropping the reader.", key);
      BeamReaderCache.invalidate(key);
      return;
    }
    if (!cached.started()) {
      byte[] startMark = cached.positionMark();
      byte[] codedMark = startMark == null ? new byte[0] : startMark;
      checkpoint.writeMark(splitId, endEpoch, codedMark);
      cached.endBatch(endEpoch, null, codedMark);
      return;
    }
    CheckpointMark mark = cached.reader().getCheckpointMark();
    byte[] codedMark = encodeMark(split, mark);
    checkpoint.writeMark(splitId, endEpoch, codedMark);
    cached.endBatch(endEpoch, mark, codedMark);
    LOG.debug("Beam reader {} read {} record(s) up to epoch {}.", key, recordsRead, endEpoch);
  }

  private static boolean attemptDiscarded() {
    TaskContext context = TaskContext.get();
    return context != null && (context.isInterrupted() || context.isFailed());
  }

  private static <MarkT extends CheckpointMark> byte[] encodeMark(
      UnboundedSource<?, MarkT> source, CheckpointMark mark) {
    @SuppressWarnings("unchecked") // getCheckpointMark returns the source's own mark type
    MarkT typed = (MarkT) mark;
    return CoderHelpers.toByteArray(typed, source.getCheckpointMarkCoder());
  }

  private static BackOff backOff(long remainingMillis) {
    Duration remaining = Duration.millis(remainingMillis);
    return FluentBackoff.DEFAULT
        .withInitialBackoff(INITIAL_BACKOFF)
        .withMaxBackoff(remaining)
        .withMaxCumulativeBackoff(remaining)
        .backoff();
  }

  private InternalRow toRow() {
    Instant timestamp = cached.reader().getCurrentTimestamp();
    WindowedValue<T> value =
        WindowedValues.timestampedValueInGlobalWindow(cached.reader().getCurrent(), timestamp);
    byte[] payload = CoderHelpers.toByteArray(value, coder);
    // Spark stores TimestampType as microseconds.
    return new GenericInternalRow(new Object[] {payload, timestamp.getMillis() * 1000L});
  }
}
