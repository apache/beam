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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.classic.Dataset$;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.reflect.ClassTag;

/**
 * Translator facing entry point turning a Beam {@link UnboundedSource} into a streaming Spark
 * {@link Dataset} of rows, with the DataSourceV2 micro-batch glue as nested classes.
 *
 * <p>The dataset has two columns, {@value #COL_PAYLOAD} of type {@code BINARY} holding the element
 * encoded with the supplied {@code WindowedValue} coder, and {@value #COL_EVENT_TS} of type {@code
 * TIMESTAMP} holding the event timestamp of that element.
 *
 * <p>The event time watermark is declared here and only here. Spark 4 rejects a second {@code
 * withWatermark} further down the plan, so downstream translators must never call it again.
 */
public final class UnboundedSourceDataset {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceDataset.class);

  public static final String COL_PAYLOAD = "payload";

  public static final String COL_EVENT_TS = "eventTimestamp";

  public static final StructType SCHEMA =
      new StructType()
          .add(COL_PAYLOAD, DataTypes.BinaryType, false)
          .add(COL_EVENT_TS, DataTypes.TimestampType, false);

  private static final String SOURCE_NAME = "beam-unbounded";

  private UnboundedSourceDataset() {}

  /**
   * Builds the streaming {@link Dataset} for {@code source} with the event time watermark applied.
   *
   * @param session the active Spark session
   * @param source the Beam unbounded source to read
   * @param windowedValueCoder the coder of the {@value #COL_PAYLOAD} column
   * @param options the pipeline options, supplying the watermark delay and the micro-batch limits
   * @param transformName the full name of the read transform, used for naming only
   * @param <T> the element type of the source
   * @param <CheckpointMarkT> the checkpoint mark type of the source
   */
  public static <T, CheckpointMarkT extends UnboundedSource.CheckpointMark> Dataset<Row> of(
      SparkSession session,
      UnboundedSource<T, CheckpointMarkT> source,
      Coder<WindowedValue<T>> windowedValueCoder,
      SparkStructuredStreamingPipelineOptions options,
      String transformName) {
    org.apache.spark.sql.classic.SparkSession classic =
        (org.apache.spark.sql.classic.SparkSession) session;
    Configuration hadoopConf = classic.sessionState().newHadoopConf();
    BeamTable<T> table =
        new BeamTable<>(
            source,
            windowedValueCoder,
            broadcast(
                session,
                new SerializablePipelineOptions(options),
                SerializablePipelineOptions.class),
            broadcast(
                session,
                new SerializableConfiguration(hadoopConf),
                SerializableConfiguration.class),
            session.sparkContext().defaultParallelism(),
            options.getMaxRecordsPerBatch(),
            Math.max(1L, options.getMaxBatchDurationMillis()),
            options.getReaderIdleTimeoutMillis(),
            transformName);
    LogicalPlan plan =
        new StreamingRelationV2(
            Option.empty(),
            SOURCE_NAME,
            table,
            CaseInsensitiveStringMap.empty(),
            DataTypeUtils.toAttributes(SCHEMA),
            Option.empty(),
            Option.empty(),
            Option.empty());
    Dataset<Row> rows = Dataset$.MODULE$.ofRows(classic, plan);
    return rows.withWatermark(COL_EVENT_TS, options.getWatermarkDelayMillis() + " milliseconds");
  }

  private static <T> Broadcast<T> broadcast(SparkSession session, T value, Class<T> type) {
    return session.sparkContext().broadcast(value, ClassTag.apply(type));
  }

  /**
   * Opaque epoch counter used as the Spark {@link Offset} of a Beam unbounded source.
   *
   * <p>The read position lives in Beam checkpoint marks on the executors, see {@link
   * BeamSourceCheckpoint}. Equality is the base class comparison of {@link #json()}.
   */
  static class BeamOffset extends Offset {

    public static final BeamOffset ZERO = new BeamOffset(0L);

    private final long epoch;

    public BeamOffset(long epoch) {
      this.epoch = epoch;
    }

    public long epoch() {
      return epoch;
    }

    @Override
    public String json() {
      return Long.toString(epoch);
    }

    public static BeamOffset fromJson(String json) {
      try {
        return new BeamOffset(Long.parseLong(json.trim()));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Not a valid BeamOffset: " + json, e);
      }
    }

    @Override
    public String toString() {
      return json();
    }
  }

  /** DataSourceV2 {@link Table} over one Beam unbounded source, micro-batch reads only. */
  static final class BeamTable<T> implements Table, SupportsRead {
    final UnboundedSource<T, ?> source;
    final Coder<WindowedValue<T>> coder;
    final Broadcast<SerializablePipelineOptions> options;
    final Broadcast<SerializableConfiguration> hadoopConf;
    final int desiredNumSplits;

    /** Records per micro-batch across all splits, below 1 means unlimited. */
    final long maxRecordsPerBatch;

    final long maxBatchDurationMillis;
    final long readerIdleTimeoutMillis;
    final String transformName;

    BeamTable(
        UnboundedSource<T, ?> source,
        Coder<WindowedValue<T>> coder,
        Broadcast<SerializablePipelineOptions> options,
        Broadcast<SerializableConfiguration> hadoopConf,
        int desiredNumSplits,
        long maxRecordsPerBatch,
        long maxBatchDurationMillis,
        long readerIdleTimeoutMillis,
        String transformName) {
      this.source = source;
      this.coder = coder;
      this.options = options;
      this.hadoopConf = hadoopConf;
      this.desiredNumSplits = desiredNumSplits;
      this.maxRecordsPerBatch = maxRecordsPerBatch;
      this.maxBatchDurationMillis = maxBatchDurationMillis;
      this.readerIdleTimeoutMillis = readerIdleTimeoutMillis;
      this.transformName = transformName;
    }

    @Override
    public String name() {
      return "BeamUnboundedSource[" + transformName + "]";
    }

    @Override
    public StructType schema() {
      return SCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
      return ImmutableSet.of(TableCapability.MICRO_BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap ignored) {
      return () -> new BeamScan<>(this);
    }
  }

  private static final class BeamScan<T> implements Scan {
    private final BeamTable<T> table;

    BeamScan(BeamTable<T> table) {
      this.table = table;
    }

    @Override
    public StructType readSchema() {
      return SCHEMA;
    }

    @Override
    public String description() {
      return table.name();
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
      return new BeamMicroBatchStream<>(table, checkpointLocation);
    }
  }

  /**
   * Driver side {@link MicroBatchStream} over a Beam {@link UnboundedSource}.
   *
   * <p>Offsets are opaque epochs, {@link #latestOffset()} advances by one every trigger. The source
   * is split once and the splits are pinned under the checkpoint location, every batch of every run
   * plans the same partitions. {@link #commit} purges marks below the committed epoch on a
   * background thread, Spark never asks for those again. Partitions carry no locality hint, the
   * reader cache restores a split from its durable mark wherever it lands.
   */
  static class BeamMicroBatchStream<T> implements MicroBatchStream {

    private final BeamTable<T> table;
    private final String checkpointLocation;
    private final BeamSourceCheckpoint checkpoint;

    private final ExecutorService purger =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "beam-source-mark-purge");
              thread.setDaemon(true);
              return thread;
            });
    private final AtomicBoolean purgeInFlight = new AtomicBoolean();
    private final AtomicLong purgeRequested = new AtomicLong();

    private long epoch;
    private @Nullable List<UnboundedSource<T, ?>> splits;

    BeamMicroBatchStream(BeamTable<T> table, String checkpointLocation) {
      this.table = table;
      this.checkpointLocation = checkpointLocation;
      this.checkpoint =
          new BeamSourceCheckpoint(checkpointLocation, table.hadoopConf.value().value());
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
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
      long startEpoch = ((BeamOffset) start).epoch();
      long endEpoch = ((BeamOffset) end).epoch();
      fastForwardEpoch(endEpoch);
      List<UnboundedSource<T, ?>> pinned = splits();
      try {
        checkpoint.prepareEpoch(endEpoch);
      } catch (IOException e) {
        LOG.warn(
            "Failed to prepare mark directory of epoch {} at {}.", endEpoch, checkpointLocation, e);
      }
      long[] quotas = splitQuotas(table.maxRecordsPerBatch, pinned.size(), endEpoch);
      InputPartition[] partitions = new InputPartition[pinned.size()];
      for (int i = 0; i < pinned.size(); i++) {
        partitions[i] =
            new BeamInputPartition<>(
                pinned.get(i),
                table.coder,
                table.options,
                table.hadoopConf,
                checkpointLocation,
                i,
                startEpoch,
                endEpoch,
                quotas[i],
                table.maxBatchDurationMillis,
                table.readerIdleTimeoutMillis);
      }
      return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return new BeamPartitionReaderFactory();
    }

    /** Purges marks below {@code end} off the stream thread, one purge runs at a time. */
    @Override
    public void commit(Offset end) {
      purgeRequested.accumulateAndGet(((BeamOffset) end).epoch(), Math::max);
      if (purgeInFlight.compareAndSet(false, true)) {
        purger.execute(this::purgeRequested);
      }
    }

    private void purgeRequested() {
      long epoch;
      do {
        epoch = purgeRequested.get();
        try {
          checkpoint.purgeMarksBelow(epoch);
        } catch (IOException | RuntimeException e) {
          LOG.warn("Failed to purge marks below epoch {} at {}.", epoch, checkpointLocation, e);
        }
        purgeInFlight.set(false);
      } while (purgeRequested.get() > epoch && purgeInFlight.compareAndSet(false, true));
    }

    @Override
    public void stop() {
      LOG.info(
          "Stopping Beam micro-batch stream {} at {}.", table.transformName, checkpointLocation);
      purger.shutdown();
    }

    /** Keeps {@link #latestOffset()} ahead of every epoch Spark logged before a restart. */
    private synchronized void fastForwardEpoch(long seen) {
      if (seen > epoch) {
        LOG.info("Fast forwarding epoch of {} from {} to {}.", table.transformName, epoch, seen);
        epoch = seen;
      }
    }

    private synchronized List<UnboundedSource<T, ?>> splits() {
      if (splits != null) {
        return splits;
      }
      List<UnboundedSource<?, ?>> pinned;
      try {
        pinned = checkpoint.readSplits();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to read pinned splits at " + checkpointLocation, e);
      }
      if (pinned == null) {
        pinned = new ArrayList<>(splitSource());
        try {
          checkpoint.writeSplits(pinned);
        } catch (IOException e) {
          throw new IllegalStateException("Failed to pin splits at " + checkpointLocation, e);
        }
      } else {
        LOG.info("Restored {} pinned split(s) from {}.", pinned.size(), checkpointLocation);
      }
      List<UnboundedSource<T, ?>> typed = new ArrayList<>(pinned.size());
      for (UnboundedSource<?, ?> split : pinned) {
        @SuppressWarnings("unchecked") // splits of this source share its element type
        UnboundedSource<T, ?> cast = (UnboundedSource<T, ?>) split;
        typed.add(cast);
      }
      splits = typed;
      return typed;
    }

    private List<? extends UnboundedSource<T, ?>> splitSource() {
      UnboundedSource<T, ?> source = table.source;
      PipelineOptions options = table.options.value().get();
      List<? extends UnboundedSource<T, ?>> result;
      try {
        result = source.split(table.desiredNumSplits, options);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to split UnboundedSource " + source.getClass().getCanonicalName(), e);
      }
      if (result.isEmpty()) {
        result = Collections.singletonList(source);
      }
      LOG.info(
          "Split {} into {} partition(s), desired {}.",
          table.transformName,
          result.size(),
          table.desiredNumSplits);
      return result;
    }

    /**
     * Divides the batch limit over the splits. A limit below 1 means unlimited and yields -1 for
     * every split. Otherwise the remainder rotates with the epoch, so a limit below the split count
     * gives one record to a rotating subset of splits per batch and 0 to the others.
     */
    static long[] splitQuotas(long maxRecordsPerBatch, int numSplits, long epoch) {
      long[] quotas = new long[numSplits];
      if (maxRecordsPerBatch < 1) {
        Arrays.fill(quotas, -1L);
        return quotas;
      }
      long base = maxRecordsPerBatch / numSplits;
      long remainder = maxRecordsPerBatch % numSplits;
      for (int i = 0; i < numSplits; i++) {
        quotas[i] = base + ((i + epoch) % numSplits < remainder ? 1 : 0);
      }
      return quotas;
    }
  }

  /** One split of a Beam unbounded source for one micro-batch, from epoch start to epoch end. */
  static final class BeamInputPartition<T> implements InputPartition {

    private static final long serialVersionUID = 1L;

    final UnboundedSource<T, ?> split;
    final Coder<WindowedValue<T>> coder;
    final Broadcast<SerializablePipelineOptions> options;
    final Broadcast<SerializableConfiguration> hadoopConf;
    final String checkpointLocation;
    final int splitId;
    final long startEpoch;
    final long endEpoch;

    /** Records this split may emit in this micro-batch, below 0 means unlimited, 0 means none. */
    final long maxRecords;

    final long maxBatchDurationMillis;
    final long readerIdleTimeoutMillis;

    BeamInputPartition(
        UnboundedSource<T, ?> split,
        Coder<WindowedValue<T>> coder,
        Broadcast<SerializablePipelineOptions> options,
        Broadcast<SerializableConfiguration> hadoopConf,
        String checkpointLocation,
        int splitId,
        long startEpoch,
        long endEpoch,
        long maxRecords,
        long maxBatchDurationMillis,
        long readerIdleTimeoutMillis) {
      this.split = split;
      this.coder = coder;
      this.options = options;
      this.hadoopConf = hadoopConf;
      this.checkpointLocation = checkpointLocation;
      this.splitId = splitId;
      this.startEpoch = startEpoch;
      this.endEpoch = endEpoch;
      this.maxRecords = maxRecords;
      this.maxBatchDurationMillis = maxBatchDurationMillis;
      this.readerIdleTimeoutMillis = readerIdleTimeoutMillis;
    }

    @Override
    public String toString() {
      return "BeamInputPartition{checkpointLocation="
          + checkpointLocation
          + ", split="
          + splitId
          + ", epochs="
          + startEpoch
          + ".."
          + endEpoch
          + "}";
    }
  }

  /** Creates a {@link BeamPartitionReader} for a {@link BeamInputPartition} on the executor. */
  static final class BeamPartitionReaderFactory implements PartitionReaderFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      try {
        return new BeamPartitionReader<>((BeamInputPartition<?>) partition);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to open Beam reader for " + partition, e);
      }
    }
  }

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
  static final class BeamPartitionReader<T> implements PartitionReader<InternalRow> {

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
      this.split = partition.split;
      this.coder = partition.coder;
      this.splitId = partition.splitId;
      this.endEpoch = partition.endEpoch;
      this.maxRecords = partition.maxRecords;
      this.maxBatchDurationMillis = partition.maxBatchDurationMillis;
      PipelineOptions options = partition.options.value().get();
      Configuration conf = partition.hadoopConf.value().value();
      BeamSourceCheckpoint checkpoint =
          new BeamSourceCheckpoint(partition.checkpointLocation, conf);
      this.checkpoint = checkpoint;
      this.key = BeamReaderCache.key(partition.checkpointLocation, splitId);
      long startEpoch = partition.startEpoch;
      int splitId = this.splitId;
      this.cached =
          BeamReaderCache.acquire(
              key,
              startEpoch,
              split,
              options,
              partition.readerIdleTimeoutMillis,
              checkpoint::readSparkCommittedEpoch,
              () -> checkpoint.readMark(splitId, startEpoch));
    }

    @Override
    public boolean next() throws IOException {
      if (deadlineMillis < 0) {
        deadlineMillis = System.currentTimeMillis() + maxBatchDurationMillis;
      }
      BackOff backOff = null;
      while (true) {
        if (maxRecords >= 0 && recordsRead >= maxRecords) {
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
     * Ends the batch once. A discarded attempt drops the reader and writes nothing. A reader that
     * was never started has not moved, its start mark is written forward, an empty file standing
     * for a fresh start.
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
}
