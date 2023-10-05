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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Read;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads data from multiple kinesis shards in a single thread. It uses simple round robin algorithm
 * when fetching data from shards.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class KinesisReader extends UnboundedSource.UnboundedReader<KinesisRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisReader.class);

  private final Read spec;
  private final SimplifiedKinesisClient kinesis;
  private final KinesisSource source;

  private final KinesisReaderCheckpoint initCheckpoint;
  private final Duration backlogBytesCheckThreshold;
  private CustomOptional<KinesisRecord> currentRecord = CustomOptional.absent();
  private long lastBacklogBytes;
  private Instant backlogBytesLastCheckTime = new Instant(0L);
  private ShardReadersPool shardReadersPool;

  KinesisReader(
      Read spec,
      SimplifiedKinesisClient kinesis,
      KinesisReaderCheckpoint initCheckpoint,
      KinesisSource source) {
    this(spec, kinesis, initCheckpoint, source, Duration.standardSeconds(30));
  }

  KinesisReader(
      Read spec,
      SimplifiedKinesisClient kinesis,
      KinesisReaderCheckpoint initCheckpoint,
      KinesisSource source,
      Duration backlogBytesCheckThreshold) {
    this.spec = checkNotNull(spec, "spec");
    this.kinesis = checkNotNull(kinesis, "kinesis");
    this.initCheckpoint = checkNotNull(initCheckpoint);
    this.source = source;
    this.backlogBytesCheckThreshold = backlogBytesCheckThreshold;
  }

  /** Generates initial checkpoint and instantiates iterators for shards. */
  @Override
  public boolean start() throws IOException {
    LOG.info("Starting reader using {}", initCheckpoint);

    try {
      shardReadersPool = createShardReadersPool();
      shardReadersPool.start();
    } catch (TransientKinesisException e) {
      throw new IOException(e);
    }

    return advance();
  }

  /** Retrieves next record from internal buffer. */
  @Override
  public boolean advance() throws IOException {
    currentRecord = shardReadersPool.nextRecord();
    return currentRecord.isPresent();
  }

  @Override
  public byte[] getCurrentRecordId() throws NoSuchElementException {
    return currentRecord.get().getUniqueId();
  }

  @Override
  public KinesisRecord getCurrent() throws NoSuchElementException {
    return currentRecord.get();
  }

  /**
   * Returns the approximate time that the current record was inserted into the stream. It is not
   * guaranteed to be accurate - this could lead to mark some records as "late" even if they were
   * not. Beware of this when setting {@link
   * org.apache.beam.sdk.values.WindowingStrategy#withAllowedLateness}
   */
  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    return currentRecord.get().getApproximateArrivalTimestamp();
  }

  @Override
  public void close() throws IOException {
    try {
      try (AutoCloseable c = kinesis) {
        shardReadersPool.stop();
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Instant getWatermark() {
    return shardReadersPool.getWatermark();
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return shardReadersPool.getCheckpointMark();
  }

  @Override
  public UnboundedSource<KinesisRecord, ?> getCurrentSource() {
    return source;
  }

  /**
   * Returns total size of all records that remain in Kinesis stream. The size is estimated taking
   * into account size of the records that were added to the stream after timestamp of the most
   * recent record returned by the reader. If no records have yet been retrieved from the reader
   * {@link UnboundedSource.UnboundedReader#BACKLOG_UNKNOWN} is returned. When currently processed
   * record is not further behind than {@link Read#getUpToDateThreshold()} then this method returns
   * 0.
   *
   * <p>The method can over-estimate size of the records for the split as it reports the backlog
   * across all shards. This can lead to unnecessary decisions to scale up the number of workers but
   * will never fail to scale up when this is necessary due to backlog size.
   *
   * @see <a href="https://issues.apache.org/jira/browse/BEAM-9439">BEAM-9439</a>
   */
  @Override
  public long getSplitBacklogBytes() {
    // Safety check in case a progress check is made for the start method is called.
    if (shardReadersPool == null) {
      return UnboundedReader.BACKLOG_UNKNOWN;
    }

    Instant latestRecordTimestamp = shardReadersPool.getLatestRecordTimestamp();

    if (latestRecordTimestamp.equals(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
      LOG.debug("Split backlog bytes for stream {} unknown", spec.getStreamName());
      return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
    }

    if (latestRecordTimestamp.plus(spec.getUpToDateThreshold()).isAfterNow()) {
      LOG.debug(
          "Split backlog bytes for stream {} with latest record timestamp {}: 0 (latest record timestamp is up-to-date with threshold of {})",
          spec.getStreamName(),
          latestRecordTimestamp,
          spec.getUpToDateThreshold());
      return 0L;
    }

    if (backlogBytesLastCheckTime.plus(backlogBytesCheckThreshold).isAfterNow()) {
      LOG.debug(
          "Split backlog bytes for {} stream with latest record timestamp {}: {} (cached value)",
          spec.getStreamName(),
          latestRecordTimestamp,
          lastBacklogBytes);
      return lastBacklogBytes;
    }

    try {
      lastBacklogBytes = kinesis.getBacklogBytes(spec.getStreamName(), latestRecordTimestamp);
      backlogBytesLastCheckTime = Instant.now();
    } catch (TransientKinesisException e) {
      LOG.warn(
          "Transient exception occurred during backlog estimation for stream {}.",
          spec.getStreamName(),
          e);
    }
    LOG.info(
        "Split backlog bytes for {} stream with {} latest record timestamp: {}",
        spec.getStreamName(),
        latestRecordTimestamp,
        lastBacklogBytes);
    return lastBacklogBytes;
  }

  ShardReadersPool createShardReadersPool() throws TransientKinesisException {
    return new ShardReadersPool(spec, kinesis, initCheckpoint);
  }
}
