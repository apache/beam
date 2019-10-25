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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads data from multiple kinesis shards in a single thread. It uses simple round robin algorithm
 * when fetching data from shards.
 */
class KinesisReader extends UnboundedSource.UnboundedReader<KinesisRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisReader.class);

  private final SimplifiedKinesisClient kinesis;
  private final KinesisSource source;
  private final CheckpointGenerator initialCheckpointGenerator;
  private final WatermarkPolicyFactory watermarkPolicyFactory;
  private final RateLimitPolicyFactory rateLimitPolicyFactory;
  private final Duration upToDateThreshold;
  private final Duration backlogBytesCheckThreshold;
  private CustomOptional<KinesisRecord> currentRecord = CustomOptional.absent();
  private long lastBacklogBytes;
  private Instant backlogBytesLastCheckTime = new Instant(0L);
  private ShardReadersPool shardReadersPool;
  private final Integer maxCapacityPerShard;

  KinesisReader(
      SimplifiedKinesisClient kinesis,
      CheckpointGenerator initialCheckpointGenerator,
      KinesisSource source,
      WatermarkPolicyFactory watermarkPolicyFactory,
      RateLimitPolicyFactory rateLimitPolicyFactory,
      Duration upToDateThreshold,
      Integer maxCapacityPerShard) {
    this(
        kinesis,
        initialCheckpointGenerator,
        source,
        watermarkPolicyFactory,
        rateLimitPolicyFactory,
        upToDateThreshold,
        Duration.standardSeconds(30),
        maxCapacityPerShard);
  }

  KinesisReader(
      SimplifiedKinesisClient kinesis,
      CheckpointGenerator initialCheckpointGenerator,
      KinesisSource source,
      WatermarkPolicyFactory watermarkPolicyFactory,
      RateLimitPolicyFactory rateLimitPolicyFactory,
      Duration upToDateThreshold,
      Duration backlogBytesCheckThreshold,
      Integer maxCapacityPerShard) {
    this.kinesis = checkNotNull(kinesis, "kinesis");
    this.initialCheckpointGenerator =
        checkNotNull(initialCheckpointGenerator, "initialCheckpointGenerator");
    this.watermarkPolicyFactory = watermarkPolicyFactory;
    this.rateLimitPolicyFactory = rateLimitPolicyFactory;
    this.source = source;
    this.upToDateThreshold = upToDateThreshold;
    this.backlogBytesCheckThreshold = backlogBytesCheckThreshold;
    this.maxCapacityPerShard = maxCapacityPerShard;
  }

  /** Generates initial checkpoint and instantiates iterators for shards. */
  @Override
  public boolean start() throws IOException {
    LOG.info("Starting reader using {}", initialCheckpointGenerator);

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
    shardReadersPool.stop();
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
   * Returns total size of all records that remain in Kinesis stream after current watermark. If the
   * watermark was not already set then it returns {@link
   * UnboundedSource.UnboundedReader#BACKLOG_UNKNOWN}. When currently processed record is not
   * further behind than {@link #upToDateThreshold} then this method returns 0.
   */
  @Override
  public long getTotalBacklogBytes() {
    Instant watermark = getWatermark();

    if (watermark.equals(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
      return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
    }

    if (watermark.plus(upToDateThreshold).isAfterNow()) {
      return 0L;
    }
    if (backlogBytesLastCheckTime.plus(backlogBytesCheckThreshold).isAfterNow()) {
      return lastBacklogBytes;
    }
    try {
      lastBacklogBytes = kinesis.getBacklogBytes(source.getStreamName(), watermark);
      backlogBytesLastCheckTime = Instant.now();
    } catch (TransientKinesisException e) {
      LOG.warn("Transient exception occurred.", e);
    }
    LOG.info(
        "Total backlog bytes for {} stream with {} watermark: {}",
        source.getStreamName(),
        watermark,
        lastBacklogBytes);
    return lastBacklogBytes;
  }

  ShardReadersPool createShardReadersPool() throws TransientKinesisException {
    return new ShardReadersPool(
        kinesis,
        initialCheckpointGenerator.generate(kinesis),
        watermarkPolicyFactory,
        rateLimitPolicyFactory,
        maxCapacityPerShard);
  }
}
