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
package org.apache.beam.sdk.io.kinesis;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads data from multiple kinesis shards in a single thread.
 * It uses simple round robin algorithm when fetching data from shards.
 */
class KinesisReader extends UnboundedSource.UnboundedReader<KinesisRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisReader.class);

  private final SimplifiedKinesisClient kinesis;
  private final UnboundedSource<KinesisRecord, ?> source;
  private final CheckpointGenerator initialCheckpointGenerator;
  private RoundRobin<ShardRecordsIterator> shardIterators;
  private CustomOptional<KinesisRecord> currentRecord = CustomOptional.absent();

  public KinesisReader(SimplifiedKinesisClient kinesis,
      CheckpointGenerator initialCheckpointGenerator,
      UnboundedSource<KinesisRecord, ?> source) {
    this.kinesis = checkNotNull(kinesis, "kinesis");
    this.initialCheckpointGenerator =
        checkNotNull(initialCheckpointGenerator, "initialCheckpointGenerator");
    this.source = source;
  }

  /**
   * Generates initial checkpoint and instantiates iterators for shards.
   */
  @Override
  public boolean start() throws IOException {
    LOG.info("Starting reader using {}", initialCheckpointGenerator);

    try {
      KinesisReaderCheckpoint initialCheckpoint =
          initialCheckpointGenerator.generate(kinesis);
      List<ShardRecordsIterator> iterators = newArrayList();
      for (ShardCheckpoint checkpoint : initialCheckpoint) {
        iterators.add(checkpoint.getShardRecordsIterator(kinesis));
      }
      shardIterators = new RoundRobin<>(iterators);
    } catch (TransientKinesisException e) {
      throw new IOException(e);
    }

    return advance();
  }

  /**
   * Moves to the next record in one of the shards.
   * If current shard iterator can be move forward (i.e. there's a record present) then we do it.
   * If not, we iterate over shards in a round-robin manner.
   */
  @Override
  public boolean advance() throws IOException {
    try {
      for (int i = 0; i < shardIterators.size(); ++i) {
        currentRecord = shardIterators.getCurrent().next();
        if (currentRecord.isPresent()) {
          return true;
        } else {
          shardIterators.moveForward();
        }
      }
    } catch (TransientKinesisException e) {
      LOG.warn("Transient exception occurred", e);
    }
    return false;
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
   * When {@link KinesisReader} was advanced to the current record.
   * We cannot use approximate arrival timestamp given for each record by Kinesis as it
   * is not guaranteed to be accurate - this could lead to mark some records as "late"
   * even if they were not.
   */
  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    return currentRecord.get().getReadTime();
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * Current time.
   * We cannot give better approximation of the watermark with current semantics of
   * {@link KinesisReader#getCurrentTimestamp()}, because we don't know when the next
   * {@link KinesisReader#advance()} will be called.
   */
  @Override
  public Instant getWatermark() {
    return Instant.now();
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return KinesisReaderCheckpoint.asCurrentStateOf(shardIterators);
  }

  @Override
  public UnboundedSource<KinesisRecord, ?> getCurrentSource() {
    return source;
  }

}
