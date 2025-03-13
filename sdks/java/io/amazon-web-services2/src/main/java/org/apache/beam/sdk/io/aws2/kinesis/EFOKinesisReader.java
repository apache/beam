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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

class EFOKinesisReader extends UnboundedSource.UnboundedReader<KinesisRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(EFOKinesisReader.class);

  private final KinesisIO.Read spec;
  private final String consumerArn;
  private final KinesisAsyncClient kinesis;
  private final KinesisSource source;
  private final KinesisReaderCheckpoint initCheckpoint;

  private @Nullable KinesisRecord currentRecord = null;
  private @Nullable EFOShardSubscribersPool shardSubscribersPool = null;

  EFOKinesisReader(
      KinesisIO.Read spec,
      String consumerArn,
      KinesisAsyncClient kinesis,
      KinesisReaderCheckpoint initCheckpoint,
      KinesisSource source) {
    this.spec = spec;
    this.consumerArn = consumerArn;
    this.kinesis = kinesis;
    this.initCheckpoint = initCheckpoint;
    this.source = source;
  }

  @Override
  public boolean start() throws IOException {
    LOG.info("Starting reader using {}", initCheckpoint);
    try {
      shardSubscribersPool = createPool();
      shardSubscribersPool().start(initCheckpoint);
      return advance();
    } catch (TransientKinesisException e) {
      throw new IOException(e);
    }
  }

  private EFOShardSubscribersPool shardSubscribersPool() {
    return checkStateNotNull(shardSubscribersPool, "Reader was not started");
  }

  @Override
  public boolean advance() throws IOException {
    currentRecord = shardSubscribersPool().getNextRecord();
    return currentRecord != null;
  }

  @Override
  public byte[] getCurrentRecordId() throws NoSuchElementException {
    return getOrThrow().getUniqueId();
  }

  @Override
  public KinesisRecord getCurrent() throws NoSuchElementException {
    return getOrThrow();
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    return getOrThrow().getApproximateArrivalTimestamp();
  }

  @Override
  public void close() throws IOException {
    try {
      try (AutoCloseable c = kinesis) {
        shardSubscribersPool().stop();
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Instant getWatermark() {
    return shardSubscribersPool().getWatermark();
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return shardSubscribersPool().getCheckpointMark();
  }

  @Override
  public UnboundedSource<KinesisRecord, ?> getCurrentSource() {
    return source;
  }

  EFOShardSubscribersPool createPool() throws TransientKinesisException {
    return new EFOShardSubscribersPool(spec, consumerArn, kinesis);
  }

  private KinesisRecord getOrThrow() throws NoSuchElementException {
    if (currentRecord != null) {
      return currentRecord;
    } else {
      throw new NoSuchElementException();
    }
  }
}
