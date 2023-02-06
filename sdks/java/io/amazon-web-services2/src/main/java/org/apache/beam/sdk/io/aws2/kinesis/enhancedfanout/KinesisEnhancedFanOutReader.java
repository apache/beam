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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisReaderCheckpoint;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.TransientKinesisException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

@SuppressWarnings("unused")
public class KinesisEnhancedFanOutReader extends UnboundedSource.UnboundedReader<KinesisRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisEnhancedFanOutReader.class);

  private final KinesisIO.Read spec;
  private final KinesisAsyncClient kinesis;
  private final KinesisEnhancedFanOutSource source;
  private final CheckpointGenerator checkpointGenerator;

  private @Nullable KinesisRecord currentRecord = null;
  private CustomOptional<EFOShardSubscribersPool> shardSubscribersPool = CustomOptional.absent();

  KinesisEnhancedFanOutReader(
      KinesisIO.Read spec,
      KinesisAsyncClient kinesis,
      CheckpointGenerator checkpointGenerator,
      KinesisEnhancedFanOutSource source) {
    this.spec = checkArgumentNotNull(spec);
    this.kinesis = checkArgumentNotNull(kinesis);
    this.checkpointGenerator = checkArgumentNotNull(checkpointGenerator);
    this.source = source;
  }

  @Override
  public boolean start() throws IOException {
    LOG.info("Starting reader using {}", checkpointGenerator);
    try {
      EFOShardSubscribersPool pool = createPool();
      KinesisReaderCheckpoint initialCheckpoint = checkpointGenerator.generate(kinesis);
      pool.start(initialCheckpoint);
      shardSubscribersPool = CustomOptional.of(pool);
      return advance(); // should return false if no input is currently available
    } catch (TransientKinesisException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean advance() throws IOException {
    currentRecord = shardSubscribersPool.get().getNextRecord();
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
        boolean isStoppedCleanly = shardSubscribersPool.get().stop();
        if (!isStoppedCleanly) {
          LOG.warn("Pool was not stopped correctly");
        }
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Instant getWatermark() {
    return shardSubscribersPool.get().getWatermark();
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return shardSubscribersPool.get().getCheckpointMark();
  }

  @Override
  public UnboundedSource<KinesisRecord, ?> getCurrentSource() {
    return source;
  }

  private EFOShardSubscribersPool createPool() throws TransientKinesisException {
    return new EFOShardSubscribersPool(spec, kinesis);
  }

  private KinesisRecord getOrThrow() throws NoSuchElementException {
    if (currentRecord != null) {
      return currentRecord;
    } else {
      throw new NoSuchElementException();
    }
  }
}
