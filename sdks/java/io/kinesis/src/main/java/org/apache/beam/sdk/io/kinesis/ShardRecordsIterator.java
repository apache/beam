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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.Shard;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterates over records in a single shard. Records are retrieved in batches via calls to {@link
 * ShardRecordsIterator#readNextBatch()}. Client has to confirm processed records by calling {@link
 * ShardRecordsIterator#ackRecord(KinesisRecord)} method.
 */
class ShardRecordsIterator {

  private static final Logger LOG = LoggerFactory.getLogger(ShardRecordsIterator.class);

  private final SimplifiedKinesisClient kinesis;
  private final RecordFilter filter;
  private final String streamName;
  private final String shardId;
  private final AtomicReference<ShardCheckpoint> checkpoint;
  private final WatermarkPolicy watermarkPolicy;
  private final WatermarkPolicyFactory watermarkPolicyFactory;
  private final WatermarkPolicy latestRecordTimestampPolicy =
      WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();
  private String shardIterator;

  ShardRecordsIterator(
      ShardCheckpoint initialCheckpoint,
      SimplifiedKinesisClient simplifiedKinesisClient,
      WatermarkPolicyFactory watermarkPolicyFactory)
      throws TransientKinesisException {
    this(initialCheckpoint, simplifiedKinesisClient, watermarkPolicyFactory, new RecordFilter());
  }

  ShardRecordsIterator(
      ShardCheckpoint initialCheckpoint,
      SimplifiedKinesisClient simplifiedKinesisClient,
      WatermarkPolicyFactory watermarkPolicyFactory,
      RecordFilter filter)
      throws TransientKinesisException {
    this.checkpoint = new AtomicReference<>(checkNotNull(initialCheckpoint, "initialCheckpoint"));
    this.filter = checkNotNull(filter, "filter");
    this.kinesis = checkNotNull(simplifiedKinesisClient, "simplifiedKinesisClient");
    this.streamName = initialCheckpoint.getStreamName();
    this.shardId = initialCheckpoint.getShardId();
    this.shardIterator = initialCheckpoint.getShardIterator(kinesis);
    this.watermarkPolicy = watermarkPolicyFactory.createWatermarkPolicy();
    this.watermarkPolicyFactory = watermarkPolicyFactory;
  }

  List<KinesisRecord> readNextBatch()
      throws TransientKinesisException, KinesisShardClosedException {
    if (shardIterator == null) {
      throw new KinesisShardClosedException(
          String.format(
              "Shard iterator reached end of the shard: streamName=%s, shardId=%s",
              streamName, shardId));
    }
    GetKinesisRecordsResult response = fetchRecords();
    LOG.debug(
        "Fetched {} new records from shard: streamName={}, shardId={}",
        response.getRecords().size(),
        streamName,
        shardId);

    List<KinesisRecord> filteredRecords = filter.apply(response.getRecords(), checkpoint.get());
    return filteredRecords;
  }

  private GetKinesisRecordsResult fetchRecords() throws TransientKinesisException {
    try {
      GetKinesisRecordsResult response = kinesis.getRecords(shardIterator, streamName, shardId);
      shardIterator = response.getNextShardIterator();
      return response;
    } catch (ExpiredIteratorException e) {
      LOG.info(
          "Refreshing expired iterator for shard: streamName={}, shardId={}",
          streamName,
          shardId,
          e);
      shardIterator = checkpoint.get().getShardIterator(kinesis);
      return fetchRecords();
    }
  }

  ShardCheckpoint getCheckpoint() {
    return checkpoint.get();
  }

  void ackRecord(KinesisRecord record) {
    checkpoint.set(checkpoint.get().moveAfter(record));
    watermarkPolicy.update(record);
    latestRecordTimestampPolicy.update(record);
  }

  Instant getShardWatermark() {
    return watermarkPolicy.getWatermark();
  }

  Instant getLatestRecordTimestamp() {
    return latestRecordTimestampPolicy.getWatermark();
  }

  String getShardId() {
    return shardId;
  }

  List<ShardRecordsIterator> findSuccessiveShardRecordIterators() throws TransientKinesisException {
    List<Shard> shards = kinesis.listShards(streamName);
    List<ShardRecordsIterator> successiveShardRecordIterators = new ArrayList<>();
    for (Shard shard : shards) {
      if (shardId.equals(shard.getParentShardId())) {
        ShardCheckpoint shardCheckpoint =
            new ShardCheckpoint(
                streamName,
                shard.getShardId(),
                new StartingPoint(InitialPositionInStream.TRIM_HORIZON));
        successiveShardRecordIterators.add(
            new ShardRecordsIterator(shardCheckpoint, kinesis, watermarkPolicyFactory));
      }
    }
    return successiveShardRecordIterators;
  }
}
