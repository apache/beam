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

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterates over records in a single shard.
 * Records are retrieved in batches via calls to {@link ShardRecordsIterator#readNextBatch()}.
 * Client has to confirm processed records by calling
 * {@link ShardRecordsIterator#ackRecord(KinesisRecord)} method.
 */
class ShardRecordsIterator {

  private static final Logger LOG = LoggerFactory.getLogger(ShardRecordsIterator.class);

  private final SimplifiedKinesisClient kinesis;
  private final RecordFilter filter;
  private volatile ShardCheckpoint checkpoint;
  private String shardIterator;
  private volatile long millisBehindLatest = Long.MAX_VALUE;

  ShardRecordsIterator(final ShardCheckpoint initialCheckpoint,
      SimplifiedKinesisClient simplifiedKinesisClient) throws TransientKinesisException {
    this(initialCheckpoint, simplifiedKinesisClient, new RecordFilter());
  }

  ShardRecordsIterator(final ShardCheckpoint initialCheckpoint,
      SimplifiedKinesisClient simplifiedKinesisClient,
      RecordFilter filter) throws TransientKinesisException {
    this.checkpoint = checkNotNull(initialCheckpoint, "initialCheckpoint");
    this.filter = checkNotNull(filter, "filter");
    this.kinesis = checkNotNull(simplifiedKinesisClient, "simplifiedKinesisClient");
    shardIterator = checkpoint.getShardIterator(kinesis);
  }

  List<KinesisRecord> readNextBatch() throws TransientKinesisException {
    GetKinesisRecordsResult response = fetchRecords();
    LOG.debug("Fetched {} new records", response.getRecords().size());

    List<KinesisRecord> filteredRecords = filter.apply(response.getRecords(), checkpoint);
    millisBehindLatest = response.getMillisBehindLatest();
    return filteredRecords;
  }

  private GetKinesisRecordsResult fetchRecords() throws TransientKinesisException {
    try {
      GetKinesisRecordsResult response = kinesis.getRecords(shardIterator,
          checkpoint.getStreamName(), checkpoint.getShardId());
      shardIterator = response.getNextShardIterator();
      return response;
    } catch (ExpiredIteratorException e) {
      LOG.info("Refreshing expired iterator", e);
      shardIterator = checkpoint.getShardIterator(kinesis);
      return fetchRecords();
    }
  }

  ShardCheckpoint getCheckpoint() {
    return checkpoint;
  }

  boolean isUpToDate() {
    return millisBehindLatest == 0L;
  }

  void ackRecord(KinesisRecord record) {
    checkpoint = checkpoint.moveAfter(record);
  }

  String getShardId() {
    return checkpoint.getShardId();
  }
}
