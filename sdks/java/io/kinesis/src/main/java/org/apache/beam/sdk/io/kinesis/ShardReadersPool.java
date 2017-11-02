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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal shard iterators pool.
 * It maintains the thread pool for reading Kinesis shards in separate threads.
 * Read records are stored in a blocking queue of limited capacity.
 */
class ShardReadersPool {

  private static final Logger LOG = LoggerFactory.getLogger(ShardReadersPool.class);
  private static final int DEFAULT_CAPACITY_PER_SHARD = 10_000;
  private ExecutorService executorService;
  private BlockingQueue<KinesisRecord> recordsQueue;
  private Map<String, ShardRecordsIterator> shardIteratorsMap;
  private SimplifiedKinesisClient kinesis;
  private KinesisReaderCheckpoint initialCheckpoint;
  private final int queueCapacityPerShard;
  private AtomicBoolean poolOpened = new AtomicBoolean(true);

  ShardReadersPool(SimplifiedKinesisClient kinesis, KinesisReaderCheckpoint initialCheckpoint) {
    this(kinesis, initialCheckpoint, DEFAULT_CAPACITY_PER_SHARD);
  }

  ShardReadersPool(SimplifiedKinesisClient kinesis, KinesisReaderCheckpoint initialCheckpoint,
      int queueCapacityPerShard) {
    this.kinesis = kinesis;
    this.initialCheckpoint = initialCheckpoint;
    this.queueCapacityPerShard = queueCapacityPerShard;
  }

  void start() throws TransientKinesisException {
    ImmutableMap.Builder<String, ShardRecordsIterator> shardsMap = ImmutableMap.builder();
    for (ShardCheckpoint checkpoint : initialCheckpoint) {
      shardsMap.put(checkpoint.getShardId(), createShardIterator(kinesis, checkpoint));
    }
    shardIteratorsMap = shardsMap.build();
    executorService = Executors.newFixedThreadPool(shardIteratorsMap.size());
    recordsQueue = new LinkedBlockingQueue<>(queueCapacityPerShard * shardIteratorsMap.size());
    for (final ShardRecordsIterator shardRecordsIterator : shardIteratorsMap.values()) {
      executorService.submit(new Runnable() {

        @Override
        public void run() {
          readLoop(shardRecordsIterator);
        }
      });
    }
  }

  private void readLoop(ShardRecordsIterator shardRecordsIterator) {
    while (poolOpened.get()) {
      try {
        List<KinesisRecord> kinesisRecords = shardRecordsIterator.readNextBatch();
        for (KinesisRecord kinesisRecord : kinesisRecords) {
          recordsQueue.put(kinesisRecord);
        }
      } catch (TransientKinesisException e) {
        LOG.warn("Transient exception occurred.", e);
      } catch (InterruptedException e) {
        LOG.warn("Thread was interrupted, finishing the read loop", e);
        break;
      } catch (Throwable e) {
        LOG.error("Unexpected exception occurred", e);
      }
    }
    LOG.info("Kinesis Shard read loop has finished");
  }

  CustomOptional<KinesisRecord> nextRecord() {
    try {
      KinesisRecord record = recordsQueue.poll(1, TimeUnit.SECONDS);
      if (record == null) {
        return CustomOptional.absent();
      }
      shardIteratorsMap.get(record.getShardId()).ackRecord(record);
      return CustomOptional.of(record);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for KinesisRecord from the buffer");
      return CustomOptional.absent();
    }
  }

  void stop() {
    LOG.info("Closing shard iterators pool");
    poolOpened.set(false);
    executorService.shutdownNow();
    boolean isShutdown = false;
    int attemptsLeft = 3;
    while (!isShutdown && attemptsLeft-- > 0) {
      try {
        isShutdown = executorService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for the executor service to shutdown");
        throw new RuntimeException(e);
      }
      if (!isShutdown && attemptsLeft > 0) {
        LOG.warn("Executor service is taking long time to shutdown, will retry. {} attempts left",
            attemptsLeft);
      }
    }
  }

  boolean allShardsUpToDate() {
    boolean shardsUpToDate = true;
    for (ShardRecordsIterator shardRecordsIterator : shardIteratorsMap.values()) {
      shardsUpToDate &= shardRecordsIterator.isUpToDate();
    }
    return shardsUpToDate;
  }

  KinesisReaderCheckpoint getCheckpointMark() {
    return new KinesisReaderCheckpoint(transform(shardIteratorsMap.values(),
        new Function<ShardRecordsIterator, ShardCheckpoint>() {
          @Override
          public ShardCheckpoint apply(ShardRecordsIterator shardRecordsIterator) {
            checkArgument(shardRecordsIterator != null, "shardRecordsIterator can not be null");
            return shardRecordsIterator.getCheckpoint();
          }
        }));
  }

  ShardRecordsIterator createShardIterator(SimplifiedKinesisClient kinesis,
      ShardCheckpoint checkpoint) throws TransientKinesisException {
    return new ShardRecordsIterator(checkpoint, kinesis);
  }

}
