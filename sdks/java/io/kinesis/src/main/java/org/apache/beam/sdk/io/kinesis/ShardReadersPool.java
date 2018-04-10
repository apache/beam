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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
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

  /**
   * Executor service for running the threads that read records from shards handled by this pool.
   * Each thread runs the {@link ShardReadersPool#readLoop(ShardRecordsIterator)} method and
   * handles exactly one shard.
   */
  private final ExecutorService executorService;

  /**
   * A Bounded buffer for read records. Records are added to this buffer within
   * {@link ShardReadersPool#readLoop(ShardRecordsIterator)} method and removed
   * in {@link ShardReadersPool#nextRecord()}.
   */
  private BlockingQueue<KinesisRecord> recordsQueue;

  /**
   * A reference to an immutable mapping of {@link ShardRecordsIterator} instances to shard ids.
   * This map is replaced with a new one when resharding operation on any handled shard occurs.
   */
  private final AtomicReference<ImmutableMap<String, ShardRecordsIterator>> shardIteratorsMap;

  /**
   * A map for keeping the current number of records stored in a buffer per shard.
   */
  private final ConcurrentMap<String, AtomicInteger> numberOfRecordsInAQueueByShard;
  private final SimplifiedKinesisClient kinesis;
  private final KinesisReaderCheckpoint initialCheckpoint;
  private final int queueCapacityPerShard;
  private final AtomicBoolean poolOpened = new AtomicBoolean(true);

  ShardReadersPool(SimplifiedKinesisClient kinesis, KinesisReaderCheckpoint initialCheckpoint) {
    this(kinesis, initialCheckpoint, DEFAULT_CAPACITY_PER_SHARD);
  }

  ShardReadersPool(SimplifiedKinesisClient kinesis, KinesisReaderCheckpoint initialCheckpoint,
      int queueCapacityPerShard) {
    this.kinesis = kinesis;
    this.initialCheckpoint = initialCheckpoint;
    this.queueCapacityPerShard = queueCapacityPerShard;
    this.executorService = Executors.newCachedThreadPool();
    this.numberOfRecordsInAQueueByShard = new ConcurrentHashMap<>();
    this.shardIteratorsMap = new AtomicReference<>();
  }

  void start() throws TransientKinesisException {
    ImmutableMap.Builder<String, ShardRecordsIterator> shardsMap = ImmutableMap.builder();
    for (ShardCheckpoint checkpoint : initialCheckpoint) {
      shardsMap.put(checkpoint.getShardId(), createShardIterator(kinesis, checkpoint));
    }
    shardIteratorsMap.set(shardsMap.build());
    if (!shardIteratorsMap.get().isEmpty()) {
      recordsQueue = new ArrayBlockingQueue<>(
          queueCapacityPerShard * shardIteratorsMap.get().size());
      startReadingShards(shardIteratorsMap.get().values());
    } else {
      // There are no shards to handle when restoring from an empty checkpoint. Empty checkpoints
      // are generated when the last shard handled by this pool was closed
      recordsQueue = new ArrayBlockingQueue<>(1);
    }
  }

  void startReadingShards(Iterable<ShardRecordsIterator> shardRecordsIterators) {
    for (final ShardRecordsIterator recordsIterator : shardRecordsIterators) {
      numberOfRecordsInAQueueByShard.put(recordsIterator.getShardId(), new AtomicInteger());
      executorService.submit(() -> readLoop(recordsIterator));
    }
  }

  private void readLoop(ShardRecordsIterator shardRecordsIterator) {
    while (poolOpened.get()) {
      try {
        List<KinesisRecord> kinesisRecords;
        try {
          kinesisRecords = shardRecordsIterator.readNextBatch();
        } catch (KinesisShardClosedException e) {
          LOG.info("Shard iterator for {} shard is closed, finishing the read loop",
              shardRecordsIterator.getShardId(), e);
          // Wait until all records from already closed shard are taken from the buffer and only
          // then start reading successive shards. This guarantees that checkpoints will contain
          // either parent or child shard and never both. Such approach allows for more
          // straightforward checkpoint restoration than in a case when new shards are read
          // immediately.
          waitUntilAllShardRecordsRead(shardRecordsIterator);
          readFromSuccessiveShards(shardRecordsIterator);
          break;
        }
        for (KinesisRecord kinesisRecord : kinesisRecords) {
          recordsQueue.put(kinesisRecord);
          numberOfRecordsInAQueueByShard.get(kinesisRecord.getShardId()).incrementAndGet();
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
      shardIteratorsMap.get().get(record.getShardId()).ackRecord(record);

      // numberOfRecordsInAQueueByShard contains the counter for a given shard until the shard is
      // closed and then it's counter reaches 0. Thus the access here is safe
      numberOfRecordsInAQueueByShard.get(record.getShardId()).decrementAndGet();
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
    ImmutableMap<String, ShardRecordsIterator> currentShardIterators = shardIteratorsMap.get();
    for (ShardRecordsIterator shardRecordsIterator : currentShardIterators.values()) {
      shardsUpToDate &= shardRecordsIterator.isUpToDate();
    }
    return shardsUpToDate;
  }

  KinesisReaderCheckpoint getCheckpointMark() {
    ImmutableMap<String, ShardRecordsIterator> currentShardIterators = shardIteratorsMap.get();
    return new KinesisReaderCheckpoint(
            currentShardIterators.values().stream().map(shardRecordsIterator -> {
              checkArgument(shardRecordsIterator != null, "shardRecordsIterator can not be null");
              return shardRecordsIterator.getCheckpoint();
            }).collect(Collectors.toList()));
  }

  ShardRecordsIterator createShardIterator(SimplifiedKinesisClient kinesis,
      ShardCheckpoint checkpoint) throws TransientKinesisException {
    return new ShardRecordsIterator(checkpoint, kinesis);
  }

  /**
   * Waits until all records read from given shardRecordsIterator are taken from
   * {@link #recordsQueue} and acked.
   * Uses {@link #numberOfRecordsInAQueueByShard} map to track the amount of remaining events.
   */
  private void waitUntilAllShardRecordsRead(ShardRecordsIterator shardRecordsIterator)
      throws InterruptedException {
    // Given shard is already closed so no more records will be read from it. Thus the counter for
    // that shard will be strictly decreasing to 0.
    AtomicInteger numberOfShardRecordsInAQueue = numberOfRecordsInAQueueByShard
        .get(shardRecordsIterator.getShardId());
    while (!(numberOfShardRecordsInAQueue.get() == 0)) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
  }

  /**
   * <p>
   * Tries to find successors of a given shard and start reading them. Each closed shard can have
   * 0, 1 or 2 successors
   * <ul>
   *   <li>0 successors - when shard was merged with another shard and this one is considered
   *   adjacent by merge operation</li>
   *   <li>1 successor - when shard was merged with another shard and this one is considered a
   *   parent by merge operation</li>
   *   <li>2 successors - when shard was split into two shards</li>
   * </ul>
   * </p>
   * <p>
   * Once shard successors are established, the transition to reading new shards can begin.
   * During this operation, the immutable {@link ShardReadersPool#shardIteratorsMap}
   * is replaced with a new one holding references to {@link ShardRecordsIterator} instances for
   * open shards only. Potentially there might be more shard iterators closing at the same time so
   * {@link ShardReadersPool#shardIteratorsMap} is updated in a loop using CAS pattern to keep all
   * the updates. Then, the counter for already closed shard is removed from
   * {@link ShardReadersPool#numberOfRecordsInAQueueByShard} map.
   * </p>
   * <p>
   * Finally when update is finished, new threads are spawned for reading the successive shards.
   * The thread that handled reading from already closed shard can finally complete.
   * </p>
   */
  private void readFromSuccessiveShards(final ShardRecordsIterator closedShardIterator)
      throws TransientKinesisException {
    List<ShardRecordsIterator> successiveShardRecordIterators = closedShardIterator
        .findSuccessiveShardRecordIterators();

    ImmutableMap<String, ShardRecordsIterator> current;
    ImmutableMap<String, ShardRecordsIterator> updated;
    do {
      current = shardIteratorsMap.get();
      updated = createMapWithSuccessiveShards(current, closedShardIterator,
          successiveShardRecordIterators);
    } while (!shardIteratorsMap.compareAndSet(current, updated));
    numberOfRecordsInAQueueByShard.remove(closedShardIterator.getShardId());
    startReadingShards(successiveShardRecordIterators);
  }

  private ImmutableMap<String, ShardRecordsIterator> createMapWithSuccessiveShards(
      ImmutableMap<String, ShardRecordsIterator> current, ShardRecordsIterator closedShardIterator,
      List<ShardRecordsIterator> successiveShardRecordIterators) throws TransientKinesisException {
    ImmutableMap.Builder<String, ShardRecordsIterator> shardsMap = ImmutableMap.builder();
    Iterable<ShardRecordsIterator> allShards = Iterables
        .concat(current.values(), successiveShardRecordIterators);
    for (ShardRecordsIterator iterator : allShards) {
      if (!closedShardIterator.getShardId().equals(iterator.getShardId())) {
        shardsMap.put(iterator.getShardId(), iterator);
      }
    }
    return shardsMap.build();
  }

}
